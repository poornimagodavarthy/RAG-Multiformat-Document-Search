from openai import OpenAI
import os
from qdrant_client.models import Filter, FieldCondition, MatchValue, FilterSelector
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

from api.config import ClientConfig

# Initialize Qdrant
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("VECTORDB_KEY")
COLLECTION_NAME = os.getenv("knowledge_base")
COLLECTION_NAME = ClientConfig.COLLECTION_NAME

# Get client_id from environment
CLIENT_ID = os.getenv("DB_KEY")
if not CLIENT_ID:
    raise ValueError("DB_KEY environment variable must be set")

client = QdrantClient(
    url=QDRANT_URL,
    api_key=QDRANT_API_KEY,
)

# Get existing collection (or create if doesn't exist)
try:
    client.get_collection(COLLECTION_NAME)
    print(f"Connected to existing collection: {COLLECTION_NAME}")
except Exception:
    print(f"Collection {COLLECTION_NAME} not found, creating...")
    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(size=1536, distance=Distance.COSINE)
    )
    print(f"Created new collection: {COLLECTION_NAME}")

openai_client = OpenAI()

def get_embedding(text):
    """Get embedding from OpenAI"""
    response = openai_client.embeddings.create(
        model="text-embedding-3-small",
        input=text
    )
    return response.data[0].embedding
    
def retrieve_rag_context(query, client_id: str = None, top_k=5):
    """
    Query the Qdrant vector store filtered by client_id.
    """
    if client_id is None:
        raise ValueError("client_id is required")
    
    query_embedding = get_embedding(query)

    # Use query_points with query parameter (not query_vector)
    results = client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_embedding,
        query_filter=Filter(
            must=[FieldCondition(key="client_id", match=MatchValue(value=client_id))]
        ),
        limit=top_k
    ).points

    chunks = [hit.payload.get("document", "") for hit in results]
    
    # Extract metadata and ensure all necessary fields are present
    metadatas = []
    for hit in results:
        meta = {
            "title": hit.payload.get("title", "Unknown"),
            "doc_type": hit.payload.get("doc_type", "Unknown"),
            "page_number": hit.payload.get("page_number", 1),
            "section_heading": hit.payload.get("section_heading", ""),
            "original_filename": hit.payload.get("original_filename", ""),
            "original_s3_url": hit.payload.get("original_s3_url", ""),  # For viewing (PDF)
            "download_s3_url": hit.payload.get("download_s3_url", ""),  # For downloading 
            "date_updated": hit.payload.get("date_updated", ""),
            "document_id": hit.payload.get("document_id", ""),
            "client_id": hit.payload.get("client_id", client_id),
        }
        metadatas.append(meta)
    
    # Create formatted source citations
    sources = []
    for meta in metadatas:
        source = f"{meta.get('title', 'Unknown')} (Page {meta.get('page_number', 'N/A')})"
        if meta.get('section_heading'):
            source += f" - Section: {meta['section_heading']}"
        sources.append(source)
    
    # Join chunks into a single text block for prompt injection
    context = "\n\n".join(chunks)
    
    return {
        "context": context,
        "chunks": chunks,
        "metadata": metadatas,
        "sources": sources
    }

def delete_document_chunks(document_id: str, client_id: str) -> int:
    """
    Delete all chunks belonging to a document from Qdrant.
    Filters by both document_id and client_id for security.
    """
    if client_id is None:
        client_id = CLIENT_ID
    try:
        # First, count how many chunks exist before deletion
        results = client.scroll(
            collection_name=COLLECTION_NAME,
            scroll_filter=Filter(
                must=[
                    FieldCondition(key="document_id", match=MatchValue(value=document_id)),
                    FieldCondition(key="client_id", match=MatchValue(value=client_id))
                ]
            ),
            limit=10000
        )
        chunk_count = len(results[0])
        
        if chunk_count == 0:
            print(f"[VECTOR DB] No chunks found for document {document_id} (client {client_id})")
            return 0
        
        # Delete the chunks
        client.delete(
            collection_name=COLLECTION_NAME,
            points_selector=FilterSelector(
                filter=Filter(
                    must=[
                        FieldCondition(key="document_id", match=MatchValue(value=document_id)),
                        FieldCondition(key="client_id", match=MatchValue(value=client_id))
                    ]
                )
            )
        )
        
        print(f"[VECTOR DB] Client {client_id}: Deleted {chunk_count} chunks for document {document_id}")
        return chunk_count
        
    except Exception as e:
        print(f"[VECTOR DB DELETE ERROR] {str(e)}")
        return 0