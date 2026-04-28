from pathlib import Path
from tqdm import tqdm
import uuid
import regex as re
from api.database import SessionLocal, DocumentMetadata
from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
from qdrant_client.models import PayloadSchemaType

from api.config import ClientConfig

# Initialize clients
openai_client = OpenAI(api_key=ClientConfig.OPENAI_API_KEY)
client = QdrantClient(
    url=ClientConfig.QDRANT_URL,
    api_key=ClientConfig.QDRANT_API_KEY,
)
COLLECTION_NAME = ClientConfig.COLLECTION_NAME

try:
    client.get_collection(COLLECTION_NAME)
    print(f"Connected to existing collection: {COLLECTION_NAME}")
except Exception:
    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(size=1536, distance=Distance.COSINE)
    )
    print(f"Created new collection: {COLLECTION_NAME}")

# Create payload indexes for filters
client.create_payload_index(
    collection_name=COLLECTION_NAME,
    field_name="client_id",
    field_schema=PayloadSchemaType.KEYWORD
)

client.create_payload_index(
    collection_name=COLLECTION_NAME,
    field_name="document_id",
    field_schema=PayloadSchemaType.KEYWORD
)


def get_embedding(text):
    response = openai_client.embeddings.create(
        model="text-embedding-3-small",
        input=text
    )
    return response.data[0].embedding

def check_document_in_vectordb(document_id: str, client_id: str) -> bool:
    """ Check if a document already has chunks in the vector database."""
    try:
        results = client.scroll(
            collection_name=COLLECTION_NAME,
            scroll_filter=Filter(
                must=[
                    FieldCondition(key="document_id", match=MatchValue(value=document_id)),
                    FieldCondition(key="client_id", match=MatchValue(value=client_id))
                ]
            ),
            limit=1
        )
        has_chunks = len(results[0]) > 0
        return has_chunks
    except Exception as e:
        print(f"[WARNING] Error checking vector DB for document {document_id}: {e}")
        return False
    
def load_metadata_from_db(client_id: str):
    """Load all document metadata from database for specific client"""
    db = SessionLocal()
    try:
        docs = db.query(DocumentMetadata).filter(
            DocumentMetadata.client_id == client_id
        ).all()
        metadata = {}
        for doc in docs:
            metadata[doc.markdown_filename] = {
                "document_id": doc.document_id or "",
                "title": doc.title or "Unknown",
                "type": doc.type or "Unknown",
                "original_filename": doc.original_filename or "",
                "original_s3_url": doc.original_s3_url or "",
                "download_s3_url": doc.download_s3_url or "",  
                "parsed_s3_url": doc.parsed_s3_url or "",
                "date_updated": doc.date_updated or "",
                "total_pages": doc.total_pages or 0,
                "file_size": doc.file_size or ""
            }
        return metadata
    finally:
        db.close()

def chunk_document(file_path, metadata_entry, client_id: str, batch_size=32):
    '''
    Detects file type and routes to appropriate chunking method.
    Supports: markdown (.md), CSV (.csv), Excel (.xlsx, .xls)
    '''
    file_path = Path(file_path)
    extension = file_path.suffix.lower()
    
    if extension == '.md':
        return markdown_to_vectorDB(file_path, metadata_entry, client_id, batch_size)
    elif extension == '.csv':
        return csv_to_vectorDB(file_path, metadata_entry, client_id, batch_size)
    else:
        print(f"Unsupported file type: {extension}")
        return

def csv_to_vectorDB(csv_path, metadata_entry, client_id: str, batch_size=32):
    '''
    Chunks a CSV file (each row = chunk), converts to embeddings,
    and stores in VectorDB with metadata using batching.
    '''
    import csv
    
    chunks = []
    chunk_metadatas = []
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        
        for row_num, row in enumerate(reader, start=2):
            chunk_lines = [f"{col}: {row[col]}" for col in headers if row[col]]
            chunk_text = "\n".join(chunk_lines)
            
            chunks.append(chunk_text)
            chunk_metadatas.append({
                "row_number": row_num,
                "columns": ", ".join(headers),
            })
    
    print(f"Processing {len(chunks)} chunks from {csv_path.name}...")
    
    successful_chunks = 0
    failed_chunks = 0
    
    for batch_start in tqdm(range(0, len(chunks), batch_size), desc=f"Embedding {csv_path.name}"):
        batch_end = min(batch_start + batch_size, len(chunks))
        batch_chunks = chunks[batch_start:batch_end]
        
        try:
            # Try to process entire batch
            batch_embeddings = [get_embedding(chunk) for chunk in batch_chunks]
            
            batch_ids = []
            batch_documents = []
            batch_embeddings_list = []
            batch_metadatas = []
            
            for i, (chunk, embedding) in enumerate(zip(batch_chunks, batch_embeddings)):
                chunk_idx = batch_start + i
                chunk_id = str(uuid.uuid4())
                
                chunk_metadata = {
                    "client_id": client_id,
                    "source_file": csv_path.name,
                    "chunk_index": chunk_idx,
                    "total_chunks": len(chunks),
                    "row_number": chunk_metadatas[chunk_idx]["row_number"],
                    "columns": chunk_metadatas[chunk_idx]["columns"],
                    "sheet_name": "",  # Empty for pure CSV, populated for converted Excel
                }
                
                # Add document metadata if available
                if metadata_entry:
                    chunk_metadata.update({
                        "document_id": metadata_entry.get("document_id") or "",
                        "title": metadata_entry.get("title") or "Unknown",
                        "doc_type": metadata_entry.get("type") or "Unknown",
                        "original_filename": metadata_entry.get("original_filename") or "",
                        "original_s3_url": metadata_entry.get("original_s3_url") or "",
                        "download_s3_url": metadata_entry.get("download_s3_url") or "",
                        "parsed_s3_url": metadata_entry.get("parsed_s3_url") or "",
                        "date_updated": metadata_entry.get("date_updated") or "",
                    })
                
                batch_ids.append(chunk_id)
                batch_documents.append(chunk)
                batch_embeddings_list.append(embedding)
                batch_metadatas.append(chunk_metadata)
            
            points = [
                PointStruct(
                    id=chunk_id,
                    vector=embedding,
                    payload={
                        **chunk_metadata,
                        "document": doc
                    }
                )
                for chunk_id, embedding, chunk_metadata, doc in zip(
                    batch_ids,
                    batch_embeddings_list,
                    batch_metadatas,
                    batch_documents
                )
            ]

            client.upsert(
                collection_name=COLLECTION_NAME,
                points=points
            )
            
            successful_chunks += len(batch_chunks)
            
        except Exception as batch_error:
            print(f"\n[BATCH ERROR] Failed to process batch starting at chunk {batch_start}: {batch_error}")
            print(f"[RETRY] Processing {len(batch_chunks)} chunks individually...")
            
            # Fallback: process each chunk in the batch individually
            for i, chunk in enumerate(batch_chunks):
                chunk_idx = batch_start + i
                try:
                    # Get embedding for single chunk
                    embedding = get_embedding(chunk)
                    chunk_id = str(uuid.uuid4())
                    
                    chunk_metadata = {
                        "client_id": client_id,
                        "source_file": csv_path.name,
                        "chunk_index": chunk_idx,
                        "total_chunks": len(chunks),
                        "row_number": chunk_metadatas[chunk_idx]["row_number"],
                        "columns": chunk_metadatas[chunk_idx]["columns"],
                        "sheet_name": "",
                    }
                    
                    # Add document metadata if available
                    if metadata_entry:
                        chunk_metadata.update({
                            "document_id": metadata_entry.get("document_id") or "",
                            "title": metadata_entry.get("title") or "Unknown",
                            "doc_type": metadata_entry.get("type") or "Unknown",
                            "original_filename": metadata_entry.get("original_filename") or "",
                            "original_s3_url": metadata_entry.get("original_s3_url") or "",
                            "download_s3_url": metadata_entry.get("download_s3_url") or "",
                            "parsed_s3_url": metadata_entry.get("parsed_s3_url") or "",
                            "date_updated": metadata_entry.get("date_updated") or "",
                        })
                    
                    point = PointStruct(
                        id=chunk_id,
                        vector=embedding,
                        payload={
                            **chunk_metadata,
                            "document": chunk
                        }
                    )
                    
                    client.upsert(
                        collection_name=COLLECTION_NAME,
                        points=[point]
                    )
                    
                    successful_chunks += 1
                    
                except Exception as chunk_error:
                    print(f"[CHUNK ERROR] Failed to process chunk {chunk_idx}: {chunk_error}")
                    failed_chunks += 1
                    continue
        
    # Calculate success rate
    total_chunks = len(chunks)
    success_rate = (successful_chunks / total_chunks * 100) if total_chunks > 0 else 0
    
    # Determine status
    if successful_chunks == 0:
        status = "FAILED"
        print(f"{status}: 0/{total_chunks} chunks embedded from {csv_path.name}")
    elif success_rate < 70:
        status = "PARTIAL"
        print(f"{status}: {successful_chunks}/{total_chunks} chunks ({success_rate:.1f}%) embedded from {csv_path.name}")
        print(f"   → {failed_chunks} chunks failed (likely images/tables)")
    else:
        status = "SUCCESS"
        print(f"{status}: {successful_chunks}/{total_chunks} chunks ({success_rate:.1f}%) embedded from {csv_path.name}")
        if failed_chunks > 0:
            print(f"{failed_chunks} chunks skipped (likely images/tables)")
    
    return {
        "status": "failed" if successful_chunks == 0 else ("partial" if success_rate < 70 else "success"),
        "successful_chunks": successful_chunks,
        "failed_chunks": failed_chunks,
        "total_chunks": total_chunks,
        "success_rate": success_rate
    }

def add_chunk(chunk_text, heading, page, chunks, chunk_headings, chunk_pages, max_size=6000):
    if len(chunk_text) > max_size:
        words = chunk_text.split()
        temp_chunk = []
        temp_size = 0
        for word in words:
            temp_size += len(word) + 1
            if temp_size > max_size:
                chunks.append(" ".join(temp_chunk))
                chunk_headings.append(heading)
                chunk_pages.append(page)
                temp_chunk = [word]
                temp_size = len(word)
            else:
                temp_chunk.append(word)
        if temp_chunk:
            chunks.append(" ".join(temp_chunk))
            chunk_headings.append(heading)
            chunk_pages.append(page)
    else:
        chunks.append(chunk_text)
        chunk_headings.append(heading)
        chunk_pages.append(page)


def markdown_to_vectorDB(markdown_path, metadata_entry, client_id: str, batch_size=32):
    '''
    This function chunks a markdown file, converts to embeddings, 
    and stores in a VectorDB collection with metadata using batching.
    '''
    prev_line = ""
    chunks, current_chunk = [], []
    current_heading = "Introduction"
    chunk_headings = []
    chunk_pages = []
    current_page = 1
    
    with open(markdown_path, 'r', encoding='utf-8') as markdown_data:
        for line in markdown_data:
            line = line.strip()
            if not line:
                continue
            
            page_match = re.search(r'<!-- PAGE (\d+) -->', line)
            if page_match:
                current_page = int(page_match.group(1))
                continue
            
            if line.startswith('#') or line.startswith('*'):
                if prev_line.startswith('#') or prev_line.startswith('*'):
                    current_chunk.append(line)
                else:
                    if current_chunk:
                        chunk_text = "\n".join(current_chunk).strip()
                        chunk_with_heading = f"## {current_heading}\n\n{chunk_text}"
                        add_chunk(chunk_with_heading, current_heading, current_page, chunks, chunk_headings, chunk_pages)
                    current_chunk = [line]
                
                if line.startswith('#'):
                    heading_text = line.lstrip('#').strip()
                    if heading_text:
                        current_heading = heading_text
            else:
                current_chunk.append(line)

            prev_line = line
        
        if current_chunk:
            chunk_text = "\n".join(current_chunk).strip()
            chunk_with_heading = f"## {current_heading}\n\n{chunk_text}"
            
            add_chunk(chunk_with_heading, current_heading, current_page, chunks, chunk_headings, chunk_pages)
    
    print(f"Processing {len(chunks)} chunks from {markdown_path.name}...")
    
    successful_chunks = 0
    failed_chunks = 0
    
    for batch_start in tqdm(range(0, len(chunks), batch_size), desc=f"Embedding {markdown_path.name}"):
        batch_end = min(batch_start + batch_size, len(chunks))
        batch_chunks = chunks[batch_start:batch_end]
        
        try:
            # Try to process entire batch
            batch_embeddings = [get_embedding(chunk) for chunk in batch_chunks]
            
            batch_ids = []
            batch_documents = []
            batch_embeddings_list = []
            batch_metadatas = []
            
            for i, (chunk, embedding) in enumerate(zip(batch_chunks, batch_embeddings)):
                chunk_idx = batch_start + i
                chunk_id = str(uuid.uuid4())
                
                chunk_metadata = {
                    "client_id": client_id,
                    "source_file": str(markdown_path.name),
                    "chunk_index": chunk_idx,
                    "total_chunks": len(chunks),
                    "section_heading": chunk_headings[chunk_idx],
                    "page_number": chunk_pages[chunk_idx],
                }
                
                # Add document metadata if available
                if metadata_entry:
                    chunk_metadata.update({
                        "document_id": metadata_entry.get("document_id") or "",
                        "title": metadata_entry.get("title") or "Unknown",
                        "doc_type": metadata_entry.get("type") or "Unknown",
                        "original_filename": metadata_entry.get("original_filename") or "",
                        "original_s3_url": metadata_entry.get("original_s3_url") or "",
                        "download_s3_url": metadata_entry.get("download_s3_url") or "", 
                        "parsed_s3_url": metadata_entry.get("parsed_s3_url") or "",
                        "date_updated": metadata_entry.get("date_updated") or "",
                    })
                
                batch_ids.append(chunk_id)
                batch_documents.append(chunk)
                batch_embeddings_list.append(embedding)
                batch_metadatas.append(chunk_metadata)
            
            points = [
                PointStruct(
                    id=chunk_id,
                    vector=embedding,
                    payload={
                        **chunk_metadata,
                        "document": doc
                    }
                )
                for chunk_id, embedding, chunk_metadata, doc in zip(
                    batch_ids,
                    batch_embeddings_list,
                    batch_metadatas,
                    batch_documents
                )
            ]

            client.upsert(
                collection_name=COLLECTION_NAME,
                points=points
            )
            
            successful_chunks += len(batch_chunks)
            
        except Exception as batch_error:
            print(f"\n[BATCH ERROR] Failed to process batch starting at chunk {batch_start}: {batch_error}")
            print(f"[RETRY] Processing {len(batch_chunks)} chunks individually...")
            
            # Fallback: process each chunk in the batch individually
            for i, chunk in enumerate(batch_chunks):
                chunk_idx = batch_start + i
                try:
                    # Get embedding for single chunk
                    embedding = get_embedding(chunk)
                    chunk_id = str(uuid.uuid4())
                    
                    chunk_metadata = {
                        "client_id": client_id,
                        "source_file": str(markdown_path.name),
                        "chunk_index": chunk_idx,
                        "total_chunks": len(chunks),
                        "section_heading": chunk_headings[chunk_idx],
                        "page_number": chunk_pages[chunk_idx],
                    }
                    
                    # Add document metadata if available
                    if metadata_entry:
                        chunk_metadata.update({
                            "document_id": metadata_entry.get("document_id") or "",
                            "title": metadata_entry.get("title") or "Unknown",
                            "doc_type": metadata_entry.get("type") or "Unknown",
                            "original_filename": metadata_entry.get("original_filename") or "",
                            "original_s3_url": metadata_entry.get("original_s3_url") or "",
                            "download_s3_url": metadata_entry.get("download_s3_url") or "",
                            "parsed_s3_url": metadata_entry.get("parsed_s3_url") or "",
                            "date_updated": metadata_entry.get("date_updated") or "",
                        })
                    
                    point = PointStruct(
                        id=chunk_id,
                        vector=embedding,
                        payload={
                            **chunk_metadata,
                            "document": chunk
                        }
                    )
                    
                    client.upsert(
                        collection_name=COLLECTION_NAME,
                        points=[point]
                    )
                    
                    successful_chunks += 1
                    
                except Exception as chunk_error:
                    print(f"[CHUNK ERROR] Failed to process chunk {chunk_idx}: {chunk_error}")
                    failed_chunks += 1
                    continue

    # Calculate success rate
    total_chunks = len(chunks)
    success_rate = (successful_chunks / total_chunks * 100) if total_chunks > 0 else 0
    
    # Determine status
    if successful_chunks == 0:
        status = "FAILED"
        print(f"{status}: 0/{total_chunks} chunks embedded from {markdown_path.name}")
    elif success_rate < 70:
        status = "PARTIAL"
        print(f"{status}: {successful_chunks}/{total_chunks} chunks ({success_rate:.1f}%) embedded from {markdown_path.name}")
        print(f"   → {failed_chunks} chunks failed (likely images/tables)")
    else:
        status = "SUCCESS"
        print(f"{status}: {successful_chunks}/{total_chunks} chunks ({success_rate:.1f}%) embedded from {markdown_path.name}")
        if failed_chunks > 0:
            print(f"   → {failed_chunks} chunks skipped (likely images/tables)")
    
    return {
        "status": "failed" if successful_chunks == 0 else ("partial" if success_rate < 70 else "success"),
        "successful_chunks": successful_chunks,
        "failed_chunks": failed_chunks,
        "total_chunks": total_chunks,
        "success_rate": success_rate
    }


def chunk_all_documents(client_id: str, parsed_dir="parsed_data", batch_size=32):
    '''
    Process all documents in the parsed_data directory for a specific client
    
    Args:
        client_id: Client identifier
        parsed_dir: Directory containing parsed files
        batch_size: Number of chunks to process at once (default: 32)
    '''
    # Use client-specific directory
    parsed_path = Path(parsed_dir) / client_id
    
    if not parsed_path.exists():
        raise FileNotFoundError(f"Directory not found: {parsed_path}")
    
    # Load metadata from SQL database for this client
    metadata = load_metadata_from_db(client_id=client_id)
    
    if not metadata:
        print("⚠️  No metadata found in database. Make sure to run ingestion first.")
        return
    
    # Get all supported files (markdown, CSV, Excel)
    supported_extensions = ['*.md', '*.csv']
    all_files = []
    for ext in supported_extensions:
        all_files.extend(parsed_path.glob(ext))
    
    if not all_files:
        print("No supported files found in parsed_data directory")
        return
    
    print(f"\nFound {len(all_files)} files to process")
    print(f"Loaded metadata for {len(metadata)} documents from database")
    print(f"Using batch size: {batch_size}")
    print("=" * 60)
    
    # Process each file
    results = {
        "success": [],
        "partial": [],
        "failed": [],
        "skipped": []
    }
    
    for file_path in all_files:
        try:
            # Get metadata for this file from database
            metadata_entry = metadata.get(file_path.name)
            
            if not metadata_entry:
                print(f"\n Skipping {file_path.name}, no metadata found in database")
                results["skipped"].append(file_path.name)
                continue
            
            print(f"\n Processing: {file_path.name}")
            print(f" Type: {metadata_entry.get('type', 'Unknown')}")
            print(f" Title: {metadata_entry.get('title', 'Unknown')}")
            
            # Use chunk_document to route to the right function
            result = chunk_document(file_path, metadata_entry, client_id=client_id, batch_size=batch_size)
            
            if result:
                results[result["status"]].append({
                    "filename": file_path.name,
                    "title": metadata_entry.get('title', 'Unknown'),
                    **result
                })
            
        except Exception as e:
            print(f"Error processing {file_path.name}: {e}")
            results["failed"].append({
                "filename": file_path.name,
                "error": str(e)
            })
            continue
    
    # Print final summary
    print("\n" + "=" * 80)
    print("INGESTION SUMMARY")
    print("=" * 80)
    
    print(f"\n SUCCESS ({len(results['success'])} documents - >= 70% chunks embedded):")
    for doc in results["success"]:
        print(f"   • {doc['title']}: {doc['successful_chunks']}/{doc['total_chunks']} chunks ({doc['success_rate']:.1f}%)")
    
    if results["partial"]:
        print(f"\n⚠️  PARTIAL ({len(results['partial'])} documents - <70% chunks embedded):")
        for doc in results["partial"]:
            print(f" {doc['title']}: {doc['successful_chunks']}/{doc['total_chunks']} chunks ({doc['success_rate']:.1f}%)")
            print(f" Likely contains images/tables that couldn't be embedded")
    
    if results["failed"]:
        print(f"\n FAILED ({len(results['failed'])} documents - 0 chunks embedded):")
        for doc in results["failed"]:
            if "error" in doc:
                print(f"{doc['filename']}: {doc.get('error', 'Unknown error')}")
            else:
                print(f"{doc.get('title', doc['filename'])}: No chunks could be embedded")
    
    if results["skipped"]:
        print(f"\n⏭️  SKIPPED ({len(results['skipped'])} documents - no metadata):")
        for filename in results["skipped"]:
            print(f" {filename}")
    
    print("\n" + "=" * 80)
    print(f"TOTALS:")
    print(f" Success: {len(results['success'])}")
    print(f" Partial: {len(results['partial'])}")
    print(f" Failed:  {len(results['failed'])}")
    print(f" Skipped: {len(results['skipped'])}")
    print(f"\n Total vectors in collection: {client.count(COLLECTION_NAME).count}")
    print("=" * 80)