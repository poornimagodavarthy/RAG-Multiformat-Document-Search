from fastapi import FastAPI, Depends, Header, HTTPException, UploadFile, File, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
from sqlalchemy.orm import Session
from sqlalchemy import text
import os
import re
from pathlib import Path

from .database import get_db, DocumentMetadata, init_db, SessionLocal
from retrieval.retrieve import retrieve_rag_context, delete_document_chunks
from retrieval.generate import generate_response
from openai import OpenAI
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
import threading
from redis import Redis
import uuid
import json 
from datetime import datetime
from .config import ClientConfig
from collections import defaultdict
from ingestion.chunking import load_metadata_from_db
from retrieval.retrieve import client as qdrant_client, COLLECTION_NAME
from qdrant_client.models import Filter, FieldCondition, MatchValue, FilterSelector

load_dotenv('.env')


# Validate config on startup
ClientConfig.validate()

# Initialize clients using config
client = OpenAI(api_key=ClientConfig.OPENAI_API_KEY)
model = 'gpt-4o-mini-2024-07-18'

s3_client = boto3.client(
    's3',
    region_name=ClientConfig.S3_REGION,
    aws_access_key_id=ClientConfig.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=ClientConfig.AWS_SECRET_ACCESS_KEY
)

redis_conn = Redis.from_url(ClientConfig.REDIS_URL, decode_responses=True)

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# CLIENT ID AUTHENTICATION

def get_client_id(x_api_key: str = Header(None)) -> str:
    """ Extract client_id from API key header"""
    # Development mode: use test client if no API key
    if not x_api_key:
        test_client = os.getenv("DB_KEY")
        if test_client:
            return test_client
        raise HTTPException(status_code=401, detail="API key required")
    
    # Production: Map API keys to client IDs
    client_id = ClientConfig.get_client_id_from_api_key(x_api_key)
    if not client_id:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    return client_id

# Organization
class QueryRequest(BaseModel):
    query: str

class Source(BaseModel):
    id: int
    title: str
    type: str
    page: int
    section: str
    excerpt: str
    s3_url: str
    download_url: str  
    date: str
    original_filename: str

class SearchResponse(BaseModel):
    answer: str
    sources: List[Source]
    query: str

class UploadResponse(BaseModel):
    success: bool
    message: str
    filename: str

# UTILITY FUNCTIONS

def cleanup_orphaned_vectors(client_id: str):
    """ Remove vector DB chunks for documents that no longer exist in SQL."""
    try:
        print(f"[CLEANUP] Starting orphan cleanup for client {client_id}")
        
        # Get all valid document_ids from SQL
        metadata = load_metadata_from_db(client_id=client_id)
        valid_document_ids = set(
            meta.get("document_id") 
            for meta in metadata.values() 
            if meta.get("document_id")
        )
        
        print(f"[CLEANUP] Found {len(valid_document_ids)} valid documents in SQL")
        
        # Get all document_ids from Vector DB for this client
        scroll_result = qdrant_client.scroll(
            collection_name=COLLECTION_NAME,
            scroll_filter=Filter(
                must=[FieldCondition(key="client_id", match=MatchValue(value=client_id))]
            ),
            limit=10000,
            with_payload=True
        )
        
        vector_points = scroll_result[0]
        print(f"[CLEANUP] Found {len(vector_points)} chunks in Vector DB")
        
        # Find orphaned document_ids (in Vector DB but not in SQL)
        vector_document_ids = set()
        for point in vector_points:
            doc_id = point.payload.get("document_id")
            if doc_id:
                vector_document_ids.add(doc_id)
        
        orphaned_ids = vector_document_ids - valid_document_ids
        
        if not orphaned_ids:
            print(f"[CLEANUP No orphaned vectors found")
            return 0
        
        print(f"[CLEANUP] Found {len(orphaned_ids)} orphaned document(s)")
        
        # Delete orphaned vectors
        total_deleted = 0
        for orphaned_id in orphaned_ids:
            print(f"[CLEANUP] Deleting orphaned document: {orphaned_id}")
            
            qdrant_client.delete(
                collection_name=COLLECTION_NAME,
                points_selector=FilterSelector(
                    filter=Filter(
                        must=[
                            FieldCondition(key="document_id", match=MatchValue(value=orphaned_id)),
                            FieldCondition(key="client_id", match=MatchValue(value=client_id))
                        ]
                    )
                )
            )
            
            # Count how many were deleted 
            remaining = qdrant_client.scroll(
                collection_name=COLLECTION_NAME,
                scroll_filter=Filter(
                    must=[
                        FieldCondition(key="document_id", match=MatchValue(value=orphaned_id)),
                        FieldCondition(key="client_id", match=MatchValue(value=client_id))
                    ]
                ),
                limit=1
            )
            
            if len(remaining[0]) == 0:
                total_deleted += 1
                print(f"[CLEANUP] Deleted orphaned document {orphaned_id}")
        
        print(f"[CLEANUP COMPLETE] Removed {total_deleted} orphaned document(s)")
        return total_deleted
        
    except Exception as e:
        print(f"[CLEANUP ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return 0
    
def generate_signed_url(s3_url: str, page_number: int = 1, expiration: int = 3600) -> str:
    """Generate a presigned URL for S3 object"""
    try:
        if ClientConfig.S3_BUCKET in s3_url:
            s3_key = s3_url.split(f"{ClientConfig.S3_BUCKET}.s3.us-east-2.amazonaws.com/")[1]
        
        # Determine content type based on file extension
        file_ext = s3_key.lower().split('.')[-1]
        content_type_map = {
            'pdf': 'application/pdf',
            'csv': 'text/csv',
            'md': 'text/markdown',
            'txt': 'text/plain',
            'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'xls': 'application/vnd.ms-excel'
        }
        content_type = content_type_map.get(file_ext, 'application/octet-stream')
        
        signed_url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': ClientConfig.S3_BUCKET,
                'Key': s3_key,
                'ResponseContentDisposition': 'inline',
                'ResponseContentType': content_type
            },
            ExpiresIn=expiration
        )
        
        # Return clean URL without page anchor
        return signed_url
        
    except ClientError as e:
        print(f"[S3 ERROR] Failed to generate signed URL: {e}")
        return s3_url
    except Exception as e:
        print(f"[ERROR] URL parsing failed: {e}")
        return s3_url

def clean_markdown_for_display(text: str) -> str:
    """Remove markdown formatting from text for clean UI display."""
    text = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)
    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
    text = re.sub(r'\*(.+?)\*', r'\1', text)
    text = re.sub(r'^\s*[\*\-]\s+', '', text, flags=re.MULTILINE)
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    return text.strip()

# API ENDPOINTS

@app.get("/")
def read_root():
    return {"message": "Multiformat RAG API", "status": "running"}

@app.get("/health")
def health_check(
    client_id: str = Depends(get_client_id),
    db: Session = Depends(get_db)
):
    """Check if API and database are healthy"""
    try:
        db.execute(text("SELECT 1"))
        
        doc_count = db.query(DocumentMetadata).filter(
            DocumentMetadata.client_id == client_id
        ).count()
        
        return {
            "status": "healthy",
            "database": "connected",
            "client_id": client_id,
            "total_documents": doc_count
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "error",
            "error": str(e)
        }

@app.post("/search", response_model=SearchResponse)
def search_knowledge_base(
    req: QueryRequest,
    client_id: str = Depends(get_client_id)
):
    """Search knowledge base filtered by client"""
    result = retrieve_rag_context(req.query, client_id=client_id, top_k=5)
    llm_answer = generate_response(
        query=req.query,
        model=model,
        client=client,
        context=result["context"]
    )
    # group chunks by document + page
    grouped_pages = defaultdict(lambda: {
        "chunks": [],
        "meta": None
    })

    for i, meta in enumerate(result["metadata"]):
        page_number = meta.get("page_number", 1)
        document_id = meta.get("document_id")

        key = (document_id, page_number)

        grouped_pages[key]["chunks"].append(result["chunks"][i])
        grouped_pages[key]["meta"] = meta

    sources = []
    source_id = 1

    for (_, page_number), data in grouped_pages.items():
        meta = data["meta"]
        merged_text = "\n\n".join(data["chunks"])

        original_s3_url = meta.get("original_s3_url", "")

        sources.append({
            "id": source_id,
            "title": meta.get("title", "Unknown Document"),
            "type": meta.get("doc_type", "Unknown"),
            "page": page_number,
            "section": meta.get("section_heading", ""),
            "excerpt": clean_markdown_for_display(merged_text)[:500],
            "s3_url": generate_signed_url(original_s3_url, page_number),
            "download_url": generate_signed_url(
                meta.get("download_s3_url", original_s3_url),
                1
            ),
            "date": meta.get("date_updated", "Unknown"),
            "original_filename": meta.get("original_filename", "")
        })

        source_id += 1

    return {
        "answer": llm_answer,
        "sources": sources,
        "query": req.query
    }

@app.get("/documents")
def get_all_documents(
    client_id: str = Depends(get_client_id),
    db: Session = Depends(get_db)
):
    """Return all documents for this client"""
    docs = db.query(DocumentMetadata).filter(
        DocumentMetadata.client_id == client_id
    ).all()
    
    documents = []
    for i, doc in enumerate(docs, 1):
        if not doc.document_id:
            print(f"[WARNING] Document missing document_id: {doc.title}")
            continue
            
        # Generate signed URLs for both viewing and downloading
        view_url = generate_signed_url(doc.original_s3_url or "", page_number=1, expiration=3600)
        download_url = generate_signed_url(doc.download_s3_url or doc.original_s3_url or "", page_number=1, expiration=3600)

        documents.append({
        "id": i,
        "document_id": doc.document_id,
        "name": doc.title,
        "type": doc.type,
        "category": doc.type,
        "date": doc.date_updated,
        "pages": doc.total_pages,
        "size": doc.file_size,
        "filename": doc.original_filename,
        "original_filename": doc.original_filename,
        "s3_url": view_url, # For viewing (PDF version with page numbers)
        "download_url": download_url # For downloading (original file)
    })
    
    return {"documents": documents}

@app.delete("/documents/{document_id}")
async def delete_document(
    document_id: str,
    client_id: str = Depends(get_client_id),
    db: Session = Depends(get_db)
):
    """Delete document (only if it belongs to this client)"""
    doc = db.query(DocumentMetadata).filter(
        DocumentMetadata.document_id == document_id,
        DocumentMetadata.client_id == client_id
    ).first()
    
    if not doc:
        return {"success": False, "error": "Document not found or access denied"}
    
    db.delete(doc)
    db.commit()
    print(f"[DELETE] Client {client_id}: Removed {doc.title}")
    
    deleted_count = delete_document_chunks(document_id, client_id)
    print(f"[DELETE] Removed {deleted_count} chunks from Qdrant")
    
    return {
        "success": True,
        "message": f"Deleted document and {deleted_count} chunks"
    }

@app.get("/job/{job_id}")
def get_job_status(
    job_id: str,
    client_id: str = Depends(get_client_id)
):
    """Check status of a processing job"""
    try:
        job_data = redis_conn.get(f"job:{job_id}")
        if not job_data:
            return {"job_id": job_id, "status": "not_found", "error": "Job not found"}
        
        job = json.loads(job_data)
        return {
            "job_id": job_id,
            "status": job.get("status", "unknown"),
            "result": job.get("result"),
            "error": job.get("error")
        }
    except Exception as e:
        return {"job_id": job_id, "status": "error", "error": str(e)}
    
@app.post("/upload", response_model=UploadResponse)
async def upload_document(
    file: UploadFile = File(...),
    client_id: str = Depends(get_client_id)
):
    """Upload file directly to S3 and queue for processing"""
    try:
        # Generate S3 key for raw uploads
        db: Session = SessionLocal()
        existing = db.query(DocumentMetadata).filter(
            DocumentMetadata.client_id == client_id,
            DocumentMetadata.original_filename == file.filename
        ).first()
        db.close()


        if existing:
            return {
                "success": True,
                "message": "File already exists, skipping processing",
                "filename": file.filename,
                "skipped": True
            }
        
        file_extension = Path(file.filename).suffix
        s3_key = f"raw_uploads/{client_id}/{uuid.uuid4()}{file_extension}"
        
        # Read file contents
        contents = await file.read()
        
        # Upload directly to S3
        s3_client.put_object(
            Bucket=ClientConfig.S3_BUCKET,
            Key=s3_key,
            Body=contents,
            ContentType=file.content_type or 'application/octet-stream'
        )
        
        s3_url = f"https://{ClientConfig.S3_BUCKET}.s3.us-east-2.amazonaws.com/{s3_key}"
        print(f"[UPLOAD] Client {client_id}: Uploaded to S3 → {s3_url}")
        
        # Queue job with S3 location
        job_id = str(uuid.uuid4())
        job_data = {
            "job_id": job_id,
            "s3_key": s3_key, # Changed from file_path
            "s3_url": s3_url,
            "client_id": client_id,
            "filename": file.filename,
            "status": "queued",
            "created_at": datetime.now().isoformat()
        }
        
        redis_conn.lpush("processing_queue", json.dumps(job_data))
        redis_conn.setex(f"job:{job_id}", 3600, json.dumps(job_data))
        
        print(f"[QUEUE] Job {job_id} queued for {file.filename}")
        
        return {
            "success": True,
            "message": f"File {file.filename} queued for processing",
            "filename": file.filename
        }
    except Exception as e:
        print(f"[UPLOAD ERROR] {str(e)}")
        return {"success": False, "message": str(e), "filename": file.filename}

# STARTUP
@app.on_event("startup")
def startup_event():
    init_db()
    print("Database initialized")
    
    # Run sync in background thread so it doesn't block startup
    sync_thread = threading.Thread(target=startup_vectordb_sync)
    sync_thread.daemon = True
    sync_thread.start()
    print("Vector DB sync started in background")

def startup_vectordb_sync():
    """Background thread to sync vector DB on startup"""
    try:
        print("[STARTUP SYNC] Starting vector DB sync...")
        
        from sqlalchemy import select
        
        db = SessionLocal()
        try:
            # Get all unique client_ids
            result = db.execute(
                select(DocumentMetadata.client_id).distinct()
            )
            client_ids = [row[0] for row in result]
            
            print(f"[STARTUP SYNC] Found {len(client_ids)} clients")
            
            # Sync each client
            for client_id in client_ids:
                print(f"[STARTUP SYNC] Processing client: {client_id}")
                
                # Step 1: Add missing documents to Vector DB
                sync_all_documents_to_vectordb(client_id)
                
                # Step 2: Remove orphaned documents from Vector DB
                cleanup_orphaned_vectors(client_id)
            
            print("[STARTUP SYNC] Complete")
            
        finally:
            db.close()
            
    except Exception as e:
        print(f"[STARTUP SYNC ERROR] {e}")
        import traceback
        traceback.print_exc()

def sync_all_documents_to_vectordb(client_id: str):
    """Background task to sync all documents to vector DB - SQL is source of truth"""
    try:
        print(f"[SYNC] Starting vector DB sync for client {client_id}")
        
        from ingestion.chunking import chunk_document, load_metadata_from_db, check_document_in_vectordb
        
        # Load all metadata from SQL - this is the source of truth
        metadata = load_metadata_from_db(client_id=client_id)
        
        if not metadata:
            print(f"[SYNC] No metadata found for client {client_id}")
            return
        
        PARSED_DIR = Path("./parsed_data") / client_id
        
        if not PARSED_DIR.exists():
            print(f"[SYNC] No parsed directory for client {client_id}")
            return
        
        print(f"[SYNC] Found {len(metadata)} documents in SQL for client {client_id}")
        
        synced_count = 0
        skipped_count = 0
        missing_files = 0
        
        # Iterate through SQL entries (source of truth)
        for parsed_filename, metadata_entry in metadata.items():
            parsed_file = PARSED_DIR / parsed_filename
            
            # Check if parsed file exists on disk
            if not parsed_file.exists():
                print(f"[SYNC SKIP] {parsed_filename} - file missing from disk")
                missing_files += 1
                continue
            
            document_id = metadata_entry.get("document_id")
            if not document_id:
                print(f"[SYNC SKIP] {parsed_filename} - no document_id")
                continue
            
            # Check if already in vector DB
            if check_document_in_vectordb(document_id, client_id):
                print(f"[SYNC SKIP] {parsed_filename} - already in vector DB")
                skipped_count += 1
                continue
            
            # Embed missing document
            print(f"[SYNC EMBED] {parsed_filename}")
            chunk_document(parsed_file, metadata_entry, client_id=client_id, batch_size=32)
            synced_count += 1
        
        print(f"[SYNC COMPLETE] Client {client_id}: Embedded {synced_count}, Skipped {skipped_count}, Missing files {missing_files}")
        
    except Exception as e:
        print(f"[SYNC ERROR] {str(e)}")
        import traceback
        traceback.print_exc()