# RAG Multiformat Document Search

Semantic search system that lets you upload documents and ask questions in natural language. Built with a FastAPI backend, React frontend, and a full async processing pipeline.

**Demo:** [rag-multiformat-document-search.vercel.app](https://rag-multiformat-document-search.vercel.app)

---

## How it works

Documents are uploaded through the UI, queued via Redis, parsed into markdown or CSV, chunked, embedded using OpenAI's `text-embedding-3-small`, and stored in Qdrant. Search queries are embedded and matched against stored chunks filtered by client, then passed to GPT-4o mini for a grounded answer with source citations.

---

## Stack

| Layer | Tech |
|---|---|
| Frontend | React, Vite, Tailwind CSS |
| Backend | FastAPI, Python |
| Vector DB | Qdrant |
| Embeddings | OpenAI `text-embedding-3-small` |
| LLM | GPT-4o mini |
| Job queue | Redis |
| Storage | AWS S3 |
| Database | PostgreSQL (SQLAlchemy) |
| Deployment | Vercel (frontend), Fly.io (backend + worker) |

---

## Supported formats

PDF, DOCX, PPTX, CSV, XLSX, Markdown, TXT

---

## Architecture

```
Upload → S3 → Redis queue → Worker → Parser → Chunker → OpenAI embeddings → Qdrant
                                                                    ↓
Query → OpenAI embedding → Qdrant retrieval → GPT-4o mini → Answer + sources
```

Each document type has its own parsing handler. PDFs are parsed page-by-page with page markers preserved for source citation. DOCX and PPTX files are converted to PDF for viewing and markdown for chunking. Excel files are converted to CSV.

---

## Project structure

```
RAG-Multiformat-Document-Search/
├── frontend/          # React app
│   └── src/
│       ├── App.jsx
│       └── services/api.js
├── backend/
│   ├── api/
│   │   ├── server.py      # FastAPI routes
│   │   ├── database.py    # PostgreSQL models
│   │   └── config.py      # Environment config
│   ├── ingestion/
│   │   ├── ingest.py      # File handlers per format
│   │   └── chunking.py    # Embedding + Qdrant upsert
│   ├── retrieval/
│   │   ├── retrieve.py    # Vector search
│   │   └── generate.py    # LLM response generation
│   └── worker.py          # Redis queue worker
└── Dockerfile
```

---

## Running locally

```bash
# Backend
pip install -r requirements.txt
uvicorn api.server:app --reload

# Worker (separate process)
python worker.py

# Frontend
cd frontend
npm install
npm run dev
```

Requires `.env` with `OPENAI_API_KEY`, `QDRANT_URL`, `VECTORDB_KEY`, `DATABASE_URL`, `REDIS_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.
