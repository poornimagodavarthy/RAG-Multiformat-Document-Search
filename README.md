# RAG Multiformat Document Search

## Description
A semantic search system that lets you upload documents in any format and ask questions in natural language. Documents are parsed, chunked, and embedded into a vector database. Queries are matched against stored chunks filtered by client, and a grounded answer is generated with source citations and page references.

The system supports PDF, DOCX, PPTX, CSV, XLSX, Markdown, and TXT files, with each format handled by a dedicated parser. An async Redis queue processes uploads in the background so the UI stays responsive.

**Live demo:** [rag-multiformat-document-search.vercel.app](https://rag-multiformat-document-search.vercel.app)

---

## Stack

| Layer | Tech |
|---|---|
| Frontend | React, Vite, Tailwind CSS |
| Backend | FastAPI |
| Vector DB | Qdrant |
| Embeddings | OpenAI `text-embedding-3-small` |
| LLM | GPT-4o mini |
| Job queue | Redis |
| Storage | AWS S3 |
| Database | PostgreSQL (SQLAlchemy) |
| Deployment | Vercel (frontend), Fly.io (backend + worker) |

---

## Choose How to Run
1. [Live Demo](#live-demo)
2. [On Local Machine](#running-on-local-machine)

---

## Live Demo

Visit the deployed app at [rag-multiformat-document-search.vercel.app](https://rag-multiformat-document-search.vercel.app)

1. Click **Upload** and drop any supported document
2. Wait for processing (30–60 seconds)
3. Ask a question in the search bar
4. View the answer with source citations and page references

---

## Running on Local Machine

Prerequisites:
- Python 3.10+ installed
- Node.js 18+ installed
- Redis running locally or a Redis URL
- Qdrant instance (cloud or local)
- OpenAI API key
- AWS S3 bucket

### Instructions:

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/poornimagodavarthy/RAG-Multiformat-Document-Search.git
   ```

2. **Navigate to the Cloned Directory:**
   ```bash
   cd RAG-Multiformat-Document-Search
   ```

3. **Set Up Environment Variables:**
   - Create a `.env` file in the root directory:
   ```
   OPENAI_API_KEY=your_key
   QDRANT_URL=your_qdrant_url
   VECTORDB_KEY=your_qdrant_api_key
   DATABASE_URL=your_postgres_url
   REDIS_URL=your_redis_url
   AWS_ACCESS_KEY_ID=your_aws_key
   AWS_SECRET_ACCESS_KEY=your_aws_secret
   S3_BUCKET=your_bucket_name
   ```

4. **Set Up Virtual Environment (Recommended):**
   - On macOS/Linux:
     ```bash
     python3 -m venv venv
     source venv/bin/activate
     ```
   - On Windows:
     ```bash
     python -m venv venv
     .\venv\Scripts\activate
     ```

5. **Install Backend Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

6. **Start the Backend:**
   ```bash
   uvicorn api.server:app --reload
   ```

7. **Start the Worker (separate terminal):**
   ```bash
   python worker.py
   ```

8. **Install and Start the Frontend:**
   ```bash
   cd frontend
   npm install
   npm run dev
   ```

9. **Open the App:**
   - Navigate to `http://localhost:5173`

---

## Architecture

```
Upload → S3 → Redis queue → Worker → Parser → Chunker → OpenAI embeddings → Qdrant
                                                                    ↓
Query → OpenAI embedding → Qdrant retrieval → GPT-4o mini → Answer + sources
```
