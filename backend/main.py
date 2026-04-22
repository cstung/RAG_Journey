import os
import shutil
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import chromadb

from rag import query as rag_query
from ingest import ingest_pdf, ingest_all, collection as chroma_collection

app = FastAPI(title="Internal Chatbot API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DOCS_DIR = "/data/docs"


# ── Models ──────────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    question: str


# ── API Routes ───────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok", "docs_indexed": chroma_collection.count()}


@app.get("/api/stats")
def stats():
    count = chroma_collection.count()
    # List uploaded PDFs
    pdf_files = []
    if os.path.exists(DOCS_DIR):
        pdf_files = [f for f in os.listdir(DOCS_DIR) if f.lower().endswith(".pdf")]
    return {
        "total_chunks": count,
        "total_files": len(pdf_files),
        "files": sorted(pdf_files)
    }


@app.post("/api/chat")
def chat(req: ChatRequest):
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")
    result = rag_query(req.question.strip())
    return result


@app.post("/api/upload")
async def upload(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Chỉ hỗ trợ file PDF")

    os.makedirs(DOCS_DIR, exist_ok=True)
    dest = os.path.join(DOCS_DIR, file.filename)

    with open(dest, "wb") as f:
        shutil.copyfileobj(file.file, f)

    chunks = ingest_pdf(dest)
    return {
        "file": file.filename,
        "chunks": chunks,
        "status": "indexed",
        "message": f"Đã index {chunks} chunks từ file {file.filename}"
    }


@app.post("/api/ingest-all")
def ingest_all_docs():
    """Re-index tất cả PDF trong thư mục docs."""
    results = ingest_all(DOCS_DIR)
    total = sum(r["chunks"] for r in results)
    return {
        "results": results,
        "total_chunks": total,
        "message": f"Đã index {len(results)} file, tổng {total} chunks"
    }


# ── Serve static frontend ────────────────────────────────────────────────────
app.mount("/", StaticFiles(directory="/app/static", html=True), name="static")
