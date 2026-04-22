import os, shutil
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from db import collection
from rag import query as rag_query, rebuild_index, get_departments
from ingest import ingest_pdf, ingest_all

app = FastAPI(title="Internal Chatbot API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
DOCS_DIR = "/data/docs"


class ChatRequest(BaseModel):
    question: str
    department: str = "all"


@app.get("/api/health")
def health():
    return {"status": "ok", "chunks": collection.count()}


@app.get("/api/stats")
def stats():
    pdf_files = []
    for root, _, files in os.walk(DOCS_DIR):
        for f in files:
            if f.lower().endswith(".pdf"):
                pdf_files.append(os.path.relpath(os.path.join(root, f), DOCS_DIR))
    return {
        "total_chunks": collection.count(),
        "total_files":  len(pdf_files),
        "files":        sorted(pdf_files),
        "departments":  get_departments(),
    }


@app.get("/api/departments")
def departments():
    return {"departments": get_departments()}


@app.post("/api/chat")
def chat(req: ChatRequest):
    if not req.question.strip():
        raise HTTPException(400, "Question cannot be empty")
    dept = None if req.department == "all" else req.department
    return rag_query(req.question.strip(), department=dept)


@app.post("/api/upload")
async def upload(
    file: UploadFile = File(...),
    department: str = Query(default="General"),
    category:   str = Query(default="general"),
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Chỉ hỗ trợ file PDF")

    dest_dir = os.path.join(DOCS_DIR, department)
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, file.filename)

    with open(dest, "wb") as f:
        shutil.copyfileobj(file.file, f)

    chunks = ingest_pdf(dest, department=department, category=category)
    rebuild_index()  # refresh BM25 after every upload

    print(f"[Upload] {file.filename} → {chunks} chunks | DB total: {collection.count()}")
    return {
        "file":       file.filename,
        "department": department,
        "chunks":     chunks,
        "db_total":   collection.count(),
        "message":    f"Đã index {chunks} chunks từ {file.filename} [{department}]"
    }


@app.post("/api/ingest-all")
def ingest_all_docs():
    results = ingest_all(DOCS_DIR)
    total   = sum(r["chunks"] for r in results)
    rebuild_index()
    return {
        "results":      results,
        "total_chunks": total,
        "message":      f"Đã index {len(results)} file, tổng {total} chunks"
    }


app.mount("/", StaticFiles(directory="/app/static", html=True), name="static")
