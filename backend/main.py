import base64
import os, shutil
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from db import collection
from database import (
    add_message,
    create_session,
    end_session,
    get_session,
    init_db,
    session_exists,
    verify_admin_credentials,
)
from rag import query as rag_query, rebuild_index, get_departments
from ingest import ingest_pdf, ingest_all

def _parse_basic_auth(authorization: str | None) -> tuple[str, str] | None:
    if not authorization:
        return None
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "basic":
        return None
    try:
        raw = base64.b64decode(parts[1].strip()).decode("utf-8")
    except Exception:
        return None
    if ":" not in raw:
        return None
    username, password = raw.split(":", 1)
    return username, password


def verify_admin(
    x_admin_key: str | None = Header(default=None),
    x_admin_user: str | None = Header(default=None),
    x_admin_pass: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
):
    creds: tuple[str, str] | None = None

    if x_admin_user and x_admin_pass:
        creds = (x_admin_user, x_admin_pass)
    elif authorization:
        creds = _parse_basic_auth(authorization)
    elif x_admin_key:
        creds = ("admin", x_admin_key)

    if creds and verify_admin_credentials(creds[0], creds[1]):
        return True

    raise HTTPException(status_code=401, detail="Unauthorized: Admin access required")

app = FastAPI(title="Internal Chatbot API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
DOCS_DIR = "/data/docs"


class ChatRequest(BaseModel):
    question: str
    department: str = "all"
    session_id: str


class SessionStartRequest(BaseModel):
    user_name: str
    user_lang: str = "vi"


@app.get("/api/health")
def health():
    return {"status": "ok", "chunks": collection.count()}


@app.on_event("startup")
def _startup():
    init_db()


@app.get("/api/stats")
def stats(admin: bool = Depends(verify_admin)):
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
    if not req.session_id or not req.session_id.strip():
        raise HTTPException(400, "session_id is required")
    if not session_exists(req.session_id.strip()):
        raise HTTPException(404, "Session not found")
    dept = None if req.department == "all" else req.department

    question = req.question.strip()
    add_message(req.session_id.strip(), role="user", content=question)

    result = rag_query(question, department=dept)
    answer = result.get("answer", "")
    sources = result.get("sources", [])
    rewritten = result.get("rewritten_query")
    bot_message_id = add_message(
        req.session_id.strip(),
        role="assistant",
        content=answer,
        sources=sources if isinstance(sources, list) else None,
        rewritten_query=rewritten,
    )
    result["session_id"] = req.session_id.strip()
    result["message_id"] = bot_message_id
    return result


@app.post("/api/session/start")
def session_start(req: SessionStartRequest):
    try:
        sess = create_session(req.user_name, user_lang=req.user_lang)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"session_id": sess["id"], "user_name": sess["user_name"], "user_lang": sess["user_lang"], "created_at": sess["created_at"]}


@app.get("/api/session/{session_id}")
def session_get(session_id: str):
    try:
        return get_session(session_id)
    except KeyError:
        raise HTTPException(404, "Session not found")


@app.post("/api/session/{session_id}/end")
def session_end(session_id: str):
    try:
        end_session(session_id)
        return {"status": "ok", "session_id": session_id}
    except KeyError:
        raise HTTPException(404, "Session not found")


@app.post("/api/upload")
async def upload(
    file: UploadFile = File(...),
    department: str = Query(default="General"),
    category:   str = Query(default="general"),
    admin: bool = Depends(verify_admin)
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
def ingest_all_docs(admin: bool = Depends(verify_admin)):
    results = ingest_all(DOCS_DIR)
    total   = sum(r["chunks"] for r in results)
    rebuild_index()
    return {
        "results":      results,
        "total_chunks": total,
        "message":      f"Đã index {len(results)} file, tổng {total} chunks"
    }


@app.post("/api/reset")
def reset_db(admin: bool = Depends(verify_admin)):
    try:
        # Get all IDs
        all_data = collection.get(include=[])
        if all_data["ids"]:
            collection.delete(ids=all_data["ids"])
        rebuild_index()
        return {"status": "ok", "message": "Đã xóa sạch database"}
    except Exception as e:
        raise HTTPException(500, f"Lỗi khi xóa DB: {str(e)}")


app.mount("/", StaticFiles(directory="/app/static", html=True), name="static")
