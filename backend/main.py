import base64
import os, shutil
import re
import time
from urllib.parse import urlparse
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Depends, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import httpx
from db import collection
from admin_auth import create_admin_token, verify_admin_token
from database import (
    add_message,
    create_document_version,
    create_session,
    end_session,
    feedback_summary,
    get_session,
    get_recent_messages,
    get_active_document,
    get_session_detail,
    init_db,
    list_documents,
    list_negative_feedback,
    list_sessions,
    get_document,
    list_document_versions,
    prune_document_versions,
    ensure_document_record_for_existing_file,
    set_document_chunk_count,
    update_document_file_path,
    upsert_feedback,
    session_exists,
    verify_admin_credentials,
)
from rag import query as rag_query, rebuild_index, get_departments
from ingest import ingest_all, ingest_file, extract_html_text

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
VERSIONS_DIRNAME = ".versions"
MAX_DOC_VERSIONS = 5


def _safe_stem(filename: str) -> str:
    stem = os.path.splitext(filename)[0]
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("._-")
    return stem or "file"


def _file_id_for_path(filepath: str) -> str:
    try:
        return os.path.relpath(filepath, DOCS_DIR).replace("\\", "/")
    except Exception:
        return os.path.basename(filepath)


class ChatRequest(BaseModel):
    question: str
    department: str = "all"
    session_id: str


class SessionStartRequest(BaseModel):
    user_name: str
    user_lang: str = "vi"


class AdminLoginRequest(BaseModel):
    username: str
    password: str


class FeedbackRequest(BaseModel):
    message_id: int
    session_id: str
    rating: int
    reason: str | None = None


class CrawlRequest(BaseModel):
    url: str
    department: str = "General"
    category: str = "general"
    filename: str | None = None


@app.get("/api/health")
def health():
    return {"status": "ok", "chunks": collection.count()}


@app.on_event("startup")
def _startup():
    init_db()
    try:
        rebuild_index()
    except Exception as e:
        print(f"[Startup] Warning: rebuild_index failed: {e}")


@app.middleware("http")
async def _admin_jwt_middleware(request: Request, call_next):
    path = request.url.path
    if request.method == "OPTIONS":
        return await call_next(request)
    if path.startswith("/api/admin/") and path != "/api/admin/login":
        auth = request.headers.get("Authorization", "")
        if not auth.lower().startswith("bearer "):
            return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

        token = auth.split(" ", 1)[1].strip()
        try:
            payload = verify_admin_token(token)
        except Exception:
            return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

        request.state.admin_user = payload.get("sub")

    return await call_next(request)


@app.post("/api/admin/login")
def admin_login(req: AdminLoginRequest):
    if not verify_admin_credentials(req.username, req.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token, exp = create_admin_token(req.username)
    return {"token": token, "token_type": "bearer", "expires_at": exp}


@app.get("/api/stats")
def stats(admin: bool = Depends(verify_admin)):
    return _stats_payload()


@app.get("/api/admin/stats")
def admin_stats():
    return _stats_payload()


@app.get("/api/admin/sessions")
def admin_list_sessions(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    user_name: str | None = Query(default=None),
    user_lang: str | None = Query(default=None),
    status: str | None = Query(default=None),  # active | ended | all
    created_from: str | None = Query(default=None),
    created_to: str | None = Query(default=None),
):
    status_norm = None if (not status or status == "all") else status
    return list_sessions(
        page=page,
        page_size=page_size,
        user_name=user_name,
        user_lang=user_lang,
        status=status_norm,
        created_from=created_from,
        created_to=created_to,
    )


@app.get("/api/admin/sessions/{session_id}")
def admin_get_session(session_id: str):
    try:
        return get_session_detail(session_id)
    except KeyError:
        raise HTTPException(404, "Session not found")


@app.get("/api/admin/feedback/negative")
def admin_negative_feedback(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    threshold: int = Query(default=-1, ge=-5, le=0),
    session_id: str | None = Query(default=None),
    user_name: str | None = Query(default=None),
):
    return list_negative_feedback(
        page=page,
        page_size=page_size,
        threshold=threshold,
        session_id=session_id,
        user_name=user_name,
    )


@app.get("/api/admin/documents")
def admin_list_documents(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    filename: str | None = Query(default=None),
    department: str | None = Query(default=None),
    category: str | None = Query(default=None),
    active: int | None = Query(default=None),  # 1|0|None
):
    return list_documents(
        page=page,
        page_size=page_size,
        filename=filename,
        department=department,
        category=category,
        active=active,
    )


@app.get("/api/admin/documents/{document_id}")
def admin_get_document(document_id: int):
    doc = get_document(document_id)
    if not doc:
        raise HTTPException(404, "Document not found")
    doc["versions"] = list_document_versions(doc["filename"], doc["department"], doc["category"])
    return doc


def _stats_payload() -> dict:
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
    session_id = req.session_id.strip()
    if not session_exists(session_id):
        raise HTTPException(404, "Session not found")
    dept = None if req.department == "all" else req.department

    question = req.question.strip()
    history = get_recent_messages(session_id, limit=8)
    add_message(session_id, role="user", content=question)

    result = rag_query(question, department=dept, history=history)
    answer = result.get("answer", "")
    sources = result.get("sources", [])
    rewritten = result.get("rewritten_query")
    bot_message_id = add_message(
        session_id,
        role="assistant",
        content=answer,
        sources=sources if isinstance(sources, list) else None,
        rewritten_query=rewritten,
    )
    result["session_id"] = session_id
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


@app.post("/api/feedback")
def submit_feedback(req: FeedbackRequest):
    if req.rating not in (1, -1):
        raise HTTPException(400, "rating must be 1 or -1")
    if req.rating == -1 and (not req.reason or not req.reason.strip()):
        raise HTTPException(400, "reason is required for negative feedback")
    try:
        fb = upsert_feedback(
            message_id=req.message_id,
            session_id=req.session_id,
            rating=req.rating,
            reason=req.reason.strip() if req.reason else None,
        )
    except KeyError:
        raise HTTPException(404, "Message not found")
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"status": "ok", "feedback": fb}


@app.get("/api/feedback/summary")
def feedback_summary_api(session_id: str | None = Query(default=None)):
    return feedback_summary(session_id=session_id)


@app.post("/api/upload")
async def upload(
    request: Request,
    file: UploadFile = File(...),
    department: str = Query(default="General"),
    category:   str = Query(default="general"),
    admin: bool = Depends(verify_admin)
):
    uploaded_by = "admin"
    return await _handle_upload(request=request, file=file, department=department, category=category, uploaded_by=uploaded_by)


@app.post("/api/admin/upload")
async def admin_upload(
    request: Request,
    file: UploadFile = File(...),
    department: str = Query(default="General"),
    category:   str = Query(default="general"),
):
    uploaded_by = getattr(request.state, "admin_user", None) or "admin"
    return await _handle_upload(request=request, file=file, department=department, category=category, uploaded_by=uploaded_by)


async def _handle_upload(request: Request, file: UploadFile, department: str, category: str, uploaded_by: str = "admin"):
    if not (file.filename.lower().endswith(".pdf") or file.filename.lower().endswith(".docx")):
        raise HTTPException(400, "Chỉ hỗ trợ file PDF hoặc DOCX")

    dest_dir = os.path.join(DOCS_DIR, department)
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, file.filename)

    active = get_active_document(file.filename, department=department, category=category)

    # Move current active file (if exists on disk) into versions folder
    if os.path.exists(dest):
        old_version = int(active["version"]) if active else 1
        versions_root = os.path.join(dest_dir, VERSIONS_DIRNAME, _safe_stem(file.filename), f"v{old_version}")
        os.makedirs(versions_root, exist_ok=True)
        moved_path = os.path.join(versions_root, file.filename)
        if os.path.exists(moved_path):
            base, ext = os.path.splitext(file.filename)
            moved_path = os.path.join(versions_root, f"{base}_{old_version}{ext}")

        shutil.move(dest, moved_path)

        if active:
            update_document_file_path(active["id"], moved_path)
        else:
            ensure_document_record_for_existing_file(
                filename=file.filename,
                department=department,
                category=category,
                file_path=moved_path,
                version=old_version,
                is_active=0,
                uploaded_by="admin",
            )

    with open(dest, "wb") as f:
        shutil.copyfileobj(file.file, f)

    doc = create_document_version(
        filename=file.filename,
        department=department,
        category=category,
        file_path=dest,
        uploaded_by=uploaded_by,
    )

    # Remove old chunks for this logical path before re-indexing latest
    try:
        collection.delete(where={"file_id": _file_id_for_path(dest)})
    except Exception as e:
        print(f"[Upload] Warning: could not delete old chunks for {dest}: {e}")

    chunks = ingest_file(dest, department=department, category=category, document_id=doc["id"], version=doc["version"])
    set_document_chunk_count(doc["id"], chunks)
    rebuild_index()  # refresh BM25 after every upload

    # Keep only last 5 versions
    removed = prune_document_versions(file.filename, department, category, keep=MAX_DOC_VERSIONS)
    for r in removed:
        fp = r.get("file_path")
        if fp and os.path.exists(fp):
            try:
                docs_root = os.path.realpath(DOCS_DIR) + os.sep
                fp_real = os.path.realpath(fp)
                if fp_real.startswith(docs_root):
                    os.remove(fp)
            except Exception as e:
                print(f"[Upload] Warning: could not remove old version file {fp}: {e}")
        if fp:
            try:
                collection.delete(where={"file_id": _file_id_for_path(fp)})
            except Exception:
                pass

    print(f"[Upload] {file.filename} → {chunks} chunks | DB total: {collection.count()}")
    return {
        "file":       file.filename,
        "department": department,
        "chunks":     chunks,
        "db_total":   collection.count(),
        "message":    f"Đã index v{doc['version']} ({chunks} chunks) từ {file.filename} [{department}]"
    }


def _sanitize_filename(name: str, ext: str = ".txt") -> str:
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", (name or "").strip()).strip("._-")
    if not name:
        name = "crawled"
    if not name.lower().endswith(ext):
        name += ext
    return name


def _is_blocked_crawl_target(u: str) -> bool:
    try:
        p = urlparse(u)
    except Exception:
        return True
    if p.scheme not in ("http", "https"):
        return True
    host = (p.hostname or "").lower()
    if not host:
        return True
    if host in ("localhost",):
        return True
    if host.startswith("127."):
        return True
    if host == "169.254.169.254":
        return True
    return False


async def _fetch_url(url: str) -> tuple[str, str]:
    headers = {"User-Agent": "internal-chatbot/1.0"}
    timeout = httpx.Timeout(15.0, connect=10.0)
    async with httpx.AsyncClient(follow_redirects=True, headers=headers, timeout=timeout) as client:
        r = await client.get(url)
        r.raise_for_status()
        content_type = (r.headers.get("content-type") or "").lower()
        text = r.text
        return content_type, text


@app.post("/api/admin/crawl")
async def admin_crawl(req: CrawlRequest, request: Request):
    if _is_blocked_crawl_target(req.url):
        raise HTTPException(400, "URL not allowed")

    dept = (req.department or "General").strip() or "General"
    cat = (req.category or "general").strip() or "general"

    content_type, body = await _fetch_url(req.url)

    if "text/html" in content_type or body.lstrip().lower().startswith("<!doctype") or "<html" in body[:200].lower():
        text = extract_html_text(body)
    else:
        text = body.strip()

    if not text:
        raise HTTPException(400, "No extractable content")

    # Determine filename
    parsed = urlparse(req.url)
    base = req.filename or (parsed.path.split("/")[-1] or parsed.hostname or "crawled")
    base = os.path.splitext(base)[0]
    base = f"{base}_{int(time.time())}"
    filename = _sanitize_filename(base, ext=".txt")

    dest_dir = os.path.join(DOCS_DIR, dept)
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, filename)

    with open(dest, "w", encoding="utf-8") as f:
        f.write(text)

    doc = create_document_version(
        filename=filename,
        department=dept,
        category=cat,
        file_path=dest,
        uploaded_by=getattr(request.state, "admin_user", None) or "admin",
    )

    try:
        collection.delete(where={"file_id": _file_id_for_path(dest)})
    except Exception:
        pass

    chunks = ingest_file(dest, department=dept, category=cat, document_id=doc["id"], version=doc["version"])
    set_document_chunk_count(doc["id"], chunks)
    rebuild_index()

    removed = prune_document_versions(filename, dept, cat, keep=MAX_DOC_VERSIONS)
    for r in removed:
        fp = r.get("file_path")
        if fp and os.path.exists(fp):
            try:
                docs_root = os.path.realpath(DOCS_DIR) + os.sep
                fp_real = os.path.realpath(fp)
                if fp_real.startswith(docs_root):
                    os.remove(fp)
            except Exception:
                pass
        if fp:
            try:
                collection.delete(where={"file_id": _file_id_for_path(fp)})
            except Exception:
                pass

    return {
        "status": "ok",
        "url": req.url,
        "file": filename,
        "department": dept,
        "category": cat,
        "document_id": doc["id"],
        "version": doc["version"],
        "chunks": chunks,
    }


@app.post("/api/ingest-all")
def ingest_all_docs(admin: bool = Depends(verify_admin)):
    return _handle_ingest_all()


@app.post("/api/admin/ingest-all")
def admin_ingest_all_docs():
    return _handle_ingest_all()


def _handle_ingest_all():
    results = ingest_all(DOCS_DIR)
    total = sum(r["chunks"] for r in results)
    rebuild_index()
    return {
        "results": results,
        "total_chunks": total,
        "message": f"Đã index {len(results)} file, tổng {total} chunks",
    }


@app.post("/api/reset")
def reset_db(admin: bool = Depends(verify_admin)):
    return _handle_reset()


@app.post("/api/admin/reset")
def admin_reset_db():
    return _handle_reset()


def _handle_reset():
    try:
        all_data = collection.get(include=[])
        if all_data["ids"]:
            collection.delete(ids=all_data["ids"])
        rebuild_index()
        return {"status": "ok", "message": "Đã xóa sạch database"}
    except Exception as e:
        raise HTTPException(500, f"Lỗi khi xóa DB: {str(e)}")


app.mount("/", StaticFiles(directory="/app/static", html=True), name="static")
