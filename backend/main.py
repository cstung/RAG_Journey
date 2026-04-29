import base64
import os, shutil
import re
import time
from urllib.parse import urlparse
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Depends, Header, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
import httpx
import asyncio
from concurrent.futures import ThreadPoolExecutor
from fastapi import BackgroundTasks
import json
from pathlib import Path

from data_adapters import get_connector, REGISTRY
from pipeline import IngestRunner

_executor = ThreadPoolExecutor(max_workers=1)   # one ingestion at a time
_ingest_state: dict = {}                        # in-memory progress cache

class IngestRequest(BaseModel):
    dataset_id: str                              # e.g. "th1nhng0/vietnamese-legal-documents"
    sectors:    list[str] | None = None
    min_year:   int              = 2000
    legal_types: list[str] | None = None
    max_docs:   int | None = None                # set to 100 for smoke test

from vector_store import get_client, count_points, delete_points_by_file, clear_collection, COLLECTION_PDF, COLLECTION_LEGAL
from admin_auth import create_admin_token, verify_admin_token
from emailer import get_notify_emails, send_email
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
    log_email,
    list_email_logs,
    session_exists,
    verify_admin_credentials,
)
from rag import answer, query as rag_query, rebuild_index, get_departments
from ingest import ingest_all, ingest_file, extract_html_text
from middleware.rate_limit import ip_limiter, session_limiter
from utils.input_guard import sanitise_question

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


class SPAStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)
        if response.status_code == 404 and self.html:
            return await super().get_response("index.html", scope)
        return response


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
    question:    str
    collections: list[str] | None = None   # NEW: optional override
    filters:     dict | None = None         # NEW: e.g. {"legal_type": "Nghị định"}
    session_id:  str | None = None
    department:  str = "all"


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


class AdminTestEmailRequest(BaseModel):
    to_emails: list[str] | None = None
    subject: str = "Internal Chatbot: test email"
    body: str = "This is a test email from Internal Chatbot."


class DocumentUpdateRequest(BaseModel):
    department: str
    category: str


@app.post("/api/datasets/ingest")
async def trigger_ingest(req: IngestRequest, background_tasks: BackgroundTasks,
                         _=Depends(verify_admin)):
    """Start ingestion in background. Returns immediately with job_id."""
    job_id = f"{req.dataset_id.replace('/', '_')}_{int(time.time())}"

    def run_job():
        try:
            connector = get_connector(
                req.dataset_id,
                sectors=req.sectors,
                min_year=req.min_year,
                legal_types=req.legal_types,
                max_docs=req.max_docs,
            )
            runner = IngestRunner(connector)
            _ingest_state[job_id] = runner.get_progress()
            runner.run(progress_callback=lambda s: _ingest_state.update({job_id: s}))
        except Exception as e:
            _ingest_state[job_id] = {"status": f"error: {e}"}

    background_tasks.add_task(lambda: asyncio.get_event_loop().run_in_executor(_executor, run_job))
    return {"job_id": job_id, "status": "queued"}

@app.get("/api/datasets/status/{job_id}")
async def ingest_status(job_id: str, _=Depends(verify_admin)):
    state = _ingest_state.get(job_id)
    if state is None:
        # Try reading from disk (survive restarts)
        state_file = Path(f"data/ingest_state/{job_id}.json")
        if state_file.exists():
            state = json.loads(state_file.read_text())
        else:
            raise HTTPException(404, "Job not found")
    return state

@app.get("/api/datasets")
async def list_datasets():
    return {"datasets": list(REGISTRY.keys())}

@app.get("/api/health")
def health():
    return {"status": "ok", "chunks": (count_points(get_client(), COLLECTION_PDF) + count_points(get_client(), COLLECTION_LEGAL))}


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
def stats():
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


@app.patch("/api/admin/documents/{document_id}")
def admin_update_document(document_id: int, req: DocumentUpdateRequest):
    from rag.core import sync_document_metadata
    
    doc = get_document(document_id)
    if not doc:
        raise HTTPException(404, "Document not found")
        
    dept = req.department.strip() or "General"
    cat = req.category.strip() or "general"
    
    # 1. Update SQLite
    from database import update_document_metadata
    update_document_metadata(document_id, dept, cat)
    
    # 2. Sync to ChromaDB (Crucial for search filters)
    try:
        sync_document_metadata(document_id, dept, cat)
    except Exception as e:
        print(f"[Admin] Warning: ChromaDB sync failed for doc {document_id}: {e}")
        # We don't fail the whole request because SQLite is updated, 
        # but the user should know search might be stale.
        return {"status": "partial_ok", "warning": f"SQLite updated but ChromaDB sync failed: {e}"}

    return {"status": "ok", "message": f"Đã cập nhật metadata cho {doc['filename']}"}


@app.get("/api/admin/notifications/emails")
def admin_list_email_logs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    kind: str | None = Query(default=None),
    status: str | None = Query(default=None),
):
    return list_email_logs(page=page, page_size=page_size, kind=kind, status=status)


@app.post("/api/admin/notifications/test-email")
def admin_test_email(req: AdminTestEmailRequest, request: Request):
    to_emails = req.to_emails or get_notify_emails()
    if not to_emails:
        raise HTTPException(400, "No recipients configured (NOTIFY_EMAILS)")

    subject = req.subject
    body = req.body + f"\n\nSent by: {getattr(request.state, 'admin_user', None) or 'admin'}"
    try:
        send_email(to_emails, subject=subject, body=body)
        entry = log_email("test_email", to_emails, subject, body, status="sent")
    except Exception as e:
        entry = log_email("test_email", to_emails, subject, body, status="failed", error=str(e))
        raise HTTPException(500, f"Failed to send email: {e}")

    return {"status": "ok", "log": entry}


def _stats_payload() -> dict:
    valid_exts = (".pdf", ".doc", ".docx", ".txt")
    doc_files = []
    for root, _, files in os.walk(DOCS_DIR):
        for f in files:
            if f.lower().endswith(valid_exts):
                doc_files.append(os.path.relpath(os.path.join(root, f), DOCS_DIR))
    return {
        "total_chunks": (count_points(get_client(), COLLECTION_PDF) + count_points(get_client(), COLLECTION_LEGAL)),
        "total_files":  len(doc_files),
        "files":        sorted(doc_files),
        "departments":  get_departments(),
    }


@app.get("/api/departments")
def departments():
    return {"departments": get_departments()}


@app.post("/api/chat")
def chat(req: ChatRequest, request: Request):
    cleaned_question, is_suspicious = sanitise_question(req.question or "")
    if is_suspicious:
        raise HTTPException(status_code=400, detail="Suspicious input detected")

    if not cleaned_question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    xff = request.headers.get("x-forwarded-for")
    ip = (xff.split(",", 1)[0].strip() if xff else "") or (request.client.host if request.client else "") or "unknown"
    session_key = (req.session_id or "").strip() or ip

    if not session_limiter.is_allowed(f"sess:{session_key}"):
        raise HTTPException(status_code=429, detail="Too many requests. Please try again later.")
    if not ip_limiter.is_allowed(f"ip:{ip}"):
        raise HTTPException(status_code=429, detail="Too many requests. Please try again later.")
    dept = None if req.department == "all" else req.department
    
    # 1. Save user message to history
    if req.session_id:
        try:
            add_message(req.session_id, "user", cleaned_question.strip())
        except Exception as e:
            print(f"[Chat] Warning: could not save user message: {e}")

    # 2. Run RAG Pipeline
    result = answer(
        question=cleaned_question.strip(),
        collections=req.collections,
        filters=req.filters,
    )
    result["rewritten_query"] = result.get("rewritten_query", cleaned_question.strip())

    # 3. Save assistant message to history
    if req.session_id:
        try:
            add_message(
                req.session_id, 
                "assistant", 
                result["answer"], 
                sources=result["sources"],
                rewritten_query=result["rewritten_query"]
            )
        except Exception as e:
            print(f"[Chat] Warning: could not save assistant message: {e}")

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

    if req.rating == -1:
        to_emails = get_notify_emails()
        if to_emails:
            subject = f"[Internal Chatbot] Negative feedback (session {req.session_id})"
            body = (
                f"Session: {req.session_id}\n"
                f"Message ID: {req.message_id}\n"
                f"Reason: {req.reason or ''}\n"
            )
            try:
                send_email(to_emails, subject=subject, body=body)
                log_email("negative_feedback", to_emails, subject, body, status="sent")
            except Exception as e:
                log_email("negative_feedback", to_emails, subject, body, status="failed", error=str(e))
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
    if not (
        file.filename.lower().endswith(".pdf")
        or file.filename.lower().endswith(".doc")
        or file.filename.lower().endswith(".docx")
    ):
        raise HTTPException(400, "Chỉ hỗ trợ file PDF/DOC/DOCX")

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
        delete_points_by_file(get_client(), COLLECTION_PDF, _file_id_for_path(dest)); delete_points_by_file(get_client(), COLLECTION_LEGAL, _file_id_for_path(dest))
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
                delete_points_by_file(get_client(), COLLECTION_PDF, _file_id_for_path(fp)); delete_points_by_file(get_client(), COLLECTION_LEGAL, _file_id_for_path(fp))
            except Exception:
                pass

    print(f"[Upload] {file.filename} → {chunks} chunks | DB total: {(count_points(get_client(), COLLECTION_PDF) + count_points(get_client(), COLLECTION_LEGAL))}")
    return {
        "file":       file.filename,
        "department": department,
        "chunks":     chunks,
        "db_total":   (count_points(get_client(), COLLECTION_PDF) + count_points(get_client(), COLLECTION_LEGAL)),
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
        delete_points_by_file(get_client(), COLLECTION_PDF, _file_id_for_path(dest)); delete_points_by_file(get_client(), COLLECTION_LEGAL, _file_id_for_path(dest))
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
                delete_points_by_file(get_client(), COLLECTION_PDF, _file_id_for_path(fp)); delete_points_by_file(get_client(), COLLECTION_LEGAL, _file_id_for_path(fp))
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
        client = get_client()
        clear_collection(client, COLLECTION_PDF)
        clear_collection(client, COLLECTION_LEGAL)
        rebuild_index()
        return {"status": "ok", "message": "Đã xóa sạch database"}
    except Exception as e:
        raise HTTPException(500, f"Lỗi khi xóa DB: {str(e)}")


def _first_existing_dir(paths: list[str]) -> str | None:
    for p in paths:
        try:
            if p and os.path.isdir(p):
                return p
        except Exception:
            continue
    return None


@app.post("/api/admin/eval")
async def run_eval(_=Depends(verify_admin)):
    """Run RAGAS eval in background, return results path."""
    import subprocess
    proc = subprocess.Popen(
        ["python", "eval/run_eval.py"],
        cwd=os.path.dirname(__file__),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = proc.communicate(timeout=300)
    return {"stdout": stdout.decode(), "stderr": stderr.decode()}

@app.get("/admin/datasets")
async def admin_datasets_page():
    # Check protected location first, then fallback to local static
    for p in ["/frontend_dist/admin_datasets.html", os.path.join(os.path.dirname(__file__), "static", "admin_datasets.html")]:
        if os.path.exists(p):
            return FileResponse(p)
    raise HTTPException(404, "Admin page not found")


# Prefer serving the built React frontend when present.
_BACKEND_DIR = os.path.dirname(__file__)
_FRONTEND_DIST = _first_existing_dir(
    [
        "/frontend_dist",
        os.path.join(_BACKEND_DIR, "frontend", "dist"),
        os.path.abspath(os.path.join(_BACKEND_DIR, "..", "frontend", "dist")),
    ]
)

if _FRONTEND_DIST:
    app.mount("/", SPAStaticFiles(directory=_FRONTEND_DIST, html=True), name="frontend")
else:
    _STATIC_DIR = _first_existing_dir(
        [
            os.path.join(_BACKEND_DIR, "static"),
            "/app/static",
        ]
    )
    if _STATIC_DIR:
        app.mount("/", SPAStaticFiles(directory=_STATIC_DIR, html=True), name="static")
