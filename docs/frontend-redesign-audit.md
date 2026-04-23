# Frontend Redesign — Phase 0.1 Audit (internal-chatbot)

Date: 2026-04-23 (Asia/Saigon)

## Repo snapshot

- Backend: `backend/main.py` (FastAPI) + ChromaDB + SQLite (`data/sqlite`) + documents in `data/docs`.
- Current UI: static HTML served by FastAPI from `backend/static/index.html` via `StaticFiles` mount at `/`.
  - Tech: plain HTML/CSS/JS, uses CDN `marked` for Markdown rendering (no React).
  - Backup created at `frontend_old/static/index.html` (copied, original unchanged).

## FastAPI endpoint inventory (current)

### Public (no auth)

- `GET /api/health` → `{ status: "ok", chunks: number }`
- `GET /api/departments` → `{ departments: string[] }`
- `POST /api/session/start` body `{ user_name: string, user_lang?: string }`
  - → `{ session_id, user_name, user_lang, created_at }`
- `GET /api/session/{session_id}` → session with messages:
  - `{ id, user_name, user_lang, created_at, ended_at, messages: [...] }`
- `POST /api/session/{session_id}/end` → `{ status: "ok", session_id }`
- `POST /api/chat` body `{ question: string, department?: string, session_id: string }`
  - Requires `session_id` exists; otherwise `404 Session not found`
  - → RAG result + metadata: `{ answer, sources?, rewritten_query?, session_id, message_id }`
- `POST /api/feedback` body `{ message_id: number, session_id: string, rating: 1|-1, reason?: string }`
  - Negative feedback (`rating=-1`) requires `reason`
  - → `{ status: "ok", feedback: {...} }`
- `GET /api/feedback/summary?session_id=` → `{ total, positive, negative, reason_counts: [...], session_id }`

### Admin (JWT Bearer)

All `/api/admin/*` routes **except** `/api/admin/login` require `Authorization: Bearer <token>` (enforced by middleware in `backend/main.py`).

- `POST /api/admin/login` body `{ username: string, password: string }`
  - → `{ token, token_type: "bearer", expires_at }`
- `GET /api/admin/stats` → stats payload (see below)
- `GET /api/admin/sessions?page&page_size&user_name&user_lang&status&created_from&created_to`
  - → `{ items: [...], page, page_size, total }`
- `GET /api/admin/sessions/{session_id}` → session detail (session + `messages[]` + `feedback[]`)
- `GET /api/admin/feedback/negative?page&page_size&threshold&session_id&user_name`
  - → `{ items: [...], page, page_size, total, threshold }`
- `GET /api/admin/documents?page&page_size&filename&department&category&active`
  - → `{ items: [...], page, page_size, total }`
- `GET /api/admin/documents/{document_id}` → document + `versions[]`
- `GET /api/admin/notifications/emails?page&page_size&kind&status`
  - → `{ items: [...], page, page_size, total }`
- `POST /api/admin/notifications/test-email` body `{ to_emails?: string[], subject?: string, body?: string }`
  - → `{ status: "ok", log: {...} }`
- `POST /api/admin/upload?department=&category=` (multipart `file`)
  - → `{ file, department, chunks, db_total, message }`
- `POST /api/admin/crawl` body `{ url: string, department?: string, category?: string, filename?: string }`
  - → `{ status: "ok", url, file, department, category, document_id, version, chunks }`
- `POST /api/admin/ingest-all` → `{ results: [...], total_chunks, message }`
- `POST /api/admin/reset` → `{ status: "ok", message }`

### Admin (non-JWT “verify_admin” dependency)

These routes are protected by `verify_admin(...)` (accepts either `X-Admin-User` + `X-Admin-Pass`, **or** `Authorization: Basic ...`, **or** `X-Admin-Key`):

- `GET /api/stats` → stats payload:
  - `{ total_chunks, total_files, files: string[], departments: string[] }`
- `POST /api/upload?department=&category=` (multipart `file`) → same response as `/api/admin/upload`
- `POST /api/ingest-all` → same as `/api/admin/ingest-all`
- `POST /api/reset` → same as `/api/admin/reset`

## Differences vs “new frontend expects” list in redesign request

Mostly compatible, but there are notable shape/auth differences:

- `GET /api/stats` is **admin-protected** (via `verify_admin`), not public.
- `POST /api/admin/login` returns `{ token, token_type, expires_at }` (not just `{ token }`).
- `POST /api/chat` returns extra fields `{ session_id, message_id }` and requires a valid session.
- `POST /api/session/start` returns extra fields `{ user_name, user_lang, created_at }` (not just `{ session_id }`).
- `POST /api/admin/upload` supports both `department` **and** `category` query params.
- `POST /api/admin/crawl` request/response are richer than `{ file, version, chunks }` (includes `url`, `document_id`, etc.).
- Admin list endpoints support more filters than listed:
  - `/api/admin/sessions` supports `user_lang`, `created_from`, `created_to`
  - `/api/admin/documents` supports `department`, `category`

Additional endpoints available (not in the “expects” list): `/api/health`, `/api/departments`, `/api/session/{id}`, `/api/session/{id}/end`, `/api/feedback`, `/api/feedback/summary`, plus non-JWT admin variants (`/api/upload`, `/api/ingest-all`, `/api/reset`).

## Notes / follow-ups

- Root `README.md` “API” section does not reflect current auth requirements (e.g., `/api/stats`, `/api/upload`, `/api/ingest-all`, `/api/reset` all require admin auth in code).
- There is no React app in this repo today; the current UI is the static file at `backend/static/index.html` (now backed up under `frontend_old/`).

