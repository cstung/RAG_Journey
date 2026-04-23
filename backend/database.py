import os
import json
import sqlite3
import uuid

import bcrypt


DB_PATH = os.getenv("SQLITE_PATH", "/data/sqlite/chatbot.db")


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  user_name TEXT NOT NULL,
  user_lang TEXT DEFAULT 'vi',
  created_at DATETIME DEFAULT (datetime('now')),
  ended_at DATETIME
);

CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL REFERENCES sessions(id),
  role TEXT NOT NULL,
  content TEXT NOT NULL,
  sources TEXT,
  rewritten_query TEXT,
  created_at DATETIME DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS feedback (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id INTEGER NOT NULL REFERENCES messages(id),
  session_id TEXT NOT NULL REFERENCES sessions(id),
  rating INTEGER NOT NULL,
  reason TEXT,
  created_at DATETIME DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_feedback_message_id ON feedback(message_id);

CREATE TABLE IF NOT EXISTS admins (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  created_at DATETIME DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename TEXT NOT NULL,
  department TEXT NOT NULL,
  category TEXT DEFAULT 'general',
  version INTEGER DEFAULT 1,
  is_active INTEGER DEFAULT 1,
  file_path TEXT NOT NULL,
  chunk_count INTEGER DEFAULT 0,
  uploaded_at DATETIME DEFAULT (datetime('now')),
  uploaded_by TEXT DEFAULT 'admin'
);
"""


def get_db() -> sqlite3.Connection:
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(
        DB_PATH,
        check_same_thread=False,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db() -> None:
    conn = get_db()
    try:
        conn.executescript(SCHEMA_SQL)
        _seed_admin(conn)
        conn.commit()
    finally:
        conn.close()


def _seed_admin(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT COUNT(1) AS c FROM admins").fetchone()
    if row and int(row["c"]) > 0:
        return

    username = "admin"
    password = "admin123"
    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    conn.execute(
        "INSERT INTO admins (username, password_hash) VALUES (?, ?)",
        (username, password_hash),
    )


def verify_admin_credentials(username: str, password: str) -> bool:
    if not username or not password:
        return False
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT password_hash FROM admins WHERE username = ?",
            (username,),
        ).fetchone()
        if not row:
            return False
        return bcrypt.checkpw(
            password.encode("utf-8"),
            str(row["password_hash"]).encode("utf-8"),
        )
    finally:
        conn.close()


def create_session(user_name: str, user_lang: str = "vi") -> dict:
    if not user_name or not user_name.strip():
        raise ValueError("user_name is required")
    session_id = str(uuid.uuid4())
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO sessions (id, user_name, user_lang) VALUES (?, ?, ?)",
            (session_id, user_name.strip(), (user_lang or "vi").strip()),
        )
        row = conn.execute(
            "SELECT id, user_name, user_lang, created_at, ended_at FROM sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
        conn.commit()
        return dict(row) if row else {"id": session_id, "user_name": user_name.strip(), "user_lang": user_lang or "vi"}
    finally:
        conn.close()


def end_session(session_id: str) -> None:
    conn = get_db()
    try:
        cur = conn.execute(
            "UPDATE sessions SET ended_at = COALESCE(ended_at, datetime('now')) WHERE id = ?",
            (session_id,),
        )
        if cur.rowcount == 0:
            raise KeyError("session not found")
        conn.commit()
    finally:
        conn.close()


def session_exists(session_id: str) -> bool:
    if not session_id:
        return False
    conn = get_db()
    try:
        row = conn.execute("SELECT 1 FROM sessions WHERE id = ? LIMIT 1", (session_id,)).fetchone()
        return row is not None
    finally:
        conn.close()


def add_message(
    session_id: str,
    role: str,
    content: str,
    sources: list[str] | None = None,
    rewritten_query: str | None = None,
) -> int:
    conn = get_db()
    try:
        cur = conn.execute(
            "INSERT INTO messages (session_id, role, content, sources, rewritten_query) VALUES (?, ?, ?, ?, ?)",
            (
                session_id,
                role,
                content,
                json.dumps(sources, ensure_ascii=False) if sources is not None else None,
                rewritten_query,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def get_session(session_id: str) -> dict:
    conn = get_db()
    try:
        sess = conn.execute(
            "SELECT id, user_name, user_lang, created_at, ended_at FROM sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
        if not sess:
            raise KeyError("session not found")

        msgs = conn.execute(
            "SELECT id, session_id, role, content, sources, rewritten_query, created_at "
            "FROM messages WHERE session_id = ? ORDER BY id ASC",
            (session_id,),
        ).fetchall()

        messages = []
        for m in msgs:
            d = dict(m)
            if d.get("sources"):
                try:
                    d["sources"] = json.loads(d["sources"])
                except Exception:
                    pass
            messages.append(d)

        out = dict(sess)
        out["messages"] = messages
        return out
    finally:
        conn.close()


def get_recent_messages(session_id: str, limit: int = 8) -> list[dict]:
    if not session_id:
        return []
    limit = max(0, min(int(limit), 50))
    if limit == 0:
        return []

    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT id, session_id, role, content, sources, rewritten_query, created_at "
            "FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT ?",
            (session_id, limit),
        ).fetchall()
        rows = list(reversed(rows))

        messages = []
        for r in rows:
            d = dict(r)
            if d.get("sources"):
                try:
                    d["sources"] = json.loads(d["sources"])
                except Exception:
                    pass
            messages.append(d)
        return messages
    finally:
        conn.close()


def list_sessions(
    page: int = 1,
    page_size: int = 20,
    user_name: str | None = None,
    user_lang: str | None = None,
    status: str | None = None,  # "active" | "ended" | None
    created_from: str | None = None,  # ISO8601 date/datetime
    created_to: str | None = None,    # ISO8601 date/datetime
) -> dict:
    page = max(1, int(page))
    page_size = max(1, min(100, int(page_size)))
    offset = (page - 1) * page_size

    where = []
    args: list = []

    if user_name:
        where.append("user_name LIKE ?")
        args.append(f"%{user_name}%")
    if user_lang:
        where.append("user_lang = ?")
        args.append(user_lang)
    if status == "active":
        where.append("ended_at IS NULL")
    elif status == "ended":
        where.append("ended_at IS NOT NULL")

    if created_from:
        where.append("created_at >= ?")
        args.append(created_from)
    if created_to:
        where.append("created_at <= ?")
        args.append(created_to)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    conn = get_db()
    try:
        total_row = conn.execute(
            f"SELECT COUNT(1) AS c FROM sessions {where_sql}",
            args,
        ).fetchone()
        total = int(total_row["c"]) if total_row else 0

        rows = conn.execute(
            "SELECT id, user_name, user_lang, created_at, ended_at "
            f"FROM sessions {where_sql} "
            "ORDER BY created_at DESC "
            "LIMIT ? OFFSET ?",
            [*args, page_size, offset],
        ).fetchall()
        sessions = [dict(r) for r in rows]

        return {
            "items": sessions,
            "page": page,
            "page_size": page_size,
            "total": total,
        }
    finally:
        conn.close()


def get_session_detail(session_id: str) -> dict:
    out = get_session(session_id)
    conn = get_db()
    try:
        fb = conn.execute(
            "SELECT id, message_id, session_id, rating, reason, created_at "
            "FROM feedback WHERE session_id = ? ORDER BY id ASC",
            (session_id,),
        ).fetchall()
        out["feedback"] = [dict(r) for r in fb]
        return out
    finally:
        conn.close()


def list_negative_feedback(
    page: int = 1,
    page_size: int = 20,
    threshold: int = -1,
    session_id: str | None = None,
    user_name: str | None = None,
) -> dict:
    page = max(1, int(page))
    page_size = max(1, min(100, int(page_size)))
    offset = (page - 1) * page_size
    threshold = int(threshold)
    where = ["f.rating < 0"]
    args: list = []
    if threshold is not None:
        where.append("f.rating <= ?")
        args.append(threshold)

    if session_id:
        where.append("f.session_id = ?")
        args.append(session_id)
    if user_name:
        where.append("s.user_name LIKE ?")
        args.append(f"%{user_name}%")

    where_sql = "WHERE " + " AND ".join(where)

    conn = get_db()
    try:
        total_row = conn.execute(
            "SELECT COUNT(1) AS c "
            "FROM feedback f "
            "JOIN sessions s ON s.id = f.session_id "
            f"{where_sql}",
            args,
        ).fetchone()
        total = int(total_row["c"]) if total_row else 0

        rows = conn.execute(
            "SELECT "
            "f.id, f.message_id, f.session_id, f.rating, f.reason, f.created_at, "
            "s.user_name, s.user_lang, "
            "m.content AS message_content "
            "FROM feedback f "
            "JOIN sessions s ON s.id = f.session_id "
            "LEFT JOIN messages m ON m.id = f.message_id "
            f"{where_sql} "
            "ORDER BY f.created_at DESC "
            "LIMIT ? OFFSET ?",
            [*args, page_size, offset],
        ).fetchall()
        items = [dict(r) for r in rows]

        return {
            "items": items,
            "page": page,
            "page_size": page_size,
            "total": total,
            "threshold": threshold,
        }
    finally:
        conn.close()


def get_active_document(filename: str, department: str, category: str = "general") -> dict | None:
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT id, filename, department, category, version, is_active, file_path, chunk_count, uploaded_at, uploaded_by "
            "FROM documents WHERE filename = ? AND department = ? AND category = ? AND is_active = 1 "
            "ORDER BY version DESC LIMIT 1",
            (filename, department, category),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def update_document_file_path(document_id: int, file_path: str) -> None:
    conn = get_db()
    try:
        conn.execute("UPDATE documents SET file_path = ? WHERE id = ?", (file_path, document_id))
        conn.commit()
    finally:
        conn.close()


def set_document_chunk_count(document_id: int, chunk_count: int) -> None:
    conn = get_db()
    try:
        conn.execute("UPDATE documents SET chunk_count = ? WHERE id = ?", (int(chunk_count), document_id))
        conn.commit()
    finally:
        conn.close()


def create_document_version(
    filename: str,
    department: str,
    category: str,
    file_path: str,
    uploaded_by: str = "admin",
) -> dict:
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT COALESCE(MAX(version), 0) AS v FROM documents WHERE filename = ? AND department = ? AND category = ?",
            (filename, department, category),
        ).fetchone()
        next_version = int(row["v"] or 0) + 1

        conn.execute(
            "UPDATE documents SET is_active = 0 WHERE filename = ? AND department = ? AND category = ? AND is_active = 1",
            (filename, department, category),
        )
        cur = conn.execute(
            "INSERT INTO documents (filename, department, category, version, is_active, file_path, uploaded_by) "
            "VALUES (?, ?, ?, ?, 1, ?, ?)",
            (filename, department, category, next_version, file_path, uploaded_by),
        )
        doc_id = int(cur.lastrowid)
        doc = conn.execute(
            "SELECT id, filename, department, category, version, is_active, file_path, chunk_count, uploaded_at, uploaded_by "
            "FROM documents WHERE id = ?",
            (doc_id,),
        ).fetchone()
        conn.commit()
        return dict(doc)
    finally:
        conn.close()


def ensure_document_record_for_existing_file(
    filename: str,
    department: str,
    category: str,
    file_path: str,
    version: int = 1,
    is_active: int = 0,
    uploaded_by: str = "admin",
) -> dict:
    conn = get_db()
    try:
        cur = conn.execute(
            "INSERT INTO documents (filename, department, category, version, is_active, file_path, uploaded_by) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (filename, department, category, int(version), int(is_active), file_path, uploaded_by),
        )
        doc_id = int(cur.lastrowid)
        doc = conn.execute(
            "SELECT id, filename, department, category, version, is_active, file_path, chunk_count, uploaded_at, uploaded_by "
            "FROM documents WHERE id = ?",
            (doc_id,),
        ).fetchone()
        conn.commit()
        return dict(doc)
    finally:
        conn.close()


def prune_document_versions(
    filename: str,
    department: str,
    category: str,
    keep: int = 5,
) -> list[dict]:
    keep = max(1, min(50, int(keep)))
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT id, filename, department, category, version, is_active, file_path "
            "FROM documents WHERE filename = ? AND department = ? AND category = ? "
            "ORDER BY version DESC",
            (filename, department, category),
        ).fetchall()
        docs = [dict(r) for r in rows]
        to_delete = docs[keep:]
        if to_delete:
            ids = [d["id"] for d in to_delete]
            conn.execute(
                f"DELETE FROM documents WHERE id IN ({','.join(['?'] * len(ids))})",
                ids,
            )
        conn.commit()
        return to_delete
    finally:
        conn.close()


def list_documents(
    page: int = 1,
    page_size: int = 20,
    filename: str | None = None,
    department: str | None = None,
    category: str | None = None,
    active: int | None = None,  # 1|0|None
) -> dict:
    page = max(1, int(page))
    page_size = max(1, min(100, int(page_size)))
    offset = (page - 1) * page_size

    where = []
    args: list = []
    if filename:
        where.append("filename LIKE ?")
        args.append(f"%{filename}%")
    if department:
        where.append("department = ?")
        args.append(department)
    if category:
        where.append("category = ?")
        args.append(category)
    if active is not None:
        where.append("is_active = ?")
        args.append(int(active))

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    conn = get_db()
    try:
        total_row = conn.execute(
            f"SELECT COUNT(1) AS c FROM documents {where_sql}",
            args,
        ).fetchone()
        total = int(total_row["c"]) if total_row else 0

        rows = conn.execute(
            "SELECT id, filename, department, category, version, is_active, file_path, chunk_count, uploaded_at, uploaded_by "
            f"FROM documents {where_sql} "
            "ORDER BY uploaded_at DESC, id DESC "
            "LIMIT ? OFFSET ?",
            [*args, page_size, offset],
        ).fetchall()
        items = [dict(r) for r in rows]
        return {"items": items, "page": page, "page_size": page_size, "total": total}
    finally:
        conn.close()


def get_document(document_id: int) -> dict | None:
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT id, filename, department, category, version, is_active, file_path, chunk_count, uploaded_at, uploaded_by "
            "FROM documents WHERE id = ?",
            (int(document_id),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def list_document_versions(filename: str, department: str, category: str) -> list[dict]:
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT id, filename, department, category, version, is_active, file_path, chunk_count, uploaded_at, uploaded_by "
            "FROM documents WHERE filename = ? AND department = ? AND category = ? "
            "ORDER BY version DESC",
            (filename, department, category),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def upsert_feedback(message_id: int, session_id: str, rating: int, reason: str | None) -> dict:
    message_id = int(message_id)
    rating = int(rating)
    session_id = (session_id or "").strip()

    conn = get_db()
    try:
        msg = conn.execute(
            "SELECT id, session_id, role FROM messages WHERE id = ?",
            (message_id,),
        ).fetchone()
        if not msg:
            raise KeyError("message not found")
        if session_id and str(msg["session_id"]) != session_id:
            raise ValueError("session_id does not match message")
        if str(msg["role"]).lower() not in ("assistant", "bot"):
            raise ValueError("feedback can only be attached to assistant message")

        existing_rows = conn.execute(
            "SELECT id FROM feedback WHERE message_id = ? ORDER BY id DESC",
            (message_id,),
        ).fetchall()

        if existing_rows:
            keep_id = int(existing_rows[0]["id"])
            extra_ids = [int(r["id"]) for r in existing_rows[1:]]
            if extra_ids:
                conn.execute(
                    f"DELETE FROM feedback WHERE id IN ({','.join(['?'] * len(extra_ids))})",
                    extra_ids,
                )
            conn.execute(
                "UPDATE feedback SET rating = ?, reason = ?, created_at = datetime('now') WHERE id = ?",
                (rating, reason, keep_id),
            )
        else:
            conn.execute(
                "INSERT INTO feedback (message_id, session_id, rating, reason) VALUES (?, ?, ?, ?)",
                (message_id, str(msg["session_id"]), rating, reason),
            )

        row = conn.execute(
            "SELECT id, message_id, session_id, rating, reason, created_at FROM feedback WHERE message_id = ?",
            (message_id,),
        ).fetchone()
        conn.commit()
        return dict(row) if row else {}
    finally:
        conn.close()


def feedback_summary(session_id: str | None = None) -> dict:
    session_id = (session_id or "").strip()
    where = ""
    args: list = []
    if session_id:
        where = "WHERE session_id = ?"
        args.append(session_id)

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT "
            "COUNT(1) AS total, "
            "SUM(CASE WHEN rating = 1 THEN 1 ELSE 0 END) AS positive, "
            "SUM(CASE WHEN rating = -1 THEN 1 ELSE 0 END) AS negative "
            f"FROM feedback {where}",
            args,
        ).fetchone()

        reasons = conn.execute(
            "SELECT COALESCE(reason, '') AS reason, COUNT(1) AS c "
            f"FROM feedback {where} "
            "AND rating = -1 "
            "GROUP BY COALESCE(reason, '') "
            "ORDER BY c DESC",
            args,
        ).fetchall() if where else conn.execute(
            "SELECT COALESCE(reason, '') AS reason, COUNT(1) AS c "
            "FROM feedback WHERE rating = -1 "
            "GROUP BY COALESCE(reason, '') "
            "ORDER BY c DESC",
        ).fetchall()

        return {
            "total": int(row["total"] or 0) if row else 0,
            "positive": int(row["positive"] or 0) if row else 0,
            "negative": int(row["negative"] or 0) if row else 0,
            "reason_counts": [{"reason": r["reason"], "count": int(r["c"])} for r in reasons],
            "session_id": session_id or None,
        }
    finally:
        conn.close()
