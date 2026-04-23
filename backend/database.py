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
            "UPDATE sessions SET ended_at = datetime('now') WHERE id = ?",
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
