import os
from openai import OpenAI
from rank_bm25 import BM25Okapi
from db import collection

client       = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
COMPANY_NAME = os.getenv("COMPANY_NAME", "công ty")
LLM_MODEL    = os.getenv("LLM_MODEL", "gpt-4o-mini")
TOP_K        = int(os.getenv("TOP_K", "6"))
RETRIEVE_N   = 15
HISTORY_MAX  = 8
HISTORY_CHARS_PER_MSG = 800


# ── BM25 Index ─────────────────────────────────────────────────────────────

class BM25Index:
    def __init__(self):
        self.ids:   list[str]  = []
        self.texts: list[str]  = []
        self.metas: list[dict] = []
        self.bm25: BM25Okapi | None = None
        self.build()

    def _tok(self, text: str) -> list[str]:
        return text.lower().split()

    def build(self):
        # Explicitly fetch ALL chunks — no implicit limit
        results = collection.get(
            limit=200_000,
            include=["documents", "metadatas"]
        )
        if not results["ids"]:
            self.bm25 = None
            self.ids, self.texts, self.metas = [], [], []
            print("[BM25] No documents in DB yet. Index cleared.")
            return

        self.ids   = results["ids"]
        self.texts = results["documents"]
        self.metas = results["metadatas"]
        self.bm25  = BM25Okapi([self._tok(t) for t in self.texts])
        print(f"[BM25] Rebuilt: {len(self.ids)} chunks from {len(self.departments())} departments")

    def search(self, query: str, n: int, department: str = None) -> list[tuple]:
        if not self.bm25 or not self.ids:
            return []
        scores = self.bm25.get_scores(self._tok(query))
        items  = list(zip(self.ids, self.metas, scores))
        # Filter by department if needed
        if department and department != "all":
            items = [(id_, m, s) for id_, m, s in items if m.get("department") == department]
        
        # Filter out 0-score items to prevent irrelevant 'first chunks' from dominating
        items = [it for it in items if it[2] > 0]
        
        items.sort(key=lambda x: x[2], reverse=True)
        return items[:n]

    def get_text(self, id_: str) -> str:
        try:
            return self.texts[self.ids.index(id_)]
        except ValueError:
            return ""

    def get_meta(self, id_: str) -> dict:
        try:
            return self.metas[self.ids.index(id_)]
        except ValueError:
            return {}

    def departments(self) -> list[str]:
        return sorted({m.get("department", "General") for m in self.metas})


_idx = BM25Index()


def rebuild_index():
    _idx.build()


def get_departments() -> list[str]:
    return _idx.departments()


# ── Core functions ─────────────────────────────────────────────────────────

def get_embedding(text: str) -> list[float]:
    return client.embeddings.create(
        model="text-embedding-3-small", input=text[:8000]
    ).data[0].embedding


def _trim(text: str, n: int) -> str:
    text = (text or "").strip()
    if len(text) <= n:
        return text
    return text[:n].rstrip() + "…"


def _history_for_llm(history: list[dict] | None) -> list[dict]:
    if not history:
        return []
    out: list[dict] = []
    for m in history[-HISTORY_MAX:]:
        role = (m.get("role") or "").strip().lower()
        if role not in ("user", "assistant"):
            continue
        out.append({"role": role, "content": _trim(str(m.get("content", "")), HISTORY_CHARS_PER_MSG)})
    return out


def rewrite_query(question: str, history: list[dict] | None = None) -> str:
    hist = _history_for_llm(history)
    convo = "\n".join(
        ("User: " if m["role"] == "user" else "Assistant: ") + m["content"]
        for m in hist
    ).strip()

    resp = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": (
            "Bạn là trợ lý tìm kiếm tài liệu nội bộ công ty.\n"
            "Viết lại câu hỏi sau thành câu tìm kiếm đầy đủ hơn, thêm từ khóa liên quan.\n"
            "Chỉ trả về câu tìm kiếm, không giải thích.\n\n"
            + (f"CONVERSATION CONTEXT:\n{convo}\n\n" if convo else "")
            + f"Câu hỏi: {question}\nCâu tìm kiếm:"
        )}],
        temperature=0, max_tokens=120
    )
    return resp.choices[0].message.content.strip()


def vector_search(query_text: str, n: int, department: str = None) -> list[str]:
    total = collection.count()
    if total == 0:
        return []
    
    kwargs = dict(
        query_embeddings=[get_embedding(query_text)],
        n_results=min(n, total),
        include=["metadatas"]
    )
    
    if department and department != "all":
        kwargs["where"] = {"department": department}
        # ChromaDB might error if n_results > number of matching documents
        # We'll try to get as many as possible
        try:
            res = collection.query(**kwargs)
            return res["ids"][0]
        except Exception as e:
            print(f"[RAG] Vector search retry for department {department}: {e}")
            # Fallback: get all IDs for this department and then use a safe n_results
            try:
                all_dept = collection.get(where={"department": department}, include=[])
                count = len(all_dept["ids"])
                if count == 0: return []
                kwargs["n_results"] = min(n, count)
                res = collection.query(**kwargs)
                return res["ids"][0]
            except Exception:
                return []
    
    res = collection.query(**kwargs)
    return res["ids"][0]


def rrf_merge(vec_ids: list[str], bm25_ids: list[str], k: int = 60) -> list[str]:
    scores: dict[str, float] = {}
    for rank, id_ in enumerate(vec_ids):
        scores[id_] = scores.get(id_, 0) + 1 / (k + rank + 1)
    for rank, id_ in enumerate(bm25_ids):
        scores[id_] = scores.get(id_, 0) + 1 / (k + rank + 1)
    return sorted(scores, key=lambda x: scores[x], reverse=True)


# ── Main query ─────────────────────────────────────────────────────────────

def query(question: str, department: str = None, history: list[dict] | None = None) -> dict:
    total = collection.count()
    print(f"[RAG] Total chunks in DB: {total} | BM25 index size: {len(_idx.ids)}")

    if total == 0:
        return {"answer": "Chưa có tài liệu nào. Vui lòng upload tài liệu trước.",
                "sources": [], "rewritten_query": question}

    rewritten  = rewrite_query(question, history=history)
    print(f"[RAG] Q: {question!r} → {rewritten!r}")

    vec_ids   = vector_search(rewritten, RETRIEVE_N, department)
    bm25_hits = _idx.search(rewritten, RETRIEVE_N, department)
    bm25_ids  = [h[0] for h in bm25_hits]

    print(f"[RAG] Vector hits: {len(vec_ids)} | BM25 hits: {len(bm25_ids)}")

    merged_ids = rrf_merge(vec_ids, bm25_ids)[:TOP_K]
    relevant   = []
    for id_ in merged_ids:
        t = _idx.get_text(id_)
        m = _idx.get_meta(id_)
        if t and m:
            relevant.append((t, m))

    if not relevant:
        return {"answer": "Không tìm thấy thông tin liên quan trong tài liệu nội bộ.",
                "sources": [], "rewritten_query": rewritten}

    # Log source diversity
    source_counts = {}
    for _, m in relevant:
        fname = m.get("filename", "?")
        source_counts[fname] = source_counts.get(fname, 0) + 1
    print(f"[RAG] Retrieved {len(relevant)} chunks from {len(source_counts)} files: {source_counts}")

    context = "\n\n---\n\n".join(
        f"[{m.get('department','?')} | {m.get('filename','?')} | Trang {m.get('page','?')}]\n{d}"
        for d, m in relevant
    )

    hist_msgs = _history_for_llm(history)

    resp = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": (
                f"Bạn là trợ lý nội bộ chuyên tra cứu tài liệu của {COMPANY_NAME}.\n\n"
                "QUY TẮC BẮT BUỘC:\n"
                "1. CHỈ trả lời dựa trên phần 'TÀI LIỆU' được cung cấp bên dưới.\n"
                "2. Nếu thông tin KHÔNG có trong tài liệu, hãy trả lời: 'Tôi không tìm thấy thông tin này trong tài liệu nội bộ.' Tuyệt đối không tự bịa ra câu trả lời hoặc dùng kiến thức bên ngoài.\n"
                "3. Luôn trích dẫn tên tài liệu ở cuối câu trả lời nếu có thông tin (ví dụ: [Nguồn: file_name.pdf]).\n"
                "4. Trả lời bằng tiếng Việt, ngắn gọn, trung thực và chuyên nghiệp."
            )},
            *hist_msgs,
            {"role": "user", "content": f"TÀI LIỆU:\n{context}\n\nCÂU HỎI: {question}"}
        ],
        temperature=0.0, max_tokens=1000
    )

    return {
        "answer":          resp.choices[0].message.content,
        "sources":         list({m.get("filename","?") for _, m in relevant}),
        "rewritten_query": rewritten,
    }
