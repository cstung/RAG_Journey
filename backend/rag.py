import os
from openai import OpenAI
from rank_bm25 import BM25Okapi
from db import collection

client       = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
COMPANY_NAME = os.getenv("COMPANY_NAME", "công ty")
LLM_MODEL    = os.getenv("LLM_MODEL", "gpt-4o-mini")
TOP_K        = int(os.getenv("TOP_K", "6"))
RETRIEVE_N   = 15


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
            print("[BM25] No documents in DB yet.")
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
        if department and department != "all":
            items = [(id_, m, s) for id_, m, s in items if m.get("department") == department]
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


def rewrite_query(question: str) -> str:
    resp = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": (
            "Bạn là trợ lý tìm kiếm tài liệu nội bộ công ty.\n"
            "Viết lại câu hỏi sau thành câu tìm kiếm đầy đủ hơn, thêm từ khóa liên quan.\n"
            "Chỉ trả về câu tìm kiếm, không giải thích.\n\n"
            f"Câu hỏi: {question}\nCâu tìm kiếm:"
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
        # Count matching docs first to avoid n_results > matched
        kwargs["where"] = {"department": department}
        try:
            res = collection.query(**kwargs)
            return res["ids"][0]
        except Exception:
            # Fewer matching docs than n_results — retry with smaller n
            kwargs["n_results"] = 1
            try:
                res = collection.query(**kwargs)
                return res["ids"][0]
            except Exception:
                pass
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

def query(question: str, department: str = None) -> dict:
    total = collection.count()
    print(f"[RAG] Total chunks in DB: {total} | BM25 index size: {len(_idx.ids)}")

    if total == 0:
        return {"answer": "Chưa có tài liệu nào. Vui lòng upload tài liệu trước.",
                "sources": [], "rewritten_query": question}

    rewritten  = rewrite_query(question)
    print(f"[RAG] Q: {question!r} → {rewritten!r}")

    vec_ids   = vector_search(rewritten, RETRIEVE_N, department)
    bm25_hits = _idx.search(rewritten, RETRIEVE_N, department)
    bm25_ids  = [h[0] for h in bm25_hits]

    print(f"[RAG] Vector hits: {len(vec_ids)} | BM25 hits: {len(bm25_ids)}")

    merged_ids = rrf_merge(vec_ids, bm25_ids)[:TOP_K]
    relevant   = [(t, m) for id_ in merged_ids
                  if (t := _idx.get_text(id_)) and (m := _idx.get_meta(id_))]

    if not relevant:
        return {"answer": "Không tìm thấy thông tin liên quan trong tài liệu nội bộ.",
                "sources": [], "rewritten_query": rewritten}

    context = "\n\n---\n\n".join(
        f"[{m.get('department','?')} | {m.get('filename','?')} | Trang {m.get('page','?')}]\n{d}"
        for d, m in relevant
    )

    resp = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": (
                f"Bạn là trợ lý nội bộ của {COMPANY_NAME}. "
                "Trả lời dựa trên tài liệu được cung cấp, ngắn gọn, rõ ràng bằng tiếng Việt. "
                "Không bịa đặt ngoài phạm vi tài liệu."
            )},
            {"role": "user", "content": f"TÀI LIỆU:\n{context}\n\nCÂU HỎI: {question}"}
        ],
        temperature=0.1, max_tokens=1000
    )

    return {
        "answer":          resp.choices[0].message.content,
        "sources":         list({m.get("filename","?") for _, m in relevant}),
        "rewritten_query": rewritten,
    }
