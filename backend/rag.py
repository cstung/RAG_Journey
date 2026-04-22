"""
RAG Engine with:
  1. Query Rewriting   — expand short queries before searching
  2. Hybrid Search     — BM25 (keyword) + Vector (semantic) merged via RRF
  3. Metadata Filter   — filter by department before searching
"""
import os
import chromadb
from openai import OpenAI
from rank_bm25 import BM25Okapi

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
chroma_client = chromadb.PersistentClient(path="/data/chroma")
collection = chroma_client.get_or_create_collection(
    name="internal_docs",
    metadata={"hnsw:space": "cosine"}
)

COMPANY_NAME = os.getenv("COMPANY_NAME", "công ty")
LLM_MODEL    = os.getenv("LLM_MODEL", "gpt-4o-mini")
TOP_K        = int(os.getenv("TOP_K", "6"))
RETRIEVE_N   = 15   # candidates per method before RRF merge


# ── BM25 index ─────────────────────────────────────────────────────────────

class BM25Index:
    def __init__(self):
        self.ids:   list[str]  = []
        self.texts: list[str]  = []
        self.metas: list[dict] = []
        self.bm25: BM25Okapi | None = None
        self.build()

    def _tokenize(self, text: str) -> list[str]:
        # Simple whitespace tokenizer — works well for Vietnamese
        return text.lower().split()

    def build(self):
        """Load all chunks from ChromaDB and rebuild BM25 index."""
        results = collection.get(include=["documents", "metadatas"])
        if not results["ids"]:
            self.bm25 = None
            return
        self.ids   = results["ids"]
        self.texts = results["documents"]
        self.metas = results["metadatas"]
        tokenized  = [self._tokenize(t) for t in self.texts]
        self.bm25  = BM25Okapi(tokenized)
        print(f"[BM25] Index rebuilt: {len(self.ids)} chunks")

    def search(self, query: str, n: int, department: str = None) -> list[tuple[str, dict, float]]:
        if not self.bm25 or not self.ids:
            return []
        scores = self.bm25.get_scores(self._tokenize(query))
        items  = list(zip(self.ids, self.metas, scores))
        if department and department != "all":
            items = [(id_, m, s) for id_, m, s in items if m.get("department") == department]
        items.sort(key=lambda x: x[2], reverse=True)
        return items[:n]

    @property
    def id_to_text(self) -> dict:
        return dict(zip(self.ids, self.texts))

    @property
    def id_to_meta(self) -> dict:
        return dict(zip(self.ids, self.metas))

    def departments(self) -> list[str]:
        return sorted({m.get("department", "General") for m in self.metas})


# Global index — rebuilt after each ingestion
_bm25_index = BM25Index()


def rebuild_index():
    _bm25_index.build()


# ── Helpers ────────────────────────────────────────────────────────────────

def get_embedding(text: str) -> list[float]:
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=text[:8000]
    )
    return response.data[0].embedding


def rewrite_query(question: str) -> str:
    """Use LLM to expand the user's query with related keywords."""
    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{
            "role": "user",
            "content": (
                "Bạn là trợ lý tìm kiếm tài liệu nội bộ công ty.\n"
                "Hãy viết lại câu hỏi sau thành một câu tìm kiếm đầy đủ hơn, "
                "bổ sung các từ khóa liên quan, thuật ngữ chuyên ngành nếu có.\n"
                "Chỉ trả về câu tìm kiếm, không giải thích, không thêm ký tự đặc biệt.\n\n"
                f"Câu hỏi gốc: {question}\n"
                "Câu tìm kiếm mở rộng:"
            )
        }],
        temperature=0,
        max_tokens=120
    )
    return response.choices[0].message.content.strip()


def vector_search(query_text: str, n: int, department: str = None) -> list[tuple[str, dict]]:
    """Search ChromaDB by embedding similarity, optionally filtered by department."""
    total = collection.count()
    if total == 0:
        return []

    kwargs = dict(
        query_embeddings=[get_embedding(query_text)],
        n_results=min(n, total),
        include=["metadatas", "distances"]
    )
    if department and department != "all":
        kwargs["where"] = {"department": department}

    try:
        results = collection.query(**kwargs)
    except Exception:
        # Fallback without filter if no matching docs
        results = collection.query(
            query_embeddings=[get_embedding(query_text)],
            n_results=min(n, total),
            include=["metadatas", "distances"]
        )

    return list(zip(results["ids"][0], results["metadatas"][0]))


def rrf_merge(vec_ids: list[str], bm25_ids: list[str], k: int = 60) -> list[str]:
    """Reciprocal Rank Fusion — merge two ranked lists into one."""
    scores: dict[str, float] = {}
    for rank, id_ in enumerate(vec_ids):
        scores[id_] = scores.get(id_, 0) + 1 / (k + rank + 1)
    for rank, id_ in enumerate(bm25_ids):
        scores[id_] = scores.get(id_, 0) + 1 / (k + rank + 1)
    return sorted(scores, key=lambda x: scores[x], reverse=True)


# ── Main query function ─────────────────────────────────────────────────────

def query(question: str, department: str = None) -> dict:
    if collection.count() == 0:
        return {
            "answer": "Chưa có tài liệu nào được nạp vào hệ thống. Vui lòng upload tài liệu trước.",
            "sources": [],
            "rewritten_query": question
        }

    # 1. Rewrite query
    rewritten = rewrite_query(question)
    print(f"[RAG] Original : {question}")
    print(f"[RAG] Rewritten: {rewritten}")

    # 2. Hybrid search
    vec_results  = vector_search(rewritten, RETRIEVE_N, department)
    bm25_results = _bm25_index.search(rewritten, RETRIEVE_N, department)

    vec_ids  = [r[0] for r in vec_results]
    bm25_ids = [r[0] for r in bm25_results]

    # 3. RRF merge → top-K
    merged_ids = rrf_merge(vec_ids, bm25_ids)[:TOP_K]

    if not merged_ids:
        return {
            "answer": "Không tìm thấy thông tin liên quan trong tài liệu nội bộ. "
                      "Vui lòng liên hệ bộ phận phụ trách để được hỗ trợ.",
            "sources": [],
            "rewritten_query": rewritten
        }

    # 4. Fetch full text for merged IDs
    id2text = _bm25_index.id_to_text
    id2meta = _bm25_index.id_to_meta
    relevant = [(id2text[id_], id2meta[id_]) for id_ in merged_ids if id_ in id2text]

    if not relevant:
        return {
            "answer": "Không tìm thấy thông tin liên quan.",
            "sources": [],
            "rewritten_query": rewritten
        }

    # 5. Build context
    context = "\n\n---\n\n".join(
        f"[{m.get('department','?')} | {m.get('filename','?')} | Trang {m.get('page','?')}]\n{d}"
        for d, m in relevant
    )

    # 6. Generate answer
    system_prompt = (
        f"Bạn là trợ lý nội bộ của {COMPANY_NAME}. "
        "Hãy trả lời câu hỏi của nhân viên dựa trên tài liệu quy định nội bộ được cung cấp.\n\n"
        "Nguyên tắc:\n"
        "- Chỉ trả lời dựa trên thông tin có trong tài liệu được cung cấp\n"
        "- Nếu không tìm thấy thông tin, hãy nói rõ và đề nghị liên hệ bộ phận phụ trách\n"
        "- Trả lời ngắn gọn, rõ ràng bằng tiếng Việt\n"
        "- Dùng danh sách bullet nếu có nhiều điểm cần liệt kê\n"
        "- Không bịa đặt hoặc suy đoán ngoài phạm vi tài liệu"
    )
    user_prompt = f"TÀI LIỆU THAM KHẢO:\n{context}\n\nCÂU HỎI: {question}"

    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt}
        ],
        temperature=0.1,
        max_tokens=1000
    )

    answer  = response.choices[0].message.content
    sources = list({m.get("filename", "?") for _, m in relevant})

    return {
        "answer": answer,
        "sources": sources,
        "rewritten_query": rewritten
    }


def get_departments() -> list[str]:
    return _bm25_index.departments()
