import os

from openai import OpenAI
from rank_bm25 import BM25Okapi

from db import collection
from language import detect_language, build_system_prompt, build_user_prompt


client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
COMPANY_NAME = os.getenv("COMPANY_NAME", "cong ty")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
TOP_K = int(os.getenv("TOP_K", "6"))
RETRIEVE_N = 15
HISTORY_MAX = 8
HISTORY_CHARS_PER_MSG = 800

NO_DOCS_MESSAGES = {
    "vi": "Chua co tai lieu nao. Vui long upload tai lieu truoc.",
    "ko": "아직 업로드된 문서가 없습니다. 먼저 문서를 업로드해 주세요.",
    "en": "There are no documents yet. Please upload documents first.",
}

NO_MATCH_MESSAGES = {
    "vi": "Toi chua co thong tin ve van de nay.",
    "ko": "해당 사항에 대한 정보가 없습니다.",
    "en": "I don't have information on this topic.",
}


class BM25Index:
    def __init__(self):
        self.ids: list[str] = []
        self.texts: list[str] = []
        self.metas: list[dict] = []
        self.bm25: BM25Okapi | None = None
        self.build()

    def _tok(self, text: str) -> list[str]:
        return text.lower().split()

    def build(self):
        results = collection.get(limit=200_000, include=["documents", "metadatas"])
        if not results["ids"]:
            self.bm25 = None
            self.ids, self.texts, self.metas = [], [], []
            print("[BM25] No documents in DB yet. Index cleared.")
            return

        self.ids = results["ids"]
        self.texts = results["documents"]
        self.metas = results["metadatas"]
        self.bm25 = BM25Okapi([self._tok(text) for text in self.texts])
        print(f"[BM25] Rebuilt: {len(self.ids)} chunks from {len(self.departments())} departments")

    def search(self, query: str, n: int, department: str = None) -> list[tuple]:
        if not self.bm25 or not self.ids:
            return []

        scores = self.bm25.get_scores(self._tok(query))
        items = list(zip(self.ids, self.metas, scores))
        if department and department != "all":
            items = [(item_id, meta, score) for item_id, meta, score in items if meta.get("department") == department]

        items = [item for item in items if item[2] > 0]
        items.sort(key=lambda item: item[2], reverse=True)
        return items[:n]

    def get_text(self, item_id: str) -> str:
        try:
            return self.texts[self.ids.index(item_id)]
        except ValueError:
            return ""

    def get_meta(self, item_id: str) -> dict:
        try:
            return self.metas[self.ids.index(item_id)]
        except ValueError:
            return {}

    def departments(self) -> list[str]:
        return sorted({meta.get("department", "General") for meta in self.metas})


_idx = BM25Index()


def rebuild_index():
    _idx.build()


def get_departments() -> list[str]:
    return _idx.departments()


def get_embedding(text: str) -> list[float]:
    return client.embeddings.create(model="text-embedding-3-small", input=text[:8000]).data[0].embedding


def _trim(text: str, n: int) -> str:
    text = (text or "").strip()
    if len(text) <= n:
        return text
    return text[:n].rstrip() + "..."


def _history_for_llm(history: list[dict] | None) -> list[dict]:
    if not history:
        return []

    out: list[dict] = []
    for message in history[-HISTORY_MAX:]:
        role = (message.get("role") or "").strip().lower()
        if role not in ("user", "assistant"):
            continue
        out.append(
            {
                "role": role,
                "content": _trim(str(message.get("content", "")), HISTORY_CHARS_PER_MSG),
            }
        )
    return out


def rewrite_query(question: str) -> str:
    """
    Rewrite query in Vietnamese for accurate document search.
    Documents are in Vietnamese regardless of what language the user asked in.
    """
    resp = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": (
            "You are a search query optimizer for a Vietnamese internal document system.\n"
            "Rewrite the following question as a Vietnamese search query with relevant keywords.\n"
            "Output ONLY the rewritten query in Vietnamese, nothing else.\n\n"
            f"Question: {question}\n"
            "Vietnamese search query:"
        )}],
        temperature=0, max_tokens=120
    )
    return resp.choices[0].message.content.strip()


def vector_search(query_text: str, n: int, department: str = None) -> list[str]:
    total = collection.count()
    if total == 0:
        return []

    kwargs = {
        "query_embeddings": [get_embedding(query_text)],
        "n_results": min(n, total),
        "include": ["metadatas"],
    }

    if department and department != "all":
        kwargs["where"] = {"department": department}
        try:
            result = collection.query(**kwargs)
            return result["ids"][0]
        except Exception as exc:
            print(f"[RAG] Vector search retry for department {department}: {exc}")
            try:
                all_dept = collection.get(where={"department": department}, include=[])
                count = len(all_dept["ids"])
                if count == 0:
                    return []
                kwargs["n_results"] = min(n, count)
                result = collection.query(**kwargs)
                return result["ids"][0]
            except Exception:
                return []

    result = collection.query(**kwargs)
    return result["ids"][0]


def rrf_merge(vec_ids: list[str], bm25_ids: list[str], k: int = 60) -> list[str]:
    scores: dict[str, float] = {}
    for rank, item_id in enumerate(vec_ids):
        scores[item_id] = scores.get(item_id, 0) + 1 / (k + rank + 1)
    for rank, item_id in enumerate(bm25_ids):
        scores[item_id] = scores.get(item_id, 0) + 1 / (k + rank + 1)
    return sorted(scores, key=lambda item_id: scores[item_id], reverse=True)


def query(question: str, session_id: str = None, department: str = None) -> dict:
    # Detect language
    detected_lang = detect_language(question)
    print(f"[RAG] Detected language: {detected_lang} | Q: {question!r}")

    total = collection.count()
    print(f"[RAG] Total chunks in DB: {total} | BM25 index size: {len(_idx.ids)}")

    if total == 0:
        return {
            "answer": NO_DOCS_MESSAGES.get(detected_lang, NO_DOCS_MESSAGES["vi"]),
            "sources": [],
            "rewritten_query": question,
            "detected_lang": detected_lang,
        }

    rewritten = rewrite_query(question)
    print(f"[RAG] Q: {question!r} -> {rewritten!r}")

    vec_ids = vector_search(rewritten, RETRIEVE_N, department)
    bm25_hits = _idx.search(rewritten, RETRIEVE_N, department)
    bm25_ids = [hit[0] for hit in bm25_hits]

    print(f"[RAG] Vector hits: {len(vec_ids)} | BM25 hits: {len(bm25_ids)}")

    merged_ids = rrf_merge(vec_ids, bm25_ids)[:TOP_K]
    relevant = []
    for item_id in merged_ids:
        text = _idx.get_text(item_id)
        meta = _idx.get_meta(item_id)
        if text and meta:
            relevant.append((text, meta))

    if not relevant:
        not_found = {
            "vi": "Tôi không tìm thấy thông tin liên quan trong tài liệu nội bộ. Vui lòng liên hệ bộ phận phụ trách.",
            "ko": "내부 문서에서 관련 정보를 찾을 수 없습니다. 담당 부서에 문의해 주세요.",
            "en": "I could not find relevant information in the internal documents. Please contact the responsible department.",
        }
        return {
            "answer": not_found.get(detected_lang, not_found["vi"]),
            "sources": [],
            "rewritten_query": rewritten,
            "detected_lang": detected_lang,
        }

    source_counts = {}
    for _, meta in relevant:
        filename = meta.get("filename", "?")
        source_counts[filename] = source_counts.get(filename, 0) + 1
    print(f"[RAG] Retrieved {len(relevant)} chunks from {len(source_counts)} files: {source_counts}")

    context = "\n\n---\n\n".join(
        f"[{meta.get('department', '?')} | {meta.get('filename', '?')} | Trang {meta.get('page', '?')}]\n{text}"
        for text, meta in relevant
    )

    system_prompt = build_system_prompt(detected_lang, COMPANY_NAME)
    user_prompt   = build_user_prompt(question, context, detected_lang)

    resp = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt}
        ],
        temperature=0.1,
        max_tokens=1000
    )

    return {
        "answer": resp.choices[0].message.content,
        "sources": list({meta.get("filename", "?") for _, meta in relevant}),
        "rewritten_query": rewritten,
        "detected_lang": detected_lang,
    }
