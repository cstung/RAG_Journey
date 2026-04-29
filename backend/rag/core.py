import os

from openai import OpenAI
from rank_bm25 import BM25Okapi

from database import get_recent_messages
from vector_store import get_client, search, COLLECTION_PDF, COLLECTION_LEGAL, count_points
from qdrant_client.models import Filter, FieldCondition, MatchValue
from language import detect_language
from .chunk_sanitiser import sanitise_chunk
from .citation_checker import extract_and_check_citation
from .confidence_parser import extract_confidence
from .output_guard import validate_output
from .prompt_builder import build_prompt
from .retrieval_gate import (
    NO_RESULTS_SENTINEL, 
    SIMILARITY_THRESHOLD, 
    BM25_MIN_SCORE, 
    RETRIEVE_N, 
    TOP_K,
    LLM_TEMPERATURE,
    filter_chunks, 
    is_relevant
)


client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
COMPANY_NAME = os.getenv("COMPANY_NAME", "cong ty")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")


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
        client = get_client()
        self.ids = []
        self.texts = []
        self.metas = []
        for coll in [COLLECTION_PDF, COLLECTION_LEGAL]:
            try:
                records, _ = client.scroll(collection_name=coll, limit=100000, with_payload=True, with_vectors=False)
                for r in records:
                    self.ids.append(str(r.id))
                    self.texts.append(r.payload.get("text", ""))
                    self.metas.append({k:v for k,v in r.payload.items() if k != "text"})
            except Exception:
                pass
        
        if not self.ids:
            self.bm25 = None
            return
            
        self.bm25 = BM25Okapi([self._tok(text) for text in self.texts])
        print(f"[BM25] Rebuilt: {len(self.ids)} chunks")

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


def rewrite_query(question: str, history: list[dict] | None = None) -> str:
    """
    Rewrite query in Vietnamese for accurate document search.
    Documents are in Vietnamese regardless of what language the user asked in.
    """
    # Only use the last 3 USER messages for entity resolution.
    # This prevents 'topic drift' while still allowing pronoun resolution.
    user_messages = [m for m in (history or []) if m.get("role") == "user"][-3:]
    
    history_lines: list[str] = []
    for message in user_messages:
        content = str(message.get("content", "")).strip()
        if content:
            history_lines.append(f"USER: {content}")

    history_block = ""
    if history_lines:
        history_block = "Recent context (for pronoun resolution only):\n" + "\n".join(history_lines) + "\n\n"

    resp = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{
            "role": "user",
            "content": (
                "You are a search query optimizer for a Vietnamese internal document system.\n"
                "Your goal is to rewrite the current question into a keyword-rich Vietnamese search query.\n"
                "Use the provided context ONLY to resolve pronouns (e.g., 'him', 'it', 'that law') or omitted subjects.\n"
                "If the question is already clear, output it exactly as is.\n\n"
                f"{history_block}"
                f"Current Question: {question}\n"
                "Vietnamese search query:"
            ),
        }],
        temperature=0,
        max_tokens=120,
    )
    return resp.choices[0].message.content.strip()


def detect_domain(question: str) -> str | None:
    """Classifies the user question into a specific domain for scoped retrieval."""
    q = question.lower()
    # Keyword based fast-path
    if any(k in q for k in ["lao động", "nghỉ việc", "lương", "hợp đồng", "labour", "người lao động"]):
        return "lao_dong"
    if any(k in q for k in ["giao thông", "đèn đỏ", "xe máy", "ô tô", "nồng độ cồn", "traffic"]):
        return "giao_thong"
    if any(k in q for k in ["doanh nghiệp", "vốn", "cổ đông", "điều lệ", "enterprise"]):
        return "doanh_nghiep"
    if any(k in q for k in [
        "luật", "nghị định", "thông tư", "quyết định", "pháp luật",
        "quy định", "điều khoản", "bộ luật", "hiến pháp", "pháp lệnh",
        "sắc lệnh", "chỉ thị", "công văn", "nghị quyết",
        "legal", "law", "decree", "regulation",
        "văn bản pháp luật", "điều luật", "khoản", "chương",
    ]):
        return "legal"
    
    # Optional: LLM based classifier if keywords miss
    return None


def vector_search(query_text: str, n: int, department: str = None, domain: str = None) -> tuple[list[str], list[str], list[float]]:
    client = get_client()
    target_collection = COLLECTION_LEGAL if domain == "legal" else COLLECTION_PDF
    
    must_conditions = []
    if department and department != "all":
        must_conditions.append(FieldCondition(key="department", match=MatchValue(value=department)))
    if domain:
        must_conditions.append(FieldCondition(key="domain", match=MatchValue(value=domain)))
        
    payload_filter = Filter(must=must_conditions) if must_conditions else None
    
    try:
        results = search(client, target_collection, get_embedding(query_text), top_k=n, payload_filter=payload_filter)
        ids = [str(r.id) for r in results]
        docs = [r.payload.get("text", "") for r in results]
        sims = [r.score for r in results]
        return ids, docs, sims
    except Exception as exc:
        print(f"[RAG] Vector search error: {exc}")
        return [], [], []

    # Build the 'where' filter
    filters = {}
    if department and department != "all":
        filters["department"] = department
    if domain:
        filters["domain"] = domain
    
    kwargs = {
        "query_embeddings": [get_embedding(query_text)],
        "n_results": min(n, total),
        "include": ["documents", "metadatas", "distances"],
    }
    
    if len(filters) > 1:
        kwargs["where"] = {"$and": [{k: v} for k, v in filters.items()]}
    elif len(filters) == 1:
        kwargs["where"] = filters

    try:
        result = collection.query(**kwargs)
        ids = result.get("ids", [[]])[0]
        docs = result.get("documents", [[]])[0]
        dists = result.get("distances", [[]])[0]
        sims = [_distance_to_similarity(d) for d in dists]
        return ids, docs, sims
    except Exception as exc:
        print(f"[RAG] Vector search error with filters {filters}: {exc}")
        # Fallback: try without domain filter if it failed
        if domain:
            return vector_search(query_text, n, department, domain=None)
        return [], [], []


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
    
    history = get_recent_messages(session_id) if session_id else []
    total = count_points(get_client(), COLLECTION_PDF) + count_points(get_client(), COLLECTION_LEGAL)
    print(f"[RAG] Total chunks in DB: {total} | BM25 index size: {len(_idx.ids)}")

    if total == 0:
        return {
            "answer": NO_DOCS_MESSAGES.get(detected_lang, NO_DOCS_MESSAGES["vi"]),
            "sources": [],
            "rewritten_query": question,
            "detected_lang": detected_lang,
        }

    rewritten = rewrite_query(question, history=history)
    print(f"[RAG] Q: {question!r} -> {rewritten!r}")

    # Detect domain from query or rewritten query
    detected_domain = detect_domain(rewritten)
    if detected_domain:
        print(f"[RAG] Explicit domain scoping triggered: {detected_domain}")

    vec_ids, vec_docs, vec_sims = vector_search(rewritten, RETRIEVE_N, department, domain=detected_domain)
    bm25_hits = _idx.search(rewritten, RETRIEVE_N, department)
    bm25_ids = [hit[0] for hit in bm25_hits]

    print(f"[RAG] Vector hits: {len(vec_ids)} | BM25 hits: {len(bm25_ids)}")
    if vec_sims:
        print(f"[RAG] Max Vector similarity: {max(vec_sims):.4f} | Min: {min(vec_sims):.4f}")

    # Step 1: Filter vector hits by threshold
    gated_vec_ids = [vid for vid, sim in zip(vec_ids, vec_sims) if sim >= SIMILARITY_THRESHOLD]

    # Step 2: Filter BM25 hits by their own threshold (don't force them through vector gate)
    # hit[2] is the BM25 score
    gated_bm25_ids = [hit[0] for hit in bm25_hits if hit[2] >= BM25_MIN_SCORE]
    
    if not gated_vec_ids and not gated_bm25_ids:
        context = NO_RESULTS_SENTINEL
        available_sources = []
        relevant = []
    else:
        # Merge results (RRF handles ranking across sources)
        merged_ids = rrf_merge(gated_vec_ids, gated_bm25_ids)[:TOP_K]
        relevant = []
        for item_id in merged_ids:
            text = _idx.get_text(item_id)
            meta = _idx.get_meta(item_id)
            if text and meta:
                relevant.append((text, meta))

        if not relevant:
            context = NO_RESULTS_SENTINEL
            available_sources = []
        else:
            source_counts: dict[str, int] = {}
            for _, meta in relevant:
                filename = meta.get("filename", "?")
                source_counts[filename] = source_counts.get(filename, 0) + 1
            print(f"[RAG] Retrieved {len(relevant)} chunks from {len(source_counts)} files: {source_counts}")

            available_sources = list({meta.get("filename", "?") for _, meta in relevant})
            context = "\n\n---\n\n".join(
                f"[{meta.get('department', '?')} | {meta.get('filename', '?')} | Trang {meta.get('page', '?')}]\n{sanitise_chunk(text)}"
                for text, meta in relevant
            )

    resp = client.chat.completions.create(
        model=LLM_MODEL,
        messages=build_prompt(context=context, question=question, history=history),
        temperature=LLM_TEMPERATURE, 
        max_tokens=1000,
    )

    raw_answer = resp.choices[0].message.content
    final_answer, was_flagged = validate_output(raw_answer)
    if was_flagged:
        print(f"[RAG] Output flagged by output_guard. Raw answer: {_trim(str(raw_answer), 500)!r}")
        clean_answer = final_answer
        confidence = "THẤP"
        citation_valid = False
        sources = []
    else:
        clean_answer, citation_valid = extract_and_check_citation(final_answer, available_sources)
        clean_answer, confidence = extract_confidence(clean_answer)
        if not citation_valid:
            confidence = "THẤP"
        sources = available_sources

    return {
        "answer": clean_answer,
        "sources": sources,
        "confidence": confidence,
        "citation_valid": citation_valid,
        "rewritten_query": rewritten,
        "detected_lang": detected_lang,
    }
