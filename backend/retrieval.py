# backend/retrieval.py
"""
Hybrid retrieval: dense (Qdrant) + BM25 (rank_bm25), fused via Reciprocal Rank Fusion,
then re-ranked by a local cross-encoder (CPU).
"""
from rank_bm25 import BM25Okapi
from vector_store import get_client, search
from qdrant_client.models import Filter, FieldCondition, MatchValue
from reranker import rerank
import os

TOP_K_DENSE  = int(os.getenv("TOP_K_DENSE", "20"))   # larger candidate pool on server
TOP_K_BM25   = int(os.getenv("TOP_K_BM25",  "20"))
TOP_K_RERANK = int(os.getenv("TOP_K_RERANK", "10"))  # pass this many to cross-encoder
TOP_K_FINAL  = int(os.getenv("TOP_K",        "5"))    # final results after reranking


def _build_payload_filter(filters: dict | None) -> Filter | None:
    """
    filters = {"legal_type": "Nghị định", "dataset": "..."}
    """
    if not filters:
        return None
    conditions = [
        FieldCondition(key=k, match=MatchValue(value=v))
        for k, v in filters.items() if v
    ]
    return Filter(must=conditions) if conditions else None


def _rrf(ranked_lists: list[list], k: int = 60) -> list:
    """Reciprocal Rank Fusion over multiple ranked lists of (id, payload) tuples."""
    scores: dict[str, float] = {}
    payloads: dict[str, dict] = {}

    for ranked in ranked_lists:
        for rank, (doc_id, payload) in enumerate(ranked):
            scores[doc_id]   = scores.get(doc_id, 0) + 1.0 / (k + rank + 1)
            payloads[doc_id] = payload

    sorted_ids = sorted(scores, key=scores.__getitem__, reverse=True)
    return [(doc_id, payloads[doc_id], scores[doc_id]) for doc_id in sorted_ids]


def hybrid_search(
    query: str,
    query_embedding: list[float],
    collection: str,
    filters: dict | None = None,
    top_k: int = TOP_K_FINAL,
) -> list[dict]:
    """
    Returns list of:
    {
        "text": str,
        "score": float,
        "metadata": { document_number, title, url, legal_type, ... }
    }
    """
    client = get_client()
    payload_filter = _build_payload_filter(filters)

    # 1. Dense search
    dense_hits = search(client, collection, query_embedding, TOP_K_DENSE, payload_filter)
    dense_ranked = [(h.id, h.payload) for h in dense_hits]

    # 2. BM25 search over the same dense candidates (lightweight in-memory)
    if dense_hits:
        corpus      = [h.payload.get("text", "") for h in dense_hits]
        tokenized   = [doc.split() for doc in corpus]
        bm25        = BM25Okapi(tokenized)
        query_tokens = query.split()
        bm25_scores  = bm25.get_scores(query_tokens)
        bm25_ranked  = [
            (dense_hits[i].id, dense_hits[i].payload)
            for i in sorted(range(len(bm25_scores)), key=lambda x: -bm25_scores[x])
        ]
    else:
        bm25_ranked = []

    # 3. RRF fusion
    fused = _rrf([dense_ranked, bm25_ranked])[:TOP_K_RERANK]

    candidates = [
        {
            "text":     payload.get("text", ""),
            "score":    score,
            "metadata": {k: v for k, v in payload.items() if k != "text"},
        }
        for _, payload, score in fused
    ]

    # 4. Cross-encoder reranking (CPU — ~100–200 ms on server hardware)
    reranked = rerank(query, candidates, top_k=top_k)
    return reranked
