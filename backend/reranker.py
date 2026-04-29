# backend/reranker.py
"""
Local cross-encoder reranker (CPU).
Loads once at startup, reuses across requests.
"""
from sentence_transformers import CrossEncoder
import os

_MODEL_NAME = os.getenv("RERANKER_MODEL", "cross-encoder/ms-marco-MiniLM-L-6-v2")
_reranker: CrossEncoder | None = None

def get_reranker() -> CrossEncoder:
    global _reranker
    if _reranker is None:
        print(f"[reranker] Loading {_MODEL_NAME} (CPU)...")
        _reranker = CrossEncoder(_MODEL_NAME, max_length=512)
    return _reranker

def rerank(query: str, candidates: list[dict], top_k: int) -> list[dict]:
    """
    candidates = [{"text": "...", "score": float, "metadata": {...}}, ...]
    Returns top_k candidates sorted by cross-encoder score.
    """
    if not candidates:
        return []
    ranker  = get_reranker()
    pairs   = [(query, c["text"]) for c in candidates]
    scores  = ranker.predict(pairs)
    ranked  = sorted(zip(scores, candidates), key=lambda x: -x[0])
    return [c for _, c in ranked[:top_k]]
