import os

# Tune these thresholds for your embedding model.
SIMILARITY_THRESHOLD = float(os.getenv("SIMILARITY_THRESHOLD", "0.35"))
BM25_MIN_SCORE = float(os.getenv("BM25_MIN_SCORE", "1.0"))
RETRIEVE_N = int(os.getenv("RETRIEVE_N", "15"))
TOP_K = int(os.getenv("TOP_K", "6"))
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.3"))

NO_RESULTS_SENTINEL = "[KHÔNG TÌM THẤY TÀI LIỆU LIÊN QUAN]"


def filter_chunks(
    chunks: list[str],
    scores: list[float],
    threshold: float = SIMILARITY_THRESHOLD,
) -> list[str]:
    """Filters chunks by similarity score. Returns sentinel if none pass."""
    filtered = [chunk for chunk, score in zip(chunks, scores) if score >= threshold]
    return filtered if filtered else [NO_RESULTS_SENTINEL]


def is_relevant(vec_score: float = 0.0, bm25_score: float = 0.0) -> bool:
    """Checks if a chunk is relevant via either vector or keyword score."""
    return vec_score >= SIMILARITY_THRESHOLD or bm25_score >= BM25_MIN_SCORE

