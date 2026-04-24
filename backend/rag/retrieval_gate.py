# Tune these thresholds for your embedding model.
SIMILARITY_THRESHOLD = 0.50  # Increased from 0.45 for stricter relevance
BM25_MIN_SCORE = 1.0         # Minimum BM25 score to be considered relevant

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

