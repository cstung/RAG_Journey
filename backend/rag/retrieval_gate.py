# Tune this threshold for your embedding model.
# For cosine similarity: 0.45 is a safe starting point.
# Lower = more chunks pass (more recall, more hallucination risk).
# Higher = fewer chunks pass (less recall, safer answers).
SIMILARITY_THRESHOLD = 0.45

# Sentinel returned when no chunks meet the threshold.
# The system prompt's "no information" rule fires when this appears in context.
NO_RESULTS_SENTINEL = "[KHÔNG TÌM THẤY TÀI LIỆU LIÊN QUAN]"


def filter_chunks(
    chunks: list[str],
    scores: list[float],
    threshold: float = SIMILARITY_THRESHOLD,
) -> list[str]:
    """
    Filters retrieved chunks by similarity score.

    Args:
        chunks: List of raw text chunks from the vector store.
        scores: Corresponding similarity scores (same order as chunks).
        threshold: Minimum score to keep a chunk.

    Returns:
        Filtered list of chunks, or [NO_RESULTS_SENTINEL] if none pass.
    """
    filtered = [chunk for chunk, score in zip(chunks, scores) if score >= threshold]

    if not filtered:
        return [NO_RESULTS_SENTINEL]

    return filtered

