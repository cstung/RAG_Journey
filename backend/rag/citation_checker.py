import re

SOURCE_RE = re.compile(r"NGUỒN\s*:\s*(.+)", re.IGNORECASE)


def extract_and_check_citation(
    answer: str,
    available_sources: list[str],
) -> tuple[str, bool]:
    """
    Parses the NGUỒN tag and verifies the cited source is real.

    Args:
        answer: Raw LLM answer (still containing the NGUỒN tag).
        available_sources: List of source names from the retrieved chunks
                           (e.g. document filenames or section IDs).

    Returns:
        (clean_answer, citation_valid)
        citation_valid=False means the model cited nothing or cited a
        source that was not in the retrieved context (potential hallucination).
    """
    match = SOURCE_RE.search(answer or "")

    if not match:
        # Model didn't include a source tag — treat as uncited
        return (answer or "").strip(), False

    cited = match.group(1).strip()

    # Remove the tag line from the displayed answer
    clean = SOURCE_RE.sub("", answer or "").strip()

    # Check if the cited name appears in any of the real source names
    citation_valid = any(cited.lower() in src.lower() for src in available_sources)

    return clean, citation_valid

