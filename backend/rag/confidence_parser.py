import re

CONFIDENCE_RE = re.compile(
    r"ĐỘ\s+TIN\s+CẬY\s*:\s*(CAO|TRUNG\s+BÌNH|THẤP)",
    re.IGNORECASE,
)

VALID_LEVELS = {"CAO", "TRUNG BÌNH", "THẤP"}
DEFAULT_LEVEL = "THẤP"  # treat missing tag as low confidence


def extract_confidence(answer: str) -> tuple[str, str]:
    """
    Parses and removes the confidence tag from the LLM answer.

    Returns:
        (clean_answer, confidence_level)
        confidence_level is one of: "CAO", "TRUNG BÌNH", "THẤP"
    """
    match = CONFIDENCE_RE.search(answer or "")

    if match:
        level = match.group(1).upper().strip()
        if level not in VALID_LEVELS:
            level = DEFAULT_LEVEL
    else:
        level = DEFAULT_LEVEL

    # Remove the tag line from the displayed answer
    clean = CONFIDENCE_RE.sub("", answer or "").strip()

    return clean, level

