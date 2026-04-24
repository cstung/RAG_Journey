import re

OUTPUT_LEAK_PATTERNS = [
    # --- System prompt leakage ---
    r"system prompt",
    r"my\s+(system\s+)?instructions?",
    r"i\s+(am|was)\s+told\s+to",
    r"i\s+(was\s+)?instructed\s+to",
    r"as\s+(per|per\s+my)\s+instructions?",

    # --- Signs of successful jailbreak ---
    r"ignore\s+(previous|above)",
    r"i\s+(will|can)\s+now",
    r"entering\s+(developer|DAN|jailbreak)\s+mode",
    r"i('m|\s+am)\s+(now\s+)?(acting\s+as|pretending|simulating)",
]

OUTPUT_COMPILED = [re.compile(p, re.IGNORECASE) for p in OUTPUT_LEAK_PATTERNS]

REFUSAL_MESSAGE = (
    "Xin lỗi, tôi không thể trả lời câu hỏi này. "
    "Vui lòng liên hệ quản lý."
)


def validate_output(answer: str) -> tuple[str, bool]:
    """
    Validates LLM output before returning to the frontend.

    Returns:
        (final_answer, was_flagged)
        If flagged: final_answer is the refusal message.
        Caller is responsible for logging the raw answer when was_flagged=True.
    """
    for pattern in OUTPUT_COMPILED:
        if pattern.search(answer or ""):
            return REFUSAL_MESSAGE, True

    return answer, False

