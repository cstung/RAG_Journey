import re

DOCUMENT_INJECTION_PATTERNS = [
    # --- Instruction overrides embedded in document text ---
    r"ignore\s+(previous|above)\s+instructions?",
    r"disregard\s+(previous|above|all)",
    r"(new\s+)?system\s+(prompt|instruction)",
    r"system:\s",

    # --- Special tokens that break prompt structure ---
    r"<\|im_start\|>",
    r"<\|im_end\|>",
    r"<\|end_of_turn\|>",
    r"\[INST\]",
    r"\[\/INST\]",
    r"###\s*(instruction|system|prompt|assistant)",

    # --- Role injection via document content ---
    r"you\s+are\s+now\s+a",
    r"act\s+as\s+(a|an)\b",
]

DOC_COMPILED = [re.compile(p, re.IGNORECASE) for p in DOCUMENT_INJECTION_PATTERNS]

MAX_CHUNK_LEN = 2000  # characters — hard cap per retrieved chunk


def sanitise_chunk(text: str) -> str:
    """
    Cleans a single retrieved document chunk.

    Steps:
      1. Replace each injection pattern with a safe placeholder
      2. Truncate to MAX_CHUNK_LEN
    """
    # Step 1: replace injection patterns
    for pattern in DOC_COMPILED:
        text = pattern.sub("[NỘI DUNG ĐÃ BỊ LỌC]", text)

    # Step 2: length cap
    if len(text) > MAX_CHUNK_LEN:
        text = text[:MAX_CHUNK_LEN] + "… [đã cắt bớt]"

    return text

