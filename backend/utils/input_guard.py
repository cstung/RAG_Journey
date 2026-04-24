import re
from unicodedata import normalize

INJECTION_PATTERNS = [
    # --- Direct instruction override ---
    r"ignore\s+(previous|above|all)\s+instructions?",
    r"disregard\s+(your|the|all)",
    r"forget\s+(everything|your\s+instructions?)",
    r"(new|override)\s+(role|persona|instructions?)",

    # --- Role / identity hijacking ---
    r"you\s+are\s+now",
    r"act\s+as\s+(a|an)\b",
    r"pretend\s+(you\s+are|to\s+be)",
    r"simulate\s+(a|an)\b",

    # --- Known jailbreak keywords ---
    r"jailbreak",
    r"DAN\s+mode",
    r"developer\s+mode",
    r"do\s+anything\s+now",

    # --- System prompt probing ---
    r"(reveal|show|print|output|repeat)\s+(your\s+)?(system\s+prompt|instructions?|prompt)",
    r"what\s+(are|were)\s+your\s+instructions?",

    # --- Code injection ---
    r"```[\s\S]{0,2000}?```",  # fenced code blocks
    r"`[^`]{0,500}`",  # inline code
    r"<(script|iframe|img|svg|object|embed)[^>]{0,200}>",  # HTML injection

    # --- Prompt delimiter smuggling ---
    r"###\s*(system|instruction|prompt|assistant)",
    r"\[INST\]",  # Llama token
    r"<\|im_start\|>",  # ChatML token
    r"<\|end_of_turn\|>",
]

COMPILED = [re.compile(p, re.IGNORECASE) for p in INJECTION_PATTERNS]

MAX_QUESTION_LEN = 800

# Common Cyrillic/Greek homoglyphs that can be used to bypass simple regex checks.
# This is not a complete confusables implementation, but it blocks common attacks
# (e.g., Cyrillic "і" in "ignore").
HOMOGLYPH_TRANSLATION = str.maketrans(
    {
        # Cyrillic
        "А": "A",
        "а": "a",
        "В": "B",
        "Е": "E",
        "е": "e",
        "К": "K",
        "М": "M",
        "Н": "H",
        "О": "O",
        "о": "o",
        "Р": "P",
        "р": "p",
        "С": "C",
        "с": "c",
        "Т": "T",
        "Х": "X",
        "х": "x",
        "У": "Y",
        "у": "y",
        "І": "I",
        "і": "i",
        "Ј": "J",
        "ј": "j",
        # Greek
        "Α": "A",
        "Β": "B",
        "Ε": "E",
        "Ζ": "Z",
        "Η": "H",
        "Ι": "I",
        "Κ": "K",
        "Μ": "M",
        "Ν": "N",
        "Ο": "O",
        "Ρ": "P",
        "Τ": "T",
        "Υ": "Y",
        "Χ": "X",
        "α": "a",
        "β": "b",
        "γ": "y",
        "δ": "d",
        "ε": "e",
        "ι": "i",
        "κ": "k",
        "ο": "o",
        "ρ": "p",
        "τ": "t",
        "υ": "u",
        "χ": "x",
    }
)


def sanitise_question(text: str) -> tuple[str, bool]:
    """
    Returns (cleaned_text, is_suspicious).

    Steps:
      1. NFKC-normalise unicode (defeats homoglyph / escape attacks)
      2. Truncate to MAX_QUESTION_LEN
      3. Scan for injection patterns
    """
    # Step 1: normalise unicode
    text = normalize("NFKC", text)
    text = text.translate(HOMOGLYPH_TRANSLATION)

    # Step 2: hard length cap
    if len(text) > MAX_QUESTION_LEN:
        text = text[:MAX_QUESTION_LEN]

    # Step 3: pattern scan
    for pattern in COMPILED:
        if pattern.search(text):
            return text, True

    return text, False
