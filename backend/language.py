import os
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SUPPORTED = {"vi", "ko", "en"}
DEFAULT   = "vi"

LANG_NAMES = {
    "vi": "Vietnamese",
    "ko": "Korean",
    "en": "English",
}

SYSTEM_PROMPTS = {
    "vi": (
        "You are the internal assistant of {company}. "
        "Answer ONLY based on the provided documents. "
        "If information is not found, say so clearly. "
        "YOU MUST RESPOND IN VIETNAMESE (tiếng Việt). No exceptions."
    ),
    "ko": (
        "You are the internal assistant of {company}. "
        "Answer ONLY based on the provided documents. "
        "If information is not found, say so clearly. "
        "YOU MUST RESPOND IN KOREAN (한국어). No exceptions."
    ),
    "en": (
        "You are the internal assistant of {company}. "
        "Answer ONLY based on the provided documents. "
        "If information is not found, say so clearly. "
        "YOU MUST RESPOND IN ENGLISH. No exceptions."
    ),
}

def detect_language(text: str) -> str:
    """Detect input language. Returns 'vi' | 'ko' | 'en'. Falls back to 'vi' on error."""
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": (
                    "Detect the language of this text. "
                    "Reply with ONLY one word: vi, ko, or en\n\n"
                    f"Text: {text[:300]}"
                )
            }],
            max_tokens=5,
            temperature=0,
        )
        lang = resp.choices[0].message.content.strip().lower()
        return lang if lang in SUPPORTED else DEFAULT
    except Exception:
        return DEFAULT


def build_system_prompt(lang: str, company: str) -> str:
    template = SYSTEM_PROMPTS.get(lang, SYSTEM_PROMPTS[DEFAULT])
    return template.format(company=company)


def build_user_prompt(question: str, context: str, lang: str) -> str:
    """
    Wraps the final user prompt with an explicit language instruction.
    This forces the model to respond in the correct language even when
    the source documents are in a different language (Vietnamese).
    """
    lang_name = LANG_NAMES.get(lang, "Vietnamese")
    return (
        f"REFERENCE DOCUMENTS:\n{context}\n\n"
        f"---\n"
        f"QUESTION: {question}\n\n"
        f"IMPORTANT: Your answer MUST be written entirely in {lang_name}. "
        f"Do not use any other language, even if the documents are in a different language."
    )
