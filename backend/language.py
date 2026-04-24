import os

from openai import OpenAI


client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SUPPORTED_LANGUAGES = {"vi", "ko", "en"}
DEFAULT_LANGUAGE = "vi"


def detect_language(text: str) -> str:
    """
    Detect the language of the input text using OpenAI.
    Returns: 'vi' | 'ko' | 'en'
    Falls back to 'vi' on any error.
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": (
                        "Detect the language of the following text. "
                        "Reply with ONLY one of these codes: vi, ko, en\n\n"
                        f"Text: {text[:200]}"
                    ),
                }
            ],
            max_tokens=5,
            temperature=0,
        )
        lang = (response.choices[0].message.content or "").strip().lower()
        return lang if lang in SUPPORTED_LANGUAGES else DEFAULT_LANGUAGE
    except Exception:
        return DEFAULT_LANGUAGE


def get_system_prompt(lang: str, company_name: str = "LWAH") -> str:
    """
    Returns the appropriate system prompt for the detected language.
    The bot always responds in the same language the user asked in.
    """
    prompts = {
        "vi": (
            f"Ban la tro ly noi bo cua {company_name}. "
            "Hay tra loi cau hoi cua nhan vien dua tren tai lieu quy dinh noi bo duoc cung cap. "
            "Nguyen tac:\n"
            "- Chi tra loi dua tren thong tin co trong tai lieu\n"
            "- Neu khong tim thay thong tin, hay noi ro: 'Toi chua co thong tin ve van de nay'\n"
            "- Tra loi ngan gon, ro rang bang tieng Viet\n"
            "- Khong bia dat hoac suy doan ngoai pham vi tai lieu"
        ),
        "ko": (
            f"당신은 {company_name}의 내부 어시스턴트입니다. "
            "제공된 내부 규정 문서를 바탕으로 직원들의 질문에 답변하세요. "
            "원칙:\n"
            "- 문서에 있는 정보만을 바탕으로 답변하세요\n"
            "- 정보를 찾을 수 없는 경우 명확하게 말하세요: '해당 사항에 대한 정보가 없습니다'\n"
            "- 한국어로 간결하고 명확하게 답변하세요\n"
            "- 문서 범위 밖의 내용을 추측하거나 지어내지 마세요"
        ),
        "en": (
            f"You are the internal assistant of {company_name}. "
            "Answer employee questions based on the internal policy documents provided. "
            "Rules:\n"
            "- Only answer based on information found in the documents\n"
            "- If the information is not available, clearly state: 'I don't have information on this topic'\n"
            "- Respond concisely and clearly in English\n"
            "- Do not fabricate or speculate beyond the scope of the documents"
        ),
    }
    return prompts.get(lang, prompts["vi"])
