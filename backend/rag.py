# backend/rag.py  (updated)
import os
from openai import OpenAI
from retrieval import hybrid_search
from vector_store import COLLECTION_PDF, COLLECTION_LEGAL

openai_client = OpenAI()

SYSTEM_PROMPT = """Bạn là trợ lý tư vấn pháp lý nội bộ. Trả lời dựa trên các văn bản được cung cấp.
- Trích dẫn số hiệu văn bản và điều khoản cụ thể khi trả lời
- Nếu không có thông tin, hãy nói rõ thay vì suy đoán
- Ưu tiên văn bản mới nhất nếu có nhiều văn bản liên quan
- Luôn trả lời bằng tiếng Việt"""


def embed_text(text: str) -> list[float]:
    resp = openai_client.embeddings.create(
        model=os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"),
        input=[text],
    )
    return resp.data[0].embedding


def answer(
    question:   str,
    collections: list[str] | None = None,   # None = search all
    filters:    dict | None = None,
) -> dict:
    if collections is None:
        collections = [COLLECTION_PDF, COLLECTION_LEGAL]

    q_emb = embed_text(question)

    all_chunks: list[dict] = []
    for col in collections:
        try:
            chunks = hybrid_search(question, q_emb, col, filters)
            all_chunks.extend(chunks)
        except Exception:
            pass  # collection may not exist yet

    # Sort fused chunks by score, keep top TOP_K_FINAL
    top_k = int(os.getenv("TOP_K", "5"))
    all_chunks.sort(key=lambda x: x["score"], reverse=True)
    context_chunks = all_chunks[:top_k]

    # Build context string with citations
    context_parts = []
    for i, chunk in enumerate(context_chunks, 1):
        m  = chunk["metadata"]
        ref = m.get("document_number") or m.get("source") or f"Source {i}"
        context_parts.append(f"[{i}] {ref}\n{chunk['text']}")
    context = "\n\n---\n\n".join(context_parts)

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": f"Tài liệu tham chiếu:\n\n{context}\n\n---\nCâu hỏi: {question}"},
    ]

    resp = openai_client.chat.completions.create(
        model=os.getenv("CHAT_MODEL", "gpt-4o-mini"),
        messages=messages,
        temperature=0,
    )

    return {
        "answer":  resp.choices[0].message.content,
        "sources": [
            {
                "ref":   c["metadata"].get("document_number") or c["metadata"].get("source"),
                "title": c["metadata"].get("title", ""),
                "url":   c["metadata"].get("url", ""),
                "score": round(c["score"], 4),
            }
            for c in context_chunks
        ],
    }
