import os
import chromadb
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
chroma_client = chromadb.PersistentClient(path="/data/chroma")
collection = chroma_client.get_or_create_collection(
    name="internal_docs",
    metadata={"hnsw:space": "cosine"}
)

COMPANY_NAME = os.getenv("COMPANY_NAME", "công ty")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
TOP_K = int(os.getenv("TOP_K", "5"))


def get_embedding(text: str) -> list[float]:
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=text
    )
    return response.data[0].embedding


def query(question: str) -> dict:
    """Run RAG: retrieve relevant chunks, then generate answer."""

    # Check if there are any documents indexed
    if collection.count() == 0:
        return {
            "answer": "Chưa có tài liệu nào được nạp vào hệ thống. Vui lòng upload tài liệu trước.",
            "sources": []
        }

    # Embed the question and retrieve top-k similar chunks
    q_embedding = get_embedding(question)
    results = collection.query(
        query_embeddings=[q_embedding],
        n_results=min(TOP_K, collection.count()),
        include=["documents", "metadatas", "distances"]
    )

    docs = results["documents"][0]
    metas = results["metadatas"][0]
    distances = results["distances"][0]

    # Filter out low-relevance chunks (cosine distance > 0.7 = not very relevant)
    relevant = [
        (d, m) for d, m, dist in zip(docs, metas, distances) if dist < 0.7
    ]

    if not relevant:
        return {
            "answer": "Tôi không tìm thấy thông tin liên quan trong tài liệu nội bộ. Vui lòng liên hệ bộ phận phụ trách để được hỗ trợ.",
            "sources": []
        }

    # Build context string
    context_parts = []
    for doc, meta in relevant:
        context_parts.append(f"[Nguồn: {meta['filename']}, trang {meta['page']}]\n{doc}")
    context = "\n\n---\n\n".join(context_parts)

    # Build prompt
    system_prompt = f"""Bạn là trợ lý nội bộ của {COMPANY_NAME}. Nhiệm vụ của bạn là trả lời câu hỏi của nhân viên dựa trên tài liệu quy định nội bộ được cung cấp.

Nguyên tắc:
- Chỉ trả lời dựa trên thông tin có trong tài liệu được cung cấp
- Nếu không tìm thấy thông tin, hãy nói rõ và đề nghị liên hệ bộ phận phụ trách
- Trả lời ngắn gọn, rõ ràng bằng tiếng Việt
- Có thể dùng danh sách bullet nếu có nhiều điểm cần liệt kê
- Không bịa đặt hoặc suy đoán ngoài phạm vi tài liệu"""

    user_prompt = f"""TÀI LIỆU THAM KHẢO:
{context}

CÂU HỎI: {question}"""

    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.1,
        max_tokens=1000
    )

    answer = response.choices[0].message.content
    sources = list({m["filename"] for _, m in relevant})

    return {"answer": answer, "sources": sources}
