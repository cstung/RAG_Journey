import os
import hashlib
from openai import OpenAI
from pypdf import PdfReader
import tiktoken
from db import collection

client  = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
encoder = tiktoken.get_encoding("cl100k_base")

MAX_CHUNK_TOKENS = 600
MIN_CHUNK_TOKENS = 80
OVERLAP_TOKENS   = 60


def count_tokens(text: str) -> int:
    return len(encoder.encode(text))


def _make_chunk(text, filename, page, idx, department, category):
    chunk_id = hashlib.md5(f"{filename}_p{page}_c{idx}".encode()).hexdigest()
    return {
        "id": chunk_id,
        "text": text.strip(),
        "metadata": {
            "filename":   filename,
            "page":       page,
            "chunk_idx":  idx,
            "department": department,
            "category":   category,
        }
    }


def _split_by_tokens(text, filename, page, start_idx, department, category):
    tokens = encoder.encode(text)
    chunks, pos, idx = [], 0, start_idx
    while pos < len(tokens):
        end = min(pos + MAX_CHUNK_TOKENS, len(tokens))
        chunks.append(_make_chunk(encoder.decode(tokens[pos:end]),
                                  filename, page, idx, department, category))
        pos += MAX_CHUNK_TOKENS - OVERLAP_TOKENS
        idx += 1
    return chunks


def structural_chunk(text, filename, page, department, category):
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks, buffer, buffer_tokens, chunk_idx = [], "", 0, 0

    for para in paragraphs:
        pt = count_tokens(para)
        if pt > MAX_CHUNK_TOKENS:
            if buffer and buffer_tokens >= MIN_CHUNK_TOKENS:
                chunks.append(_make_chunk(buffer, filename, page, chunk_idx, department, category))
                chunk_idx += 1
                buffer, buffer_tokens = "", 0
            sub = _split_by_tokens(para, filename, page, chunk_idx, department, category)
            chunks.extend(sub); chunk_idx += len(sub)
        elif buffer_tokens + pt > MAX_CHUNK_TOKENS:
            if buffer_tokens >= MIN_CHUNK_TOKENS:
                chunks.append(_make_chunk(buffer, filename, page, chunk_idx, department, category))
                chunk_idx += 1
            buffer, buffer_tokens = para, pt
        else:
            buffer = (buffer + "\n\n" + para).strip() if buffer else para
            buffer_tokens += pt

    if buffer and buffer_tokens >= MIN_CHUNK_TOKENS:
        chunks.append(_make_chunk(buffer, filename, page, chunk_idx, department, category))
    return chunks


def get_embedding(text: str) -> list[float]:
    return client.embeddings.create(
        model="text-embedding-3-small", input=text[:8000]
    ).data[0].embedding


def detect_department_category(filepath: str) -> tuple[str, str]:
    parts = filepath.replace("\\", "/").split("/")
    try:
        docs_idx = next(i for i, p in enumerate(parts) if p == "docs")
    except StopIteration:
        return "General", "general"
    rel = parts[docs_idx + 1:]
    if len(rel) == 1: return "General", "general"
    if len(rel) == 2: return rel[0], "general"
    return rel[0], rel[1]


def ingest_pdf(filepath: str, department: str = None, category: str = None) -> int:
    filename = os.path.basename(filepath)
    if department is None or category is None:
        d, c = detect_department_category(filepath)
        department = department or d
        category   = category   or c

    print(f"  [{department}/{category}] {filename}")
    try:
        reader = PdfReader(filepath)
    except Exception as e:
        print(f"  ERROR: {e}"); return 0

    all_chunks = []
    for page_num, page in enumerate(reader.pages, 1):
        text = (page.extract_text() or "").strip()
        if len(text) < 50: continue
        all_chunks.extend(structural_chunk(text, filename, page_num, department, category))

    if not all_chunks:
        print(f"  WARNING: No extractable text in {filename}"); return 0

    BATCH = 50
    for i in range(0, len(all_chunks), BATCH):
        batch = all_chunks[i: i + BATCH]
        collection.upsert(
            ids=[c["id"] for c in batch],
            embeddings=[get_embedding(c["text"]) for c in batch],
            documents=[c["text"] for c in batch],
            metadatas=[c["metadata"] for c in batch],
        )

    print(f"  → {len(all_chunks)} chunks | total in DB: {collection.count()}")
    return len(all_chunks)


def ingest_all(docs_dir: str = "/data/docs") -> list[dict]:
    results = []
    for root, _, files in os.walk(docs_dir):
        for fname in sorted(files):
            if not fname.lower().endswith(".pdf"): continue
            chunks = ingest_pdf(os.path.join(root, fname))
            results.append({"file": fname, "chunks": chunks})
    total = sum(r["chunks"] for r in results)
    print(f"\nTotal: {len(results)} files, {total} chunks.")
    return results


if __name__ == "__main__":
    ingest_all()
