import os
import hashlib
import chromadb
from openai import OpenAI
from pypdf import PdfReader
import tiktoken

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
chroma_client = chromadb.PersistentClient(path="/data/chroma")
collection = chroma_client.get_or_create_collection(
    name="internal_docs",
    metadata={"hnsw:space": "cosine"}
)

encoder = tiktoken.get_encoding("cl100k_base")

MAX_CHUNK_TOKENS = 600
MIN_CHUNK_TOKENS = 80
OVERLAP_TOKENS   = 60


# ── Helpers ────────────────────────────────────────────────────────────────

def count_tokens(text: str) -> int:
    return len(encoder.encode(text))


def _make_chunk(text: str, filename: str, page: int, idx: int,
                department: str, category: str) -> dict:
    chunk_id = hashlib.md5(f"{filename}_p{page}_c{idx}".encode()).hexdigest()
    return {
        "id": chunk_id,
        "text": text.strip(),
        "metadata": {
            "filename": filename,
            "page": page,
            "chunk_idx": idx,
            "department": department,
            "category": category,
        }
    }


def _split_by_tokens(text: str, filename: str, page: int, start_idx: int,
                     department: str, category: str) -> list[dict]:
    """Fallback: split oversized text by token windows with overlap."""
    tokens = encoder.encode(text)
    chunks = []
    pos, idx = 0, start_idx
    while pos < len(tokens):
        end = min(pos + MAX_CHUNK_TOKENS, len(tokens))
        chunk_text = encoder.decode(tokens[pos:end])
        chunks.append(_make_chunk(chunk_text, filename, page, idx, department, category))
        pos += MAX_CHUNK_TOKENS - OVERLAP_TOKENS
        idx += 1
    return chunks


def structural_chunk(text: str, filename: str, page: int,
                     department: str, category: str) -> list[dict]:
    """
    Chunk by paragraph structure:
    - Split on double newlines (paragraph breaks)
    - Merge short paragraphs together
    - Split oversized paragraphs by token window
    """
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]

    chunks = []
    buffer = ""
    buffer_tokens = 0
    chunk_idx = 0

    for para in paragraphs:
        para_tokens = count_tokens(para)

        # Oversized single paragraph → split by tokens directly
        if para_tokens > MAX_CHUNK_TOKENS:
            if buffer and buffer_tokens >= MIN_CHUNK_TOKENS:
                chunks.append(_make_chunk(buffer, filename, page, chunk_idx, department, category))
                chunk_idx += 1
                buffer, buffer_tokens = "", 0
            sub = _split_by_tokens(para, filename, page, chunk_idx, department, category)
            chunks.extend(sub)
            chunk_idx += len(sub)
            continue

        # Adding this paragraph would overflow → flush buffer first
        if buffer_tokens + para_tokens > MAX_CHUNK_TOKENS:
            if buffer_tokens >= MIN_CHUNK_TOKENS:
                chunks.append(_make_chunk(buffer, filename, page, chunk_idx, department, category))
                chunk_idx += 1
            buffer = para
            buffer_tokens = para_tokens
        else:
            buffer = (buffer + "\n\n" + para).strip() if buffer else para
            buffer_tokens += para_tokens

    # Flush remaining buffer
    if buffer and buffer_tokens >= MIN_CHUNK_TOKENS:
        chunks.append(_make_chunk(buffer, filename, page, chunk_idx, department, category))

    return chunks


def get_embedding(text: str) -> list[float]:
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=text[:8000]   # safety trim
    )
    return response.data[0].embedding


def detect_department_category(filepath: str) -> tuple[str, str]:
    """
    Auto-detect from folder structure:
      data/docs/HR/file.pdf          → department=HR, category=general
      data/docs/HR/Quy-dinh/file.pdf → department=HR, category=Quy-dinh
      data/docs/file.pdf             → department=General, category=general
    """
    parts = filepath.replace("\\", "/").split("/")
    # Find index of "docs" folder
    try:
        docs_idx = next(i for i, p in enumerate(parts) if p == "docs")
    except StopIteration:
        return "General", "general"

    relative = parts[docs_idx + 1 :]   # everything after docs/
    if len(relative) == 1:
        return "General", "general"
    elif len(relative) == 2:
        return relative[0], "general"
    else:
        return relative[0], relative[1]


def ingest_pdf(filepath: str, department: str = None, category: str = None) -> int:
    filename = os.path.basename(filepath)

    if department is None or category is None:
        det_dept, det_cat = detect_department_category(filepath)
        department = department or det_dept
        category   = category   or det_cat

    print(f"  [{department}/{category}] {filename}")

    try:
        reader = PdfReader(filepath)
    except Exception as e:
        print(f"  ERROR: {e}")
        return 0

    all_chunks = []
    for page_num, page in enumerate(reader.pages, 1):
        text = (page.extract_text() or "").strip()
        if len(text) < 50:
            continue
        chunks = structural_chunk(text, filename, page_num, department, category)
        all_chunks.extend(chunks)

    if not all_chunks:
        print(f"  WARNING: No extractable text in {filename}")
        return 0

    BATCH = 50
    for i in range(0, len(all_chunks), BATCH):
        batch = all_chunks[i : i + BATCH]
        embeddings = [get_embedding(c["text"]) for c in batch]
        collection.upsert(
            ids=[c["id"] for c in batch],
            embeddings=embeddings,
            documents=[c["text"] for c in batch],
            metadatas=[c["metadata"] for c in batch],
        )

    print(f"  → {len(all_chunks)} chunks indexed")
    return len(all_chunks)


def ingest_all(docs_dir: str = "/data/docs") -> list[dict]:
    """Walk docs_dir recursively, ingest all PDFs."""
    results = []
    for root, _, files in os.walk(docs_dir):
        for fname in sorted(files):
            if not fname.lower().endswith(".pdf"):
                continue
            fpath = os.path.join(root, fname)
            chunks = ingest_pdf(fpath)
            results.append({"file": fname, "path": fpath, "chunks": chunks})

    total = sum(r["chunks"] for r in results)
    print(f"\nTotal: {len(results)} files, {total} chunks indexed.")
    return results


if __name__ == "__main__":
    ingest_all()
