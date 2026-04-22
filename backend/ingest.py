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


def _make_chunk(text, file_id, page, idx, department, category, filename):
    chunk_id = hashlib.md5(f"{file_id}_p{page}_c{idx}".encode()).hexdigest()
    return {
        "id": chunk_id,
        "text": text.strip(),
        "metadata": {
            "file_id":    file_id,
            "filename":   filename,
            "page":       page,
            "chunk_idx":  idx,
            "department": department,
            "category":   category,
        }
    }


def _split_by_tokens(text, file_id, page, start_idx, department, category, filename):
    tokens = encoder.encode(text)
    chunks, pos, idx = [], 0, start_idx
    while pos < len(tokens):
        end = min(pos + MAX_CHUNK_TOKENS, len(tokens))
        chunks.append(_make_chunk(encoder.decode(tokens[pos:end]),
                                  file_id, page, idx, department, category, filename))
        pos += MAX_CHUNK_TOKENS - OVERLAP_TOKENS
        idx += 1
    return chunks


def structural_chunk(text, file_id, page, department, category, filename):
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks, buffer, buffer_tokens, chunk_idx = [], "", 0, 0

    for para in paragraphs:
        pt = count_tokens(para)
        if pt > MAX_CHUNK_TOKENS:
            if buffer and buffer_tokens >= MIN_CHUNK_TOKENS:
                chunks.append(_make_chunk(buffer, file_id, page, chunk_idx, department, category, filename))
                chunk_idx += 1
                buffer, buffer_tokens = "", 0
            sub = _split_by_tokens(para, file_id, page, chunk_idx, department, category, filename)
            chunks.extend(sub); chunk_idx += len(sub)
        elif buffer_tokens + pt > MAX_CHUNK_TOKENS:
            if buffer_tokens >= MIN_CHUNK_TOKENS:
                chunks.append(_make_chunk(buffer, file_id, page, chunk_idx, department, category, filename))
                chunk_idx += 1
            buffer, buffer_tokens = para, pt
        else:
            buffer = (buffer + "\n\n" + para).strip() if buffer else para
            buffer_tokens += pt

    if buffer and buffer_tokens >= MIN_CHUNK_TOKENS:
        chunks.append(_make_chunk(buffer, file_id, page, chunk_idx, department, category, filename))
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
    # Use relative path as a unique ID to prevent collisions between files with same name
    docs_dir = "/data/docs"
    try:
        file_id = os.path.relpath(filepath, docs_dir).replace("\\", "/")
    except Exception:
        file_id = os.path.basename(filepath)

    filename = os.path.basename(filepath)
    if department is None or category is None:
        d, c = detect_department_category(filepath)
        department = department or d
        category   = category   or c

    print(f"  [{department}/{category}] {file_id}")
    try:
        reader = PdfReader(filepath)
    except Exception as e:
        print(f"  ERROR reading {filepath}: {e}"); return 0

    all_chunks = []
    for page_num, page in enumerate(reader.pages, 1):
        text = (page.extract_text() or "").strip()
        if len(text) < 40: continue
        all_chunks.extend(structural_chunk(text, file_id, page_num, department, category, filename))

    if not all_chunks:
        print(f"  WARNING: No extractable text in {file_id}"); return 0

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


def prune_orphans(docs_dir: str = "/data/docs"):
    """Delete chunks from ChromaDB whose source files no longer exist on disk."""
    results = collection.get(include=["metadatas"])
    if not results["ids"]:
        return 0

    ids_to_delete = []
    seen_files    = set()
    deleted_files = set()

    for id_, meta in zip(results["ids"], results["metadatas"]):
        file_id = meta.get("file_id")
        if not file_id: continue

        if file_id in seen_files:
            continue # already checked this file
        
        # Check if file exists on disk
        if not os.path.exists(os.path.join(docs_dir, file_id)):
            # Find all chunks for this file_id (could be optimized but safe for now)
            deleted_files.add(file_id)
        
        seen_files.add(file_id)

    if deleted_files:
        print(f"[Prune] Found {len(deleted_files)} orphan files in DB. Deleting chunks...")
        for fid in deleted_files:
            # Delete by metadata filter
            collection.delete(where={"file_id": fid})
            print(f"  - Deleted: {fid}")
    
    return len(deleted_files)


def ingest_all(docs_dir: str = "/data/docs") -> list[dict]:
    # 1. Prune orphans first
    pruned_count = prune_orphans(docs_dir)
    
    # 2. Ingest current files
    results = []
    for root, _, files in os.walk(docs_dir):
        for fname in sorted(files):
            if not fname.lower().endswith(".pdf"): continue
            chunks = ingest_pdf(os.path.join(root, fname))
            results.append({"file": fname, "chunks": chunks})
    
    total = sum(r["chunks"] for r in results)
    print(f"\nTotal: {len(results)} files, {total} chunks. (Pruned {pruned_count} files)")
    return results


if __name__ == "__main__":
    ingest_all()
