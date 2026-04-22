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

CHUNK_SIZE = 500     # tokens per chunk
CHUNK_OVERLAP = 80   # token overlap between chunks


def chunk_text(text: str, filename: str, page: int) -> list[dict]:
    """Split text into overlapping chunks by token count."""
    tokens = encoder.encode(text)
    chunks = []
    start = 0
    idx = 0
    while start < len(tokens):
        end = min(start + CHUNK_SIZE, len(tokens))
        chunk_str = encoder.decode(tokens[start:end])
        chunk_id = hashlib.md5(f"{filename}_p{page}_c{idx}".encode()).hexdigest()
        chunks.append({
            "id": chunk_id,
            "text": chunk_str,
            "metadata": {"filename": filename, "page": page, "chunk_idx": idx}
        })
        start += CHUNK_SIZE - CHUNK_OVERLAP
        idx += 1
    return chunks


def get_embedding(text: str) -> list[float]:
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=text
    )
    return response.data[0].embedding


def ingest_pdf(filepath: str) -> int:
    """Ingest a single PDF file. Returns number of chunks stored."""
    filename = os.path.basename(filepath)
    print(f"  Reading: {filename}")

    try:
        reader = PdfReader(filepath)
    except Exception as e:
        print(f"  ERROR reading {filename}: {e}")
        return 0

    all_chunks = []
    for page_num, page in enumerate(reader.pages, 1):
        text = page.extract_text() or ""
        text = text.strip()
        if len(text) < 50:
            continue
        chunks = chunk_text(text, filename, page_num)
        all_chunks.extend(chunks)

    if not all_chunks:
        print(f"  WARNING: No extractable text in {filename}")
        return 0

    # Batch upsert into ChromaDB
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
    """Ingest all PDFs in the docs directory."""
    results = []
    pdf_files = [f for f in os.listdir(docs_dir) if f.lower().endswith(".pdf")]

    if not pdf_files:
        print("No PDF files found in", docs_dir)
        return results

    print(f"Found {len(pdf_files)} PDF files. Starting ingestion...")
    for fname in sorted(pdf_files):
        fpath = os.path.join(docs_dir, fname)
        chunks = ingest_pdf(fpath)
        results.append({"file": fname, "chunks": chunks})

    total = sum(r["chunks"] for r in results)
    print(f"\nDone! Total chunks indexed: {total}")
    return results


if __name__ == "__main__":
    ingest_all()
