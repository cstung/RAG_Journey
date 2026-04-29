"""
Ingest Vietnamese Legal Documents from HuggingFace into ChromaDB.

Dataset: https://huggingface.co/datasets/th1nhng0/vietnamese-legal-documents
Configs used: 'metadata' (153K rows) + 'content' (178K rows, HTML)

Usage:
    # Dry-run: preview what would be ingested (no API calls)
    python ingest_legal_dataset.py --dry-run --max-docs 10

    # Test with 50 documents (costs < $0.01)
    python ingest_legal_dataset.py --max-docs 50

    # Filtered ingestion (only currently-in-effect documents)
    python ingest_legal_dataset.py --max-docs 500 --effect-status "Còn hiệu lực"

    # Full filtered ingestion
    python ingest_legal_dataset.py --effect-status "Còn hiệu lực"

    # Resume after interruption
    python ingest_legal_dataset.py --max-docs 500 --resume
"""

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path

# Fix Windows console encoding for Unicode output
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from dotenv import load_dotenv

load_dotenv()

from openai import OpenAI
from bs4 import BeautifulSoup
import tiktoken

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATASET_NAME = "th1nhng0/vietnamese-legal-documents"
CHECKPOINT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "legal_dataset_checkpoint.json"
)

DEPARTMENT = "Pháp lý"
DOMAIN = "legal"

# Chunking parameters (aligned with existing ingest.py)
MAX_CHUNK_TOKENS = 800
MIN_CHUNK_TOKENS = 20
OVERLAP_TOKENS = 100
EMBED_BATCH_SIZE = 40

encoder = tiktoken.get_encoding("cl100k_base")


# ---------------------------------------------------------------------------
# Text extraction (mirrors ingest.py extract_html_text)
# ---------------------------------------------------------------------------


def extract_html_text(html: str) -> str:
    """Convert HTML content to clean text."""
    soup = BeautifulSoup(html or "", "html.parser")
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside"]):
        tag.decompose()

    main = soup.find("article") or soup.find("main") or soup.body or soup
    text = main.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return "\n".join(lines).strip()


# ---------------------------------------------------------------------------
# Chunking (mirrors ingest.py structural_chunk)
# ---------------------------------------------------------------------------


def count_tokens(text: str) -> int:
    return len(encoder.encode(text))


def _make_chunk(text, file_id, page, idx, department, category, domain, filename, extra_meta=None):
    chunk_id = hashlib.md5(f"{file_id}_p{page}_c{idx}".encode()).hexdigest()
    meta = {
        "file_id": file_id,
        "filename": filename,
        "page": page,
        "chunk_idx": idx,
        "department": department,
        "category": category,
        "domain": domain,
        "document_id": None,
        "doc_version": None,
    }
    if extra_meta:
        meta.update(extra_meta)
    return {"id": chunk_id, "text": text.strip(), "metadata": meta}


def _split_by_tokens(text, file_id, page, start_idx, department, category, domain, filename, extra_meta=None):
    tokens = encoder.encode(text)
    chunks, pos, idx = [], 0, start_idx
    while pos < len(tokens):
        end = min(pos + MAX_CHUNK_TOKENS, len(tokens))
        chunk_text = encoder.decode(tokens[pos:end])
        chunks.append(
            _make_chunk(chunk_text, file_id, page, idx, department, category, domain, filename, extra_meta)
        )
        pos += MAX_CHUNK_TOKENS - OVERLAP_TOKENS
        idx += 1
    return chunks


def structural_chunk(text, file_id, page, department, category, domain, filename, extra_meta=None):
    """Split text into semantic chunks by paragraphs, respecting token limits."""
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks, buffer, buffer_tokens, chunk_idx = [], "", 0, 0

    for para in paragraphs:
        pt = count_tokens(para)
        if pt > MAX_CHUNK_TOKENS:
            if buffer and buffer_tokens >= MIN_CHUNK_TOKENS:
                chunks.append(
                    _make_chunk(buffer, file_id, page, chunk_idx, department, category, domain, filename, extra_meta)
                )
                chunk_idx += 1
                buffer, buffer_tokens = "", 0
            sub = _split_by_tokens(
                para, file_id, page, chunk_idx, department, category, domain, filename, extra_meta
            )
            chunks.extend(sub)
            chunk_idx += len(sub)
        elif buffer_tokens + pt > MAX_CHUNK_TOKENS:
            if buffer_tokens >= MIN_CHUNK_TOKENS:
                chunks.append(
                    _make_chunk(buffer, file_id, page, chunk_idx, department, category, domain, filename, extra_meta)
                )
                chunk_idx += 1
            buffer, buffer_tokens = para, pt
        else:
            buffer = (buffer + "\n\n" + para).strip() if buffer else para
            buffer_tokens += pt

    if buffer and buffer_tokens >= MIN_CHUNK_TOKENS:
        chunks.append(
            _make_chunk(buffer, file_id, page, chunk_idx, department, category, domain, filename, extra_meta)
        )
    return chunks


# ---------------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------------

_client: OpenAI | None = None


def _get_client() -> OpenAI:
    global _client
    if _client is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            print("CRITICAL: OPENAI_API_KEY is not set!")
            sys.exit(1)
        _client = OpenAI(api_key=api_key)
    return _client


def get_embedding(text: str) -> list[float]:
    return _get_client().embeddings.create(
        model="text-embedding-3-small", input=text[:8000]
    ).data[0].embedding


# ---------------------------------------------------------------------------
# Checkpoint / Resume
# ---------------------------------------------------------------------------


def load_checkpoint() -> set:
    """Load set of already-ingested document IDs."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data.get("ingested_ids", []))
    return set()


def save_checkpoint(ingested_ids: set):
    """Persist ingested document IDs for resume support."""
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump({"ingested_ids": sorted(ingested_ids), "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")}, f)


# ---------------------------------------------------------------------------
# Category detection from Vietnamese legal document type
# ---------------------------------------------------------------------------


def detect_category(loai_van_ban: str | None) -> str:
    """Map loai_van_ban (document type) to a simplified category."""
    if not loai_van_ban:
        return "general"
    lvb = loai_van_ban.strip().lower()
    mapping = {
        "luật": "luat",
        "bộ luật": "bo_luat",
        "nghị định": "nghi_dinh",
        "thông tư": "thong_tu",
        "quyết định": "quyet_dinh",
        "nghị quyết": "nghi_quyet",
        "chỉ thị": "chi_thi",
        "pháp lệnh": "phap_lenh",
        "sắc lệnh": "sac_lenh",
        "hiến pháp": "hien_phap",
        "công văn": "cong_van",
        "thông báo": "thong_bao",
    }
    for vn_name, cat in mapping.items():
        if vn_name in lvb:
            return cat
    return "general"


# ---------------------------------------------------------------------------
# Content fetching — download parquet and read row-by-row via PyArrow
# ---------------------------------------------------------------------------

_CONTENT_PARQUET_URL = (
    "https://huggingface.co/api/datasets/th1nhng0/vietnamese-legal-documents/"
    "parquet/content/data/0.parquet"
)


def _download_content_parquet(cache_dir: str) -> str:
    """Download the content parquet file to a local cache directory."""
    os.makedirs(cache_dir, exist_ok=True)
    dest = os.path.join(cache_dir, "content_data_0.parquet")
    if os.path.exists(dest):
        size_mb = os.path.getsize(dest) / (1024 * 1024)
        print(f"  Content parquet already cached ({size_mb:.0f} MB): {dest}")
        return dest

    print(f"  Downloading content parquet (~412 MB)...")
    import urllib.request
    urllib.request.urlretrieve(_CONTENT_PARQUET_URL, dest)
    size_mb = os.path.getsize(dest) / (1024 * 1024)
    print(f"  Downloaded: {size_mb:.0f} MB")
    return dest


def build_content_lookup(parquet_path: str, target_ids: set[int]) -> dict[int, str]:
    """
    Read the parquet file in small batches using PyArrow.
    Only keeps content for document IDs we actually need.
    This avoids loading the entire 3.2 GB into memory.
    """
    import pyarrow.parquet as pq

    print(f"  Scanning parquet for {len(target_ids)} target documents...")
    content_map: dict[int, str] = {}
    remaining = set(target_ids)

    pf = pq.ParquetFile(parquet_path)
    total_groups = pf.metadata.num_row_groups

    for rg_idx in range(total_groups):
        if not remaining:
            break

        table = pf.read_row_group(rg_idx, columns=["id", "content_html"])
        ids = table.column("id").to_pylist()
        htmls = table.column("content_html").to_pylist()

        for doc_id, html in zip(ids, htmls):
            try:
                doc_id_int = int(doc_id)
            except (ValueError, TypeError):
                continue
            if doc_id_int in remaining:
                content_map[doc_id_int] = html or ""
                remaining.discard(doc_id_int)

        del table  # Free memory eagerly
        print(f"    Row group {rg_idx + 1}/{total_groups} scanned | found {len(content_map)}/{len(target_ids)}", end="\r")

    print(f"\n  Content found for {len(content_map)}/{len(target_ids)} documents")
    return content_map


# ---------------------------------------------------------------------------
# Core ingestion logic
# ---------------------------------------------------------------------------


def _truncate(s: str, n: int = 80) -> str:
    s = (s or "").strip()
    return s[:n] + "..." if len(s) > n else s


def ingest_legal_dataset(
    max_docs: int | None = None,
    effect_status: str | None = None,
    doc_types: list[str] | None = None,
    linh_vuc_filter: str | None = None,
    dry_run: bool = False,
    resume: bool = False,
):
    """
    Main entry point: download, filter, chunk, embed, and upsert legal documents.

    Args:
        max_docs:        Max number of documents to ingest (None = all).
        effect_status:   Filter by tinh_trang_hieu_luc (e.g., "Còn hiệu lực").
        doc_types:       Filter by loai_van_ban (e.g., ["Luật", "Nghị định"]).
        linh_vuc_filter: Filter by linh_vuc (legal field) substring.
        dry_run:         Preview mode -- no API calls, no DB writes.
        resume:          Resume from checkpoint (skip already-ingested docs).
    """
    from datasets import load_dataset

    print("=" * 70)
    print("  Vietnamese Legal Documents -> ChromaDB Ingestion")
    print("=" * 70)
    print(f"  Dataset:       {DATASET_NAME}")
    print(f"  Max docs:      {max_docs or 'unlimited'}")
    print(f"  Effect status: {effect_status or 'any'}")
    print(f"  Doc types:     {doc_types or 'any'}")
    print(f"  Linh vuc:      {linh_vuc_filter or 'any'}")
    print(f"  Dry run:       {dry_run}")
    print(f"  Resume:        {resume}")
    print("=" * 70)

    # ------------------------------------------------------------------
    # Step 1: Load metadata (small — 14 MB parquet, 33 MB in memory)
    # ------------------------------------------------------------------
    print("\n[1/5] Loading metadata...")
    t0 = time.time()
    meta_ds = load_dataset(DATASET_NAME, "metadata", split="data")
    print(f"  Metadata loaded: {len(meta_ds)} rows in {time.time() - t0:.1f}s")

    # Build filtered dict: id -> metadata row
    meta_dict: dict[int, dict] = {}
    meta_skipped = 0
    for row in meta_ds:
        doc_id = row["id"]

        # Apply filters
        if effect_status:
            row_status = (row.get("tinh_trang_hieu_luc") or "").strip()
            if effect_status.lower() not in row_status.lower():
                meta_skipped += 1
                continue

        if doc_types:
            row_type = (row.get("loai_van_ban") or "").strip()
            if not any(dt.lower() in row_type.lower() for dt in doc_types):
                meta_skipped += 1
                continue

        if linh_vuc_filter:
            row_lv = (row.get("linh_vuc") or "").strip()
            if linh_vuc_filter.lower() not in row_lv.lower():
                meta_skipped += 1
                continue

        meta_dict[doc_id] = row

        if max_docs and len(meta_dict) >= max_docs:
            break

    print(f"  Filtered metadata: {len(meta_dict)} docs (skipped {meta_skipped} by filters)")

    if not meta_dict:
        print("  No documents match your filters. Exiting.")
        return

    # ------------------------------------------------------------------
    # Step 2: Load checkpoint (if resuming)
    # ------------------------------------------------------------------
    ingested_ids: set = set()
    if resume:
        ingested_ids = load_checkpoint()
        before = len(meta_dict)
        meta_dict = {k: v for k, v in meta_dict.items() if str(k) not in ingested_ids}
        print(f"\n[2/5] Resume: {before - len(meta_dict)} already ingested, {len(meta_dict)} remaining")
    else:
        print(f"\n[2/5] No resume -- starting fresh")

    if not meta_dict:
        print("  All documents already ingested. Nothing to do.")
        return

    # ------------------------------------------------------------------
    # Step 3: Download content parquet and build lookup
    # ------------------------------------------------------------------
    print(f"\n[3/5] Loading content for {len(meta_dict)} documents...")
    cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".legal_dataset_cache")
    parquet_path = _download_content_parquet(cache_dir)

    target_ids = set(meta_dict.keys())
    content_map = build_content_lookup(parquet_path, target_ids)

    # ChromaDB collection (only if not dry-run)
    collection = None
    if not dry_run:
        from db import collection as chroma_collection
        collection = chroma_collection

    # ------------------------------------------------------------------
    # Step 4: Process documents — chunk, embed, upsert
    # ------------------------------------------------------------------
    processed = 0
    total_chunks = 0
    skipped_no_text = 0
    start_time = time.time()

    print(f"\n[4/5] Processing documents...")
    print(f"  Target: {len(content_map)} documents with content")
    print("-" * 70)

    for doc_id_int, html_content in content_map.items():
        meta = meta_dict.get(doc_id_int)
        if not meta:
            continue

        # Extract text from HTML
        text = extract_html_text(html_content)
        if not text or len(text) < 20:
            skipped_no_text += 1
            continue

        # Build metadata for chunks
        so_ky_hieu = (meta.get("so_ky_hieu") or "").strip()
        title = (meta.get("title") or "").strip()
        loai_van_ban = (meta.get("loai_van_ban") or "").strip()
        category = detect_category(loai_van_ban)

        # Construct a readable filename for the chunk metadata
        filename_parts = [so_ky_hieu, _truncate(title, 60)] if so_ky_hieu else [_truncate(title, 80)]
        filename = " - ".join(p for p in filename_parts if p) or f"legal_doc_{doc_id_int}"

        file_id = f"legal_dataset/{doc_id_int}"

        # Extra legal metadata stored on each chunk
        extra_meta = {
            "source_url": f"https://vbpl.vn/bo-luat/doc/{doc_id_int}",
            "legal_doc_id": str(doc_id_int),
            "so_ky_hieu": so_ky_hieu or "",
            "ngay_ban_hanh": (meta.get("ngay_ban_hanh") or "").strip(),
            "co_quan_ban_hanh": (meta.get("co_quan_ban_hanh") or "").strip(),
            "tinh_trang_hieu_luc": (meta.get("tinh_trang_hieu_luc") or "").strip(),
            "linh_vuc": (meta.get("linh_vuc") or "").strip(),
            "loai_van_ban": loai_van_ban,
        }

        # Chunk the text
        chunks = structural_chunk(
            text, file_id, page=1,
            department=DEPARTMENT, category=category,
            domain=DOMAIN, filename=filename,
            extra_meta=extra_meta,
        )
        if not chunks and len(text) >= 20:
            chunks = [_make_chunk(text, file_id, 1, 0, DEPARTMENT, category, DOMAIN, filename, extra_meta)]

        if not chunks:
            skipped_no_text += 1
            continue

        processed += 1
        total_chunks += len(chunks)

        # Progress logging
        elapsed = time.time() - start_time
        rate = processed / elapsed if elapsed > 0 else 0
        remaining = (len(content_map) - processed) / rate if rate > 0 else 0

        if dry_run:
            print(
                f"  [DRY-RUN] #{processed:>5} | id={doc_id_int:>6} | "
                f"{len(chunks):>3} chunks | {loai_van_ban:>15} | {_truncate(title, 40)}"
            )
        else:
            # Embed and upsert in batches
            for i in range(0, len(chunks), EMBED_BATCH_SIZE):
                batch = chunks[i : i + EMBED_BATCH_SIZE]
                try:
                    collection.upsert(
                        ids=[c["id"] for c in batch],
                        embeddings=[get_embedding(c["text"]) for c in batch],
                        documents=[c["text"] for c in batch],
                        metadatas=[c["metadata"] for c in batch],
                    )
                except Exception as e:
                    print(f"\n  ERROR upserting doc {doc_id_int}: {e}")
                    # Save checkpoint before bailing
                    save_checkpoint(ingested_ids)
                    raise

            # Mark as ingested
            ingested_ids.add(str(doc_id_int))

            # Periodic checkpoint (every 50 docs)
            if processed % 50 == 0:
                save_checkpoint(ingested_ids)

            print(
                f"  #{processed:>5} | id={doc_id_int:>6} | "
                f"{len(chunks):>3} chunks | {loai_van_ban:>15} | "
                f"ETA {remaining/60:.1f}m | {_truncate(title, 35)}"
            )

    # ------------------------------------------------------------------
    # Step 5: Finalize
    # ------------------------------------------------------------------
    print("-" * 70)
    elapsed_total = time.time() - start_time

    if not dry_run and ingested_ids:
        save_checkpoint(ingested_ids)

    print(f"\n[5/5] Summary")
    print(f"  Documents processed: {processed}")
    print(f"  Total chunks:        {total_chunks}")
    print(f"  Skipped (no text):   {skipped_no_text}")
    print(f"  Time elapsed:        {elapsed_total:.1f}s ({elapsed_total/60:.1f}m)")
    if not dry_run and collection:
        print(f"  ChromaDB total:      {collection.count()}")
    if dry_run:
        est_tokens = total_chunks * 400
        est_cost = est_tokens * 0.02 / 1_000_000
        print(f"\n  [DRY-RUN] Estimated embedding tokens: ~{est_tokens:,}")
        print(f"  [DRY-RUN] Estimated embedding cost:   ~${est_cost:.4f}")

    # Rebuild BM25 index if not dry-run
    if not dry_run and processed > 0:
        print("\n  Rebuilding BM25 index...")
        try:
            from rag.core import rebuild_index
            rebuild_index()
            print("  BM25 index rebuilt successfully.")
        except Exception as e:
            print(f"  Warning: BM25 rebuild failed: {e}")

    print("\nDone!")
    return {"processed": processed, "total_chunks": total_chunks, "skipped_no_text": skipped_no_text}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Ingest Vietnamese Legal Documents from HuggingFace into ChromaDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--max-docs", type=int, default=None, help="Maximum documents to ingest (default: all)")
    parser.add_argument("--effect-status", type=str, default=None, help='Filter by effect status, e.g. "Còn hiệu lực"')
    parser.add_argument(
        "--doc-types",
        type=str,
        nargs="+",
        default=None,
        help='Filter by document types, e.g. "Luật" "Nghị định"',
    )
    parser.add_argument("--linh-vuc", type=str, default=None, help="Filter by linh_vuc (legal field) substring")
    parser.add_argument("--dry-run", action="store_true", help="Preview mode: no API calls, no DB writes")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint (skip already-ingested docs)")
    parser.add_argument("--reset-checkpoint", action="store_true", help="Delete checkpoint file and start fresh")

    args = parser.parse_args()

    if args.reset_checkpoint and os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print(f"Checkpoint deleted: {CHECKPOINT_FILE}")

    ingest_legal_dataset(
        max_docs=args.max_docs,
        effect_status=args.effect_status,
        doc_types=args.doc_types,
        linh_vuc_filter=args.linh_vuc,
        dry_run=args.dry_run,
        resume=args.resume,
    )


if __name__ == "__main__":
    main()
