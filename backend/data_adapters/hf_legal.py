# backend/data_adapters/hf_legal.py
"""
Connector for th1nhng0/vietnamese-legal-documents

Uses the 'legacy' config which has English field names and plain-text content:
  legacy/metadata split: id, document_number, title, legal_type, legal_sectors,
                         issuing_authority, issuance_date (YYYY-MM-DD), signers, ...
  legacy/content split:  id, content (plain text)

518k documents total — much broader coverage than the 'metadata' config.
"""
import re
import hashlib
from typing import Iterator
import datasets
from datasets import load_dataset, Features, Value
import pandas as pd
import pyarrow.parquet as pq
from huggingface_hub import hf_hub_download
import os
from .base import BaseDatasetConnector, DatasetRecord
from vector_store import COLLECTION_LEGAL


# ── Filter presets ─────────────────────────────────────────────────────────────
DEFAULT_SECTORS = [
    "Employment - Wages",
    "Government finance",
    "Taxes - Fees - Charges",
    "Business",
    "Investment",
    "Accounting",
    "Insurance",
    "Trade",
]
DEFAULT_MIN_YEAR = 2000

# ── Chunking constants ──────────────────────────────────────────────────────────
MAX_CHUNK_TOKENS   = 700    # ~700 tokens ≈ safe context window for retrieval
MIN_CHUNK_TOKENS   = 80     # merge chunks shorter than this
OVERLAP_TOKENS     = 80     # overlap between sliding-window chunks
CHARS_PER_TOKEN    = 3.5    # rough estimate for Vietnamese


class VNLegalDocumentConnector(BaseDatasetConnector):

    def __init__(
        self,
        sectors: list[str] | None = None,
        min_year: int = DEFAULT_MIN_YEAR,
        legal_types: list[str] | None = None,   # e.g. ["Nghị định", "Thông tư"]
        max_docs:  int | None = None,            # cap for testing
    ):
        self.sectors     = sectors or DEFAULT_SECTORS
        self.min_year    = min_year
        self.legal_types = legal_types
        self.max_docs    = max_docs
        self._filtered_df: pd.DataFrame | None = None   # cached after first load

    @property
    def collection_name(self) -> str:
        return COLLECTION_LEGAL

    # ── Internal helpers ────────────────────────────────────────────────────────

    def _load_filtered_metadata(self) -> pd.DataFrame:
        """Load and cache the filtered metadata. Avoids double-download."""
        if self._filtered_df is not None:
            return self._filtered_df

        print("[hf_legal] Loading legacy metadata (~518k docs)...")
        datasets.utils.logging.set_verbosity_info()
        datasets.utils.logging.enable_progress_bar()
        # Use direct parquet load to avoid downloading the 4GB content file just for metadata
        ds   = load_dataset("parquet", data_files="hf://datasets/th1nhng0/vietnamese-legal-documents/legacy/metadata.parquet", split="train")
        meta = ds.to_pandas()

        # Sector filter — legal_sectors is a string field in the legacy config
        sector_pattern = "|".join(re.escape(s) for s in self.sectors)
        mask = meta["legal_sectors"].fillna("").str.contains(sector_pattern, regex=True)

        # Year filter — dates are YYYY-MM-DD in legacy config
        meta["_year"] = pd.to_datetime(
            meta["issuance_date"], errors="coerce"
        ).dt.year
        mask &= meta["_year"] >= self.min_year

        # Type filter (optional)
        if self.legal_types:
            mask &= meta["legal_type"].isin(self.legal_types)

        filtered = meta[mask].copy()
        if self.max_docs:
            filtered = filtered.head(self.max_docs)

        print(f"[hf_legal] Filtered: {len(filtered):,} documents")
        self._filtered_df = filtered
        return filtered

    def _load_content_direct(self) -> "pyarrow.Table":
        """
        Download content.parquet once via huggingface_hub and read with PyArrow.
        Bypasses datasets library cast issue with large_string columns.
        File is cached at HF_HOME so subsequent calls are instant.
        """
        print("[hf_legal] Downloading content parquet directly (large_string safe)...")
        parquet_path = hf_hub_download(
            repo_id="th1nhng0/vietnamese-legal-documents",
            filename="data/content.parquet",
            repo_type="dataset",
            cache_dir=os.getenv("HF_HOME", "/app/data/hf_cache"),
        )
        print(f"[hf_legal] Reading parquet from: {parquet_path}")
        table = pq.read_table(parquet_path)
        print(f"[hf_legal] Parquet schema: {table.schema}")
        return table

    def _chunk_markdown(self, doc_id: str, markdown: str, metadata: dict) -> list[DatasetRecord]:
        """
        Simple sliding-window chunker for plain-text Vietnamese legal documents.
        Tries to split on paragraph breaks first, then falls back to fixed window.
        """
        max_chars = int(MAX_CHUNK_TOKENS * CHARS_PER_TOKEN)
        min_chars = int(MIN_CHUNK_TOKENS * CHARS_PER_TOKEN)
        ovl_chars = int(OVERLAP_TOKENS   * CHARS_PER_TOKEN)

        # Split on double newlines (paragraph boundaries)
        paragraphs = [p.strip() for p in re.split(r'\n{2,}', markdown) if p.strip()]

        records: list[DatasetRecord] = []
        buffer  = ""
        chunk_idx = 0

        def flush(text_block: str):
            nonlocal chunk_idx
            if not text_block.strip():
                return
            chunk_id = hashlib.md5(f"{doc_id}-{chunk_idx}".encode()).hexdigest()
            records.append(DatasetRecord(
                id=chunk_id,
                text=text_block.strip(),
                metadata={**metadata, "chunk_index": chunk_idx, "doc_id": doc_id},
            ))
            chunk_idx += 1

        for para in paragraphs:
            if len(para) > max_chars:
                flush(buffer); buffer = ""
                # Sliding window on oversized paragraph
                start = 0
                while start < len(para):
                    flush(para[start: start + max_chars])
                    start += max_chars - ovl_chars
            elif len(buffer) + len(para) > max_chars:
                flush(buffer)
                buffer = para
            else:
                buffer += "\n\n" + para if buffer else para

            if len(buffer) >= min_chars:
                flush(buffer); buffer = ""

        flush(buffer)
        return records

    # ── Public interface ────────────────────────────────────────────────────────

    def total_records(self) -> int:
        meta = self._load_filtered_metadata()
        return len(meta)

    def iter_records(self) -> Iterator[DatasetRecord]:
        meta = self._load_filtered_metadata()
        filtered_id_set = set(meta["id"].tolist())
        meta_lookup = meta.set_index("id").to_dict("index")
        total_needed = len(filtered_id_set)
        found = 0

        table = self._load_content_direct()

        # Detect actual column name (content vs content_html)
        content_col = "content" if "content" in table.schema.names else "content_html"
        id_col = "id"
        print(f"[hf_legal] Using columns: id='{id_col}', content='{content_col}'")
        print(f"[hf_legal] Streaming {table.num_rows:,} rows for {total_needed} docs...")

        for batch in table.to_batches(max_chunksize=1000):
            batch_ids = batch.column(id_col).to_pylist()
            batch_contents = batch.column(content_col).to_pylist()

            for row_id, content in zip(batch_ids, batch_contents):
                # Normalize id type (parquet may store as string or int)
                norm_id = int(row_id) if isinstance(row_id, str) and row_id.isdigit() else row_id
                if norm_id not in filtered_id_set:
                    continue

                found += 1
                if found % 10 == 0 or found == total_needed:
                    print(f"[hf_legal] Content scan: {found}/{total_needed} docs found")

                meta_row = meta_lookup[norm_id]
                metadata = {
                    "document_number":   meta_row.get("document_number", ""),
                    "title":             meta_row.get("title", ""),
                    "url":               meta_row.get("url", ""),
                    "legal_type":        meta_row.get("legal_type", ""),
                    "legal_sectors":     meta_row.get("legal_sectors", ""),
                    "issuing_authority": meta_row.get("issuing_authority", ""),
                    "issuance_date":     meta_row.get("issuance_date", ""),
                    "dataset":           "th1nhng0/vietnamese-legal-documents",
                    "type":              "legal",
                }

                print(f"[hf_legal] Processing doc {norm_id} | content_html len: {len(content or '')}")
                
                from ingest import extract_html_text
                parsed_text = extract_html_text(content or "")
                
                print(f"[hf_legal] Parsed text len: {len(parsed_text)} | Sample: {parsed_text[:100]}...")

                chunks = self._chunk_markdown(
                    doc_id=str(norm_id),
                    markdown=parsed_text,
                    metadata=metadata,
                )
                
                num_chunks = len(chunks)
                print(f"[hf_legal] Created {num_chunks} chunks for doc {norm_id}")
                
                yield from chunks

                filtered_id_set.discard(norm_id)
                if not filtered_id_set:
                    print(f"[hf_legal] All {total_needed} docs found. Done.")
                    return
