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
from datasets import load_dataset
import pandas as pd
from .base import BaseDatasetConnector, DatasetRecord
from vector_store import COLLECTION_LEGAL


# ── Filter presets ─────────────────────────────────────────────────────────────
DEFAULT_SECTORS = [
    "Lao động", "Thuế", "Tài chính", "Kế toán", "Doanh nghiệp",
    "Đầu tư", "Bảo hiểm", "Ngân hàng", "Thương mại",
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

    def _chunk_text(self, doc_id: str, text: str, metadata: dict) -> list[DatasetRecord]:
        """
        Simple sliding-window chunker for plain-text Vietnamese legal documents.
        Tries to split on paragraph breaks first, then falls back to fixed window.
        """
        max_chars = int(MAX_CHUNK_TOKENS * CHARS_PER_TOKEN)
        min_chars = int(MIN_CHUNK_TOKENS * CHARS_PER_TOKEN)
        ovl_chars = int(OVERLAP_TOKENS   * CHARS_PER_TOKEN)

        # Split on double newlines (paragraph boundaries)
        paragraphs = [p.strip() for p in re.split(r'\n{2,}', text) if p.strip()]

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
        meta = self._load_filtered_metadata()  # uses cache after total_records() call
        
        # Create a fast lookup dictionary from the filtered metadata
        meta_dict = meta.set_index("id").to_dict("index")
        valid_ids = set(meta_dict.keys())

        print(f"[hf_legal] Streaming legacy content... (will filter down to {len(valid_ids):,} docs)")
        
        # Use streaming=True and direct parquet URL to prevent massive RAM/Disk usage
        content_ds = load_dataset("parquet", data_files="hf://datasets/th1nhng0/vietnamese-legal-documents/legacy/content.parquet", split="train", streaming=True)

        matched_count = 0
        for row in content_ds:
            doc_id = str(row["id"])
            if doc_id not in valid_ids:
                continue
                
            matched_count += 1
            meta_row = meta_dict[doc_id]
            
            metadata = {
                "document_number":   str(meta_row.get("document_number") or ""),
                "title":             str(meta_row.get("title") or ""),
                "legal_type":        str(meta_row.get("legal_type") or ""),
                "legal_sectors":     str(meta_row.get("legal_sectors") or ""),
                "issuing_authority": str(meta_row.get("issuing_authority") or ""),
                "issuance_date":     str(meta_row.get("issuance_date") or ""),
                "signers":           str(meta_row.get("signers") or ""),
                "dataset":           "th1nhng0/vietnamese-legal-documents",
                "type":              "legal",
            }
            chunks = self._chunk_text(
                doc_id=doc_id,
                text=str(row.get("content") or ""),
                metadata=metadata,
            )
            yield from chunks
            
            # Optimization: Stop streaming once we've found all our filtered docs
            if matched_count >= len(valid_ids):
                print("[hf_legal] Found all requested documents. Stopping stream.")
                break


