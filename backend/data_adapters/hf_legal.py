# backend/datasets/hf_legal.py
"""
Connector for th1nhng0/vietnamese-legal-documents

Schema:
  metadata config: id, document_number, title, url, legal_type,
                   legal_sectors, issuing_authority, issuance_date, signers
  content  config: id, content (full markdown)
"""
import re
import hashlib
from typing import Iterator
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
        self._filtered_ids: list[int] | None = None

    @property
    def collection_name(self) -> str:
        return COLLECTION_LEGAL

    # ── Internal helpers ────────────────────────────────────────────────────────

    def _load_filtered_metadata(self) -> pd.DataFrame:
        print("[hf_legal] Loading metadata (~82 MB)...")
        ds   = load_dataset("th1nhng0/vietnamese-legal-documents", "metadata")
        meta = ds["data"].to_pandas()

        # Sector filter
        sector_pattern = "|".join(self.sectors)
        mask = meta["legal_sectors"].str.contains(sector_pattern, na=False, regex=True)

        # Year filter
        meta["_year"] = pd.to_datetime(
            meta["issuance_date"], dayfirst=True, errors="coerce"
        ).dt.year
        mask &= meta["_year"] >= self.min_year

        # Type filter (optional)
        if self.legal_types:
            mask &= meta["legal_type"].isin(self.legal_types)

        filtered = meta[mask].copy()
        if self.max_docs:
            filtered = filtered.head(self.max_docs)

        print(f"[hf_legal] Filtered: {len(filtered):,} documents")
        return filtered

    def _chunk_markdown(self, doc_id: str, markdown: str, metadata: dict) -> list[DatasetRecord]:
        """
        Article-aware chunking strategy for Vietnamese legal Markdown:
          1. Split on Điều / Chương / Mục headings (## markers)
          2. If a section is too long → sliding window
          3. If a section is too short → merge with next
        """
        max_chars = int(MAX_CHUNK_TOKENS * CHARS_PER_TOKEN)
        min_chars = int(MIN_CHUNK_TOKENS * CHARS_PER_TOKEN)
        ovl_chars = int(OVERLAP_TOKENS   * CHARS_PER_TOKEN)

        # Split on markdown headings (##, ###, ####)
        sections = re.split(r'(?=^#{1,4} )', markdown, flags=re.MULTILINE)
        sections = [s.strip() for s in sections if s.strip()]

        records: list[DatasetRecord] = []
        buffer  = ""
        chunk_idx = 0

        def flush(text: str):
            nonlocal chunk_idx
            if not text.strip():
                return
            chunk_id = hashlib.md5(f"{doc_id}-{chunk_idx}".encode()).hexdigest()
            records.append(DatasetRecord(
                id=chunk_id,
                text=text.strip(),
                metadata={**metadata, "chunk_index": chunk_idx, "doc_id": doc_id},
            ))
            chunk_idx += 1

        for section in sections:
            if len(section) > max_chars:
                # Flush buffer first
                flush(buffer); buffer = ""
                # Sliding window on oversized section
                start = 0
                while start < len(section):
                    flush(section[start: start + max_chars])
                    start += max_chars - ovl_chars
            elif len(buffer) + len(section) > max_chars:
                flush(buffer)
                buffer = section
            else:
                buffer += "\n\n" + section if buffer else section

            # Merge short buffers
            if len(buffer) < min_chars:
                continue  # keep accumulating
            else:
                if len(buffer) >= min_chars:
                    flush(buffer); buffer = ""

        flush(buffer)
        return records

    # ── Public interface ────────────────────────────────────────────────────────

    def total_records(self) -> int:
        meta = self._load_filtered_metadata()
        self._filtered_ids = meta["id"].tolist()
        return len(self._filtered_ids)

    def iter_records(self) -> Iterator[DatasetRecord]:
        meta = self._load_filtered_metadata()
        self._filtered_ids = meta["id"].tolist()

        print("[hf_legal] Loading content (~3.6 GB) — this may take a while...")
        content_ds = load_dataset("th1nhng0/vietnamese-legal-documents", "content")
        content_df = content_ds["data"].to_pandas()

        # Join
        df = meta.merge(content_df, on="id", how="inner")
        print(f"[hf_legal] Joined {len(df):,} docs — starting chunking...")

        for _, row in df.iterrows():
            metadata = {
                "document_number":   row.get("document_number", ""),
                "title":             row.get("title", ""),
                "url":               row.get("url", ""),
                "legal_type":        row.get("legal_type", ""),
                "legal_sectors":     row.get("legal_sectors", ""),
                "issuing_authority": row.get("issuing_authority", ""),
                "issuance_date":     row.get("issuance_date", ""),
                "dataset":           "th1nhng0/vietnamese-legal-documents",
                "type":              "legal",
            }
            chunks = self._chunk_markdown(
                doc_id=str(row["id"]),
                markdown=row.get("content", ""),
                metadata=metadata,
            )
            yield from chunks
