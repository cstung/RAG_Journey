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
from datetime import datetime
import datasets
from datasets import load_dataset
import pandas as pd
import pyarrow.parquet as pq
from huggingface_hub import hf_hub_download
import os
from .base import BaseDatasetConnector, DatasetRecord
from vector_store import COLLECTION_LEGAL
from database import upsert_ingested_document, update_ingested_document

DATASET_ID = "th1nhng0/vietnamese-legal-documents"
METADATA_CONFIG = "metadata"
CONTENT_CONFIG = "content"
SPLIT = "data"

FIELD_ID = "id"
FIELD_DOC_NUMBER = "so_ky_hieu"
FIELD_TITLE = "title"
FIELD_URL = "url"
FIELD_LEGAL_TYPE = "loai_van_ban"
FIELD_SECTOR = "linh_vuc"
FIELD_INDUSTRY = "nganh"
FIELD_AUTHORITY = "co_quan_ban_hanh"
FIELD_ISSUANCE_DATE = "ngay_ban_hanh"
FIELD_EFFECT_STATUS = "tinh_trang_hieu_luc"
FIELD_MAP = {
    "document_number": FIELD_DOC_NUMBER,
    "issuance_date": FIELD_ISSUANCE_DATE,
    "legal_type": FIELD_LEGAL_TYPE,
    "legal_sectors": [FIELD_SECTOR, FIELD_INDUSTRY],
    "issuing_authority": FIELD_AUTHORITY,
}

# ── Filter presets ─────────────────────────────────────────────────────────────
DEFAULT_SECTORS = None
DEFAULT_MIN_YEAR = 2000

# ── Chunking constants ──────────────────────────────────────────────────────────
MAX_CHUNK_TOKENS   = 700    # ~700 tokens ≈ safe context window for retrieval
MIN_CHUNK_TOKENS   = 80     # merge chunks shorter than this
OVERLAP_TOKENS     = 80     # overlap between sliding-window chunks
CHARS_PER_TOKEN    = 3.5    # rough estimate for Vietnamese
MIN_CHUNK_LENGTH = 100
MAX_CHUNK_LENGTH = 2000
SKIPPED_STATUSES = {"skipped", "missing_content"}


class VNLegalDocumentConnector(BaseDatasetConnector):

    def __init__(
        self,
        sectors: list[str] | None = DEFAULT_SECTORS,
        min_year: int = DEFAULT_MIN_YEAR,
        legal_types: list[str] | None = None,   # e.g. ["Nghị định", "Thông tư"]
        max_docs:  int | None = None,            # cap for testing
    ):
        self.sectors     = sectors
        self.min_year    = min_year
        self.legal_types = legal_types
        self.max_docs    = max_docs
        self._filtered_df: pd.DataFrame | None = None   # cached after first load

    @property
    def collection_name(self) -> str:
        return COLLECTION_LEGAL

    # ── Internal helpers ────────────────────────────────────────────────────────

    def _require_columns(self, df: pd.DataFrame, columns: list[str]) -> None:
        missing = [col for col in columns if col not in df.columns]
        if missing:
            raise ValueError(
                f"Dataset schema mismatch. Missing columns: {missing}. "
                f"Available columns: {list(df.columns)}"
            )

    def _load_filtered_metadata(self) -> pd.DataFrame:
        """Load and cache filtered metadata using the dataset's official metadata config."""
        if self._filtered_df is not None:
            return self._filtered_df

        print("[hf_legal] Loading metadata from HF dataset config 'metadata'...")
        datasets.utils.logging.set_verbosity_info()
        datasets.utils.logging.enable_progress_bar()

        ds = load_dataset(DATASET_ID, METADATA_CONFIG, split=SPLIT)
        meta = ds.to_pandas()

        self._require_columns(meta, [FIELD_ID, FIELD_ISSUANCE_DATE, FIELD_LEGAL_TYPE])

        mask = pd.Series(True, index=meta.index)
        if self.sectors:
            if FIELD_SECTOR not in meta.columns and FIELD_INDUSTRY not in meta.columns:
                raise ValueError(
                    f"Dataset schema mismatch. Missing columns: {[FIELD_SECTOR, FIELD_INDUSTRY]}. "
                    f"Available columns: {list(meta.columns)}"
                )
            sector_source = meta[FIELD_SECTOR].fillna("").astype(str) if FIELD_SECTOR in meta.columns else ""
            industry_source = meta[FIELD_INDUSTRY].fillna("").astype(str) if FIELD_INDUSTRY in meta.columns else ""
            sector_pattern = "|".join(re.escape(s) for s in self.sectors)
            mask &= (
                (sector_source.str.contains(sector_pattern, regex=True, case=False) if FIELD_SECTOR in meta.columns else False)
                | (industry_source.str.contains(sector_pattern, regex=True, case=False) if FIELD_INDUSTRY in meta.columns else False)
            )

        meta["_year"] = pd.to_datetime(
            meta[FIELD_ISSUANCE_DATE].astype(str),
            format="%d/%m/%Y",
            errors="coerce",
        ).dt.year
        mask &= meta["_year"] >= self.min_year

        if self.legal_types and FIELD_LEGAL_TYPE in meta.columns:
            mask &= meta[FIELD_LEGAL_TYPE].isin(self.legal_types)

        filtered = meta[mask].copy()
        if self.max_docs:
            filtered = filtered.head(self.max_docs)

        print(f"[hf_legal] Filtered: {len(filtered):,} documents")
        for _, row in filtered.iterrows():
            upsert_ingested_document({
                "id": str(row.get(FIELD_ID)),
                "dataset_id": DATASET_ID,
                "so_ky_hieu": row.get(FIELD_DOC_NUMBER),
                "loai_van_ban": row.get(FIELD_LEGAL_TYPE),
                "linh_vuc": row.get(FIELD_SECTOR) or row.get(FIELD_INDUSTRY),
                "co_quan_ban_hanh": row.get(FIELD_AUTHORITY),
                "ngay_ban_hanh": row.get(FIELD_ISSUANCE_DATE),
                "status": "pending",
            })
        self._filtered_df = filtered
        return filtered

    def _load_content_direct(self) -> str:
        """Download content parquet directly and return local cached path."""
        print("[hf_legal] Downloading content parquet directly (cached by HF_HOME)...")
        parquet_path = hf_hub_download(
            repo_id=DATASET_ID,
            filename="data/content.parquet",
            repo_type="dataset",
            cache_dir=os.getenv("HF_HOME", "/app/data/hf_cache"),
        )
        print(f"[hf_legal] Content parquet path: {parquet_path}")
        return parquet_path

    def _chunk_markdown(self, doc_id: str, markdown: str, metadata: dict) -> list[DatasetRecord]:
        """
        Simple sliding-window chunker for plain-text Vietnamese legal documents.
        Tries to split on paragraph breaks first, then falls back to fixed window.
        """
        text = (markdown or "").strip()
        if len(text) < MIN_CHUNK_LENGTH:
            return []
        max_chars = min(int(MAX_CHUNK_TOKENS * CHARS_PER_TOKEN), MAX_CHUNK_LENGTH)
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
        filtered_id_set = set(meta[FIELD_ID].tolist())
        meta_lookup = meta.set_index(FIELD_ID).to_dict("index")
        total_needed = len(filtered_id_set)
        found = 0

        parquet_path = self._load_content_direct()
        pf = pq.ParquetFile(parquet_path)

        schema_names = pf.schema.names
        if "id" in schema_names:
            id_col = "id"
        elif "doc_id" in schema_names:
            id_col = "doc_id"
        else:
            raise RuntimeError(f"Unsupported content schema columns: {schema_names}")

        if "content" in schema_names:
            content_col = "content"
        elif "content_html" in schema_names:
            content_col = "content_html"
        else:
            raise RuntimeError(f"Unsupported content schema columns: {schema_names}")

        print(f"[hf_legal] Using columns: id='{id_col}', content='{content_col}'")
        print(f"[hf_legal] Streaming row groups for {total_needed} docs...")

        for batch in pf.iter_batches(batch_size=1000, columns=[id_col, content_col]):
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

                meta_row = meta_lookup.get(norm_id) or meta_lookup.get(str(norm_id))
                if not meta_row:
                    continue
                metadata = {
                    "document_number":   meta_row.get(FIELD_DOC_NUMBER, ""),
                    "title":             meta_row.get(FIELD_TITLE, ""),
                    "url":               meta_row.get(FIELD_URL, ""),
                    "legal_type":        meta_row.get(FIELD_LEGAL_TYPE, ""),
                    "legal_sectors":     meta_row.get(FIELD_SECTOR, ""),
                    "industry":          meta_row.get(FIELD_INDUSTRY, ""),
                    "issuing_authority": meta_row.get(FIELD_AUTHORITY, ""),
                    "issuance_date":     meta_row.get(FIELD_ISSUANCE_DATE, ""),
                    "effect_status":     meta_row.get(FIELD_EFFECT_STATUS, ""),
                    "dataset":           DATASET_ID,
                    "type":              "legal",
                }

                print(f"[hf_legal] Processing doc {norm_id} | content_html len: {len(content or '')}")
                from ingest import extract_html_text
                parsed_text = extract_html_text(content or "")
                if not parsed_text.strip():
                    print(f"[hf_legal] Empty parsed text | raw length={len(content or '')}")
                update_ingested_document(str(norm_id), {
                    "content_length": len(content or ""),
                    "parsed_length": len(parsed_text or ""),
                    "status": "parsed" if parsed_text.strip() else "skipped",
                    "error": None if parsed_text.strip() else "empty parsed text",
                })
                
                print(f"[hf_legal] Parsed text len: {len(parsed_text)} | Sample: {parsed_text[:100]}...")

                chunks = self._chunk_markdown(
                    doc_id=str(norm_id),
                    markdown=parsed_text,
                    metadata=metadata,
                )
                
                num_chunks = len(chunks)
                print(f"[hf_legal] Created {num_chunks} chunks for doc {norm_id}")
                if num_chunks:
                    print(f"[hf_legal] Total chunks: {num_chunks}")
                update_ingested_document(str(norm_id), {
                    "chunk_count": num_chunks,
                    "status": "chunked" if num_chunks > 0 else "skipped",
                    "error": None if num_chunks > 0 else (
                        "parsed text below min chunk length"
                        if len((parsed_text or "").strip()) < MIN_CHUNK_LENGTH
                        else "no chunks created"
                    ),
                })
                
                yield from chunks

                filtered_id_set.discard(norm_id)
                if not filtered_id_set:
                    print(f"[hf_legal] All {total_needed} docs found. Done.")
                    return

        if filtered_id_set:
            print(f"[hf_legal] Missing content for {len(filtered_id_set)} metadata rows")
            for missing_id in filtered_id_set:
                update_ingested_document(str(missing_id), {
                    "status": "missing_content",
                    "error": "content not found in content split",
                })
