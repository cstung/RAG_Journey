# backend/datasets/hf_generic.py
"""
Generic HuggingFace dataset adapter driven by a schema mapping config.

Example config:
  dataset_id:    "some/other-dataset"
  hf_config:     "default"
  hf_split:      "train"
  collection:    "vn_other_docs"
  text_field:    "content"          # which column is the main text
  id_field:      "id"               # unique row ID
  metadata_fields:                  # all other fields stored as payload
    - title
    - url
    - category
  filters:                          # optional: only keep rows matching these
    category: ["Thuế", "Kế toán"]
  chunk_strategy: "paragraph"       # paragraph | article | none
  max_chunk_tokens: 700
"""
import hashlib
from typing import Iterator
from datasets import load_dataset
from .base import BaseDatasetConnector, DatasetRecord


class HFGenericConnector(BaseDatasetConnector):

    def __init__(self, config: dict):
        self.cfg = config

    @property
    def collection_name(self) -> str:
        return self.cfg["collection"]

    def total_records(self) -> int:
        ds = load_dataset(self.cfg["dataset_id"],
                          self.cfg.get("hf_config", "default"),
                          split=self.cfg.get("hf_split", "train"))
        return len(ds)

    def _chunk_text(self, text: str, max_chars: int) -> list[str]:
        strategy = self.cfg.get("chunk_strategy", "paragraph")
        if strategy == "none" or len(text) <= max_chars:
            return [text]
        if strategy == "paragraph":
            paras = [p.strip() for p in text.split("\n\n") if p.strip()]
            chunks, buf = [], ""
            for p in paras:
                if len(buf) + len(p) > max_chars:
                    if buf: chunks.append(buf)
                    buf = p
                else:
                    buf = (buf + "\n\n" + p).strip()
            if buf: chunks.append(buf)
            return chunks or [text[:max_chars]]
        return [text]  # fallback

    def iter_records(self) -> Iterator[DatasetRecord]:
        ds = load_dataset(self.cfg["dataset_id"],
                          self.cfg.get("hf_config", "default"),
                          split=self.cfg.get("hf_split", "train"))

        max_tokens = self.cfg.get("max_chunk_tokens", 700)
        max_chars  = int(max_tokens * 3.5)
        text_field = self.cfg["text_field"]
        id_field   = self.cfg.get("id_field", None)
        meta_fields = self.cfg.get("metadata_fields", [])
        row_filters = self.cfg.get("filters", {})

        for i, row in enumerate(ds):
            # Apply filters
            skip = False
            for field, allowed in row_filters.items():
                val = row.get(field, "")
                if isinstance(allowed, list) and val not in allowed:
                    skip = True; break
                elif isinstance(allowed, str) and val != allowed:
                    skip = True; break
            if skip:
                continue

            text = row.get(text_field, "") or ""
            base_id = str(row[id_field]) if id_field else str(i)
            metadata = {f: row.get(f, "") for f in meta_fields}
            metadata["dataset"] = self.cfg["dataset_id"]

            for j, chunk in enumerate(self._chunk_text(text, max_chars)):
                chunk_id = hashlib.md5(f"{base_id}-{j}".encode()).hexdigest()
                yield DatasetRecord(
                    id=chunk_id,
                    text=chunk,
                    metadata={**metadata, "chunk_index": j, "doc_id": base_id},
                )
