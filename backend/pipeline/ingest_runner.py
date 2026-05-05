# backend/pipeline/ingest_runner.py
"""
Resumable ingestion runner.
State is persisted to ./data/ingest_state/<collection>.json
so a crashed run restarts from where it stopped.
"""
import json, os, time, asyncio
from pathlib import Path
from openai import OpenAI
from qdrant_client.models import PointStruct
from data_adapters.base import BaseDatasetConnector, DatasetRecord
from vector_store import get_client, ensure_collection, upsert_points, EMBEDDING_DIM
from database import update_ingested_document

STATE_DIR    = Path("data/ingest_state")
BATCH_SIZE   = 512   # x86_64 server: 512 chunks/call is safe (vs 256 on low-RAM devices)
MAX_RETRIES  = 5
RETRY_DELAY  = 10    # seconds
MAX_EMBED_BATCH_ITEMS = 128
MAX_EMBED_BATCH_TOKENS = 250_000

openai_client = OpenAI()


class IngestRunner:

    def __init__(self, connector: BaseDatasetConnector, job_id: str):
        self.connector = connector
        self.collection = connector.collection_name
        self.job_id = job_id
        self.state_file = STATE_DIR / f"{self.job_id}.json"
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        self._state = self._load_state()

    # ── State management ────────────────────────────────────────────────────────

    def _load_state(self) -> dict:
        if self.state_file.exists():
            return json.loads(self.state_file.read_text())
        return {
            "processed_ids": [],
            "total": 0,
            "embedded": 0,
            "status": "idle",
            "docs_scanned": 0,
            "chunks_created": 0,
            "embedded_chunks": 0,
        }

    def _save_state(self):
        self.state_file.write_text(json.dumps(self._state, ensure_ascii=False, indent=2))

    def get_progress(self) -> dict:
        return self._state.copy()

    # ── Embedding ───────────────────────────────────────────────────────────────

    def _embed_batch(self, texts: list[str]) -> list[list[float]]:
        for attempt in range(MAX_RETRIES):
            try:
                resp = openai_client.embeddings.create(
                    model=os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"),
                    input=texts,
                )
                return [item.embedding for item in resp.data]
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    raise
                print(f"[embed] Retry {attempt+1}/{MAX_RETRIES} after error: {e}")
                time.sleep(RETRY_DELAY * (attempt + 1))

    @staticmethod
    def _estimate_tokens(text: str) -> int:
        return max(1, len(text or "") // 4)

    def _build_embedding_batches(self, texts: list[str]) -> list[list[str]]:
        batches: list[list[str]] = []
        current_batch: list[str] = []
        current_tokens = 0
        for text in texts:
            candidate = text or ""
            tokens = self._estimate_tokens(candidate)
            if tokens > MAX_EMBED_BATCH_TOKENS:
                candidate = candidate[: MAX_EMBED_BATCH_TOKENS * 4]
                tokens = self._estimate_tokens(candidate)
            if current_batch and (
                len(current_batch) >= MAX_EMBED_BATCH_ITEMS
                or current_tokens + tokens > MAX_EMBED_BATCH_TOKENS
            ):
                batches.append(current_batch)
                current_batch = []
                current_tokens = 0
            current_batch.append(candidate)
            current_tokens += tokens
        if current_batch:
            batches.append(current_batch)
        return batches

    # ── Main run ────────────────────────────────────────────────────────────────

    def run(self, progress_callback=None):
        """
        Synchronous run — call from a background thread or subprocess.
        progress_callback(state_dict) is called after each batch.
        """
        def emit(status_override: str | None = None):
            state = self._state.copy()
            if status_override:
                state["status"] = status_override
            if progress_callback:
                progress_callback(state)

        client = get_client()
        ensure_collection(client, self.collection, EMBEDDING_DIM)

        # Phase 1: load + filter metadata to get the total count
        self._state["status"] = "loading_metadata"
        self._save_state()
        emit()
        try:
            total = self.connector.total_records()
            self._state["total"] = total
            self._save_state()
            emit()
        except Exception:
            pass  # non-fatal

        # Phase 2: download full content split (can be many GB)
        self._state["status"] = "loading_content"
        self._save_state()
        emit()

        processed_ids = set(self._state.get("processed_ids", []))
        self._state["status"] = "running"
        self._save_state()

        batch_records: list[DatasetRecord] = []

        def flush_batch():
            if not batch_records:
                return
            self._state["status"] = "embedding"
            pending_pairs = [(r.text, r) for r in batch_records]
            for text_batch in self._build_embedding_batches([t for t, _ in pending_pairs]):
                print(f"[hf_legal] Embedding batch: {len(text_batch)} items")
                embeddings = self._embed_batch(text_batch)
                current_pairs = pending_pairs[:len(text_batch)]
                pending_pairs = pending_pairs[len(text_batch):]
                points = [
                    PointStruct(id=record.id, vector=emb, payload=record.metadata)
                    for (_, record), emb in zip(current_pairs, embeddings)
                ]
                upsert_points(client, self.collection, points)
                self._state["embedded_chunks"] += len(text_batch)

            for r in batch_records:
                processed_ids.add(r.id)
                doc_id = str(r.metadata.get("doc_id", ""))
                if doc_id:
                    update_ingested_document(doc_id, {
                        "embedded_count": int(r.metadata.get("chunk_index", 0)) + 1,
                        "status": "embedded",
                        "error": None,
                    })

            self._state["embedded"]       += len(batch_records)
            self._state["processed_ids"]   = list(processed_ids)[-5000:]  # keep last 5k for resume
            self._save_state()
            if progress_callback:
                progress_callback(self._state.copy())
            batch_records.clear()

        try:
            for record in self.connector.iter_records():
                if record.id in processed_ids:
                    continue  # resume: skip already indexed
                self._state["status"] = "chunking"
                self._state["docs_scanned"] += 1
                self._state["chunks_created"] += 1
                batch_records.append(record)
                if len(batch_records) >= BATCH_SIZE:
                    flush_batch()
            flush_batch()  # flush remainder
            self._state["status"] = "completed"
        except Exception as e:
            self._state["status"] = f"error: {e}"
            for r in batch_records:
                doc_id = str(r.metadata.get("doc_id", ""))
                if doc_id:
                    update_ingested_document(doc_id, {"status": "failed", "error": str(e)})
            raise
        finally:
            self._save_state()
