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

STATE_DIR    = Path("data/ingest_state")
BATCH_SIZE   = 512   # x86_64 server: 512 chunks/call is safe (vs 256 on low-RAM devices)
MAX_RETRIES  = 5
RETRY_DELAY  = 10    # seconds

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
        return {"processed_ids": [], "total": 0, "embedded": 0, "status": "idle"}

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
            texts = [r.text for r in batch_records]
            embeddings = self._embed_batch(texts)
            points = [
                PointStruct(id=r.id, vector=emb, payload=r.metadata)
                for r, emb in zip(batch_records, embeddings)
            ]
            upsert_points(client, self.collection, points)

            for r in batch_records:
                processed_ids.add(r.id)

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
                batch_records.append(record)
                if len(batch_records) >= BATCH_SIZE:
                    flush_batch()
            flush_batch()  # flush remainder
            self._state["status"] = "completed"
        except Exception as e:
            self._state["status"] = f"error: {e}"
            raise
        finally:
            self._save_state()
