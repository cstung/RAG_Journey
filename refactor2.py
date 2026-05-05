import os
import re

with open('backend/main.py', 'r', encoding='utf-8') as f:
    main_code = f.read()

# 1. Update imports
imports_addition = '''import asyncio
from concurrent.futures import ThreadPoolExecutor
from fastapi import BackgroundTasks
import json
from pathlib import Path

from datasets import get_connector, REGISTRY
from pipeline import IngestRunner

_executor = ThreadPoolExecutor(max_workers=1)   # one ingestion at a time
_ingest_state: dict = {}                        # in-memory progress cache

class IngestRequest(BaseModel):
    dataset_id: str                              # e.g. "th1nhng0/vietnamese-legal-documents"
    sectors:    list[str] | None = None
    min_year:   int              = 2000
    legal_types: list[str] | None = None
    max_docs:   int | None = None                # set to 100 for smoke test
'''

main_code = main_code.replace('from pydantic import BaseModel\nimport httpx', 'from pydantic import BaseModel\nimport httpx\n' + imports_addition)

fastapi_imports_old = 'from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Depends, Header, Request'
fastapi_imports_new = 'from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Depends, Header, Request, BackgroundTasks'
main_code = main_code.replace(fastapi_imports_old, fastapi_imports_new)

# 2. Replace ChatRequest
chat_request_old = '''class ChatRequest(BaseModel):
    question:   str
    session_id: str | None = None
    department: str = "all"'''

chat_request_new = '''class ChatRequest(BaseModel):
    question:    str
    collections: list[str] | None = None   # NEW: optional override
    filters:     dict | None = None         # NEW: e.g. {"legal_type": "Nghị định"}
    session_id:  str | None = None
    department:  str = "all"'''
main_code = main_code.replace(chat_request_old, chat_request_new)

# 3. Import answer
main_code = main_code.replace('from rag import query as rag_query, rebuild_index, get_departments', 'from rag import answer, query as rag_query, rebuild_index, get_departments')

# 4. Replace chat endpoint logic
chat_endpoint_old = '''    # 2. Run RAG Pipeline
    result = rag_query(
        question=cleaned_question.strip(),
        session_id=req.session_id,
        department=dept
    )'''

chat_endpoint_new = '''    # 2. Run RAG Pipeline
    result = answer(
        question=cleaned_question.strip(),
        collections=req.collections,
        filters=req.filters,
    )
    result["rewritten_query"] = result.get("rewritten_query", cleaned_question.strip())'''
main_code = main_code.replace(chat_endpoint_old, chat_endpoint_new)

# 5. Add new endpoints before /api/health
new_endpoints = '''@app.post("/api/datasets/ingest")
async def trigger_ingest(req: IngestRequest, background_tasks: BackgroundTasks,
                         _=Depends(verify_admin)):
    """Start ingestion in background. Returns immediately with job_id."""
    job_id = f"{req.dataset_id.replace('/', '_')}_{int(time.time())}"

    def run_job():
        try:
            connector = get_connector(
                req.dataset_id,
                sectors=req.sectors,
                min_year=req.min_year,
                legal_types=req.legal_types,
                max_docs=req.max_docs,
            )
            runner = IngestRunner(connector)
            _ingest_state[job_id] = runner.get_progress()
            runner.run(progress_callback=lambda s: _ingest_state.update({job_id: s}))
        except Exception as e:
            _ingest_state[job_id] = {"status": f"error: {e}"}

    background_tasks.add_task(lambda: asyncio.get_event_loop().run_in_executor(_executor, run_job))
    return {"job_id": job_id, "status": "queued"}

@app.get("/api/datasets/status/{job_id}")
async def ingest_status(job_id: str, _=Depends(verify_admin)):
    state = _ingest_state.get(job_id)
    if state is None:
        # Try reading from disk (survive restarts)
        state_file = Path(f"data/ingest_state/{job_id}.json")
        if state_file.exists():
            state = json.loads(state_file.read_text())
        else:
            raise HTTPException(404, "Job not found")
    return state

@app.get("/api/datasets")
async def list_datasets():
    return {"datasets": list(REGISTRY.keys())}

'''

main_code = main_code.replace('@app.get("/api/health")', new_endpoints + '@app.get("/api/health")')

with open('backend/main.py', 'w', encoding='utf-8') as f:
    f.write(main_code)
