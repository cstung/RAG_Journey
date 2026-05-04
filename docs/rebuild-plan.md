# Rebuild Branch Plan (`dev/rebuild`)

This branch captures an execution-ready breakdown of the proposed LWHN legal RAG implementation for this repository.

## Objectives
- Align backend/frontend architecture to the target FastAPI + React stack split.
- Introduce production-grade ingestion/retrieval/citation-grounded answer flow.
- Add admin workflows for docs/users/logs/crawler.
- Add multilingual KO↔VI support, semantic cache, and operations hardening.

## Phase Execution Map

### Phase 1 — Foundation
1. Scaffold app modules under `backend/app` (`config`, `database`, `models`, `schemas`, `routers`, `services`, `utils`).
2. Add Postgres async SQLAlchemy setup + Alembic baseline migration.
3. Implement auth (`/auth/login`, `/auth/me`) with JWT + bcrypt.
4. Implement embedding + Qdrant collection bootstrap.
5. Implement chunking and ingestion pipeline.
6. Implement retrieval + streaming LLM wrapper.
7. Implement RAG pipeline + `/chat/stream` SSE.
8. Build frontend chat shell (login, sidebar, message stream, citations, disclaimer).

### Phase 2 — Internal Docs + Admin
1. Add encrypted file storage and OCR path.
2. Implement admin docs/user/log routers.
3. Implement admin UI pages and route guard.
4. Add department-scoped retrieval filtering.

### Phase 3 — Korean Multilingual
1. Add translation service wrappers.
2. Add language detection and translation hooks in RAG flow.
3. Persist KO content in message records.
4. Add frontend language toggle and i18n labels.

### Phase 4 — Cache + Crawler
1. Add semantic cache service and model-version-aware invalidation.
2. Integrate cache into RAG query path.
3. Add crawler service + scheduler + admin moderation endpoints.
4. Add frontend crawler queue UI.

### Phase 5 — Hardening
1. Add rate limits, usage/cost tracking, budget alerts.
2. Add backup scripting and health checks.
3. Add graceful upstream error handling.
4. Add prompt-injection safeguards and doc version control.

## Immediate Branch Next Steps
- [ ] Create `backend/app` package skeleton and migrate current modules incrementally.
- [ ] Add `.env.example` keys for model routing, cache controls, crawler email config.
- [ ] Stand up Docker Compose with postgres/qdrant/redis/frontend/backend services.
- [ ] Add acceptance smoke dataset path before full HF ingestion.

## Constraints
- Keep legal-answer citations mandatory and explicit.
- Keep superseded-document warnings in both retrieval metadata and UI rendering.
- Ensure OCR dependencies are lazy-loaded or isolated to avoid startup penalties.
