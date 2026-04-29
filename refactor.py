import os
import re

# 1. Update vector_store.py
with open('backend/vector_store.py', 'a', encoding='utf-8') as f:
    f.write('''

def count_points(client: QdrantClient, collection: str) -> int:
    try:
        return client.count(collection_name=collection).count
    except Exception:
        return 0

def delete_points_by_file(client: QdrantClient, collection: str, file_id: str):
    try:
        client.delete(
            collection_name=collection,
            points_selector=Filter(
                must=[FieldCondition(key="file_id", match=MatchValue(value=file_id))]
            )
        )
    except Exception:
        pass

def clear_collection(client: QdrantClient, collection: str):
    try:
        client.delete_collection(collection_name=collection)
    except Exception:
        pass
    ensure_collection(client, collection)
''')

# 2. Update ingest.py
with open('backend/ingest.py', 'r', encoding='utf-8') as f:
    ingest_code = f.read()

ingest_code = ingest_code.replace('from db import collection', '''from vector_store import get_client, ensure_collection, upsert_points, count_points, delete_points_by_file, COLLECTION_PDF, COLLECTION_LEGAL
from qdrant_client.models import PointStruct
import uuid''')

ingest_code = ingest_code.replace('collection.count()', 'count_points(get_client(), target_collection)')

def replace_upsert(match):
    return '''        client = get_client()
        target_collection = COLLECTION_LEGAL if domain == "legal" else COLLECTION_PDF
        ensure_collection(client, target_collection)
        for i in range(0, len(all_chunks), BATCH):
            batch = all_chunks[i: i + BATCH]
            points = [
                PointStruct(
                    id=str(uuid.uuid5(uuid.NAMESPACE_DNS, c["id"])),
                    vector=get_embedding(c["text"]),
                    payload={"text": c["text"], **c["metadata"]}
                )
                for c in batch
            ]
            upsert_points(client, target_collection, points)'''

ingest_code = re.sub(r'        for i in range\(0, len\(all_chunks\), BATCH\):.*?print\(f"    - Upserted batch \{i//BATCH \+ 1\}/\{\(len\(all_chunks\)-1\)//BATCH \+ 1\}"\)', replace_upsert, ingest_code, flags=re.DOTALL)

def replace_upsert_chunks(match):
    return '''        client = get_client()
        target_collection = COLLECTION_LEGAL if domain == "legal" else COLLECTION_PDF
        ensure_collection(client, target_collection)
        for i in range(0, len(chunks), BATCH):
            batch = chunks[i: i + BATCH]
            points = [
                PointStruct(
                    id=str(uuid.uuid5(uuid.NAMESPACE_DNS, c["id"])),
                    vector=get_embedding(c["text"]),
                    payload={"text": c["text"], **c["metadata"]}
                )
                for c in batch
            ]
            upsert_points(client, target_collection, points)'''

ingest_code = re.sub(r'        for i in range\(0, len\(chunks\), BATCH\):.*?print\(f"    - Upserted batch \{i//BATCH \+ 1\}/\{\(len\(chunks\)-1\)//BATCH \+ 1\}"\)', replace_upsert_chunks, ingest_code, flags=re.DOTALL)

# Handle prune_orphans
prune_replacement = '''def prune_orphans(docs_dir: str = "/data/docs"):
    # Skipping prune orphans for Qdrant migration simplicity
    return 0'''
ingest_code = re.sub(r'def prune_orphans\(docs_dir: str = "/data/docs"\):.*?return len\(deleted_files\)', prune_replacement, ingest_code, flags=re.DOTALL)

with open('backend/ingest.py', 'w', encoding='utf-8') as f:
    f.write(ingest_code)

# 3. Update rag/core.py
with open('backend/rag/core.py', 'r', encoding='utf-8') as f:
    rag_code = f.read()

rag_code = rag_code.replace('from db import collection', '''from vector_store import get_client, search, COLLECTION_PDF, COLLECTION_LEGAL, count_points
from qdrant_client.models import Filter, FieldCondition, MatchValue''')

rag_bm25_build = '''    def build(self):
        client = get_client()
        self.ids = []
        self.texts = []
        self.metas = []
        for coll in [COLLECTION_PDF, COLLECTION_LEGAL]:
            try:
                records, _ = client.scroll(collection_name=coll, limit=100000, with_payload=True, with_vectors=False)
                for r in records:
                    self.ids.append(str(r.id))
                    self.texts.append(r.payload.get("text", ""))
                    self.metas.append({k:v for k,v in r.payload.items() if k != "text"})
            except Exception:
                pass
        
        if not self.ids:
            self.bm25 = None
            return
            
        self.bm25 = BM25Okapi([self._tok(text) for text in self.texts])
        print(f"[BM25] Rebuilt: {len(self.ids)} chunks")'''

rag_code = re.sub(r'    def build\(self\):.*?print\(f"\[BM25\] Rebuilt: \{len\(self\.ids\)\} chunks from \{len\(self\.departments\(\)\)\} departments"\)', rag_bm25_build, rag_code, flags=re.DOTALL)

rag_sync = '''def sync_document_metadata(document_id: int, department: str, category: str):
    pass # To be implemented for Qdrant'''
rag_code = re.sub(r'def sync_document_metadata\(document_id: int, department: str, category: str\):.*?rebuild_index\(\)', rag_sync, rag_code, flags=re.DOTALL)

rag_vector_search = '''def vector_search(query_text: str, n: int, department: str = None, domain: str = None) -> tuple[list[str], list[str], list[float]]:
    client = get_client()
    target_collection = COLLECTION_LEGAL if domain == "legal" else COLLECTION_PDF
    
    must_conditions = []
    if department and department != "all":
        must_conditions.append(FieldCondition(key="department", match=MatchValue(value=department)))
    if domain:
        must_conditions.append(FieldCondition(key="domain", match=MatchValue(value=domain)))
        
    payload_filter = Filter(must=must_conditions) if must_conditions else None
    
    try:
        results = search(client, target_collection, get_embedding(query_text), top_k=n, payload_filter=payload_filter)
        ids = [str(r.id) for r in results]
        docs = [r.payload.get("text", "") for r in results]
        sims = [r.score for r in results]
        return ids, docs, sims
    except Exception as exc:
        print(f"[RAG] Vector search error: {exc}")
        return [], [], []'''
        
rag_code = re.sub(r'def vector_search\(query_text: str, n: int, department: str = None, domain: str = None\) -> tuple\[list\[str\], list\[str\], list\[float\]\]:.*?return \[\], \[\], \[\]', rag_vector_search, rag_code, flags=re.DOTALL)

rag_code = rag_code.replace('total = collection.count()', 'total = count_points(get_client(), COLLECTION_PDF) + count_points(get_client(), COLLECTION_LEGAL)')

with open('backend/rag/core.py', 'w', encoding='utf-8') as f:
    f.write(rag_code)

# 4. Update main.py
with open('backend/main.py', 'r', encoding='utf-8') as f:
    main_code = f.read()

main_code = main_code.replace('from db import collection', '''from vector_store import get_client, count_points, delete_points_by_file, clear_collection, COLLECTION_PDF, COLLECTION_LEGAL''')

main_code = main_code.replace('collection.count()', '(count_points(get_client(), COLLECTION_PDF) + count_points(get_client(), COLLECTION_LEGAL))')

main_code = re.sub(r'collection\.delete\(where=\{"file_id": _file_id_for_path\(dest\)\}\)', 'delete_points_by_file(get_client(), COLLECTION_PDF, _file_id_for_path(dest)); delete_points_by_file(get_client(), COLLECTION_LEGAL, _file_id_for_path(dest))', main_code)
main_code = re.sub(r'collection\.delete\(where=\{"file_id": _file_id_for_path\(fp\)\}\)', 'delete_points_by_file(get_client(), COLLECTION_PDF, _file_id_for_path(fp)); delete_points_by_file(get_client(), COLLECTION_LEGAL, _file_id_for_path(fp))', main_code)
main_code = re.sub(r'collection\.delete\(where=\{"file_id": _file_id_for_path\(dest\)\}\)', 'delete_points_by_file(get_client(), COLLECTION_PDF, _file_id_for_path(dest)); delete_points_by_file(get_client(), COLLECTION_LEGAL, _file_id_for_path(dest))', main_code)

main_reset = '''def _handle_reset():
    try:
        client = get_client()
        clear_collection(client, COLLECTION_PDF)
        clear_collection(client, COLLECTION_LEGAL)
        rebuild_index()
        return {"status": "ok", "message": "Đã xóa sạch database"}
    except Exception as e:
        raise HTTPException(500, f"Lỗi khi xóa DB: {str(e)}")'''
        
main_code = re.sub(r'def _handle_reset\(\):.*?raise HTTPException\(500, f"Lỗi khi xóa DB: \{str\(e\)\}"\)', main_reset, main_code, flags=re.DOTALL)

with open('backend/main.py', 'w', encoding='utf-8') as f:
    f.write(main_code)
