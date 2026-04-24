import os
import sys

# Add backend to sys.path
sys.path.append(os.path.join(os.getcwd(), "backend"))

from db import collection
from rag.core import vector_search, _idx, rewrite_query

query = "Tôi đang là nhân viên công ty và muốn xin nghỉ việc thì cần làm những gì?"

print(f"--- RUNNING DEBUG QUERY: {query} ---")

# 1. Test Rewriting
rewritten = rewrite_query(query, history=[])
print(f"REWRITTEN: {rewritten}")

# 2. Test Vector Search
ids, docs, sims = vector_search(rewritten, n=10)
print("\n--- VECTOR SEARCH RESULTS ---")
for i in range(len(ids)):
    meta = collection.get(ids=[ids[i]], include=["metadatas"])["metadatas"][0]
    print(f"SCORE={sims[i]:.4f} | SOURCE={meta.get('filename', '?')} | PREVIEW={docs[i][:100]}...")

# 3. Test BM25
print("\n--- BM25 RESULTS ---")
bm25_hits = _idx.search(rewritten, n=10)
for hit in bm25_hits:
    # hit = (id, meta, score)
    print(f"SCORE={hit[2]:.4f} | SOURCE={hit[1].get('filename', '?')} | PREVIEW={_idx.get_text(hit[0])[:100]}...")
