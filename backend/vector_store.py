# backend/vector_store.py
"""
Unified vector store wrapper.
All other modules import from here, never directly from qdrant_client.
"""
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct,
    Filter, FieldCondition, MatchValue,
    SparseVectorParams, SparseIndexParams,
)
import os

QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")

# Collection names — one collection per logical dataset
COLLECTION_PDF   = "pdf_docs"       # existing PDF uploads
COLLECTION_LEGAL = "vn_legal_docs"  # HuggingFace legal dataset
# Future: COLLECTION_TAX = "vn_tax_docs", etc.

EMBEDDING_DIM = 1536  # text-embedding-3-small

def get_client() -> QdrantClient:
    return QdrantClient(url=QDRANT_URL)


def ensure_collection(client: QdrantClient, name: str, dim: int = EMBEDDING_DIM):
    """Create collection if it does not exist."""
    existing = [c.name for c in client.get_collections().collections]
    if name not in existing:
        client.create_collection(
            collection_name=name,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
        )


def upsert_points(client: QdrantClient, collection: str, points: list[PointStruct]):
    client.upsert(collection_name=collection, points=points, wait=True)


def search(
    client: QdrantClient,
    collection: str,
    query_vector: list[float],
    top_k: int = 5,
    payload_filter: Filter | None = None,
) -> list:
    return client.search(
        collection_name=collection,
        query_vector=query_vector,
        limit=top_k,
        query_filter=payload_filter,
        with_payload=True,
    )


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
