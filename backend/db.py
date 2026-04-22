"""Single shared ChromaDB client — import from here, never create your own."""
import chromadb

_client = chromadb.PersistentClient(path="/data/chroma")

collection = _client.get_or_create_collection(
    name="internal_docs",
    metadata={"hnsw:space": "cosine"}
)
