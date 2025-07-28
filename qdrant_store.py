import chromadb
from chromadb.config import Settings

def get_chroma_collection(collection_name='multi_source', persist_directory='chroma_db'):
    client = chromadb.Client(Settings(
        persist_directory=persist_directory,
        chroma_db_impl="duckdb+parquet"
    ))
    if collection_name in [c.name for c in client.list_collections()]:
        collection = client.get_collection(collection_name)
    else:
        collection = client.create_collection(collection_name)
    return collection