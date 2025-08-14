from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels
import numpy as np
import os
import logging
import glob
import pandas as pd

def get_qdrant_client(host="localhost", port=6333):
    return QdrantClient(host=host, port=port)

def upsert_embeddings(collection_name, embeddings, texts, metadatas=None, ids=None, host="localhost", port=6333):
    client = get_qdrant_client(host, port)
    dim = len(embeddings[0])
    # Create collection if not exists
    if collection_name not in [c.name for c in client.get_collections().collections]:
        client.recreate_collection(
            collection_name=collection_name,
            vectors_config=qmodels.VectorParams(size=dim, distance=qmodels.Distance.COSINE)
        )
    payloads = []
    for i, text in enumerate(texts):
        meta = metadatas[i] if metadatas else {}
        payloads.append({"text": text, **meta})
    client.upsert(
        collection_name=collection_name,
        points=[
            qmodels.PointStruct(
                id=ids[i] if ids else i,
                vector=np.array(embeddings[i]).tolist(),
                payload=payloads[i]
            ) for i in range(len(embeddings))
        ]
    )

def search_qdrant(collection_name, query_embedding, top_k=5, host="localhost", port=6333):
    client = get_qdrant_client(host, port)
    hits = client.search(
        collection_name=collection_name,
        query_vector=np.array(query_embedding).tolist(),
        limit=top_k,
        with_payload=True
    )
    return hits

def setup_logger(logfile='logs/pipeline.log'):
    os.makedirs(os.path.dirname(logfile), exist_ok=True)
    logging.basicConfig(filename=logfile, level=logging.INFO,
                        format='%(asctime)s %(levelname)s:%(message)s')

def process_xlsx_files(backup_dir):
    xlsx_list = glob.glob(os.path.join(backup_dir, '*.xlsx'))
    dfs = [pd.read_excel(f) for f in xlsx_list]
    return dfs

def process_csv_files(backup_dir):
    csv_list = glob.glob(os.path.join(backup_dir, '*.csv'))
    dfs = [pd.read_csv(f) for f in csv_list]
    return dfs

if __name__ == '__main__':
    # Contoh utilitas mandiri bila file dijalankan langsung.
    # Tidak dijalankan saat di-import dari modul lain.
    backup_dir = os.path.join(os.path.dirname(__file__), 'backup')
    csv_list = glob.glob(os.path.join(backup_dir, '*processed*.csv'))
    csv_list = [f for f in csv_list if os.path.isfile(f)]
    print("File yang akan diproses:", csv_list)

    if not csv_list:
        print("Tidak ada file CSV yang ditemukan di folder backup/.")
        exit(0)

    dfs = []
    for f in csv_list:
        try:
            dfs.append(pd.read_csv(f))
        except Exception as e:
            print(f"Gagal membaca {f}: {e}")

    if not dfs:
        print("Tidak ada file CSV yang berhasil dibaca.")
        exit(0)

    all_df = pd.concat(dfs, ignore_index=True)
    print(f"Total baris: {len(all_df)}")