import pandas as pd
from sentence_transformers import SentenceTransformer
from qdrant_store import upsert_embeddings
from utils import clean_text, chunk_text, setup_logger
import yaml
import sys
import glob
import os
import uuid

def combine_clean_embed(csv_list, config):
    setup_logger()
    # Gabung, clean, dedup
    dfs = [pd.read_csv(f) for f in csv_list]
    all_df = pd.concat(dfs, ignore_index=True)
    # Coba beberapa nama kolom umum
    for col in ['processed_text', 'original_text', 'text', 'Tweet', 'tweet_text', 'isi', 'content']:
        if col in all_df.columns:
            print(f"Menggunakan kolom '{col}' untuk teks.")
            all_df['text'] = all_df[col].apply(clean_text)
            break
    else:
        raise Exception(f"Tidak ditemukan kolom teks di file: {all_df.columns}")
    all_df = all_df.drop_duplicates(subset='text')
    all_df = all_df[all_df['text'].str.strip() != '']  # Hapus baris kosong
    print(f"Combined, cleaned, and deduplicated: {len(all_df)} rows")

    # Embedding & upsert ke Qdrant
    model = SentenceTransformer(config['embedding_model'])
    all_ids, all_texts, all_embeddings, all_metas = [], [], [], []
    for idx, row in all_df.iterrows():
        chunks = chunk_text(row['text'], config['chunk_size'], config['chunk_overlap'])
        for i, chunk in enumerate(chunks):
            chunk_id = str(uuid.uuid4())
            all_ids.append(chunk_id)
            all_texts.append(chunk)
            all_metas.append({**row.to_dict(), 'chunk': i})
    if all_texts:
        all_embeddings = model.encode(all_texts, show_progress_bar=True)
        upsert_embeddings(
            collection_name=config['qdrant_collection'],
            embeddings=all_embeddings,
            texts=all_texts,
            metadatas=all_metas,
            ids=all_ids
        )
    print(f"Stored {len(all_ids)} new chunks in Qdrant.")

if __name__ == '__main__':
    with open('config.yaml') as f:
        config = yaml.safe_load(f)
    # Ambil semua file processed dari backup
    import glob, os
    csv_list = glob.glob('nyobain/backup/*processed*.csv')
    csv_list = [f for f in csv_list if os.path.isfile(f)]
    print("File yang akan diproses:", csv_list)
    if len(sys.argv) > 1:
        csv_list = sys.argv[1:]
    combine_clean_embed(csv_list, config)