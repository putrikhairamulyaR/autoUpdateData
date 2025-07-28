import pandas as pd
from sentence_transformers import SentenceTransformer
import yaml
from utils import clean_text, chunk_text, setup_logger
from qdrant_store import upsert_embeddings

def embed_and_store(csv_path='all_data.csv', config=None):
    setup_logger()
    df = pd.read_csv(csv_path)
    df['text'] = df['text'].apply(clean_text)
    df = df.drop_duplicates(subset='text')
    model = SentenceTransformer(config['embedding_model'])

    all_ids, all_texts, all_embeddings, all_metas = [], [], [], []
    for idx, row in df.iterrows():
        chunks = chunk_text(row['text'], config['chunk_size'], config['chunk_overlap'])
        for i, chunk in enumerate(chunks):
            chunk_id = f"{row['id']}_chunk{i}"
            all_ids.append(chunk_id)
            all_texts.append(chunk)
            all_metas.append({**row.to_dict(), 'chunk': i})
    if all_texts:
        all_embeddings = model.encode(all_texts, show_progress_bar=True)
        upsert_embeddings(
            collection_name=config['chroma_collection'],
            embeddings=all_embeddings,
            texts=all_texts,
            metadatas=all_metas,
            ids=all_ids
        )
    print(f"Stored {len(all_ids)} new chunks in Qdrant.")

if __name__ == '__main__':
    with open('nyobain/config.yaml') as f:
        config = yaml.safe_load(f)
    embed_and_store('all_data.csv', config)