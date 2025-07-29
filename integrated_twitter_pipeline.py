#!/usr/bin/env python3
"""
Integrated Twitter Pipeline
Collects tweets, preprocesses, embeds, and upserts to Qdrant
"""

import os
import sys
import time
import pandas as pd
from datetime import datetime
from twitter_fetch import fetch_with_harvest
import yaml
from sentence_transformers import SentenceTransformer
from utils import clean_text, chunk_text
from qdrant_store import upsert_embeddings
import uuid
import schedule

def integrated_collection_and_preprocessing():
    """Complete pipeline: collect tweets, preprocess, embed, and upsert to Qdrant"""

    print("\n" + "=" * 60)
    print(f"🔄 Integrated Twitter Pipeline - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
        with open(config_path) as f:
            config = yaml.safe_load(f)

        # Step 1: Collect tweets
        print("📥 Step 1: Collecting tweets...")
        df_raw = fetch_with_harvest(config)

        if df_raw.empty:
            print("⚠️  No tweets collected, skipping preprocessing")
            return

        print(f"✅ Collected {len(df_raw)} tweets")

        # Step 2: Preprocess tweets
        print("\n🔧 Step 2: Preprocessing tweets...")

        text_column = None
        for col in df_raw.columns:
            if 'text' in col.lower() or 'content' in col.lower():
                text_column = col
                break
        if text_column is None and 'full_text' in df_raw.columns:
            text_column = 'full_text'

        if text_column:
            print(f"✅ Using column: {text_column}")

            def better_clean_text(text):
                if not isinstance(text, str):
                    return ""
                import re, string
                text = text.lower()
                text = re.sub(r'&amp;', 'dan', text)
                text = re.sub(r'&lt;|&gt;|&quot;|&#39;', '', text)
                text = re.sub(r'http\S+', '', text)
                text = re.sub(r'@\w+', '', text)
                text = re.sub(r'#(\w+)', r'\1', text)
                text = re.sub(r'\d+', '', text)
                text = text.translate(str.maketrans('', '', string.punctuation))
                text = re.sub(r'[^\w\s]', '', text)
                text = re.sub(r'\s+', ' ', text)
                text = re.sub(r'\b[a-z]\b', '', text)
                stopwords = [
                    'yang','dan','di','ke','dari','untuk','dengan','ini','itu','atau','juga','bisa',
                    'akan','sudah','masih','belum','tidak','bukan','ada','saya','kamu','dia','mereka',
                    'kami','kita','anda','nya','lah','kah','tah','pun','per','para','oleh','kepada',
                    'terhadap','antara','dalam','atas','bawah','depan','belakang','samping','luar',
                    'sebelah','setelah','sebelum','ketika','sambil','selama','hingga','sampai','sejak',
                    'karena','jika','kalau','meskipun','walaupun','sehingga','agar','supaya','seperti',
                    'bagai','sebagai','adalah','ialah','yaitu','yakni','diantara','didalam','keluar',
                    'dikeluarkan','masuk','dimasukkan'
                ]
                words = text.split()
                filtered = [w for w in words if w not in stopwords]
                return ' '.join(filtered).strip()

            df_raw['processed_text'] = df_raw[text_column].apply(better_clean_text)

            df_processed = pd.DataFrame({
                'id': df_raw['id_str'] if 'id_str' in df_raw.columns else range(len(df_raw)),
                'original_text': df_raw[text_column],
                'processed_text': df_raw['processed_text'],
                'created_at': df_raw['created_at'] if 'created_at' in df_raw.columns else '',
                'username': df_raw['username'] if 'username' in df_raw.columns else '',
                'retweet_count': df_raw.get('retweet_count', 0),
                'favorite_count': df_raw.get('favorite_count', 0)
            })

            print("✅ Preprocessing completed")

        else:
            print("⚠️  No text column found, skipping preprocessing")
            df_processed = df_raw

        # Step 3: Create timestamped backup
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        os.makedirs('backup', exist_ok=True)
        backup_raw = f"backup/tweets_raw_{timestamp}.csv"
        backup_processed = f"backup/tweets_processed_{timestamp}.csv"

        df_raw.to_csv(backup_raw, index=False)
        df_processed.to_csv(backup_processed, index=False)

        # Step 4: Embedding & upsert ke Qdrant
        if not df_processed.empty and 'processed_text' in df_processed.columns:
            print("\n🚀 Langsung embedding dan upsert ke Qdrant...")
            df_embed = df_processed.copy()
            df_embed['text'] = df_embed['processed_text']
            df_embed = df_embed.drop_duplicates(subset='text')
            model = SentenceTransformer(config['embedding_model'])
            all_ids, all_texts, all_metas = [], [], []
            for idx, row in df_embed.iterrows():
                from utils import chunk_text
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

        print("\n✅ Pipeline completed successfully!")

    except Exception as e:
        print(f"❌ Error in pipeline: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    # Jalankan sekali saat start
    integrated_collection_and_preprocessing()
    # Jadwalkan setiap 2 jam
    schedule.every(2).hours.do(integrated_collection_and_preprocessing)
    print("Scheduler aktif, pipeline akan jalan setiap 2 jam.")
    while True:
        schedule.run_pending()
        time.sleep(60)
