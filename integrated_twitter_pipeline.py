#!/usr/bin/env python3
"""
Integrated Twitter Pipeline
Collects tweets every 2 hours, preprocesses them, and saves automatically
"""

import os
import sys
import time
import schedule
import pandas as pd
from datetime import datetime, timedelta
from twitter_fetch import fetch_with_harvest
import yaml

def integrated_collection_and_preprocessing():
    """Complete pipeline: collect tweets, preprocess, and save"""

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

        # Step 4: Update master files
        master_raw = 'tweets_master_raw.csv'
        master_processed = 'tweets_master_processed.csv'

        def append_and_dedup(existing_path, new_df, dedup_col):
            if os.path.exists(existing_path) and os.path.getsize(existing_path) > 10:
                try:
                    df_existing = pd.read_csv(existing_path)
                    df_all = pd.concat([df_existing, new_df], ignore_index=True)
                    df_all = df_all.drop_duplicates(subset=[dedup_col])
                except Exception as e:
                    print(f"⚠️  Error reading {existing_path}: {e}")
                    df_all = new_df
            else:
                df_all = new_df
            return df_all

        dedup_col_raw = 'id_str' if 'id_str' in df_raw.columns else text_column
        df_master_raw = append_and_dedup(master_raw, df_raw, dedup_col_raw)
        df_master_raw.to_csv(master_raw, index=False)

        dedup_col_processed = 'id' if 'id' in df_processed.columns else 'original_text'
        df_master_processed = append_and_dedup(master_processed, df_processed, dedup_col_processed)
        df_master_processed.to_csv(master_processed, index=False)

        # Step 5: Summary
        print("\n" + "=" * 60)
        print("📊 PIPELINE SUMMARY")
        print("=" * 60)
        print(f"✅ Tweets collected: {len(df_raw)}")
        print(f"✅ Preprocessing completed")
        print(f"✅ Backup saved: {backup_raw}")
        print(f"✅ Backup saved: {backup_processed}")
        print(f"✅ Master files updated")
        print(f"📁 Files created:")
        print(f"   - Raw data: {backup_raw}")
        print(f"   - Processed data: {backup_processed}")
        print(f"   - Master raw: {master_raw}")
        print(f"   - Master processed: {master_processed}")

        if not df_processed.empty and 'processed_text' in df_processed.columns:
            print(f"\n📝 Sample processed tweets:")
            for i, text in enumerate(df_processed['processed_text'].head(3)):
                print(f"   {i+1}. {text[:80]}...")

        next_run = datetime.now() + timedelta(hours=2)
        print(f"\n⏰ Next collection at: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        print("✅ Pipeline completed successfully!")

    except Exception as e:
        print(f"❌ Error in pipeline: {e}")
        import traceback
        traceback.print_exc()

def main():
    print("🚀 Starting Integrated Twitter Pipeline System")
    print("=" * 60)
    print("⏰ Schedule: Every 2 hours")
    print("🎯 Target: 10 tweets per collection")
    print("🏷️  Hashtags: #indihome, #telkomIndonesia, #telkom, #gangguanTelkom")
    print("🔧 Features: Auto collection + preprocessing + backup")
    print("📁 Output: Multiple files with timestamps")
    print("=" * 60)

    os.makedirs('backup', exist_ok=True)
    os.makedirs('tweets-data', exist_ok=True)

    schedule.every(2).hours.do(integrated_collection_and_preprocessing)
    print(f"\n🔄 Running initial pipeline at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    integrated_collection_and_preprocessing()

    print("\n🔄 Scheduler started. Press Ctrl+C to stop.")
    print("⏰ Will run every 2 hours automatically...")
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Stopping integrated Twitter pipeline...")
        print("👋 Goodbye!")
        sys.exit(0)
