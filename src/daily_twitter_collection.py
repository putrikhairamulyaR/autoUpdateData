#!/usr/bin/env python3
"""
Daily Twitter Collection Script
Collects 50 tweets per day with hashtags: #indihome, #telkomIndonesia, #telkom, #gangguanTelkom
"""

import os
import sys
from datetime import datetime
from twitter_fetch import fetch_with_harvest
# from preprocess_twitter_data import preprocess_twitter_data
import yaml
import pandas as pd

def main():
    """Main function to run daily Twitter collection"""
    
    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    print("=" * 60)
    print(f"Daily Twitter Collection - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    try:
        # Fetch tweets with specified hashtags
        df = fetch_with_harvest(config)
        
        # Preprocess tweets
        if len(df) > 0:
            print("\n🔧 Starting text preprocessing...")
            
            # Simple preprocessing
            try:
                df_raw = pd.read_csv('tweets-data/tweets_harvest.csv')
                
                # Find text column
                text_column = None
                for col in df_raw.columns:
                    if 'text' in col.lower() or 'content' in col.lower():
                        text_column = col
                        break
                
                # If no text column found, try 'full_text' specifically
                if text_column is None and 'full_text' in df_raw.columns:
                    text_column = 'full_text'
                
                if text_column:
                    print(f"✅ Using column: {text_column}")
                    
                    # Simple text cleaning
                    def simple_clean_text(text):
                        if not isinstance(text, str):
                            return ""
                        
                        # Convert to lowercase
                        text = text.lower()
                        
                        # Remove URLs
                        import re
                        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
                        
                        # Remove mentions (@username)
                        text = re.sub(r'@\w+', '', text)
                        
                        # Remove hashtags but keep the text
                        text = re.sub(r'#(\w+)', r'\1', text)
                        
                        # Remove numbers
                        text = re.sub(r'\d+', '', text)
                        
                        # Remove all punctuation and special characters
                        import string
                        # Remove punctuation
                        text = text.translate(str.maketrans('', '', string.punctuation))
                        
                        # Remove additional special characters
                        text = re.sub(r'[^\w\s]', '', text)
                        
                        # Remove extra whitespace
                        text = re.sub(r'\s+', ' ', text)
                        
                        # Remove single characters (like 'a', 'i', 'o')
                        text = re.sub(r'\b[a-z]\b', '', text)
                        
                        # Remove extra whitespace again
                        text = re.sub(r'\s+', ' ', text)
                        
                        return text.strip()
                    
                    # Apply preprocessing
                    df_raw['processed_text'] = df_raw[text_column].apply(simple_clean_text)
                    df_processed = df_raw
                    
                    # Save processed data
                    df_processed.to_csv('tweets_processed.csv', index=False)
                    print(f"✅ Preprocessing completed")
                    
                else:
                    print(f"⚠️  No text column found, skipping preprocessing")
                    df_processed = df_raw
                    
            except Exception as e:
                print(f"⚠️  Preprocessing error: {e}")
                df_processed = df_raw
        
        # Display summary
        print("\n" + "=" * 60)
        print("COLLECTION SUMMARY")
        print("=" * 60)
        print(f"Total tweets collected: {len(df)}")
        print(f"Target: 10 tweets every 2 hours")
        print(f"Hashtags monitored: #indihome, #telkomIndonesia, #telkom, #gangguanTelkom")
        print(f"Raw output: tweets-data/tweets_harvest.csv")
        print(f"Processed output: tweets_processed.csv")
        
        if len(df) > 0:
            print(f"\n📝 Sample original tweets:")
            for i, row in df.head(3).iterrows():
                print(f"   {i+1}. {row.get('text', 'N/A')[:80]}...")
            
            if not df_processed.empty and 'processed_text' in df_processed.columns:
                print(f"\n🔧 Sample processed tweets:")
                for i, text in enumerate(df_processed['processed_text'].head(3)):
                    print(f"   {i+1}. {text[:80]}...")
        
        print("\n✅ Collection and preprocessing completed successfully!")
        
    except Exception as e:
        print(f"Error during collection: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 