#!/usr/bin/env python3
"""
Show Comparison Script
Displays clear before/after comparison of preprocessing
"""

import pandas as pd

def show_comparison():
    """Show clear comparison between original and processed text"""
    
    print("=" * 80)
    print("PREPROCESSING COMPARISON")
    print("=" * 80)
    
    try:
        # Read the processed data
        df = pd.read_csv('tweets_processed.csv')
        
        print(f"📊 Total tweets: {len(df)}")
        print()
        
        # Show detailed comparison for each tweet
        for i in range(min(5, len(df))):
            original = df.iloc[i]['original_text']
            processed = df.iloc[i]['processed_text']
            
            print(f"Tweet #{i+1}:")
            print(f"   Original:  {original}")
            print(f"   Processed: {processed}")
            print(f"   Length:    {len(original)} → {len(processed)} chars")
            print(f"   Reduction: {((len(original) - len(processed)) / len(original) * 100):.1f}%")
            print("-" * 80)
        
        # Show statistics
        original_lengths = df['original_text'].str.len()
        processed_lengths = df['processed_text'].str.len()
        
        print(f"📈 OVERALL STATISTICS:")
        print(f"   Average original length: {original_lengths.mean():.1f} characters")
        print(f"   Average processed length: {processed_lengths.mean():.1f} characters")
        print(f"   Average reduction: {((original_lengths.mean() - processed_lengths.mean()) / original_lengths.mean() * 100):.1f}%")
        print(f"   Total reduction: {original_lengths.sum() - processed_lengths.sum()} characters")
        
        print()
        print("✅ Preprocessing successfully removes:")
        print("   - HTML entities (&amp;, &lt;, etc.)")
        print("   - URLs and mentions (@username)")
        print("   - Hashtags (but keeps the text)")
        print("   - Numbers and punctuation")
        print("   - Indonesian stopwords (yang, dan, di, ke, etc.)")
        print("   - Single characters (a, i, o, etc.)")
        print("   - Extra whitespace")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == '__main__':
    show_comparison() 