import yaml
import os
import pandas as pd

def run_fetch_twitter():
    print("Fetching Twitter data with tweet-harvest...")
    os.system("python nyobain/twitter_fetch.py")

def run_pdf_extract():
    print("Extracting PDF data...")
    os.system("python nyobain/pdf_extract.py")

def run_combine():
    print("Combining tweets and PDF data...")
    os.system("python nyobain/combine_data.py")

def run_embedding():
    print("Generating embeddings and storing to ChromaDB...")
    os.system("python nyobain/embedding_pipeline.py")

def main():
    run_fetch_twitter()
    run_pdf_extract()
    run_combine()
    run_embedding()
    print("\\nPipeline selesai! Data siap untuk RAG (nyobain/rag.py)\\n")

if __name__ == '__main__':
    main()
