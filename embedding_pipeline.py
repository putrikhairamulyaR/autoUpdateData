import pandas as pd
from sentence_transformers import SentenceTransformer
import yaml
import sys
import glob
import os
import uuid
import pypdf

# Impor fungsi-fungsi pembantu Anda
# Pastikan file-file ini ada di direktori yang sama
from qdrant_store import upsert_embeddings
from utils import clean_text, chunk_text, setup_logger

# FUNGSI EKSTRAKSI PDF (TETAP SAMA)
def extract_text_from_pdf(pdf_path: str) -> str:
    """Membaca file PDF dan mengembalikan seluruh konten teksnya."""
    try:
        reader = pypdf.PdfReader(pdf_path)
        full_text = ""
        for page in reader.pages:
            # Tambahkan spasi antar halaman untuk memastikan kata tidak menyambung
            page_text = page.extract_text()
            if page_text:
                full_text += page_text + "\n\n"
        return full_text
    except Exception as e:
        print(f"Error reading PDF {pdf_path}: {e}")
        return ""

# FUNGSI INTI BARU UNTUK EMBEDDING DAN PENYIMPANAN
def embed_and_store(text_content: str, metadata: dict, model, config):
    """
    Mengambil teks dan metadata, lalu melakukan chunking, embedding, 
    dan upsert ke Qdrant.
    """
    if not text_content or not text_content.strip():
        return 0

    chunks = chunk_text(text_content, config['chunk_size'], config['chunk_overlap'])
    if not chunks:
        return 0

    # Buat ID dan metadata untuk setiap chunk
    chunk_ids = [str(uuid.uuid4()) for _ in chunks]
    chunk_metadatas = []
    for i, chunk in enumerate(chunks):
        # Setiap chunk mendapatkan salinan metadata asli + nomor chunk-nya
        chunk_meta = metadata.copy()
        chunk_meta['chunk_number'] = i
        chunk_meta['chunk_total'] = len(chunks)
        # Hapus kunci 'text' dari metadata jika ada, karena teks sudah disimpan terpisah
        chunk_meta.pop('text', None) 
        chunk_metadatas.append(chunk_meta)
    
    # Generate embeddings untuk semua chunk sekaligus
    embeddings = model.encode(chunks, show_progress_bar=False) # Progress bar bisa diatur per file

    # Upsert ke Qdrant
    upsert_embeddings(
        collection_name=config['qdrant_collection'],
        embeddings=embeddings,
        texts=chunks,
        metadatas=chunk_metadatas,
        ids=chunk_ids
    )
    return len(chunks)

# FUNGSI KHUSUS UNTUK MEMPROSES FILE PDF
def process_pdf_files(pdf_files, model, config):
    """Mengekstrak teks dari setiap PDF dan langsung memprosesnya."""
    print(f"\n--- Memproses {len(pdf_files)} File PDF ---")
    total_chunks_stored = 0
    for pdf_path in pdf_files:
        print(f"Membaca file: {os.path.basename(pdf_path)}...")
        text = extract_text_from_pdf(pdf_path)
        
        if text.strip():
            # Metadata untuk PDF sederhana: hanya nama file sumbernya
            metadata = {'source_file': os.path.basename(pdf_path)}
            chunks_stored = embed_and_store(text, metadata, model, config)
            print(f"-> Berhasil menyimpan {chunks_stored} chunk dari {os.path.basename(pdf_path)}")
            total_chunks_stored += chunks_stored
        else:
            print(f"-> Tidak ada teks yang bisa diekstrak dari {os.path.basename(pdf_path)}, dilewati.")
    
    print(f"--- Selesai Memproses PDF. Total chunk baru: {total_chunks_stored} ---")

# FUNGSI KHUSUS UNTUK MEMPROSES FILE CSV
def process_csv_files(csv_files, model, config):
    """Menggabungkan semua CSV, membersihkan, dan memproses baris per baris."""
    print(f"\n--- Memproses {len(csv_files)} File CSV ---")
    if not csv_files:
        return

    # Gabungkan semua data CSV menjadi satu DataFrame
    df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
    
    # Cari kolom teks yang valid
    text_column = None
    for col in ['processed_text', 'original_text', 'text', 'Tweet', 'tweet_text', 'isi', 'content']:
        if col in df.columns and not df[col].dropna().empty:
            print(f"Menggunakan kolom '{col}' untuk teks dari CSV.")
            text_column = col
            break
            
    if not text_column:
        print("Warning: Tidak ditemukan kolom teks yang valid di file CSV. Proses CSV dilewati.")
        return

    # Bersihkan data
    df['text_cleaned'] = df[text_column].astype(str).apply(clean_text)
    df = df.dropna(subset=['text_cleaned'])
    df = df[df['text_cleaned'].str.strip() != '']
    df = df.drop_duplicates(subset=['text_cleaned'])
    
    print(f"Data CSV digabung dan dibersihkan. Memproses {len(df)} baris unik...")

    total_chunks_stored = 0
    # Proses baris per baris
    for _, row in df.iterrows():
        text = row['text_cleaned']
        # Metadata adalah seluruh baris dari CSV (diubah ke dict)
        metadata = row.to_dict()
        # Ganti nilai NaN dengan None agar kompatibel dengan JSON
        for k, v in metadata.items():
            if pd.isna(v):
                metadata[k] = None
        
        chunks_stored = embed_and_store(text, metadata, model, config)
        total_chunks_stored += chunks_stored
    
    print(f"--- Selesai Memproses CSV. Total chunk baru: {total_chunks_stored} ---")


if __name__ == '__main__':
    setup_logger()
    with open('config.yaml') as f:
        config = yaml.safe_load(f)

    # 1. Load Model Satu Kali
    print("Memuat model embedding...")
    model = SentenceTransformer(config['embedding_model'])
    
    # 2. Temukan semua file
    backup_path = './backup/'
    csv_files = glob.glob(os.path.join(backup_path, '*processed*.csv'))
    pdf_files = glob.glob(os.path.join(backup_path, '*.pdf'))
    
    if not csv_files and not pdf_files:
        print("Warning: Tidak ada file CSV atau PDF yang ditemukan di direktori './backup/'. Keluar.")
        sys.exit()

    # 3. Jalankan proses secara terpisah
    if pdf_files:
        process_pdf_files(pdf_files, model, config)
    
    if csv_files:
        process_csv_files(csv_files, model, config)

    print("\n✅ Semua proses selesai.")