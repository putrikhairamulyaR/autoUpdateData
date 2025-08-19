import requests
import yaml
import os
from dotenv import load_dotenv
from utils import setup_logger
from sentence_transformers import SentenceTransformer
from src.qdrant_store import search_qdrant

# Load environment variables from local 'env' file if present
load_dotenv('.env')

def rag_query(user_query, config):
    setup_logger()
    # Ambil context dari Qdrant
    try:
        
        model = SentenceTransformer(config['embedding_model'])
        query_vec = model.encode([user_query])[0]
        hits = search_qdrant(
            collection_name=config['qdrant_collection'],
            query_embedding=query_vec,
            top_k=config['top_k']
        )
        # Filter relevansi berbasis skor
        scores = [getattr(h, 'score', None) for h in hits]
        best_score = max([s for s in scores if s is not None], default=None)
        # Defaults lebih longgar agar stabil untuk query pendek
        score_ratio = float(config.get('score_ratio', 0.6))
        min_score = float(config.get('min_score', 0.0))
        filtered_hits = []
        if best_score is not None:
            for h in hits:
                s = getattr(h, 'score', None)
                if s is None:
                    continue
                if s >= best_score * score_ratio and s >= min_score:
                    filtered_hits.append(h)
        else:
            filtered_hits = hits

        # Jika filter terlalu ketat tapi ada hit, ambil top-1 sebagai fallback
        if not filtered_hits and hits:
            filtered_hits = [hits[0]]
        if not filtered_hits:
            return "Tidak ditemukan konteks yang relevan di dokumen. Mohon perjelas pertanyaan atau gunakan kata kunci lain."

        context = '\n'.join([h.payload.get('text', '') for h in filtered_hits if h.payload.get('text')])
        meta_info = '\n'.join([str(h.payload) for h in filtered_hits])
        model_context = f"Konteks berikut adalah satu-satunya sumber jawaban Anda. Jika konteks tidak cukup atau tidak relevan, jawab tepat 'Tidak ditemukan'.\n\n{context}\n\nMetadata:\n{meta_info}\n"
    except Exception as e:
        # Jika gagal mengambil konteks, jangan nebak
        return "Tidak ditemukan konteks yang relevan di dokumen. Mohon perjelas pertanyaan atau gunakan kata kunci lain."
    
    prompt = ("Anda adalah asisten AI yang ahli dalam menganalisis informasi. "
        "Tugas Anda adalah menjawab pertanyaan pengguna secara akurat HANYA berdasarkan 'KONTEN KONTEKS' yang diberikan di bawah. "
        "Sintesis informasi dari beberapa sumber konteks jika perlu untuk memberikan jawaban yang lengkap.\n\n"
        "Jika informasi untuk menjawab pertanyaan tidak ada dalam konteks, jawab dengan tegas: 'Informasi tidak ditemukan dalam dokumen yang diberikan.'\n\n"
        "--- KONTEN KONTEKS ---\n"
        f"{context}\n"
        "--- AKHIR KONTEKS ---\n\n"
        f"Pertanyaan Pengguna: {user_query}\n"
        "Jawaban (dalam Bahasa Indonesia, berdasarkan HANYA dari konteks di atas):")

    # Pakai OpenAI-compatible API (OpenAI atau OpenRouter)
    try:
        from openai import OpenAI
        api_key = config.get('openai_api_key') or os.getenv('OPENAI_API_KEY')
        if not api_key:
            return "[ERROR] OpenAI API key missing. Tambahkan 'openai_api_key' di config.yaml atau set environment variable OPENAI_API_KEY."

        api_base = config.get('openai_api_base') or os.getenv('OPENAI_API_BASE') or os.getenv('OPENAI_BASE_URL')
        if api_base:
            client = OpenAI(api_key=api_key, base_url=api_base)
        else:
            client = OpenAI(api_key=api_key)

        model_name = (
            config.get('openai_model') or os.getenv('OPENAI_MODEL') or 'gpt-3.5-turbo'
        )

        temperature = float(config.get('openai_temperature', os.getenv('OPENAI_TEMPERATURE') or 0.2))
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "Kamu hanya boleh menjawab dari konteks yang diberikan. Jika tidak ada, jawab 'Tidak ditemukan'."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=300,
            temperature=temperature
        )
        # Robust extraction across SDK and providers
        try:
            return response.choices[0].message.content.strip()
        except Exception:
            if isinstance(response, dict):
                content = (
                    response.get('choices', [{}])[0]
                            .get('message', {})
                            .get('content', '')
                )
                if content:
                    return content.strip()
            if isinstance(response, str):
                return response
            return str(response)
    except Exception as e:
        return f"[ERROR] OpenAI API error: {e}"

def print_header():
    print("="*60)
    print("🤖  Chatbot RAG - OpenAI GPT-3.5-turbo")
    print("Ketik 'exit' untuk keluar.")
    print("="*60)

if __name__ == '__main__':
    with open('config.yaml') as f:
        config = yaml.safe_load(f)
    print_header()
    while True:
        user_query = input("\nAnda  : ")
        if user_query.strip().lower() in ['exit', 'quit', 'keluar']:
            print("Bot   : Sampai jumpa! 👋")
            break
        print("Bot    : (memproses...)\n")
        answer = rag_query(user_query, config)
        print(f"Bot   : {answer}")