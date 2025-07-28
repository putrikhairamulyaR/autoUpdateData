from sentence_transformers import SentenceTransformer
from qdrant_store import search_qdrant
import openai
from utils import setup_logger
import yaml

def rag_query(user_query, config):
    setup_logger()
    model = SentenceTransformer(config['embedding_model'])
    query_vec = model.encode([user_query])[0]
    hits = search_qdrant(
        collection_name=config['chroma_collection'],
        query_embedding=query_vec,
        top_k=config['top_k']
    )
    context = '\n'.join([hit.payload['text'] for hit in hits])
    meta_info = '\n'.join([str(hit.payload) for hit in hits])
    openai.api_key = config['openai_api_key']
    prompt = f"""You are an expert assistant. Use the following context and metadata to answer the question as accurately as possible.
Context:
{context}

Metadata:
{meta_info}

Question: {user_query}
Answer (in Bahasa Indonesia):"""
    response = openai.Completion.create(
        engine='text-davinci-003',
        prompt=prompt,
        max_tokens=300
    )
    print(response.choices[0].text.strip())

if __name__ == '__main__':
    with open('nyobain/config.yaml') as f:
        config = yaml.safe_load(f)
    rag_query('Apa perkembangan terbaru AI?', config)