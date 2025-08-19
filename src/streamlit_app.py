import os
import yaml
import streamlit as st
from dotenv import load_dotenv
from src.rag import rag_query


def load_config(config_path: str = 'config.yaml') -> dict:
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def main() -> None:
    load_dotenv('env')
    config = load_config()

    st.set_page_config(page_title="Chatbot RAG", page_icon="🤖", layout="wide")

    with st.sidebar:
        st.subheader("🕘 Riwayat Chat")
        if "chat_history" not in st.session_state:
            st.session_state.chat_history = []
        if len(st.session_state.chat_history) == 0:
            st.caption("Belum ada percakapan.")
        else:
            for i, pair in enumerate(st.session_state.chat_history):
                # pair adalah dict(role, content); tampilkan hanya user messages sebagai ringkasan
                if pair.get("role") == "user":
                    preview = pair.get("content", "").strip().replace("\n", " ")
                    if len(preview) > 80:
                        preview = preview[:80] + "…"
                    st.markdown(f"- {preview}")

        st.divider()
        with st.expander("Konfigurasi Aktif", expanded=False):
            st.json({
                "qdrant_collection": config.get("qdrant_collection"),
                "embedding_model": config.get("embedding_model"),
                "openai_base": os.getenv("OPENAI_API_BASE") or os.getenv("OPENAI_BASE_URL", "(default OpenAI)"),
                "openai_model": config.get("openai_model") or os.getenv("OPENAI_MODEL", "gpt-3.5-turbo"),
            })
        if st.button("🔄 Clear Chat"):
            st.session_state.chat_history = []

    st.title("🤖 Chatbot RAG")
    st.caption("Menjawab berbasis konteks dari Qdrant")

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []  # list[dict(role, content)]

    # Render history
    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Chat input
    user_message = st.chat_input("Ketik pesan dan Enter…")
    if user_message:
        # Tampilkan pesan user
        st.session_state.chat_history.append({"role": "user", "content": user_message})
        with st.chat_message("user"):
            st.markdown(user_message)

        # Dapatkan jawaban
        try:
            with st.spinner("Sedang memproses jawaban…"):
                answer = rag_query(user_message, config)
        except Exception as e:
            answer = f"[ERROR] {e}"

        st.session_state.chat_history.append({"role": "assistant", "content": answer})
        with st.chat_message("assistant"):
            st.markdown(answer)


if __name__ == "__main__":
    main()


