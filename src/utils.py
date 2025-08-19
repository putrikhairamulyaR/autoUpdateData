import re
import logging

def clean_text(text):
    if not isinstance(text, str):
        return ""
    text = re.sub(r"http\\S+", "", text)
    text = re.sub(r"[^\\w\\s]", "", text)
    return text.strip()

def chunk_text(text, chunk_size=256, overlap=32):
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size - overlap):
        chunk = " ".join(words[i:i+chunk_size])
        if chunk:
            chunks.append(chunk)
    return chunks

def setup_logger(logfile='../logs/pipeline.log'):
    logging.basicConfig(filename=logfile, level=logging.INFO,
                        format='%(asctime)s %(levelname)s:%(message)s')