import pdfplumber
import pandas as pd
import yaml
from utils import clean_text

def extract_text_from_pdf(pdf_path):
    texts = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                texts.extend([line for line in page_text.split('\\n') if line.strip()])
    return texts

def pdf_to_csv(config):
    pdf_path = config['pdf_path']
    texts = extract_text_from_pdf(pdf_path)
    df = pd.DataFrame({'id': [f'pdf_{i}' for i in range(len(texts))],
                       'text': [clean_text(t) for t in texts],
                       'source': 'pdf'})
    df.to_csv('pdf_data.csv', index=False)
    print(f"Extracted {len(df)} lines from {pdf_path} to pdf_data.csv")
    return df

if __name__ == '__main__':
    with open('config.yaml') as f:
        config = yaml.safe_load(f)
    pdf_to_csv(config)
