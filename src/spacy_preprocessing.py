#!/usr/bin/env python3
"""
Advanced Text Preprocessing with Spacy
Provides better NLP capabilities including lemmatization, POS tagging, and entity recognition
"""

import spacy
import re
import pandas as pd
from typing import List, Optional

class SpacyPreprocessor:
    """Advanced text preprocessing using Spacy"""
    
    def __init__(self, model_name='en_core_web_sm'):
        """
        Initialize spacy preprocessor
        
        Args:
            model_name (str): Spacy model to use
        """
        try:
            self.nlp = spacy.load(model_name)
            print(f"✅ Loaded spacy model: {model_name}")
        except OSError:
            print(f"⚠️  Model {model_name} not found. Installing...")
            import subprocess
            subprocess.run(['python', '-m', 'spacy', 'download', model_name])
            self.nlp = spacy.load(model_name)
    
    def clean_text_basic(self, text: str) -> str:
        """
        Basic text cleaning
        
        Args:
            text (str): Input text
            
        Returns:
            str: Cleaned text
        """
        if not isinstance(text, str):
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove mentions (@username)
        text = re.sub(r'@\w+', '', text)
        
        # Remove hashtags but keep the text
        text = re.sub(r'#(\w+)', r'\1', text)
        
        # Remove numbers
        text = re.sub(r'\d+', '', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def preprocess_with_spacy(self, text: str, 
                             remove_stopwords: bool = True,
                             lemmatize: bool = True,
                             remove_punct: bool = True,
                             remove_entities: bool = True,
                             min_length: int = 2) -> str:
        """
        Advanced preprocessing with spacy
        
        Args:
            text (str): Input text
            remove_stopwords (bool): Remove stopwords
            lemmatize (bool): Lemmatize tokens
            remove_punct (bool): Remove punctuation
            remove_entities (bool): Remove named entities
            min_length (int): Minimum token length
            
        Returns:
            str: Preprocessed text
        """
        if not text:
            return ""
        
        # Basic cleaning first
        text = self.clean_text_basic(text)
        
        # Process with spacy
        doc = self.nlp(text)
        
        # Extract tokens based on criteria
        tokens = []
        for token in doc:
            # Skip if too short
            if len(token.text) < min_length:
                continue
            
            # Skip stopwords if requested
            if remove_stopwords and token.is_stop:
                continue
            
            # Skip punctuation if requested
            if remove_punct and token.is_punct:
                continue
            
            # Skip named entities if requested
            if remove_entities and token.ent_type_:
                continue
            
            # Get lemma or original text
            if lemmatize:
                token_text = token.lemma_
            else:
                token_text = token.text
            
            # Clean the token
            token_text = re.sub(r'[^\w\s]', '', token_text)
            token_text = token_text.strip()
            
            if token_text and len(token_text) >= min_length:
                tokens.append(token_text)
        
        return ' '.join(tokens)
    
    def extract_entities(self, text: str) -> List[str]:
        """
        Extract named entities from text
        
        Args:
            text (str): Input text
            
        Returns:
            List[str]: List of entities
        """
        doc = self.nlp(text)
        entities = []
        for ent in doc.ents:
            entities.append(f"{ent.text} ({ent.label_})")
        return entities
    
    def get_pos_tags(self, text: str) -> List[str]:
        """
        Get POS tags for tokens
        
        Args:
            text (str): Input text
            
        Returns:
            List[str]: List of POS tags
        """
        doc = self.nlp(text)
        pos_tags = []
        for token in doc:
            pos_tags.append(f"{token.text} ({token.pos_})")
        return pos_tags
    
    def preprocess_dataframe(self, df: pd.DataFrame, 
                           text_column: str = 'full_text',
                           new_column: str = 'processed_text',
                           remove_stopwords: bool = True,
                           lemmatize: bool = True,
                           remove_punct: bool = True,
                           remove_entities: bool = True,
                           min_length: int = 2) -> pd.DataFrame:
        """
        Preprocess text in a DataFrame
        
        Args:
            df (pd.DataFrame): Input DataFrame
            text_column (str): Column name containing text
            new_column (str): Column name for processed text
            remove_stopwords (bool): Remove stopwords
            lemmatize (bool): Lemmatize tokens
            remove_punct (bool): Remove punctuation
            remove_entities (bool): Remove named entities
            min_length (int): Minimum token length
            
        Returns:
            pd.DataFrame: DataFrame with processed text
        """
        df_copy = df.copy()
        
        # Apply preprocessing to text column
        df_copy[new_column] = df_copy[text_column].apply(
            lambda x: self.preprocess_with_spacy(
                str(x), 
                remove_stopwords, 
                lemmatize, 
                remove_punct, 
                remove_entities, 
                min_length
            )
        )
        
        return df_copy

def preprocess_tweets_with_spacy(csv_path: str, 
                                output_path: Optional[str] = None,
                                model_name: str = 'en_core_web_sm') -> pd.DataFrame:
    """
    Preprocess tweets using spacy
    
    Args:
        csv_path (str): Path to CSV file
        output_path (str): Path to save processed CSV
        model_name (str): Spacy model to use
        
    Returns:
        pd.DataFrame: Processed DataFrame
    """
    # Read CSV
    df = pd.read_csv(csv_path)
    
    # Initialize preprocessor
    preprocessor = SpacyPreprocessor(model_name=model_name)
    
    # Find text column
    text_column = None
    for col in df.columns:
        if 'text' in col.lower() or 'content' in col.lower():
            text_column = col
            break
    
    if text_column is None and 'full_text' in df.columns:
        text_column = 'full_text'
    
    if text_column is None:
        print("❌ Error: No text column found!")
        return pd.DataFrame()
    
    print(f"✅ Using column: {text_column}")
    
    # Preprocess text
    df_processed = preprocessor.preprocess_dataframe(
        df, 
        text_column=text_column,
        new_column='processed_text',
        remove_stopwords=True,
        lemmatize=True,
        remove_punct=True,
        remove_entities=False,  # Keep entities for analysis
        min_length=2
    )
    
    # Save processed data
    if output_path:
        df_processed.to_csv(output_path, index=False)
        print(f"Processed data saved to: {output_path}")
    
    return df_processed

if __name__ == '__main__':
    # Test the preprocessor
    preprocessor = SpacyPreprocessor()
    
    # Test text
    test_text = "RT @user123: #indihome sangat bagus! 😊 https://t.co/abc123 @telkom"
    
    print("Original text:", test_text)
    print("Basic cleaned:", preprocessor.clean_text_basic(test_text))
    print("Spacy processed:", preprocessor.preprocess_with_spacy(test_text))
    print("Entities:", preprocessor.extract_entities(test_text))
    print("POS tags:", preprocessor.get_pos_tags(test_text)) 