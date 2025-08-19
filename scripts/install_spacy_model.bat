@echo off
echo Installing Spacy Model...
echo.

echo Installing en_core_web_sm model...
python -m spacy download en_core_web_sm

echo.
echo ========================================
echo ✅ Spacy model installed successfully!
echo ========================================
echo.
echo Available preprocessing features:
echo - Advanced text cleaning
echo - Lemmatization
echo - Stopword removal
echo - POS tagging
echo - Named Entity Recognition
echo - Punctuation removal
echo.
echo Ready to use advanced NLP preprocessing!
pause 