@echo off
echo ========================================
echo Integrated Twitter Pipeline
echo ========================================
echo.
echo Features:
echo - Collects tweets every 2 hours
echo - Automatic preprocessing
echo - Timestamped backups
echo - Master file management
echo.
echo Starting pipeline...
echo.

cd /d "%~dp0"
python integrated_twitter_pipeline.py

echo.
echo Pipeline stopped. Press any key to exit...
pause >nul 