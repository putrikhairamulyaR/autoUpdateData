@echo off
echo Creating integrated Twitter pipeline task (every 2 hours)...
echo.

REM Get current directory
set CURRENT_DIR=%cd%

REM Create the scheduled task to run every 2 hours
SCHTASKS /CREATE /SC HOURLY /MO 2 /TN "Integrated Twitter Pipeline" /TR "python integrated_twitter_pipeline.py" /ST 00:00 /RU SYSTEM /F /IT

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo SUCCESS! Integrated pipeline task created.
    echo ========================================
    echo.
    echo Task Name: Integrated Twitter Pipeline
    echo Schedule: Every 2 hours (starting at midnight)
    echo Command: python integrated_twitter_pipeline.py
    echo Working Directory: %CURRENT_DIR%
    echo.
    echo Pipeline Features:
    echo - Collects 10 tweets every 2 hours
    echo - Automatic text preprocessing
    echo - Timestamped backups
    echo - Master file management
    echo.
    echo Schedule Details:
    echo - 00:00 (midnight)
    echo - 02:00
    echo - 04:00
    echo - 06:00
    echo - 08:00
    echo - 10:00
    echo - 12:00 (noon)
    echo - 14:00
    echo - 16:00
    echo - 18:00
    echo - 20:00
    echo - 22:00
    echo.
    echo Output Files:
    echo - backup/tweets_raw_YYYYMMDD_HHMMSS.csv
    echo - backup/tweets_processed_YYYYMMDD_HHMMSS.csv
    echo - tweets_master_raw.csv (accumulated)
    echo - tweets_master_processed.csv (accumulated)
    echo.
    echo To modify the schedule:
    echo 1. Open Task Scheduler (taskschd.msc)
    echo 2. Find "Integrated Twitter Pipeline"
    echo 3. Right-click and select "Properties"
    echo.
    echo To test the task:
    echo 1. Open Task Scheduler
    echo 2. Find "Integrated Twitter Pipeline"
    echo 3. Right-click and select "Run"
    echo.
) else (
    echo.
    echo ========================================
    echo ERROR! Failed to create task.
    echo ========================================
    echo.
    echo Possible reasons:
    echo - Not running as Administrator
    echo - Task already exists
    echo - Invalid path or command
    echo.
    echo Try running this script as Administrator:
    echo Right-click on this file and select "Run as administrator"
    echo.
)

pause 