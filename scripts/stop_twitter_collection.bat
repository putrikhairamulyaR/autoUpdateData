@echo off
echo Stopping Twitter Collection Processes...
echo.

echo Method 1: Stopping Python processes...
taskkill /f /im python.exe 2>nul
if %ERRORLEVEL% EQU 0 (
    echo ✅ Python processes stopped successfully
) else (
    echo ℹ️  No Python processes found or already stopped
)

echo.
echo Method 2: Disabling Windows Task Scheduler...
SCHTASKS /CHANGE /TN "Twitter Collection Every 2 Hours" /DISABLE 2>nul
if %ERRORLEVEL% EQU 0 (
    echo ✅ Task Scheduler disabled successfully
) else (
    echo ℹ️  Task Scheduler not found or already disabled
)

echo.
echo Method 3: Deleting Task Scheduler task...
SCHTASKS /DELETE /TN "Twitter Collection Every 2 Hours" /F 2>nul
if %ERRORLEVEL% EQU 0 (
    echo ✅ Task Scheduler deleted successfully
) else (
    echo ℹ️  Task Scheduler not found or already deleted
)

echo.
echo ========================================
echo 🛑 Twitter Collection STOPPED
echo ========================================
echo.
echo To restart:
echo - Continuous mode: python run_continuous_2hours.py
echo - Task Scheduler: create_2hour_task.bat
echo.
pause 