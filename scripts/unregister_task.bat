@echo off
REM unregister_task.bat — Remove STDF_DailyFetch from Windows Task Scheduler

echo.
echo [STDF] Removing Task Scheduler task: STDF_DailyFetch
echo.

schtasks /Delete /TN "STDF_DailyFetch" /F

if %ERRORLEVEL% EQU 0 (
    echo [OK] Task removed.
) else (
    echo [INFO] Task was not found or could not be removed.
)

pause
