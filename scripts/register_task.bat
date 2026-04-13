@echo off
REM register_task.bat — Register STDF_DailyFetch in Windows Task Scheduler
REM Must be run as Administrator.

setlocal

REM Resolve the project root (parent of this scripts\ directory)
set "SCRIPTS_DIR=%~dp0"
for %%i in ("%SCRIPTS_DIR%..") do set "PROJECT_ROOT=%%~fi"
set "PS_SCRIPT=%PROJECT_ROOT%\scripts\daily_fetch.ps1"

echo.
echo [STDF] Registering Task Scheduler task: STDF_DailyFetch
echo        Script : %PS_SCRIPT%
echo        Trigger: Daily at 06:00
echo.

REM Delete existing task if present (ignore error)
schtasks /Delete /TN "STDF_DailyFetch" /F >nul 2>&1

REM Create the task
schtasks /Create ^
  /TN "STDF_DailyFetch" ^
  /TR "powershell.exe -ExecutionPolicy Bypass -NoProfile -NonInteractive -File \"%PS_SCRIPT%\"" ^
  /SC DAILY ^
  /ST 06:00 ^
  /RL HIGHEST ^
  /F

if %ERRORLEVEL% EQU 0 (
    echo.
    echo [OK] Task registered successfully.
    echo.
    echo To verify:
    echo   schtasks /Query /TN STDF_DailyFetch /V
    echo.
    echo To run immediately for testing:
    echo   schtasks /Run /TN STDF_DailyFetch
) else (
    echo.
    echo [ERROR] Failed to register task. Make sure you are running as Administrator.
)

endlocal
pause
