@echo off
rem Diagnose a failing notebook save in the shared marimo. Details: scripts\diag_marimo_save.py
rem Run as the same account that runs start_marimo.bat. Keep this file ASCII and CRLF.
setlocal
cd /d "%~dp0.."
set "UV="
set "VIRTUAL_ENV="
set "UV_PROJECT_ENVIRONMENT="
set "PYTHONIOENCODING=utf-8"
"%CD%\var\venv-notebook\Scripts\python.exe" scripts\diag_marimo_save.py
set "RC=%ERRORLEVEL%"
pause
exit /b %RC%
