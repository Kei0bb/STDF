@echo off
rem Build / rebuild the marimo virtualenv (var\venv-notebook) from uv.lock.
rem Run once, and again after "git pull" changes uv.lock. Stop marimo first.
rem
rem UV_PROJECT_ENVIRONMENT redirects "uv sync" away from the project .venv (used by the
rem nightly fetch) into var\venv-notebook. setlocal keeps that variable inside this script.
rem --frozen installs exactly what uv.lock says (no re-resolve; TLS-intercepted network).
rem Keep this file ASCII and CRLF (cmd reads it in the console code page).
setlocal
cd /d "%~dp0.."
set "UV_PROJECT_ENVIRONMENT=%CD%\var\venv-notebook"
echo [marimo] building %UV_PROJECT_ENVIRONMENT%
uv sync --group notebook --frozen
if errorlevel 1 (
    echo [marimo] uv sync FAILED.
    pause
    exit /b 1
)
echo [marimo] venv ready.
