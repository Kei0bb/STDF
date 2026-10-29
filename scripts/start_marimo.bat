@echo off
rem Start the shared marimo edit server in the foreground (Ctrl+C to stop).
rem Details / setup / troubleshooting: docs\marimo-server.md
rem
rem marimo runs from its OWN venv (var\venv-notebook) and NOT through "uv run": under
rem "uv run" marimo turns the UI's install/remove buttons into "uv add"/"uv remove", which
rem rewrite the tracked pyproject.toml / uv.lock and the .venv the nightly fetch uses.
rem Keep this file ASCII and CRLF (cmd reads it in the console code page).
setlocal
cd /d "%~dp0.."

rem Never inherit uv/venv variables: marimo would detect a uv project from them.
set "UV="
set "VIRTUAL_ENV="
set "UV_PROJECT_ENVIRONMENT="
set "PYTHONIOENCODING=utf-8"

set "VENV=%CD%\var\venv-notebook"
set "TOKEN=%CD%\var\marimo_token.txt"
set "PORT=2718"
rem Idle notebooks free their kernel (and DuckDB memory) this many seconds after the
rem browser disconnects. A tab left open keeps its kernel alive.
set "TTL=3600"

rem marimo takes a lock under %USERPROFILE%\.marimo\notebook-locks on every save and creates
rem that folder with mkdir mode 0o700. With Python 3.13 on Windows this becomes an ACL that
rem locks the owner out, and saving fails with errno 13. Create it first, without a mode.
rem Repair an already broken one: scripts\marimo_fix_state_dir.bat
if not exist "%USERPROFILE%\.marimo\notebook-locks" mkdir "%USERPROFILE%\.marimo\notebook-locks"

if not exist "%VENV%\Scripts\marimo.exe" (
    echo [marimo] venv not found; building it first.
    call "%~dp0marimo_sync.bat"
    if errorlevel 1 exit /b 1
)

rem The token is written by Python so it is plain ASCII: cmd echo to a file adds a trailing
rem space, and PowerShell 5.1 writes UTF-16 which marimo cannot read.
rem Do not put a closing parenthesis in any echo or rem line inside an if block below:
rem cmd ends the block at it and runs the rest of the line as a command.
if not exist "%TOKEN%" (
    "%VENV%\Scripts\python.exe" -c "import secrets,pathlib; pathlib.Path(r'%TOKEN%').write_text(secrets.token_urlsafe(24), encoding='ascii')"
    if errorlevel 1 (
        echo [marimo] could not create the token file: var\marimo_token.txt
        pause
        exit /b 1
    )
    echo [marimo] created a new access token file: var\marimo_token.txt
)

echo [marimo] Open one of these and enter the token (the contents of var\marimo_token.txt):
echo [marimo]   this machine : http://127.0.0.1:%PORT%
rem (the inner "for" trims the space ipconfig leaves after the colon)
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /c:"IPv4"') do for %%b in (%%a) do echo [marimo]   other PCs    : http://%%b:%PORT%
echo [marimo] The banner below also shows the token in its URL: do not paste it anywhere.
echo [marimo] Starting. It runs in this window until you press Ctrl+C.

rem Run from workspace\ (what marimo serves; relative paths in notebooks resolve here).
rem No --quiet: it makes marimo print nothing, so a running server looks hung.
rem --no-sandbox: notebooks must run in this venv (stdf_platform installed), not in
rem a per-notebook venv.
cd workspace
"%VENV%\Scripts\marimo.exe" edit . --headless --host 0.0.0.0 --port %PORT% --token-password-file "%TOKEN%" --session-ttl %TTL% --no-sandbox --skip-update-check
set "RC=%ERRORLEVEL%"
echo [marimo] exited (code %RC%).
exit /b %RC%
