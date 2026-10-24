<#
.SYNOPSIS
    Start the shared marimo edit server (browser notebooks) for Windows Task Scheduler.

.DESCRIPTION
    Runs marimo from its OWN virtualenv (var\venv-notebook), not from the project's
    .venv that the nightly `uv run stdf fetch` uses, and NOT through `uv run`.
    Both matter: when marimo sees it was started by `uv run` inside a uv project it
    turns the UI's "install / remove package" buttons into `uv add` / `uv remove`,
    which rewrite the tracked pyproject.toml / uv.lock and re-sync the shared .venv.
    Started directly from a separate venv, UI installs can only touch that venv.

    The access token is read from var\marimo_token.txt (one line, create it before the
    first start; var\ is git-ignored). Output goes to var\logs\marimo-YYYYMMDD.log
    (UTF-8, logs older than 14 days are removed).

.PARAMETER Sync
    (Re)build var\venv-notebook from uv.lock (`uv sync --group notebook --frozen`).
    Needs network access to the package index on first use. Done automatically only
    when the venv does not exist; after `git pull` run it once by hand:
        powershell -ExecutionPolicy Bypass -File scripts\start_marimo.ps1 -Sync

.NOTES
    Setup / operation: docs\marimo-server.md
    Manual test:       powershell -ExecutionPolicy Bypass -File scripts\start_marimo.ps1
#>

param([switch]$Sync)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"

# Same encoding handling as daily_fetch.ps1 (cp932 consoles break rich/Python output).
$env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$LogDir      = Join-Path $ProjectRoot "var\logs"
$LogFile     = Join-Path $LogDir ("marimo-{0}.log" -f (Get-Date -Format "yyyyMMdd"))
$TokenFile   = Join-Path $ProjectRoot "var\marimo_token.txt"
$VenvDir     = Join-Path $ProjectRoot "var\venv-notebook"
$Marimo      = Join-Path $VenvDir "Scripts\marimo.exe"
$Port        = 2718
# Idle notebooks free their kernel (and its DuckDB memory) after the browser
# disconnects for this many seconds. A tab left open keeps its kernel alive.
$SessionTtl  = 3600

if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}
Get-ChildItem $LogDir -Filter "marimo-*.log" -ErrorAction SilentlyContinue |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-14) } |
    Remove-Item -ErrorAction SilentlyContinue

if (-not (Test-Path $TokenFile)) {
    Write-Host "Token file not found: $TokenFile (create it with a one-line password)"
    exit 1
}

Set-Location $ProjectRoot

if ($Sync -or -not (Test-Path $Marimo)) {
    # --frozen: install exactly what uv.lock says, never re-resolve (TLS-intercepted
    # network; the lock is the reviewed state). The env var scopes this to the
    # separate venv; the shared .venv is untouched.
    $env:UV_PROJECT_ENVIRONMENT = $VenvDir
    & uv sync --group notebook --frozen
    $syncExit = $LASTEXITCODE
    Remove-Item Env:UV_PROJECT_ENVIRONMENT -ErrorAction SilentlyContinue
    if ($syncExit -ne 0 -or -not (Test-Path $Marimo)) {
        Write-Host "uv sync failed (exit $syncExit); marimo venv not ready: $VenvDir"
        exit 1
    }
}

# Launch marimo directly so marimo does not detect `uv run` (UV / VIRTUAL_ENV unset).
Remove-Item Env:UV -ErrorAction SilentlyContinue
Remove-Item Env:VIRTUAL_ENV -ErrorAction SilentlyContinue
Remove-Item Env:UV_PROJECT_ENVIRONMENT -ErrorAction SilentlyContinue

# --quiet: without it the startup banner prints http://host:2718?access_token=<token>
#   into the log file (and the token would sit there in plain text).
# --no-sandbox: notebooks must run in this venv, where stdf_platform is installed,
#   not in a per-notebook uv venv.
# Output is appended with Add-Content (UTF-8); `*>>` writes UTF-16 on PowerShell 5.1.
& $Marimo --quiet edit workspace `
    --headless --host 0.0.0.0 --port $Port `
    --token-password-file $TokenFile `
    --session-ttl $SessionTtl `
    --no-sandbox --skip-update-check 2>&1 |
    ForEach-Object { Add-Content -Path $LogFile -Value $_.ToString() -Encoding UTF8 }
exit $LASTEXITCODE
