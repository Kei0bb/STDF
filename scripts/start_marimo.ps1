<#
.SYNOPSIS
    Start the shared marimo edit server (browser notebooks) for Windows Task Scheduler.

.DESCRIPTION
    Runs `uv run --group notebook marimo edit notebooks` from the project root.
    The access token is read from var\marimo_token.txt (one line, create it
    before the first start; the file lives under var\ and is git-ignored).
    Output is appended to var\logs\marimo.log.

.NOTES
    Setup / operation: docs\marimo-server.md
    Manual test:       powershell -ExecutionPolicy Bypass -File scripts\start_marimo.ps1
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"

# Same encoding handling as daily_fetch.ps1 (cp932 consoles break rich/Python output).
$env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$LogDir      = Join-Path $ProjectRoot "var\logs"
$LogFile     = Join-Path $LogDir "marimo.log"
$TokenFile   = Join-Path $ProjectRoot "var\marimo_token.txt"
$Port        = 2718
# Idle notebooks free their kernel (and its DuckDB memory) after this many seconds.
$SessionTtl  = 3600

if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}
if (-not (Test-Path $TokenFile)) {
    Write-Host "Token file not found: $TokenFile (create it with a one-line password)"
    exit 1
}

Set-Location $ProjectRoot

# --no-sandbox: notebooks must run in the project's own environment, where
#   stdf_platform is installed, not in a per-notebook uv venv.
& uv run --group notebook marimo edit notebooks `
    --headless --host 0.0.0.0 --port $Port `
    --token-password-file $TokenFile `
    --session-ttl $SessionTtl `
    --no-sandbox --skip-update-check *>> $LogFile
exit $LASTEXITCODE
