<#
.SYNOPSIS
    Start the shared marimo edit server (browser notebooks), in the foreground.

.DESCRIPTION
    Serves the project's workspace\ directory (also marimo's working directory, so relative
    paths in notebooks - e.g. s.run(..., out="x.csv") - resolve against workspace\).
    Output is shown on the console AND appended to var\logs\marimo-YYYYMMDD.log (UTF-8,
    logs older than 14 days are removed). Under Task Scheduler there is no console,
    so the log is where to look.

    marimo runs from its OWN virtualenv (var\venv-notebook), not from the project's .venv
    that the nightly `uv run stdf fetch` uses, and NOT through `uv run`.
    Both matter: when marimo sees it was started by `uv run` inside a uv project it
    turns the UI's "install / remove package" buttons into `uv add` / `uv remove`,
    which rewrite the tracked pyproject.toml / uv.lock and re-sync the shared .venv.
    Started directly from a separate venv, UI installs can only touch that venv.

    The access token is read from var\marimo_token.txt (one line, create it before the
    first start; var\ is git-ignored). It is never printed (output lines are redacted).

    NOTE: keep this file ASCII. Windows PowerShell 5.1 reads a BOM-less UTF-8 .ps1 as ANSI
    (cp932), so Japanese text in it turns into garbage.

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
$env:PYTHONUNBUFFERED = "1"   # piped stdout is block-buffered otherwise: banner/errors would appear late
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$ProjectRoot  = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$WorkspaceDir = Join-Path $ProjectRoot "workspace"
$LogDir       = Join-Path $ProjectRoot "var\logs"
$LogFile      = Join-Path $LogDir ("marimo-{0}.log" -f (Get-Date -Format "yyyyMMdd"))
$TokenFile    = Join-Path $ProjectRoot "var\marimo_token.txt"
$VenvDir      = Join-Path $ProjectRoot "var\venv-notebook"
$Marimo       = Join-Path $VenvDir "Scripts\marimo.exe"
$Port         = 2718
# Idle notebooks free their kernel (and its DuckDB memory) after the browser
# disconnects for this many seconds. A tab left open keeps its kernel alive.
$SessionTtl   = 3600

function Say([string]$msg) {
    # Console + log, so a failure is visible both when run by hand and under Task Scheduler.
    Write-Host "[marimo] $msg"
    Add-Content -Path $LogFile -Value ("[{0}] [marimo] {1}" -f (Get-Date -Format "HH:mm:ss"), $msg) -Encoding UTF8
}

if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}
Get-ChildItem $LogDir -Filter "marimo-*.log" -ErrorAction SilentlyContinue |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-14) } |
    Remove-Item -ErrorAction SilentlyContinue

try {
    if (-not (Test-Path $TokenFile)) {
        Say "Token file not found: $TokenFile (create it with a one-line password)"
        exit 1
    }
    # marimo reads the token file as UTF-8. `echo x > file` in Windows PowerShell 5.1 writes
    # UTF-16 (BOM FF FE), and Set-Content -Encoding UTF8 writes a UTF-8 BOM that becomes part
    # of the token. Either way marimo fails (or the token never matches): catch it here.
    $tokenBytes = [System.IO.File]::ReadAllBytes($TokenFile)
    $fixHint = "Recreate it:  Set-Content -Path var\marimo_token.txt -Value '<long random string>' -Encoding ascii"
    if ($tokenBytes.Length -ge 2 -and (($tokenBytes[0] -eq 0xFF -and $tokenBytes[1] -eq 0xFE) -or ($tokenBytes[0] -eq 0xFE -and $tokenBytes[1] -eq 0xFF))) {
        Say "Token file is UTF-16 (written by 'echo ... > file' in PowerShell 5.1); marimo cannot read it. $fixHint"
        exit 1
    }
    if ($tokenBytes.Length -ge 3 -and $tokenBytes[0] -eq 0xEF -and $tokenBytes[1] -eq 0xBB -and $tokenBytes[2] -eq 0xBF) {
        Say "Token file has a UTF-8 BOM, which would become part of the token. $fixHint"
        exit 1
    }
    if (([System.Text.Encoding]::ASCII.GetString($tokenBytes)).Trim().Length -eq 0) {
        Say "Token file is empty. $fixHint"
        exit 1
    }
    $token = ([System.Text.Encoding]::ASCII.GetString($tokenBytes)).Trim()
    if (-not (Test-Path $WorkspaceDir)) {
        Say "Workspace directory not found: $WorkspaceDir"
        exit 1
    }

    if ($Sync -or -not (Test-Path $Marimo)) {
        # --frozen: install exactly what uv.lock says, never re-resolve (TLS-intercepted
        # network; the lock is the reviewed state). The env var scopes this to the
        # separate venv; the shared .venv is untouched.
        Say "Building marimo venv: $VenvDir (uv sync --group notebook --frozen)"
        Push-Location $ProjectRoot
        $env:UV_PROJECT_ENVIRONMENT = $VenvDir
        & uv sync --group notebook --frozen
        $syncExit = $LASTEXITCODE
        Remove-Item Env:UV_PROJECT_ENVIRONMENT -ErrorAction SilentlyContinue
        Pop-Location
        if ($syncExit -ne 0 -or -not (Test-Path $Marimo)) {
            Say "uv sync failed (exit $syncExit); marimo venv not ready."
            exit 1
        }
    }

    # Launch marimo directly so marimo does not detect `uv run` (UV / VIRTUAL_ENV unset).
    Remove-Item Env:UV -ErrorAction SilentlyContinue
    Remove-Item Env:VIRTUAL_ENV -ErrorAction SilentlyContinue
    Remove-Item Env:UV_PROJECT_ENVIRONMENT -ErrorAction SilentlyContinue

    # Working directory = workspace\ (what marimo serves).
    Set-Location $WorkspaceDir

    Say "Workspace : $WorkspaceDir"
    Say "URL       : http://$($env:COMPUTERNAME):$Port  (enter the token from var\marimo_token.txt)"
    Say "Log       : $LogFile"
    Say "Starting marimo... When it is up, marimo prints its URL below (the token is hidden)."
    Say "The server runs in this window until you press Ctrl+C - no further output is normal."

    # No --quiet: with it marimo prints NOTHING, so a running server looks hung. Without it
    #   the startup banner (the "it is up" signal) includes ...?access_token=<token>, so every
    #   output line is redacted below before it reaches the console or the log.
    # --no-sandbox: notebooks must run in this venv, where stdf_platform is installed,
    #   not in a per-notebook uv venv.
    # Output is appended with Add-Content (UTF-8); `*>>` writes UTF-16 on PowerShell 5.1.
    & $Marimo edit . `
        --headless --host 0.0.0.0 --port $Port `
        --token-password-file $TokenFile `
        --session-ttl $SessionTtl `
        --no-sandbox --skip-update-check 2>&1 |
        ForEach-Object {
            $line = $_.ToString().Replace($token, "***")
            $line = $line -replace "access_token=[^\s&]+", "access_token=***"
            Write-Host $line
            Add-Content -Path $LogFile -Value $line -Encoding UTF8
        }
    $exitCode = $LASTEXITCODE
    Say "marimo exited (code $exitCode)."
    exit $exitCode
}
catch {
    Say "ERROR: $($_.Exception.Message)"
    exit 1
}
