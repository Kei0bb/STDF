<#
.SYNOPSIS
    Daily STDF fetch and ingest script for Windows Task Scheduler.

.DESCRIPTION
    Runs `uv run stdf2pq fetch` from the project root.
    Logs output to logs/fetch_YYYYMMDD_HHMMSS.log.
    Automatically deletes log files older than 30 days.

.NOTES
    Register with: scripts\register_task.bat (run as Administrator)
    Manual test:   schtasks /Run /TN STDF_DailyFetch
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Resolve project root from this script's location (scripts/../)
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$LogDir      = Join-Path $ProjectRoot "logs"
$Timestamp   = Get-Date -Format "yyyyMMdd_HHmmss"
$LogFile     = Join-Path $LogDir "fetch_$Timestamp.log"

# Ensure logs directory exists
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}

function Write-Log {
    param([string]$Message)
    $Line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message"
    Add-Content -Path $LogFile -Value $Line
    Write-Host $Line
}

Write-Log "=== stdf2pq daily fetch started ==="
Write-Log "Project root : $ProjectRoot"
Write-Log "Log file     : $LogFile"

Set-Location $ProjectRoot

# Run fetch + ingest
$ExitCode = 0
try {
    Write-Log "Running: uv run stdf2pq fetch --verbose"
    # Redirect both stdout and stderr into the log
    $Output = & uv run stdf2pq fetch --verbose 2>&1
    $ExitCode = $LASTEXITCODE
    foreach ($Line in $Output) {
        Add-Content -Path $LogFile -Value $Line
    }
} catch {
    Write-Log "ERROR: $_"
    $ExitCode = 1
}

if ($ExitCode -eq 0) {
    Write-Log "=== fetch completed successfully (exit 0) ==="
} else {
    Write-Log "=== fetch FAILED (exit $ExitCode) ==="
}

# Rotate logs: delete files older than 30 days
$CutoffDate = (Get-Date).AddDays(-30)
$OldLogs = Get-ChildItem -Path $LogDir -Filter "fetch_*.log" |
           Where-Object { $_.LastWriteTime -lt $CutoffDate }

if ($OldLogs.Count -gt 0) {
    Write-Log "Rotating $($OldLogs.Count) old log file(s)..."
    $OldLogs | Remove-Item -Force
}

exit $ExitCode
