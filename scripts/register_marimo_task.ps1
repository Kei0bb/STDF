<#
.SYNOPSIS
    Register the shared marimo server as a Windows scheduled task (run as Administrator).

.DESCRIPTION
    Starts scripts\start_marimo.ps1 at boot, whether or not anyone is logged on, with
    NO execution time limit (schtasks /Create defaults to stopping a task after 72h,
    which would silently take the server down every 3 days) and a few automatic restarts.
    It runs as the current user (asks for the password once), not SYSTEM: uv, config.yaml
    and the data live under that account. It runs NOT elevated (-RunLevel Limited): marimo
    kernels execute whatever code the browser users type, so they must not get an admin
    token. Port 2718 and the venv under var\ need no elevation. Prefer a dedicated
    non-admin account that can write only workspace\ and var\ if one can be created.

    Remove with:  Unregister-ScheduledTask -TaskName STDF_Marimo -Confirm:$false
    After stopping the task, check the port is free before restarting:
        Get-NetTCPConnection -LocalPort 2718 -ErrorAction SilentlyContinue
#>

param([string]$TaskName = "STDF_Marimo")

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Script      = Join-Path $ProjectRoot "scripts\start_marimo.ps1"

$cred = Get-Credential -UserName "$env:USERDOMAIN\$env:USERNAME" `
    -Message "Account the marimo task runs as (needed to run while logged off)"

$action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-ExecutionPolicy Bypass -NoProfile -NonInteractive -File `"$Script`"" `
    -WorkingDirectory $ProjectRoot
$trigger  = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) `
    -StartWhenAvailable -MultipleInstances IgnoreNew

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
    -Settings $settings -User $cred.UserName `
    -Password $cred.GetNetworkCredential().Password -RunLevel Limited -Force | Out-Null

Write-Host "[OK] Registered $TaskName (at startup, no time limit)."
Write-Host "Run now:  Start-ScheduledTask -TaskName $TaskName"
Write-Host "Status :  Get-ScheduledTaskInfo -TaskName $TaskName"
