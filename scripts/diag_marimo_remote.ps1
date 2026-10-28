<#
.SYNOPSIS
    Run on a REMOTE PC (a browser client) to find out why opening a marimo notebook fails with
    "WebAssembly.instantiateStreaming(): expected N bytes, fell off end".

.DESCRIPTION
    That error means a file the page needs (marimo's loro_wasm_bg-*.wasm, 3,240,077 bytes)
    arrived truncated. The marimo server sends it intact (size and SHA-256 verified on the
    server side), so something on the path between this PC and the server cuts it. This script
    downloads that file, and a larger NON-wasm control file, in two different ways and compares
    the size and SHA-256 with the known values, so the culprit can be narrowed down:

      curl.exe             ignores the Windows/IE proxy settings (talks to the server directly)
      Invoke-WebRequest    uses the Windows/IE proxy settings, like most browsers do

    Nothing is installed or changed; it only downloads two files into %TEMP%\marimo_diag.
    The output contains no token and is safe to copy into a chat.

    The expected size/SHA-256 are for marimo 0.25.2 as pinned in uv.lock. After a marimo upgrade
    the asset file names (loro_wasm_bg-XXXX.wasm, Plot-XXXX.js) change; edit $Targets below.

    This file is ASCII on purpose (Windows PowerShell 5.1 reads BOM-less UTF-8 as ANSI).

.PARAMETER HostAddress
    The marimo server: an IP address or a computer name, WITHOUT the port (the port is -Port).
    "host:port" is tolerated and split for you.

.PARAMETER Port
    marimo port (default 2718).

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File diag_marimo_remote.ps1 -HostAddress 192.168.0.76
#>

param(
    [Parameter(Mandatory = $true)][string]$HostAddress,
    [int]$Port = 2718
)

$ErrorActionPreference = "Continue"

# Be forgiving: "10.0.0.5:2718", "http://10.0.0.5:2718/" and "10.0.0.5:2718:2718" all mean host 10.0.0.5.
$HostAddress = ($HostAddress -replace "^https?://", "") -replace "/.*$", ""
if ($HostAddress -match "^([^:]+):(\d+)") {
    $HostAddress = $Matches[1]
    $Port = [int]$Matches[2]
}
$Base = "http://${HostAddress}:$Port"
$Work = Join-Path $env:TEMP "marimo_diag"
New-Item -ItemType Directory -Path $Work -Force | Out-Null

$Targets = @(
    @{ Name = "wasm (the failing file)"; Path = "/assets/loro_wasm_bg-CSolyDSZ.wasm"; Size = 3240077;
       Sha = "75ff0754c7681ec2c83345ea191e7ebf079cdf099fadea7da6401cd031217b67" },
    @{ Name = "control: bigger .js file"; Path = "/assets/Plot-B8w5_jU-.js"; Size = 4650125;
       Sha = "a60718616d96d355081dd0d7b8ecb6a198ff7a4e5adc1ef75345e653a48e7adc" }
)

function Judge([string]$file, $t) {
    if (-not (Test-Path $file)) { return "NO FILE (download failed)" }
    $len = (Get-Item $file).Length
    if ($len -lt $t.Size) { return "TRUNCATED ($len of $($t.Size) bytes)" }
    $hash = (Get-FileHash $file -Algorithm SHA256).Hash.ToLower()
    if ($len -eq $t.Size -and $hash -eq $t.Sha) { return "OK" }
    return "DIFFERENT (size $len, sha256 $hash)"
}

Write-Host "== marimo remote download check =="
Write-Host "Server : $Base"
Write-Host ("PowerShell {0} / {1}" -f $PSVersionTable.PSVersion, [System.Environment]::OSVersion.VersionString)

# 1. Is the server reachable at all? (401 without a token is the healthy answer)
$curl = Get-Command curl.exe -ErrorAction SilentlyContinue
if (-not $curl) {
    Write-Host "curl.exe not found (needs Windows 10 1803 or later). Only Invoke-WebRequest will be tested."
}
else {
    $code = & curl.exe -s -S --max-time 15 -o NUL -w "%{http_code}" "$Base/api/status" 2>&1
    Write-Host "Reach  : curl.exe $Base/api/status -> $code  (401 = server reachable)"
}

# 2. Which proxy would Windows use for this address?
try {
    $proxy = [System.Net.WebRequest]::GetSystemWebProxy()
    $uri = [System.Uri]$Base
    if ($proxy.IsBypassed($uri)) { Write-Host "Proxy  : none for this address (bypassed)" }
    else { Write-Host "Proxy  : Windows would use $($proxy.GetProxy($uri)) for this address" }
}
catch { Write-Host "Proxy  : could not determine ($($_.Exception.Message))" }

# 3. Download each file with each tool and judge it.
$results = @{}
foreach ($t in $Targets) {
    Write-Host ""
    Write-Host "-- $($t.Name): $($t.Path) (expected $($t.Size) bytes)"
    $url = "$Base$($t.Path)"

    if ($curl) {
        $f = Join-Path $Work "curl.bin"
        Remove-Item $f -ErrorAction SilentlyContinue
        & curl.exe -s -S --max-time 180 -o $f $url 2>&1 | ForEach-Object { Write-Host "   curl.exe message: $_" }
        $v = Judge $f $t
        $results["curl|$($t.Name)"] = $v
        Write-Host "   curl.exe          : $v"
    }

    $f = Join-Path $Work "iwr.bin"
    Remove-Item $f -ErrorAction SilentlyContinue
    try { Invoke-WebRequest -Uri $url -OutFile $f -UseBasicParsing -TimeoutSec 180 }
    catch { Write-Host "   Invoke-WebRequest message: $($_.Exception.Message)" }
    $v = Judge $f $t
    $results["iwr|$($t.Name)"] = $v
    Write-Host "   Invoke-WebRequest : $v"
}

# 4. Plain-language reading of the result.
function Ok([string]$k) { return $results[$k] -eq "OK" }
$w = $Targets[0].Name; $c = $Targets[1].Name
$noFile = @($results.Values | Where-Object { $_ -like "NO FILE*" }).Count
Write-Host ""
Write-Host "== Reading =="
if ($noFile -eq $results.Count) {
    Write-Host "No download produced a file at all - this is NOT a truncation result. The request itself failed"
    Write-Host "(wrong address/port, server not running, firewall). Check the 'Reach' line and the messages above."
}
elseif ($curl -and (Ok "curl|$w") -and (Ok "iwr|$w")) {
    Write-Host "Both downloads are intact: the network path is fine. The problem is in the browser (extensions, cache, a"
    Write-Host "different proxy setting). Try Ctrl+F5, a private window with no extensions, another browser."
}
elseif ($curl -and (Ok "curl|$w") -and -not (Ok "iwr|$w")) {
    Write-Host "Direct download is intact but the Windows-proxy download is not: the PROXY is cutting the file."
    Write-Host "Add the server to the proxy bypass list (Settings > Network > Proxy), or open it by IP and bypass the proxy."
}
elseif ((-not (Ok "curl|$w")) -and (Ok "curl|$c")) {
    Write-Host "The .wasm file is cut but a larger .js file is fine: something filters by file/content type"
    Write-Host "(application/wasm). Likely a security gateway / content filter. Ask IT to allow application/wasm to this host."
}
elseif ((-not (Ok "curl|$w")) -and (-not (Ok "curl|$c"))) {
    Write-Host "Large downloads are cut in general (not wasm specific): suspect the network path (IPS / firewall / MTU)."
    Write-Host "Try another port (change `$Port in start_marimo.ps1) and another network segment. Ask IT."
}
else {
    Write-Host "Mixed result - copy the lines above into the chat for analysis."
}
Write-Host ""
Write-Host "Done. Copy everything from '== marimo remote download check ==' down to here."
