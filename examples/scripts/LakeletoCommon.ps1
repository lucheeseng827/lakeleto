# LakeletoCommon.ps1 - shared helpers for the Lakeleto REST scripts (dot-source it).
#
# Talks to a *running* `lakeleto serve` over HTTP. Needs PowerShell 7+ (pwsh) for
# -SkipHttpErrorCheck; the BI step also needs the `duckdb` CLI on PATH.
#
# Config (environment):
#   LAKELETO_URL     base URL of the server   (default http://127.0.0.1:8080)
#   LAKELETO_TOKEN   bearer token, if the server was started with --token
#
# The token is sent as an Authorization header only - never a ?token= query param.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-LakeletoUrl {
    if ($env:LAKELETO_URL) { return $env:LAKELETO_URL.TrimEnd('/') }
    return 'http://127.0.0.1:8080'
}

function Get-LakeletoHeaders {
    $h = @{}
    if ($env:LAKELETO_TOKEN) { $h['Authorization'] = "Bearer $($env:LAKELETO_TOKEN)" }
    return $h
}

# Total per-request timeout so a hung/slow server can't block the pipeline forever. Generous by
# default so a large export/report isn't cut off; raise LAKELETO_TIMEOUT_SEC for very large jobs.
# A non-positive / non-numeric override is rejected (it would mean "no timeout") and the default kept.
function Get-LakeletoTimeoutSec {
    $sec = 300
    if ($env:LAKELETO_TIMEOUT_SEC) {
        $parsed = 0
        if ([int]::TryParse($env:LAKELETO_TIMEOUT_SEC, [ref]$parsed) -and $parsed -gt 0) {
            $sec = $parsed
        } else {
            Write-Warning "LAKELETO_TIMEOUT_SEC='$($env:LAKELETO_TIMEOUT_SEC)' is not a positive integer; using $sec."
        }
    }
    return $sec
}

# Timeout params for Invoke-WebRequest, returned as a splat. -TimeoutSec bounds connection setup on
# all PowerShell 7.x (and is the overall timeout before 7.4). On 7.4+ -TimeoutSec covers only the
# connection, so we also set -OperationTimeoutSeconds - the per-read inactivity timeout that stops a
# stalled body download (a slow/hung export) from hanging forever - but only when the running pwsh
# actually exposes it, so the module still loads on 7.0-7.3.
function Get-LakeletoTimeoutParams {
    $sec = Get-LakeletoTimeoutSec
    $p = @{ TimeoutSec = $sec }
    if ((Get-Command Invoke-WebRequest).Parameters.ContainsKey('OperationTimeoutSeconds')) {
        $p['OperationTimeoutSeconds'] = $sec
    }
    return $p
}

function Write-LakeletoLog { param([string]$Message) Write-Host "lakeleto: $Message" -ForegroundColor Cyan }

function Assert-Command {
    param([Parameter(ValueFromRemainingArguments)][string[]]$Names)
    $missing = $Names | Where-Object { -not (Get-Command $_ -ErrorAction SilentlyContinue) }
    if ($missing) { throw "lakeleto: missing required command(s): $($missing -join ', ')" }
}

# Build a query string from a hashtable; array values repeat the key (e.g. filter=...&filter=...).
function ConvertTo-LakeletoQuery {
    param([hashtable]$Query)
    if (-not $Query -or $Query.Count -eq 0) { return '' }
    $parts = foreach ($k in $Query.Keys) {
        foreach ($v in @($Query[$k])) {
            if ($null -eq $v -or $v -eq '') { continue }
            '{0}={1}' -f [uri]::EscapeDataString($k), [uri]::EscapeDataString([string]$v)
        }
    }
    if ($parts) { return '?' + ($parts -join '&') }
    return ''
}

# Invoke-LakeletoApi -Method GET -Path /v1/schema -Query @{ path = $p }
#   Returns the parsed JSON object; throws with the server's error body on non-2xx.
function Invoke-LakeletoApi {
    param(
        [string]$Method,
        [string]$Path,
        $Body,
        [hashtable]$Query,
        [switch]$Raw
    )
    $url = (Get-LakeletoUrl) + $Path + (ConvertTo-LakeletoQuery $Query)
    $params = @{
        Method             = $Method
        Uri                = $url
        Headers            = (Get-LakeletoHeaders)
        SkipHttpErrorCheck = $true
    }
    foreach ($kv in (Get-LakeletoTimeoutParams).GetEnumerator()) { $params[$kv.Key] = $kv.Value }
    if ($null -ne $Body) {
        $params.Body        = ($Body | ConvertTo-Json -Depth 30 -Compress)
        $params.ContentType = 'application/json'
    }
    $resp = Invoke-WebRequest @params
    if ($resp.StatusCode -lt 200 -or $resp.StatusCode -ge 300) {
        throw "lakeleto: HTTP $($resp.StatusCode) - $Method $Path`n$($resp.Content)"
    }
    if ($Raw) { return $resp.Content }
    if ($resp.Content) { return $resp.Content | ConvertFrom-Json }
    return $null
}

# Invoke-LakeletoDownload -Method GET -Path /v1/export -OutFile out.parquet -Query @{...}
# Downloads to a sibling temp file and moves it into place only on success, so a failed request
# never overwrites or deletes an existing OutFile (a good prior export survives a failed refresh).
function Invoke-LakeletoDownload {
    param([string]$Method, [string]$Path, [string]$OutFile, [hashtable]$Query)
    $url = (Get-LakeletoUrl) + $Path + (ConvertTo-LakeletoQuery $Query)
    $timeout = Get-LakeletoTimeoutParams
    $dir = Split-Path -Parent $OutFile
    if (-not $dir) { $dir = '.' }
    $tmp = Join-Path $dir (".$(Split-Path -Leaf $OutFile).part.$([System.IO.Path]::GetRandomFileName())")
    try {
        $resp = Invoke-WebRequest -Method $Method -Uri $url -Headers (Get-LakeletoHeaders) `
            -OutFile $tmp -SkipHttpErrorCheck -PassThru @timeout
        if ($resp.StatusCode -lt 200 -or $resp.StatusCode -ge 300) {
            $body = if (Test-Path $tmp) { Get-Content $tmp -Raw } else { '' }
            throw "lakeleto: HTTP $($resp.StatusCode) - $Method $Path`n$body"
        }
        # Same-directory move: atomic on the same filesystem, replacing any prior OutFile only now.
        Move-Item -LiteralPath $tmp -Destination $OutFile -Force
    } finally {
        if (Test-Path -LiteralPath $tmp) { Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue }
    }
}

function Test-LakeletoHealth {
    try {
        $r = Invoke-WebRequest -Uri "$(Get-LakeletoUrl)/healthz" -SkipHttpErrorCheck -TimeoutSec 3
        return $r.StatusCode -eq 200
    } catch { return $false }
}

function Wait-LakeletoHealth {
    param([int]$Tries = 30)
    for ($i = 0; $i -lt $Tries; $i++) {
        if (Test-LakeletoHealth) { return $true }
        Start-Sleep -Seconds 1
    }
    return $false
}

function Assert-LakeletoUp {
    if (-not (Test-LakeletoHealth)) {
        Write-LakeletoLog "no server at $(Get-LakeletoUrl) - start one first, e.g.:"
        Write-Host "    lakeleto serve --features serve,sql"
        Write-Host "    cargo run --features serve,sql --bin lakeleto -- serve"
        throw "lakeleto: server not reachable"
    }
}
