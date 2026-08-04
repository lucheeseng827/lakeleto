<#
.SYNOPSIS
    The "get started" step: load a data file into Lakeleto and create a workspace around it.

.DESCRIPTION
    Over the REST API (nothing is uploaded - the server reads the bytes locally):
      1. POST /v1/workspaces          create a named workspace (or reuse one via -Workspace)
      2. PUT  /v1/workspaces/{id}     register the file as a saved Connection (data source)
      3. GET  /v1/schema              show columns / types / row count so you see it worked
      4. POST /v1/workspaces/{id}/runs cache a preview so the source re-opens instantly

    The path is resolved by the *server* (relative to where `lakeleto serve` runs, or absolute,
    or an object-store URI like s3://bucket/key.parquet). Emits a summary object (PSCustomObject
    with WorkspaceId / ConnectionId / Path / Rows / Columns) so the other scripts can chain off it.

.EXAMPLE
    ./Import-LakeletoData.ps1 -Path examples/sample-data/orders.csv

.EXAMPLE
    $env:LAKELETO_URL='http://box:8080'; ./Import-LakeletoData.ps1 -Path s3://bucket/events.parquet -Name Events
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Path,
    [string]$Name = 'Imported data',
    [string]$Workspace,
    [string]$Label,
    [string]$Format,
    [int]$Preview = 50
)

. "$PSScriptRoot/LakeletoCommon.ps1"

if (-not $Label) { $Label = Split-Path -Leaf $Path }
Assert-LakeletoUp

# 1. Create (or reuse) the workspace ------------------------------------------------------
if (-not $Workspace) {
    Write-LakeletoLog "creating workspace '$Name'..."
    $ws = Invoke-LakeletoApi -Method POST -Path '/v1/workspaces' -Body @{ name = $Name }
} else {
    Write-LakeletoLog "using existing workspace $Workspace..."
    $ws = Invoke-LakeletoApi -Method GET -Path "/v1/workspaces/$Workspace"
}
$wsId = $ws.id

# 2. Register the file as a saved Connection (the "data source") --------------------------
$connId = "conn-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())-$(Get-Random)"
Write-LakeletoLog "registering data source '$Label' -> $Path"
$conn = @{ id = $connId; label = $Label; path = $Path; pinned = $true }
if ($Format) { $conn.format = $Format }
# Preserve existing connections; append the new one.
$conns = @()
if ($ws.PSObject.Properties['connections'] -and $ws.connections) { $conns = @($ws.connections) }
$ws.connections = @($conns + $conn)
Invoke-LakeletoApi -Method PUT -Path "/v1/workspaces/$wsId" -Body $ws | Out-Null

# 3. Confirm the source reads - schema ----------------------------------------------------
$q = @{ path = $Path }
if ($Format) { $q.format = $Format }
$schema = Invoke-LakeletoApi -Method GET -Path '/v1/schema' -Query $q
$nCols = @($schema.columns).Count
$nRows = if ($null -ne $schema.row_count) { $schema.row_count } else { 'unknown' }
Write-LakeletoLog "schema: $nCols columns, $nRows rows"
foreach ($c in $schema.columns) { Write-Host "    $($c.name): $($c.data_type)" }

# 4. Cache a preview so the source re-opens instantly (raw scan, no SQL) ------------------
Write-LakeletoLog "caching a $Preview-row preview..."
try {
    Invoke-LakeletoApi -Method POST -Path "/v1/workspaces/$wsId/runs" `
        -Body @{ path = $Path; preview = $Preview } | Out-Null
} catch {
    Write-LakeletoLog "preview cache skipped: $($_.Exception.Message)"
}

Write-LakeletoLog "imported OK  workspace=$wsId  connection=$connId"

# Machine-readable summary for chaining.
[pscustomobject]@{
    WorkspaceId  = $wsId
    ConnectionId = $connId
    Path         = $Path
    Columns      = $nCols
    Rows         = $nRows
}
