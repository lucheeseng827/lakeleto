<#
.SYNOPSIS
    Export a Lakeleto source "as a different datasource" (Parquet / CSV / JSON) for BI or DuckDB.

.DESCRIPTION
    Two modes:

      plain view (default)  GET /v1/export?path=&fmt=&filter=&sort=&cols=
          A straight extract in the format you ask for, optionally filtered / sorted / projected.
          No workspace or SQL engine required.

      SQL report (-Sql)     POST /v1/workspaces/{id}/runs then GET .../runs/{run_id}/result
          Shapes the data with a read-only SELECT (the source is the table `t`), caches the
          result, and downloads it. The cached result is always Parquet - ideal as the
          "intermediate" that ConvertTo-DuckDbTable.ps1 materializes. Needs -Workspace and a
          server built with the `sql` feature.

.EXAMPLE
    ./Export-LakeletoData.ps1 -Path examples/sample-data/orders.csv -Out out/orders.parquet

.EXAMPLE
    ./Export-LakeletoData.ps1 -Path orders.csv -Out out/by_city.parquet -Workspace $ws `
        -Sql "SELECT city, count(*) AS orders, round(sum(amount_usd),2) AS revenue FROM t GROUP BY city ORDER BY revenue DESC"
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Path,
    [Parameter(Mandatory)][string]$Out,
    [ValidateSet('parquet', 'csv', 'json')][string]$Format = 'parquet',
    [string[]]$Filter,
    [string]$Cols,
    [string]$Sort,
    [switch]$Desc,
    [string]$Sql,
    [string]$Workspace,
    [int]$Limit = 100000
)

. "$PSScriptRoot/LakeletoCommon.ps1"

$outDir = Split-Path -Parent $Out
if ($outDir -and -not (Test-Path $outDir)) { New-Item -ItemType Directory -Force -Path $outDir | Out-Null }
Assert-LakeletoUp

if ($Sql) {
    # ---- SQL report mode -----------------------------------------------------------------
    if (-not $Workspace) { throw "Export: -Sql needs -Workspace (run Import-LakeletoData.ps1 first)" }
    Write-LakeletoLog "running report SQL in workspace $Workspace..."
    $run = Invoke-LakeletoApi -Method POST -Path "/v1/workspaces/$Workspace/runs" `
        -Body @{ sql = $Sql; path = $Path; limit = $Limit; preview = 1 }
    if ($run.run.status -ne 'ok') {
        $err = if ($run.run.error) { $run.run.error } else { 'unknown error' }
        throw "lakeleto: query failed: $err"
    }
    Write-LakeletoLog "report ok: $($run.run.row_count) rows (run $($run.run.id)); downloading Parquet..."
    Invoke-LakeletoDownload -Method GET -Path "/v1/workspaces/$Workspace/runs/$($run.run.id)/result" -OutFile $Out
    if ($Format -ne 'parquet') { Write-LakeletoLog "note: SQL-report results are Parquet; ignoring -Format $Format" }
} else {
    # ---- plain view mode -----------------------------------------------------------------
    $q = @{ path = $Path; fmt = $Format }
    if ($Sort) { $q.sort = $Sort }
    if ($Desc) { $q.desc = '1' }
    if ($Cols) { $q.cols = $Cols }
    if ($Filter) { $q.filter = $Filter }   # array -> repeated filter= params
    Write-LakeletoLog "exporting $Path -> $Out ($Format)"
    Invoke-LakeletoDownload -Method GET -Path '/v1/export' -OutFile $Out -Query $q
}

$bytes = (Get-Item $Out).Length
Write-LakeletoLog "wrote $Out ($bytes bytes) OK"
$Out
