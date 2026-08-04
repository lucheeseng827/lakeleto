<#
.SYNOPSIS
    The whole loop end to end: import -> export -> DuckDB intermediate table -> BI-ready.

.DESCRIPTION
      1. Import-LakeletoData     load a sample file, create a workspace + a saved data source
      2. Export-LakeletoData     export it as Parquet (a BI-ready datasource)
      3. Export-LakeletoData     also produce a shaped SQL report (revenue by group) as Parquet
      4. ConvertTo-DuckDbTable   materialize both into out/reports.duckdb as intermediate tables

    Defaults to the bundled examples/sample-data/orders.csv, writing under ./lakeleto-out.
    The server must already be running (`lakeleto serve --features serve,sql`); pass -Serve to
    start a throwaway server for the run and stop it after. Needs PowerShell 7+ and (for step 4)
    the `duckdb` CLI.

.EXAMPLE
    ./Start-LakeletoQuickstart.ps1

.EXAMPLE
    ./Start-LakeletoQuickstart.ps1 -Serve -Path C:\data\sales.parquet -OutDir .\bi
#>
[CmdletBinding()]
param(
    [string]$Path,
    [string]$OutDir = './lakeleto-out',
    [switch]$Serve,
    [string]$ReportSql,
    [string]$AmountCol = 'amount_usd',
    [string]$GroupCol = 'city'
)

. "$PSScriptRoot/LakeletoCommon.ps1"

if (-not $Path) { $Path = (Resolve-Path "$PSScriptRoot/../sample-data/orders.csv").Path }
if (-not $ReportSql) {
    $ReportSql = "SELECT $GroupCol AS $GroupCol, count(*) AS orders, round(sum($AmountCol),2) AS revenue FROM t GROUP BY $GroupCol ORDER BY revenue DESC"
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# Optionally stand up a throwaway server for the demo, torn down on exit ------------------
$server = $null
try {
    if ($Serve -and -not (Test-LakeletoHealth)) {
        if (Get-Command lakeleto -ErrorAction SilentlyContinue) {
            $server = Start-Process -FilePath lakeleto -ArgumentList 'serve' -PassThru -WindowStyle Hidden
        } elseif (Get-Command cargo -ErrorAction SilentlyContinue) {
            Write-LakeletoLog "no 'lakeleto' binary - building via cargo (first run compiles, be patient)..."
            $server = Start-Process -FilePath cargo `
                -ArgumentList 'run', '-q', '-p', 'lakeleto', '--features', 'serve,sql', '--bin', 'lakeleto', '--', 'serve' `
                -PassThru -WindowStyle Hidden
        } else {
            throw "Quickstart: -Serve needs the 'lakeleto' binary or 'cargo' on PATH"
        }
        Write-LakeletoLog "starting a local server on $(Get-LakeletoUrl)..."
        if (-not (Wait-LakeletoHealth -Tries 300)) { throw "Quickstart: server didn't become healthy" }
    }

    Assert-LakeletoUp

    Write-Host ''
    Write-LakeletoLog "STEP 1/4 - import $Path"
    $summary = & "$PSScriptRoot/Import-LakeletoData.ps1" -Path $Path -Name 'Quickstart'
    $wsId = $summary.WorkspaceId
    Write-LakeletoLog "workspace = $wsId"

    Write-Host ''
    Write-LakeletoLog 'STEP 2/4 - export the raw table as Parquet (a BI-ready datasource)'
    $rawParquet = Join-Path $OutDir 'raw.parquet'
    & "$PSScriptRoot/Export-LakeletoData.ps1" -Path $Path -Out $rawParquet -Format parquet | Out-Null

    Write-Host ''
    Write-LakeletoLog 'STEP 3/4 - export a shaped SQL report as Parquet'
    Write-LakeletoLog "  $ReportSql"
    $reportParquet = Join-Path $OutDir "report_by_$GroupCol.parquet"
    & "$PSScriptRoot/Export-LakeletoData.ps1" -Path $Path -Out $reportParquet -Workspace $wsId -Sql $ReportSql | Out-Null

    Write-Host ''
    Write-LakeletoLog 'STEP 4/4 - build DuckDB intermediate tables for BI'
    $db = Join-Path $OutDir 'reports.duckdb'
    & "$PSScriptRoot/ConvertTo-DuckDbTable.ps1" -InputFile $rawParquet    -Db $db -Table raw_orders | Out-Null
    & "$PSScriptRoot/ConvertTo-DuckDbTable.ps1" -InputFile $reportParquet -Db $db -Table "report_by_$GroupCol" | Out-Null

    Write-Host ''
    Write-LakeletoLog 'ALL DONE'
    Write-Host "    exports : $rawParquet, $reportParquet"
    Write-Host "    duckdb  : $db  (tables: raw_orders, report_by_$GroupCol)"
    Write-Host ''
    Write-Host '  Peek at the report:'
    Write-Host "    duckdb $db -c 'SELECT * FROM report_by_$GroupCol LIMIT 10;'"
    Write-Host '  Open the DuckDB browser UI:'
    Write-Host "    duckdb -ui $db"
    Write-Host "  Or connect Power BI / Tableau / Metabase / Superset to $db via the DuckDB ODBC/JDBC driver."
} finally {
    if ($server) {
        Write-LakeletoLog 'stopping the throwaway server...'
        Stop-Process -Id $server.Id -ErrorAction SilentlyContinue
    }
}
