<#
.SYNOPSIS
    Build a DuckDB intermediate table from a Lakeleto export, ready for BI.

.DESCRIPTION
    Lakeleto exports the raw/shaped data (Export-LakeletoData.ps1); DuckDB then materializes it
    into a small, fast, file-backed table that BI tools (Power BI, Tableau, Metabase, Superset,
    Excel via the DuckDB ODBC/JDBC driver, or `duckdb -ui`) query directly - no server, no
    re-reading the source each time. The export is exposed to any -TransformSql as the view `src`.

.EXAMPLE
    ./ConvertTo-DuckDbTable.ps1 -Input out/orders.parquet -Db out/reports.duckdb -Table orders

.EXAMPLE
    ./ConvertTo-DuckDbTable.ps1 -Input out/orders.parquet -Db out/reports.duckdb -Table orders_by_city `
        -TransformSql "SELECT city, count(*) AS orders, round(sum(amount_usd),2) AS revenue FROM src GROUP BY city ORDER BY revenue DESC"
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][Alias('Input')][string]$InputFile,
    [Parameter(Mandatory)][string]$Db,
    [Parameter(Mandatory)][string]$Table,
    [string]$TransformSql,
    [string]$View,
    [string]$ViewSql,
    [ValidateSet('parquet', 'csv', 'tsv', 'json')][string]$Format
)

. "$PSScriptRoot/LakeletoCommon.ps1"

if (-not (Test-Path $InputFile)) { throw "ConvertTo-DuckDbTable: no such input file: $InputFile" }
if ($View -and -not $ViewSql) { throw "ConvertTo-DuckDbTable: -View needs -ViewSql" }
Assert-Command duckdb

$dbDir = Split-Path -Parent $Db
if ($dbDir -and -not (Test-Path $dbDir)) { New-Item -ItemType Directory -Force -Path $dbDir | Out-Null }

# Pick a DuckDB reader from the format (explicit -Format wins, else the file extension).
$ext = if ($Format) { $Format } else { ([System.IO.Path]::GetExtension($InputFile)).TrimStart('.') }
$reader = switch ($ext.ToLower()) {
    'parquet' { 'read_parquet' }
    'pq'      { 'read_parquet' }
    'csv'     { 'read_csv_auto' }
    'tsv'     { 'read_csv_auto' }
    'json'    { 'read_json_auto' }
    'ndjson'  { 'read_json_auto' }
    'jsonl'   { 'read_json_auto' }
    default   { throw "ConvertTo-DuckDbTable: don't know how to read '.$ext' - pass -Format" }
}

# Absolute path + single-quote escaping so DuckDB's string literal is safe.
$absInput = (Resolve-Path $InputFile).Path
$escInput = $absInput -replace "'", "''"
$body = if ($TransformSql) { $TransformSql } else { 'SELECT * FROM src' }

# Escape the table/view names for the two SQL contexts they land in: a "quoted identifier"
# (double any embedded ") and, for the row-count line, a 'string literal' (double any embedded ').
$tblId  = $Table -replace '"', '""'
$tblLit = $Table -replace "'", "''"
$viewId = $View  -replace '"', '""'

Write-LakeletoLog "materializing $Table in $Db from $InputFile ($reader)..."
# `src` is a scratch staging view - TEMP so it never clobbers (or leaves behind) a real `src`.
$sql = "CREATE OR REPLACE TEMP VIEW src AS SELECT * FROM $reader('$escInput');"
$sql += "CREATE OR REPLACE TABLE ""$tblId"" AS $body;"
if ($View) { $sql += "CREATE OR REPLACE VIEW ""$viewId"" AS $ViewSql;" }
$sql += "DROP VIEW src;"
$sql += "SELECT '$tblLit' AS table_name, count(*) AS rows FROM ""$tblId"";"

& duckdb $Db -c $sql
if ($LASTEXITCODE -ne 0) { throw "ConvertTo-DuckDbTable: duckdb exited $LASTEXITCODE" }

$viewNote = if ($View) { " (+ view '$View')" } else { '' }
Write-LakeletoLog "done  table '$Table'$viewNote is in $Db"
Write-LakeletoLog "BI next steps:"
Write-Host "    duckdb $Db -c 'SELECT * FROM ""$Table"" LIMIT 20;'"
Write-Host "    duckdb -ui $Db"
Write-Host "    # or point Power BI / Tableau / Metabase at $Db via the DuckDB ODBC/JDBC driver"
$Db
