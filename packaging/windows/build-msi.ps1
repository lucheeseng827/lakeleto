<#
.SYNOPSIS
    Build the Lakeleto .msi.

.DESCRIPTION
    Builds both binaries, regenerates the icon, and packages them with WiX.
    Run from anywhere; paths resolve relative to this script.

    Prerequisite:

        dotnet tool install --global wix --version 5.0.2

    Pin to WiX 5 deliberately. WiX 6 and 7 are gated behind the Open Source
    Maintenance Fee: `wix build` refuses to run with
    "error WIX7015: You must accept the Open Source Maintenance Fee (OSMF) EULA"
    until a licence is accepted. WiX 5 is the last version that builds
    unconditionally, and nothing here needs a newer schema. Revisit only if the
    project decides to pay the fee.

.PARAMETER Version
    Product version written into the MSI. Must match Cargo.toml, and must be
    numeric (MSI parses it) - a suffix like 0.1.4-rc.1 is not valid here.

.EXAMPLE
    ./build-msi.ps1 -Version 0.1.4
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^\d+\.\d+\.\d+$')]
    [string] $Version,

    # Skip cargo when the release binaries are already built (CI builds them in
    # an earlier job and only needs the packaging step here).
    [switch] $SkipBuild
)

$ErrorActionPreference = 'Stop'

$packagingWindows = Split-Path -Parent $MyInvocation.MyCommand.Path
$moduleRoot = Resolve-Path (Join-Path $packagingWindows '..\..')
$repoRoot = Resolve-Path (Join-Path $moduleRoot '..\..\..')
$releaseDir = Join-Path $repoRoot 'target\release'

# Keep this in step with `FEATURES` in ops/release.yml, which the installer job
# builds as "desktop,$FEATURES". A locally built .msi that quietly ships fewer
# connectors than the released one is worse than no local build at all.
$features = 'desktop,serve,sql,iceberg,object-store,sqlite,postgres,mysql,delta'

# cargo resolves -p against the workspace containing the *current directory*, not
# the script's location, so honour the "run from anywhere" promise above by
# moving to the repo root for both cargo calls. CI happens to sit at the root
# already; a developer following the .EXAMPLE above does not.
Push-Location $repoRoot
try {
    if (-not $SkipBuild) {
        Write-Host '==> building lakeleto.exe and lakeleto-desktop.exe'
        & cargo build --release --features $features
        if ($LASTEXITCODE -ne 0) { throw "cargo build failed ($LASTEXITCODE)" }
    }

    # Runs even with -SkipBuild: the icon art is gitignored, so a CI job that
    # built the binaries elsewhere still has no .ico until this runs.
    Write-Host '==> regenerating the icon'
    & cargo run --features serve --example gen_icons
    if ($LASTEXITCODE -ne 0) { throw "icon generation failed ($LASTEXITCODE)" }
}
finally {
    Pop-Location
}

foreach ($exe in 'lakeleto.exe', 'lakeleto-desktop.exe') {
    $path = Join-Path $releaseDir $exe
    if (-not (Test-Path $path)) {
        throw "missing $path - build the release binaries first (drop -SkipBuild)"
    }
}

$msi = Join-Path $packagingWindows "lakeleto-$Version-x64.msi"
Write-Host "==> packaging $msi"
& wix build (Join-Path $packagingWindows 'lakeleto.wxs') `
    -o $msi `
    -d Version=$Version `
    -d BinDir=$releaseDir
if ($LASTEXITCODE -ne 0) { throw "wix build failed ($LASTEXITCODE)" }

Write-Host ''
Write-Host "built $msi"
Get-Item $msi | Select-Object Name, @{n = 'MB'; e = { [math]::Round($_.Length / 1MB, 1) } }
