$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$PSScriptRoot = Split-Path -Parent -Path $MyInvocation.MyCommand.Definition
$RootDir = Join-Path $PSScriptRoot ".."

# 1. Parse versions.yaml
$versionsFile = Join-Path $RootDir "versions.yaml"
if (-not (Test-Path $versionsFile)) { throw "versions.yaml not found at root" }

# Read raw YAML for simple extraction (to avoid depending on powershell-yaml module)
$content = Get-Content $versionsFile -Raw
$goVersion = [regex]::Match($content, 'go:\s*([^\s]+)').Groups[1].Value
$pgVersion = [regex]::Match($content, 'postgresql:\s*([^\s]+)').Groups[1].Value
$pgImage = [regex]::Match($content, 'postgres:\s*([^\s]+)').Groups[1].Value
$kafkaVersion = [regex]::Match($content, 'kafka:\s*([^\s]+)').Groups[1].Value
$catalogVersion = [regex]::Match($content, 'tantor_release:\s*([^\s]+)').Groups[1].Value
$catalogNode = [regex]::Match($content, 'node:\s*([^\s]+)').Groups[1].Value
$catalogMaven = [regex]::Match($content, 'maven:\s*([^\s]+)').Groups[1].Value

Write-Host "Catalog Go version: $goVersion"
Write-Host "Catalog PG version: $pgVersion"

# 2. Check Go modules
$goModFiles = Get-ChildItem -Path $RootDir -Filter "go.mod" -Recurse
foreach ($file in $goModFiles) {
    $goContent = Get-Content $file.FullName
    $match = $goContent | Select-String -Pattern '^go\s+([0-9\.]+)'
    if ($match) {
        $actualVersion = $match.Matches[0].Groups[1].Value
        if ($actualVersion -ne $goVersion) {
            throw "Go version drift detected in $($file.FullName). Expected $goVersion, found $actualVersion."
        }
    }
}
Write-Host "Go modules version aligned." -ForegroundColor Green

# 3. Check Compose images
$composeFiles = Get-ChildItem -Path $RootDir -Filter "*compose*.yml" -Recurse
foreach ($file in $composeFiles) {
    $composeContent = Get-Content $file.FullName
    if ($composeContent -match 'image:\s*.*postgres.*latest') {
        throw "Found 'latest' postgres image in $($file.FullName)."
    }
    # Look for postgres image and compare exact string
    $pgMatch = $composeContent | Select-String -Pattern 'image:\s*(.*postgres.*)'
    if ($pgMatch) {
        $actualPgImage = $pgMatch.Matches[0].Groups[1].Value.Trim()
        if ($actualPgImage -notmatch "$pgVersion" -and $actualPgImage -notmatch '\$\{POSTGRES_IMAGE') {
            throw "PostgreSQL version drift in $($file.FullName). Expected to contain $pgVersion but found $actualPgImage"
        }
    }
    # Check Kafka
    $kafkaMatch = $composeContent | Select-String -Pattern 'image:\s*(.*cp-kafka:.*)'
    if ($kafkaMatch) {
        if ($kafkaMatch.Matches[0].Groups[1].Value -notmatch "$kafkaVersion") {
            throw "Kafka version drift in $($file.FullName)."
        }
    }
}
Write-Host "Compose files aligned." -ForegroundColor Green

# 4. Check NPM floating dependencies
$packageJsonPath = Join-Path $RootDir "tantor-ui/package.json"
if (Test-Path $packageJsonPath) {
    $pkgJson = Get-Content $packageJsonPath -Raw | ConvertFrom-Json
    foreach ($dep in $pkgJson.dependencies.psobject.properties) {
        if ($dep.Value -match '[\^~]') {
            throw "Floating dependency found in package.json: $($dep.Name) -> $($dep.Value)"
        }
    }
    Write-Host "NPM dependencies pinned." -ForegroundColor Green
}

# 5. Check Documentation for PostgreSQL version
$readmePath = Join-Path $RootDir "README.md"
if (Test-Path $readmePath) {
    $readmeContent = Get-Content $readmePath -Raw
    if ($readmeContent -match 'PostgreSQL 13') {
        throw "README.md contains stale reference to PostgreSQL 13."
    }
    Write-Host "Documentation versions aligned." -ForegroundColor Green
}

# 6. Check package-release.ps1 logic
$pkgScript = Get-Content (Join-Path $RootDir "scripts/package-release.ps1") -Raw
if ($pkgScript -match '\[string\]\$Version\s*=\s*''[0-9]+') {
    throw "package-release.ps1 has a hardcoded default version!"
}
if ($pkgScript -notmatch 'versions\.yaml') {
    throw "package-release.ps1 does not read versions.yaml"
}
Write-Host "package-release.ps1 logic aligned." -ForegroundColor Green

# 7. Check Provenance in tarball
$tarFiles = Get-ChildItem -Path (Join-Path $RootDir "output") -Filter "*.tar.gz" -ErrorAction SilentlyContinue
if ($tarFiles) {
    $tar = $tarFiles[0]
    $tempExtract = Join-Path $RootDir "temp_extract_prov"
    if (Test-Path $tempExtract) { Remove-Item -Recurse -Force $tempExtract }
    New-Item -ItemType Directory -Force $tempExtract | Out-Null
    tar -xzf $tar.FullName -C $tempExtract provenance.json
    $provPath = Join-Path $tempExtract "provenance.json"
    if (Test-Path $provPath) {
        $prov = Get-Content $provPath -Raw | ConvertFrom-Json
        if ($prov.version -ne $catalogVersion) { throw "Provenance version drift: expected $catalogVersion, got $($prov.version)" }
        if ($prov.toolchain.node -ne $catalogNode) { throw "Provenance Node drift: expected $catalogNode, got $($prov.toolchain.node)" }
        if ($prov.toolchain.maven -ne $catalogMaven) { throw "Provenance Maven drift: expected $catalogMaven, got $($prov.toolchain.maven)" }
        Write-Host "Release provenance aligned." -ForegroundColor Green
    } else {
        Write-Host "Warning: provenance.json not found in tarball." -ForegroundColor Yellow
    }
    Remove-Item -Recurse -Force $tempExtract
}

Write-Host "M-05 Version Management validation passed!" -ForegroundColor Green
