[CmdletBinding()]
param(
    [switch]$SkipGitleaks,
    [switch]$SkipHistory,
    [string]$ReleaseArchive = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

$prohibitedPathPattern = '(?i)(^|/)(Test(?:Db)?\.java|InjectCluster\.java|last_call\.json|DeleteConnections\.java|(?:apply|extract|patch_json)\.py|analyze_lint[^/]*\.py|patch_[^/]*\.py|view_cd\.py|lint_[^/]*\.json|[^/]+\.(?:bak|orig|rej|patch))$'
$tracked = @(git -C $root ls-files)
$untrackedSource = @(git -C $root ls-files --others --exclude-standard)
$candidateSourcePaths = @($tracked + $untrackedSource | Sort-Object -Unique)
$trackedProhibited = @($tracked | Where-Object {
    $_ -match $prohibitedPathPattern -and (Test-Path -LiteralPath (Join-Path $root $_))
})
if ($trackedProhibited.Count -gt 0) {
    throw "Prohibited recovery/developer artifacts are tracked: $($trackedProhibited -join ', ')"
}

$presentProhibited = @(Get-ChildItem -LiteralPath $root -Recurse -Force -File -ErrorAction SilentlyContinue |
    Where-Object {
        $_.FullName -notmatch '[\\/](?:\.git|node_modules|target|dist|\.runtime)[\\/]' -and
        [IO.Path]::GetRelativePath($root, $_.FullName).Replace('\', '/') -match $prohibitedPathPattern
    } |
    ForEach-Object { [IO.Path]::GetRelativePath($root, $_.FullName) })
if ($presentProhibited.Count -gt 0) {
    throw "Prohibited recovery/developer artifacts remain in the worktree: $($presentProhibited -join ', ')"
}

$productionCompose = Get-Content -Raw -LiteralPath (Join-Path $root 'podman-compose.production.yml')
foreach ($credential in 'TANTOR_DB_USER', 'TANTOR_DB_PASSWORD') {
    if ($productionCompose -notmatch "(?m)^\s*-\s+$credential\s*$") {
        throw "Production Compose does not inject $credential as a secret."
    }
    if ($productionCompose -match "(?m)^\s+$credential\s*:\s*[^`$\{\r\n]+$") {
        throw "Production Compose contains a literal value for $credential."
    }
}

if ($ReleaseArchive) {
    $archive = (Resolve-Path -LiteralPath $ReleaseArchive).Path
    $entries = @(tar -tf $archive)
    if ($LASTEXITCODE -ne 0) { throw "Could not inspect release archive: $archive" }
    $badEntries = @($entries | Where-Object { $_ -match $prohibitedPathPattern -or $_ -match '(?i)(^|/)(?:\.env|[^/]+\.(?:key|pem|pfx|p12|jks|keystore))$' })
    if ($badEntries.Count -gt 0) {
        throw "Release archive contains prohibited files: $($badEntries -join ', ')"
    }
}

if (-not $SkipGitleaks) {
    $gitleaks = Get-Command gitleaks -ErrorAction SilentlyContinue
    if (-not $gitleaks) {
        throw 'Gitleaks is required. Install v8.29.1 or run the pinned CI/container scan.'
    }
    $configPath = Join-Path $root '.gitleaks.toml'
    $snapshot = Join-Path ([IO.Path]::GetTempPath()) ("tantor-c08-source-" + [Guid]::NewGuid().ToString('N'))
    try {
        New-Item -ItemType Directory -Path $snapshot | Out-Null
        foreach ($relativePath in $candidateSourcePaths) {
            $sourcePath = Join-Path $root $relativePath
            if (-not (Test-Path -LiteralPath $sourcePath -PathType Leaf)) { continue }
            $destinationPath = Join-Path $snapshot $relativePath
            New-Item -ItemType Directory -Force -Path (Split-Path -Parent $destinationPath) | Out-Null
            Copy-Item -LiteralPath $sourcePath -Destination $destinationPath
        }
        & $gitleaks.Source dir "--config=$configPath" --redact --exit-code=1 $snapshot
        if ($LASTEXITCODE -ne 0) { throw 'Gitleaks current tracked-source scan failed.' }
    } finally {
        if (Test-Path -LiteralPath $snapshot) { Remove-Item -LiteralPath $snapshot -Recurse -Force }
    }
    if (-not $SkipHistory) {
        & $gitleaks.Source git "--config=$configPath" --redact --exit-code=1 $root
        if ($LASTEXITCODE -ne 0) { throw 'Gitleaks reachable-history scan failed.' }
    }

    if ($ReleaseArchive) {
        $releaseSnapshot = Join-Path ([IO.Path]::GetTempPath()) ("tantor-c08-release-" + [Guid]::NewGuid().ToString('N'))
        try {
            New-Item -ItemType Directory -Path $releaseSnapshot | Out-Null
            tar -xf $archive -C $releaseSnapshot
            if ($LASTEXITCODE -ne 0) { throw 'Could not extract release archive for secret scanning.' }
            & $gitleaks.Source dir "--config=$configPath" --redact --max-archive-depth=2 --exit-code=1 $releaseSnapshot
            if ($LASTEXITCODE -ne 0) { throw 'Gitleaks release-artifact scan failed.' }
        } finally {
            if (Test-Path -LiteralPath $releaseSnapshot) { Remove-Item -LiteralPath $releaseSnapshot -Recurse -Force }
        }
    }

    $fixture = Join-Path ([IO.Path]::GetTempPath()) ("tantor-c08-" + [Guid]::NewGuid().ToString('N'))
    try {
        New-Item -ItemType Directory -Path $fixture | Out-Null
        $syntheticValue = 'TEST_' + 'SECRET_DO_NOT_USE_a8H2m9Q4x7K6p3Z5'
        Set-Content -LiteralPath (Join-Path $fixture 'fixture.txt') -Encoding utf8NoBOM -Value ('password = "' + $syntheticValue + '"')
        & $gitleaks.Source dir "--config=$configPath" --redact --exit-code=1 $fixture
        if ($LASTEXITCODE -ne 1) { throw "Synthetic credential detection returned unexpected exit code $LASTEXITCODE." }
    } finally {
        if (Test-Path -LiteralPath $fixture) { Remove-Item -LiteralPath $fixture -Recurse -Force }
    }
}

Write-Host 'C-08 repository hygiene checks passed.'
$global:LASTEXITCODE = 0
