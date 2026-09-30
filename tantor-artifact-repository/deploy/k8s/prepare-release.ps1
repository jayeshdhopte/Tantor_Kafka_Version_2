[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^registry\.translab\.io/tantor/artifact-repository:[0-9A-Za-z._-]+@sha256:[0-9a-f]{64}$')]
    [string]$ImageReference,
    [string]$OutputPath = (Join-Path $PSScriptRoot 'artifact-repository.release.yaml')
)

$ErrorActionPreference = 'Stop'
$templatePath = Join-Path $PSScriptRoot 'artifact-repository.yaml.template'
$manifest = Get-Content -Raw -LiteralPath $templatePath
$unlockedReference = 'registry.translab.io/tantor/artifact-repository:1.0.0'

if (-not $manifest.Contains($unlockedReference)) {
    throw "Expected template image reference was not found: $unlockedReference"
}
if (Test-Path -LiteralPath $OutputPath) {
    throw "Refusing to overwrite existing release manifest: $OutputPath"
}

$manifest.Replace($unlockedReference, $ImageReference) |
    Set-Content -LiteralPath $OutputPath -Encoding utf8NoBOM

Write-Host "Created digest-locked Kubernetes manifest: $OutputPath"
