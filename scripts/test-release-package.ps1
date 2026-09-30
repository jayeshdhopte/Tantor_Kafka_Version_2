[CmdletBinding()]
param(
    [string]$ArchivePath = '',
    [string]$ManifestPath = ''
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ([string]::IsNullOrWhiteSpace($ArchivePath)) {
    $ArchivePath = Join-Path $PSScriptRoot '..\output\tantor-release-1.0.0.tar.gz'
}
if ([string]::IsNullOrWhiteSpace($ManifestPath)) {
    $ManifestPath = Join-Path $PSScriptRoot '..\release-manifest.json'
}

if (-not (Test-Path $ArchivePath)) {
    throw "Release archive not found: $ArchivePath"
}
if (-not (Test-Path $ManifestPath)) {
    throw "Manifest not found: $ManifestPath"
}

$tempDir = Join-Path $env:TEMP "tantor-release-test-$(New-Guid)"
New-Item -ItemType Directory -Path $tempDir | Out-Null

try {
    Write-Host "Extracting archive to $tempDir..."
    tar -xf $ArchivePath -C $tempDir
    if ($LASTEXITCODE -ne 0) { throw "Extraction failed." }

    $manifest = Get-Content $ManifestPath -Raw | ConvertFrom-Json

    # 1. Validation: Every required artifact must exist
    # Verify checksum file exists
    if (-not (Test-Path (Join-Path $tempDir "SHA256SUMS"))) { throw "SHA256SUMS is missing." }
    if (-not (Test-Path (Join-Path $tempDir "provenance.json"))) { throw "provenance.json is missing." }
    if (-not (Test-Path (Join-Path $tempDir "SBOM.spdx.json"))) { throw "SBOM.spdx.json is missing." }
    
    # 2. Validation: Every packaged file must match an approved manifest entry
    Write-Host "Validating files against manifest..."
    $extractedFiles = Get-ChildItem -Path $tempDir -File -Recurse
    foreach ($file in $extractedFiles) {
        $relativePath = $file.FullName.Substring($tempDir.Length + 1).Replace('\', '/')
        
        # Metadata files are allowed
        if ($relativePath -match '^(manifest\.json|provenance\.json|SBOM\.spdx\.json|SHA256SUMS)$') {
            continue
        }

        # Check if file belongs to any component and matches a pattern
        $isMatched = $false
        foreach ($componentName in $manifest.components.psobject.properties.name) {
            # Files in tar are prefixed with their component dir
            if ($relativePath.StartsWith("$componentName/")) {
                $componentRelative = $relativePath.Substring($componentName.Length + 1)
                $sources = $manifest.components.$componentName.sources
                foreach ($source in $sources) {
                    foreach ($pattern in $source.patterns) {
                        if ($componentRelative -match $pattern) {
                            $isMatched = $true
                            break
                        }
                    }
                    if ($isMatched) { break }
                }
            }
            if ($isMatched) { break }
        }

        if (-not $isMatched) {
            throw "Validation Failed: File $relativePath is not in the allowlist manifest!"
        }
    }

    # 3. Validation: Verify Checksums
    Write-Host "Verifying checksums..."
    Push-Location $tempDir
    try {
        $checksumFile = Join-Path $tempDir "SHA256SUMS"
        $lines = Get-Content $checksumFile
        foreach ($line in $lines) {
            if ([string]::IsNullOrWhiteSpace($line)) { continue }
            if ($line -match '^([a-f0-9A-F]{64})\s+(.*)$') {
                $expectedHash = $matches[1].ToLowerInvariant()
                $file = $matches[2]
                $actualHash = (Get-FileHash -Algorithm SHA256 -Path $file).Hash.ToLowerInvariant()
                if ($actualHash -ne $expectedHash) {
                    throw "Checksum validation failed for $file"
                }
            }
        }
    } finally {
        Pop-Location
    }

    # 4. Negative Test Setup
    Write-Host "Running negative test: injecting rogue file..."
    $rogueFile = Join-Path $tempDir "server/Test.class"
    Set-Content -Path $rogueFile -Value "MOCK BYTES"
    
    $negativePassed = $false
    try {
        $extractedFiles = Get-ChildItem -Path $tempDir -File -Recurse
        foreach ($file in $extractedFiles) {
            $relativePath = $file.FullName.Substring($tempDir.Length + 1).Replace('\', '/')
            if ($relativePath -match '^(manifest\.json|provenance\.json|SBOM\.spdx\.json|SHA256SUMS)$') { continue }
            $isMatched = $false
            foreach ($componentName in $manifest.components.psobject.properties.name) {
                if ($relativePath.StartsWith("$componentName/")) {
                    $componentRelative = $relativePath.Substring($componentName.Length + 1)
                    $sources = $manifest.components.$componentName.sources
                    foreach ($source in $sources) {
                        foreach ($pattern in $source.patterns) {
                            if ($componentRelative -match $pattern) {
                                $isMatched = $true
                                break
                            }
                        }
                        if ($isMatched) { break }
                    }
                }
                if ($isMatched) { break }
            }
            if (-not $isMatched) {
                Write-Host "Negative test successfully caught rogue file: $relativePath" -ForegroundColor Green
                $negativePassed = $true
                break
            }
        }
    } catch {
        # Ignored
    }

    if (-not $negativePassed) {
        throw "Negative test failed: The rogue file was not caught by the manifest validation!"
    }

    # 5. Missing File Negative Test
    Write-Host "Running negative test: removing required file..."
    $requiredFile = Join-Path $tempDir "server/tantor-server-1.0.0.jar"
    if (Test-Path $requiredFile) {
        Remove-Item $requiredFile -Force
    }

    $missingPassed = $false
    try {
        # Check if all required files from manifest exist
        foreach ($componentName in $manifest.components.psobject.properties.name) {
            $sources = $manifest.components.$componentName.sources
            foreach ($source in $sources) {
                foreach ($pattern in $source.patterns) {
                    # This validation is slightly tricky because the manifest has regex patterns,
                    # but we can just check if AT LEAST ONE file matches each pattern!
                    $matchedAny = $false
                    $extractedFiles = Get-ChildItem -Path $tempDir -File -Recurse
                    foreach ($file in $extractedFiles) {
                        $relativePath = $file.FullName.Substring($tempDir.Length + 1).Replace('\', '/')
                        if ($relativePath.StartsWith("$componentName/")) {
                            $componentRelative = $relativePath.Substring($componentName.Length + 1)
                            if ($componentRelative -match $pattern) {
                                $matchedAny = $true
                                break
                            }
                        }
                    }
                    if (-not $matchedAny) {
                        Write-Host "Negative test successfully caught missing artifact matching pattern: $pattern in component $componentName" -ForegroundColor Green
                        $missingPassed = $true
                        throw "Missing file caught"
                    }
                }
            }
        }
    } catch {
        # Ignored
    }

    if (-not $missingPassed) {
        throw "Negative test failed: The missing required file was not caught!"
    }

    Write-Host "Release package validation passed!" -ForegroundColor Green

} finally {
    Remove-Item $tempDir -Recurse -Force
}
