$distDir = "d:\AIRTEL PAYMENTS BANK - KAFKA - TANTOR\Tantor_kafka\tantor-ui\dist\assets"
if (-not (Test-Path $distDir)) {
    Write-Host "Build output not found. Run 'npm run build' first."
    exit 1
}

$jsFiles = Get-ChildItem -Path $distDir -Filter *.js
$cssFiles = Get-ChildItem -Path $distDir -Filter *.css

# The initial JS payload consists of the entry chunk, react-vendor, and auth-vendor.
# Other manual chunks (like chart-vendor) or route chunks are deferred.
$initialJsFiles = $jsFiles | Where-Object { 
    $_.Name -match "^index-.*\.js$" -or 
    $_.Name -match "^react-vendor-.*\.js$" -or 
    $_.Name -match "^auth-vendor-.*\.js$" -or
    $_.Name -match "^rolldown-runtime-.*\.js$"
}

$mainCss = $cssFiles | Where-Object { $_.Name -match "^index-.*\.css$" }

$budgetMainJs = 300 * 1024 # 300 KB uncompressed (True initial = ~286 KB)
$budgetMainCss = 400 * 1024 # 400 KB uncompressed
$budgetMaxChunk = 500 * 1024 # 500 KB uncompressed

$largestJs = $jsFiles | Sort-Object Length -Descending | Select-Object -First 1

$mainJsSize = 0
foreach ($file in $initialJsFiles) {
    $mainJsSize += $file.Length
}

$mainCssSize = if ($mainCss) { $mainCss.Length } else { 0 }
$largestJsSize = if ($largestJs) { $largestJs.Length } else { 0 }

Write-Host "Initial JS (Uncompressed):"
Write-Host "Before: ~1,144 KB"
Write-Host ("Current: " + [math]::Round($mainJsSize / 1024) + " KB")
Write-Host ("Budget: " + [math]::Round($budgetMainJs / 1024) + " KB")
$statusMainJs = if ($mainJsSize -le $budgetMainJs) { "PASS" } else { "FAIL" }
Write-Host "Status: $statusMainJs"
Write-Host ""

Write-Host "Initial CSS:"
Write-Host "Before: ~350 KB"
Write-Host ("Current: " + [math]::Round($mainCssSize / 1024) + " KB")
Write-Host ("Budget: " + [math]::Round($budgetMainCss / 1024) + " KB")
$statusMainCss = if ($mainCssSize -le $budgetMainCss) { "PASS" } else { "FAIL" }
Write-Host "Status: $statusMainCss"
Write-Host ""

Write-Host "Largest JS chunk:"
Write-Host "Before: ~1,144 KB"
Write-Host ("Current: " + [math]::Round($largestJsSize / 1024) + " KB (" + $largestJs.Name + ")")
Write-Host ("Budget: " + [math]::Round($budgetMaxChunk / 1024) + " KB")
$statusMaxChunk = if ($largestJsSize -le $budgetMaxChunk) { "PASS" } else { "FAIL" }
Write-Host "Status: $statusMaxChunk"

if ($statusMainJs -eq "FAIL" -or $statusMainCss -eq "FAIL" -or $statusMaxChunk -eq "FAIL") {
    Write-Host "`nPerformance check FAILED."
    exit 1
} else {
    Write-Host "`nPerformance check PASSED."
    exit 0
}
