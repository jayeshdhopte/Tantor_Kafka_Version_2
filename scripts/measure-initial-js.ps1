$htmlPath = "d:\AIRTEL PAYMENTS BANK - KAFKA - TANTOR\Tantor_kafka\tantor-ui\dist\index.html"
if (-not (Test-Path $htmlPath)) {
    Write-Host "No index.html found."
    exit 1
}

$html = Get-Content $htmlPath -Raw
$matches = [regex]::Matches($html, 'href="/assets/([^"]+\.js)"|src="/assets/([^"]+\.js)"')

$initialJsFiles = @()
foreach ($m in $matches) {
    if ($m.Groups[1].Value) { $initialJsFiles += $m.Groups[1].Value }
    elseif ($m.Groups[2].Value) { $initialJsFiles += $m.Groups[2].Value }
}

$initialJsFiles = $initialJsFiles | Select-Object -Unique

$totalSize = 0
Write-Host "Initial JS Chunks Loaded by index.html:"
foreach ($file in $initialJsFiles) {
    $path = "d:\AIRTEL PAYMENTS BANK - KAFKA - TANTOR\Tantor_kafka\tantor-ui\dist\assets\" + $file
    if (Test-Path $path) {
        $size = (Get-Item $path).Length
        $totalSize += $size
        Write-Host " - $file ($([math]::Round($size/1024)) KB)"
    }
}

Write-Host "True Total Initial JS: $([math]::Round($totalSize/1024)) KB"
