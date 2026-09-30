$dir = "d:\AIRTEL PAYMENTS BANK - KAFKA - TANTOR\Tantor_kafka\tantor-ui\src"
$files = Get-ChildItem -Path $dir -Recurse -File -Include *.ts,*.tsx,*.css

$hex = 0
$imp = 0
$inl = 0
$var = 0

foreach ($f in $files) {
    $content = Get-Content $f.FullName -Raw
    if ($content) {
        $hex += ([regex]::Matches($content, '#[0-9a-fA-F]{3,8}\b')).Count
        $imp += ([regex]::Matches($content, '!important')).Count
        $inl += ([regex]::Matches($content, 'style=\{\{')).Count
        $var += ([regex]::Matches($content, '--[a-zA-Z0-9-]+:')).Count
    }
}

$baselineHex = 3225
$baselineImp = 699
$baselineInl = 699
$baselineVar = 38

function Get-Reduction($base, $current) {
    if ($base -eq 0) { return "0%" }
    $diff = $base - $current
    if ($diff -lt 0) {
        return "+$([math]::Round(($diff * -1 / $base) * 100))%"
    }
    return "$([math]::Round(($diff / $base) * 100))%"
}

Write-Host "Metric                 Baseline     Current     Reduction"
Write-Host "---------------------------------------------------------"
Write-Host ("Hex colors             {0,-12} {1,-11} {2}" -f $baselineHex, $hex, (Get-Reduction $baselineHex $hex))
Write-Host ("!important             {0,-12} {1,-11} {2}" -f $baselineImp, $imp, (Get-Reduction $baselineImp $imp))
Write-Host ("Inline styles          {0,-12} {1,-11} {2}" -f $baselineInl, $inl, (Get-Reduction $baselineInl $inl))
Write-Host ("CSS variables          {0,-12} {1,-11} +{2}" -f $baselineVar, $var, ($var - $baselineVar))

if ($hex -gt $baselineHex -or $imp -gt $baselineImp -or $inl -gt $baselineInl) {
    Write-Error "Regression detected! Metrics are worse than baseline."
    exit 1
}

Write-Host "`nDesign system metrics check passed."
