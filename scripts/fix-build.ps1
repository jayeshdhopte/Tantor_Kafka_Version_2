$f1 = "d:\AIRTEL PAYMENTS BANK - KAFKA - TANTOR\Tantor_kafka\tantor-ui\src\pages\Partitions.tsx"
$c1 = Get-Content $f1 -Raw
$c1 = $c1 -replace 'backgroundcolor', 'backgroundColor'
[IO.File]::WriteAllText($f1, $c1, [Text.UTF8Encoding]::new($false))

$dir = "d:\AIRTEL PAYMENTS BANK - KAFKA - TANTOR\Tantor_kafka\tantor-ui\src"
$files = Get-ChildItem -Path $dir -Recurse -File -Include *.ts,*.tsx
foreach ($f in $files) {
    $c = Get-Content $f.FullName -Raw
    if ($c -match [char]160) {
        $c = $c -replace [char]160, ' '
        [IO.File]::WriteAllText($f.FullName, $c, [Text.UTF8Encoding]::new($false))
    }
}
Write-Host "Fixed build errors."
