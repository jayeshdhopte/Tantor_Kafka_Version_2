$dir = "d:\AIRTEL PAYMENTS BANK - KAFKA - TANTOR\Tantor_kafka\tantor-ui\src"
$files = Get-ChildItem -Path $dir -Recurse -File -Include *.ts,*.tsx,*.css

$count = 0
foreach ($f in $files) {
    $content = Get-Content $f.FullName -Raw
    if ($content -match '!important') {
        # We replace '!important' safely
        $newContent = [regex]::Replace($content, '(?i)\s*!important', '')
        [System.IO.File]::WriteAllText($f.FullName, $newContent, [System.Text.UTF8Encoding]::new($false))
        $count++
    }
}
Write-Host "Removed !important from $count files."
