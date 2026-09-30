$dir = "d:\AIRTEL PAYMENTS BANK - KAFKA - TANTOR\Tantor_kafka\tantor-ui\src"
$files = Get-ChildItem -Path $dir -Recurse -File -Include *.tsx

$styles = @{}

foreach ($f in $files) {
    $content = Get-Content $f.FullName -Raw
    if ($content) {
        $matches = [regex]::Matches($content, 'style=\{\{([^}]+)\}\}')
        foreach ($match in $matches) {
            $inner = $match.Groups[1].Value.Trim() -replace '\s+', ' '
            if (-not $styles.ContainsKey($inner)) {
                $styles[$inner] = 0
            }
            $styles[$inner]++
        }
    }
}

$styles.GetEnumerator() | Sort-Object Value -Descending | Select-Object -First 30 | Format-Table -AutoSize
