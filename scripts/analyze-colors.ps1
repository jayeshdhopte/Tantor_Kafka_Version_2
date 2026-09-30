$dir = "d:\AIRTEL PAYMENTS BANK - KAFKA - TANTOR\Tantor_kafka\tantor-ui\src"
$files = Get-ChildItem -Path $dir -Recurse -File -Include *.ts,*.tsx,*.css

$colors = @{}

foreach ($f in $files) {
    $content = Get-Content $f.FullName -Raw
    if ($content) {
        $matches = [regex]::Matches($content, '#([0-9a-fA-F]{3,8})\b')
        foreach ($match in $matches) {
            # Normalize to uppercase and 6 digits if it's 3 digits (approximate, ignoring alpha for a sec, just upper)
            $hex = "#" + $match.Groups[1].Value.ToUpper()
            
            # Simple 3-to-6 char conversion for standard colors
            if ($hex.Length -eq 4) {
                $hex = "#" + $hex[1] + $hex[1] + $hex[2] + $hex[2] + $hex[3] + $hex[3]
            }

            if (-not $colors.ContainsKey($hex)) {
                $colors[$hex] = 0
            }
            $colors[$hex]++
        }
    }
}

$colors.GetEnumerator() | Sort-Object Value -Descending | Select-Object -First 30 | Format-Table -AutoSize
