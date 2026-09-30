$dir = "d:\AIRTEL PAYMENTS BANK - KAFKA - TANTOR\Tantor_kafka\tantor-ui\src"
$files = Get-ChildItem -Path $dir -Recurse -File -Include *.ts,*.tsx,*.css

$colorMap = [ordered]@{
    '#818181' = 'var(--text-tertiary)'
    '#3E1363' = 'var(--button-primary)'
    '#332849' = 'var(--button-primary-active)'
    '#CCCCCC' = 'var(--border-default)'
    '#23252D' = 'var(--text-heading)'
    '#E2E8F0' = 'var(--border-subtle)'
    '#282F49' = 'var(--text-primary)'
    '#8E77BB' = 'var(--border-focus)'
    '#64748B' = 'var(--text-muted)'
    '#EF4D5F' = 'var(--color-danger)'
    '#F9F9F9' = 'var(--bg-neutral-light)'
    '#D2D2D7' = 'var(--border-mid)'
    '#5F5E5A' = 'var(--text-secondary)'
    '#71717A' = 'var(--text-neutral)'
    '#DF678B' = 'var(--accent-primary)'
    '#E5E7EB' = 'var(--bg-neutral)'
    '#185FA5' = 'var(--color-info-dark)'
    '#FAF8FF' = 'var(--bg-raised)'
    '#ECECF1' = 'var(--bg-neutral-2)'
    '#069B68' = 'var(--color-success-dark)'
    '#F5F6F8' = 'var(--page-grey)'
    '#5B327F' = 'var(--button-primary-hover)'
    '#36AD8F' = 'var(--color-success)'
    '#DEF0D6' = 'var(--color-success-light)'
    '#854F0B' = 'var(--color-warning)'
    '#FAEEDA' = 'var(--color-warning-light)'
    '#FECBE8' = 'var(--color-danger-light)'
    '#16ABC2' = 'var(--color-info)'
    '#C5EAF0' = 'var(--color-info-light)'
    '#FAE1E8' = 'var(--accent-light)'
}

# Add lowercase and shorthand hex variations for matching
$searchMap = @{}
foreach ($key in $colorMap.Keys) {
    $val = $colorMap[$key]
    $searchMap[$key] = $val
    $searchMap[$key.ToLower()] = $val
}

foreach ($f in $files) {
    # Don't migrate index.css itself so we don't overwrite the tokens we just defined!
    if ($f.Name -eq "index.css") { continue }
    
    $content = Get-Content $f.FullName -Raw
    $modified = $false
    
    foreach ($hex in $searchMap.Keys) {
        $var = $searchMap[$hex]
        # regex to match the hex code, ensuring it's not part of a longer hex code
        $pattern = "(?i)$hex\b"
        if ([regex]::IsMatch($content, $pattern)) {
            $content = [regex]::Replace($content, $pattern, $var)
            $modified = $true
        }
    }
    
    # Handle #FFF and #FFFFFF manually, but ONLY inside CSS property blocks (color:, background:, border:)
    # We will let a manual sweep handle #FFFFFF to ensure we don't mix --text-light and --bg-surface.
    # Actually, we can replace #FFF / #FFFFFF with a regex that checks if it's `color: #FFF` vs `background: #FFF`
    $textLightPattern = '(?i)color:\s*#(?:FFF|FFFFFF)\b'
    if ([regex]::IsMatch($content, $textLightPattern)) {
        $content = [regex]::Replace($content, $textLightPattern, 'color: var(--text-light)')
        $modified = $true
    }
    
    $bgPattern = '(?i)background(?:-color)?:\s*#(?:FFF|FFFFFF)\b'
    if ([regex]::IsMatch($content, $bgPattern)) {
        $content = [regex]::Replace($content, $bgPattern, 'background: var(--bg-surface)')
        $modified = $true
    }

    $borderPattern = '(?i)border-color:\s*#(?:FFF|FFFFFF)\b'
    if ([regex]::IsMatch($content, $borderPattern)) {
        $content = [regex]::Replace($content, $borderPattern, 'border-color: var(--bg-surface)')
        $modified = $true
    }
    
    # React style objects for #FFF
    $styleTextLightPattern = '(?i)color\s*:\s*["'']#(?:FFF|FFFFFF)["'']'
    if ([regex]::IsMatch($content, $styleTextLightPattern)) {
        $content = [regex]::Replace($content, $styleTextLightPattern, 'color: "var(--text-light)"')
        $modified = $true
    }
    
    $styleBgPattern = '(?i)backgroundColor\s*:\s*["'']#(?:FFF|FFFFFF)["'']'
    if ([regex]::IsMatch($content, $styleBgPattern)) {
        $content = [regex]::Replace($content, $styleBgPattern, 'backgroundColor: "var(--bg-surface)"')
        $modified = $true
    }
    
    $styleBgPattern2 = '(?i)background\s*:\s*["'']#(?:FFF|FFFFFF)["'']'
    if ([regex]::IsMatch($content, $styleBgPattern2)) {
        $content = [regex]::Replace($content, $styleBgPattern2, 'background: "var(--bg-surface)"')
        $modified = $true
    }

    if ($modified) {
        # use write all text to ensure UTF8 without BOM
        [System.IO.File]::WriteAllText($f.FullName, $content, [System.Text.UTF8Encoding]::new($false))
    }
}
Write-Host "Color migration completed."
