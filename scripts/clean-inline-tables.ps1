$dir = "d:\AIRTEL PAYMENTS BANK - KAFKA - TANTOR\Tantor_kafka\tantor-ui\src"
$files = Get-ChildItem -Path $dir -Recurse -File -Include *.tsx

# Find these exact strings and remove the style={{...}} entirely, because they're now in index.css
$replacements = @(
    @{ Pattern = "(?s) className=`"figma-table`" style=\{\{\s*display:\s*'flex',\s*flexDirection:\s*'column',\s*width:\s*'100%'\s*\}\}"; Replacement = ' className="figma-table"' }
    @{ Pattern = "(?s) className=`"figma-table-header`" style=\{\{\s*display:\s*'flex',\s*flexDirection:\s*'row',\s*alignItems:\s*'center',\s*width:\s*'100%',\s*height:\s*'54px',\s*background:\s*'var\(--bg-neutral-light\)',\s*borderBottom:\s*'1px solid var\(--border-default\)',\s*boxSizing:\s*'border-box'\s*\}\}"; Replacement = ' className="figma-table-header"' }
    @{ Pattern = "(?s) className=`"figma-table-body`" style=\{\{\s*display:\s*'flex',\s*flexDirection:\s*'column'\s*\}\}"; Replacement = ' className="figma-table-body"' }
    @{ Pattern = "(?s) className=`"figma-table-row table-row-hover`" style=\{\{\s*display:\s*'flex',\s*flexDirection:\s*'row',\s*alignItems:\s*'center',\s*width:\s*'100%',\s*height:\s*'52px',\s*background:\s*`"var\(--bg-surface\)`",\s*borderBottom:\s*'1px solid var\(--border-default\)',\s*boxSizing:\s*'border-box'\s*\}\}"; Replacement = ' className="figma-table-row table-row-hover"' }
)

$count = 0
foreach ($f in $files) {
    $content = Get-Content $f.FullName -Raw
    $modified = $false
    
    foreach ($rule in $replacements) {
        if ([regex]::IsMatch($content, $rule.Pattern)) {
            $content = [regex]::Replace($content, $rule.Pattern, $rule.Replacement)
            $modified = $true
            $count++
        }
    }
    
    if ($modified) {
        [System.IO.File]::WriteAllText($f.FullName, $content, [System.Text.UTF8Encoding]::new($false))
    }
}
Write-Host "Removed $count static table inline styles."
