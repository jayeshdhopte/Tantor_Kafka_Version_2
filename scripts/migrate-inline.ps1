$dir = "d:\AIRTEL PAYMENTS BANK - KAFKA - TANTOR\Tantor_kafka\tantor-ui\src"
$files = Get-ChildItem -Path $dir -Recurse -File -Include *.tsx

# We will replace very specific exact strings of inline styles with nothing (if we add a className manually) 
# or with tokenized versions. To keep it safe via automation, we will replace the literals inside the inline styles 
# with CSS variables, since moving them to className requires AST parsing to handle existing classNames.

$replacements = @(
    @{ Pattern = "padding:\s*['`]16px['`]"; Replacement = "padding: 'var(--space-4)'" }
    @{ Pattern = "padding:\s*['`]24px['`]"; Replacement = "padding: 'var(--space-6)'" }
    @{ Pattern = "padding:\s*['`]8px['`]"; Replacement = "padding: 'var(--space-2)'" }
    @{ Pattern = "padding:\s*['`]12px['`]"; Replacement = "padding: 'var(--space-3)'" }
    @{ Pattern = "margin:\s*['`]16px['`]"; Replacement = "margin: 'var(--space-4)'" }
    @{ Pattern = "margin:\s*['`]24px['`]"; Replacement = "margin: 'var(--space-6)'" }
    @{ Pattern = "gap:\s*['`]8px['`]"; Replacement = "gap: 'var(--space-2)'" }
    @{ Pattern = "gap:\s*['`]16px['`]"; Replacement = "gap: 'var(--space-4)'" }
    @{ Pattern = "gap:\s*['`]24px['`]"; Replacement = "gap: 'var(--space-6)'" }
    @{ Pattern = "borderRadius:\s*['`]8px['`]"; Replacement = "borderRadius: 'var(--radius-md)'" }
    @{ Pattern = "borderRadius:\s*['`]12px['`]"; Replacement = "borderRadius: 'var(--radius-lg)'" }
    @{ Pattern = "fontSize:\s*['`]12px['`]"; Replacement = "fontSize: 'var(--text-xs)'" }
    @{ Pattern = "fontSize:\s*['`]13px['`]"; Replacement = "fontSize: 'var(--text-sm)'" }
    @{ Pattern = "fontSize:\s*['`]14px['`]"; Replacement = "fontSize: 'var(--text-base)'" }
    @{ Pattern = "fontSize:\s*['`]16px['`]"; Replacement = "fontSize: 'var(--text-md)'" }
    @{ Pattern = "fontSize:\s*['`]20px['`]"; Replacement = "fontSize: 'var(--text-xl)'" }
    @{ Pattern = "fontSize:\s*['`]24px['`]"; Replacement = "fontSize: 'var(--text-2xl)'" }
    @{ Pattern = "fontWeight:\s*400"; Replacement = "fontWeight: 'var(--font-regular)'" }
    @{ Pattern = "fontWeight:\s*500"; Replacement = "fontWeight: 'var(--font-medium)'" }
    @{ Pattern = "fontWeight:\s*600"; Replacement = "fontWeight: 'var(--font-semibold)'" }
    @{ Pattern = "fontWeight:\s*700"; Replacement = "fontWeight: 'var(--font-bold)'" }
)

foreach ($f in $files) {
    $content = Get-Content $f.FullName -Raw
    $modified = $false
    
    foreach ($rule in $replacements) {
        if ([regex]::IsMatch($content, $rule.Pattern)) {
            $content = [regex]::Replace($content, $rule.Pattern, $rule.Replacement)
            $modified = $true
        }
    }
    
    if ($modified) {
        [System.IO.File]::WriteAllText($f.FullName, $content, [System.Text.UTF8Encoding]::new($false))
    }
}
Write-Host "Inline styles tokens migration completed."
