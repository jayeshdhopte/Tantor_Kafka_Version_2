$dir = "d:\AIRTEL PAYMENTS BANK - KAFKA - TANTOR\Tantor_kafka\tantor-ui\src"
$files = Get-ChildItem -Path $dir -Recurse -File -Include *.ts,*.tsx,*.css

# Safe context patterns for replacement
$replacements = @(
    # Radius
    @{ Pattern = 'border-radius:\s*6px'; Replacement = 'border-radius: var(--radius-sm)' }
    @{ Pattern = 'border-radius:\s*8px'; Replacement = 'border-radius: var(--radius-md)' }
    @{ Pattern = 'border-radius:\s*12px'; Replacement = 'border-radius: var(--radius-lg)' }
    @{ Pattern = 'border-radius:\s*20px'; Replacement = 'border-radius: var(--radius-xl)' }
    
    # Common paddings (1-value shorthand or specific side)
    @{ Pattern = 'padding:\s*4px'; Replacement = 'padding: var(--space-1)' }
    @{ Pattern = 'padding:\s*8px'; Replacement = 'padding: var(--space-2)' }
    @{ Pattern = 'padding:\s*12px'; Replacement = 'padding: var(--space-3)' }
    @{ Pattern = 'padding:\s*16px'; Replacement = 'padding: var(--space-4)' }
    @{ Pattern = 'padding:\s*20px'; Replacement = 'padding: var(--space-5)' }
    @{ Pattern = 'padding:\s*24px'; Replacement = 'padding: var(--space-6)' }
    @{ Pattern = 'padding:\s*32px'; Replacement = 'padding: var(--space-8)' }
    
    @{ Pattern = 'padding-(left|right|top|bottom):\s*4px'; Replacement = 'padding-$1: var(--space-1)' }
    @{ Pattern = 'padding-(left|right|top|bottom):\s*8px'; Replacement = 'padding-$1: var(--space-2)' }
    @{ Pattern = 'padding-(left|right|top|bottom):\s*12px'; Replacement = 'padding-$1: var(--space-3)' }
    @{ Pattern = 'padding-(left|right|top|bottom):\s*16px'; Replacement = 'padding-$1: var(--space-4)' }
    @{ Pattern = 'padding-(left|right|top|bottom):\s*24px'; Replacement = 'padding-$1: var(--space-6)' }
    
    # Common margins
    @{ Pattern = 'margin:\s*4px'; Replacement = 'margin: var(--space-1)' }
    @{ Pattern = 'margin:\s*8px'; Replacement = 'margin: var(--space-2)' }
    @{ Pattern = 'margin:\s*12px'; Replacement = 'margin: var(--space-3)' }
    @{ Pattern = 'margin:\s*16px'; Replacement = 'margin: var(--space-4)' }
    @{ Pattern = 'margin:\s*24px'; Replacement = 'margin: var(--space-6)' }
    
    @{ Pattern = 'margin-(left|right|top|bottom):\s*4px'; Replacement = 'margin-$1: var(--space-1)' }
    @{ Pattern = 'margin-(left|right|top|bottom):\s*8px'; Replacement = 'margin-$1: var(--space-2)' }
    @{ Pattern = 'margin-(left|right|top|bottom):\s*12px'; Replacement = 'margin-$1: var(--space-3)' }
    @{ Pattern = 'margin-(left|right|top|bottom):\s*16px'; Replacement = 'margin-$1: var(--space-4)' }
    @{ Pattern = 'margin-(left|right|top|bottom):\s*24px'; Replacement = 'margin-$1: var(--space-6)' }

    # Gap
    @{ Pattern = 'gap:\s*4px'; Replacement = 'gap: var(--space-1)' }
    @{ Pattern = 'gap:\s*8px'; Replacement = 'gap: var(--space-2)' }
    @{ Pattern = 'gap:\s*12px'; Replacement = 'gap: var(--space-3)' }
    @{ Pattern = 'gap:\s*16px'; Replacement = 'gap: var(--space-4)' }
    @{ Pattern = 'gap:\s*24px'; Replacement = 'gap: var(--space-6)' }
    
    # Z-index (exact high values replaced with tokens)
    @{ Pattern = 'z-index:\s*1000(?:00)?'; Replacement = 'z-index: var(--z-header)' }
    @{ Pattern = 'z-index:\s*12000'; Replacement = 'z-index: var(--z-dropdown)' }
    @{ Pattern = 'z-index:\s*13000'; Replacement = 'z-index: var(--z-modal)' }
    @{ Pattern = 'z-index:\s*15000'; Replacement = 'z-index: var(--z-popover)' }
    @{ Pattern = 'z-index:\s*20000'; Replacement = 'z-index: var(--z-toast)' }
    @{ Pattern = 'z-index:\s*99999+'; Replacement = 'z-index: var(--z-toast)' }
)

foreach ($f in $files) {
    if ($f.Name -eq "index.css") { continue }
    
    $content = Get-Content $f.FullName -Raw
    $modified = $false
    
    foreach ($rule in $replacements) {
        if ([regex]::IsMatch($content, '(?i)' + $rule.Pattern)) {
            $content = [regex]::Replace($content, '(?i)' + $rule.Pattern, $rule.Replacement)
            $modified = $true
        }
    }
    
    if ($modified) {
        [System.IO.File]::WriteAllText($f.FullName, $content, [System.Text.UTF8Encoding]::new($false))
    }
}
Write-Host "Spacing, radius, and z-index migration completed."
