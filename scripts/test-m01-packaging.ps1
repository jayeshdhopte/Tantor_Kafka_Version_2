[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
. (Join-Path $PSScriptRoot 'M01PackagingConfiguration.ps1')
$template = Join-Path $root 'tantor-ui\nginx.production.conf'

function Invoke-ValidConfiguration {
    param([hashtable]$Overrides = @{})
    $arguments = @{
        PublicOrigin = 'https://tantor.corp.internal'
        CorsAllowedOrigins = 'https://tantor.corp.internal'
        OidcIssuerUri = 'https://identity.corp.internal/realms/tantor'
        OidcAudience = 'tantor-ui'
        KeycloakUrl = 'https://identity.corp.internal'
        KeycloakRealm = 'tantor'
        MonitoringMode = 'direct'
        PrometheusUrl = 'http://monitoring:19090'
        NginxTemplatePath = $template
    }
    foreach ($entry in $Overrides.GetEnumerator()) { $arguments[$entry.Key] = $entry.Value }
    Get-M01DeploymentConfiguration @arguments
}

function Assert-Throws([scriptblock]$Action, [string]$ExpectedMessage) {
    try {
        & $Action
        throw "Expected failure containing: $ExpectedMessage"
    } catch {
        if ($_.Exception.Message -notlike "*$ExpectedMessage*") { throw }
    }
}

$valid = Invoke-ValidConfiguration
if ($valid.RuntimeJavaScript -notmatch '"publicOrigin":"https://tantor\.corp\.internal"') {
    throw 'Generated runtime configuration does not contain the validated public origin.'
}
if ($valid.NginxConfiguration -match 'configuration\.invalid') {
    throw 'Valid packaging left the CSP placeholder unresolved.'
}
if ($valid.PrometheusUrl -ne 'http://monitoring:19090') {
    throw 'Valid direct monitoring endpoint was not preserved.'
}

Assert-Throws { Invoke-ValidConfiguration @{ PrometheusUrl = '' } } 'PrometheusUrl is required'
Assert-Throws { Invoke-ValidConfiguration @{ PrometheusUrl = 'not-a-url' } } 'PrometheusUrl must be an absolute'
Assert-Throws { Invoke-ValidConfiguration @{ MonitoringMode = 'unsupported' } } 'MonitoringMode'
Assert-Throws { Invoke-ValidConfiguration @{ CorsAllowedOrigins = 'https://other.corp.internal' } } 'must include PublicOrigin'
Assert-Throws { Invoke-ValidConfiguration @{ ApiBasePath = '/different-api' } } 'Runtime API routes must match'

$temporaryTemplate = Join-Path ([IO.Path]::GetTempPath()) ("tantor-m01-nginx-{0}.conf" -f [Guid]::NewGuid())
try {
    $content = (Get-Content -Raw -LiteralPath $template).Replace('https://configuration.invalid', 'https://hardcoded.corp.internal')
    [System.IO.File]::WriteAllText($temporaryTemplate, $content, [System.Text.UTF8Encoding]::new($false))
    Assert-Throws { Invoke-ValidConfiguration @{ NginxTemplatePath = $temporaryTemplate } } 'missing the required Keycloak CSP placeholder'
} finally {
    Remove-Item -LiteralPath $temporaryTemplate -Force -ErrorAction SilentlyContinue
}

Write-Host 'M-01 executable packaging configuration checks passed.'
