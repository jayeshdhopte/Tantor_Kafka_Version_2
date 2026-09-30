Set-StrictMode -Version Latest

function Get-M01DeploymentConfiguration {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$PublicOrigin,
        [Parameter(Mandatory)][string]$CorsAllowedOrigins,
        [Parameter(Mandatory)][string]$OidcIssuerUri,
        [Parameter(Mandatory)][string]$OidcAudience,
        [Parameter(Mandatory)][string]$KeycloakUrl,
        [Parameter(Mandatory)][string]$KeycloakRealm,
        [Parameter(Mandatory)][ValidateSet('direct', 'grafana-proxy')][string]$MonitoringMode,
        [string]$PrometheusUrl,
        [string]$GrafanaUrl,
        [string]$GrafanaDatasourceUid,
        [string]$ApiBasePath = '/api',
        [string]$ArtifactApiBasePath = '/api/v1/artifacts',
        [Parameter(Mandatory)][string]$NginxTemplatePath
    )

    function Get-AbsoluteUri([string]$Name, [string]$Value, [bool]$HttpsOnly) {
        $parsed = $null
        if (-not [Uri]::TryCreate($Value, [UriKind]::Absolute, [ref]$parsed) -or
            [string]::IsNullOrWhiteSpace($parsed.Host) -or $parsed.UserInfo -or $parsed.Query -or $parsed.Fragment -or
            ($HttpsOnly -and $parsed.Scheme -ne 'https') -or
            (-not $HttpsOnly -and $parsed.Scheme -notin @('http', 'https'))) {
            $schemeDescription = if ($HttpsOnly) { 'HTTPS' } else { 'HTTP(S)' }
            throw "$Name must be an absolute $schemeDescription URL without credentials, query, or fragment."
        }
        if ($parsed.Host -in @('localhost', '127.0.0.1', '::1') -or
            $parsed.Host -match '\.(example|invalid|test)$') {
            throw "$Name cannot use a loopback or placeholder host."
        }
        return $parsed
    }

    $public = Get-AbsoluteUri 'PublicOrigin' $PublicOrigin $true
    if ($public.AbsolutePath -ne '/') { throw 'PublicOrigin must be an origin without a path.' }
    $keycloak = Get-AbsoluteUri 'KeycloakUrl' $KeycloakUrl $true
    if ($keycloak.AbsolutePath -ne '/') { throw 'KeycloakUrl must be an origin without a path.' }
    $issuer = Get-AbsoluteUri 'OidcIssuerUri' $OidcIssuerUri $true
    $expectedIssuer = "$($keycloak.GetLeftPart([UriPartial]::Authority))/realms/$KeycloakRealm"
    if ($issuer.AbsoluteUri.TrimEnd('/') -cne $expectedIssuer) {
        throw 'OidcIssuerUri must exactly match KeycloakUrl plus /realms/KeycloakRealm.'
    }
    if ([string]::IsNullOrWhiteSpace($OidcAudience)) { throw 'OidcAudience is required.' }

    $corsOrigins = @($CorsAllowedOrigins -split ',' | ForEach-Object { $_.Trim().TrimEnd('/') } | Where-Object { $_ })
    foreach ($origin in $corsOrigins) { $null = Get-AbsoluteUri 'CorsAllowedOrigins entry' $origin $true }
    if ($public.GetLeftPart([UriPartial]::Authority).TrimEnd('/') -cnotin $corsOrigins) {
        throw 'CorsAllowedOrigins must include PublicOrigin.'
    }

    $prometheus = $null
    $grafana = $null
    if ($MonitoringMode -eq 'direct') {
        if ([string]::IsNullOrWhiteSpace($PrometheusUrl)) { throw 'PrometheusUrl is required for direct monitoring mode.' }
        $prometheus = Get-AbsoluteUri 'PrometheusUrl' $PrometheusUrl $false
    } else {
        if ([string]::IsNullOrWhiteSpace($GrafanaUrl)) { throw 'GrafanaUrl is required for grafana-proxy monitoring mode.' }
        if ([string]::IsNullOrWhiteSpace($GrafanaDatasourceUid)) { throw 'GrafanaDatasourceUid is required for grafana-proxy monitoring mode.' }
        $grafana = Get-AbsoluteUri 'GrafanaUrl' $GrafanaUrl $true
    }

    if ($ApiBasePath -ne '/api' -or $ArtifactApiBasePath -ne '/api/v1/artifacts') {
        throw 'Runtime API routes must match the packaged Nginx /api and /api/v1/artifacts routes.'
    }
    $nginxTemplate = Get-Content -Raw -LiteralPath $NginxTemplatePath
    if ($nginxTemplate -notmatch 'location\s+/api\s*\{' -or
        $nginxTemplate -notmatch 'location\s+/api/v1/artifacts\s*\{') {
        throw 'Nginx template does not expose the generated runtime API routes.'
    }
    if ($nginxTemplate -notmatch 'https://configuration\.invalid') {
        throw 'Nginx template is missing the required Keycloak CSP placeholder.'
    }
    $nginx = $nginxTemplate.Replace('https://configuration.invalid', $keycloak.GetLeftPart([UriPartial]::Authority))
    if ($nginx -match 'configuration\.invalid') { throw 'Generated Nginx configuration contains an unresolved CSP placeholder.' }

    $runtime = [ordered]@{
        environment = 'production'
        publicOrigin = $public.GetLeftPart([UriPartial]::Authority)
        authEnabled = $true
        keycloakUrl = $keycloak.GetLeftPart([UriPartial]::Authority)
        keycloakRealm = $KeycloakRealm
        keycloakClientId = $OidcAudience
        apiBasePath = $ApiBasePath
        artifactApiBasePath = $ArtifactApiBasePath
    }
    [pscustomobject]@{
        RuntimeJavaScript = "window.__TANTOR_CONFIG__ = Object.freeze($($runtime | ConvertTo-Json -Compress));"
        NginxConfiguration = $nginx
        PublicOrigin = $public.GetLeftPart([UriPartial]::Authority)
        PrometheusUrl = if ($prometheus) { $prometheus.AbsoluteUri.TrimEnd('/') } else { '' }
        GrafanaUrl = if ($grafana) { $grafana.AbsoluteUri.TrimEnd('/') } else { '' }
    }
}
