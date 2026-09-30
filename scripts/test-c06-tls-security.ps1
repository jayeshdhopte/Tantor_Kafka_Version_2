Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

function Assert-Text([string]$Path, [string]$Pattern, [string]$Message) {
    $content = Get-Content -Raw -LiteralPath (Join-Path $repositoryRoot $Path)
    if ($content -notmatch $Pattern) { throw $Message }
}

function Assert-NoText([string[]]$Paths, [string]$Pattern, [string]$Message) {
    foreach ($relativePath in $Paths) {
        $item = Get-Item -LiteralPath (Join-Path $repositoryRoot $relativePath)
        $files = if ($item.PSIsContainer) { Get-ChildItem -LiteralPath $item.FullName -File -Recurse } else { @($item) }
        foreach ($file in $files) {
            if ($file.FullName -match '[\\/](target|node_modules|\.gocache)[\\/]') { continue }
            if ((Get-Content -Raw -LiteralPath $file.FullName) -match $Pattern) {
                throw "$Message ($($file.FullName))"
            }
        }
    }
}

Assert-NoText @('tantor-agent', 'tantor-discovery-agent') 'InsecureSkipVerify\s*:\s*true' 'Go trust bypass remains'
Assert-NoText @('tantor-server/src/main') 'setDefaultSSLSocketFactory|setDefaultHostnameVerifier|X509TrustManager|HostnameVerifier' 'Global/permissive JVM TLS logic remains'
Assert-NoText @('install-agent.sh', 'tantor-discovery-agent/install-discovery-agent.sh') 'curl\s+[^\r\n]*(?:-k(?:\s|$)|--insecure)' 'Installer permits unverified TLS'
Assert-NoText @('tantor-agent/configs', 'tantor-discovery-agent/configs', 'tantor-ui/src/pages/ExternalClusters.tsx') 'tls_insecure_skip_verify\s*:\s*true|server_url:\s*["'']http://' 'Example configuration is insecure'

Assert-Text 'tantor-ui/nginx.production.conf' 'ssl_protocols\s+TLSv1\.2\s+TLSv1\.3' 'Production proxy does not enforce TLS 1.2/1.3'
Assert-Text 'tantor-ui/nginx.production.conf' 'ssl_client_certificate\s+/run/secrets/agent-ca\.crt' 'Production proxy lacks the agent CA'
Assert-Text 'tantor-ui/nginx.production.conf' '\$ssl_client_verify\s*!=\s*SUCCESS' 'Machine routes do not enforce verified client certificates'
Assert-Text 'podman-compose.production.yml' '"80:8080"[\s\S]*"443:8443"' 'Production edge port mapping changed unexpectedly'
Assert-Text 'podman-compose.production.yml' 'agent-ca\.crt' 'Production agent CA secret is not mounted'
Assert-Text 'podman-compose.production.yml' 'monitoring-ca\.crt' 'Scoped monitoring CA secret is not mounted'
Assert-Text 'tantor-server/src/main/resources/application.yml' 'forward-headers-strategy:\s*framework' 'Spring does not preserve the proxy HTTPS scheme'
Assert-Text 'tantor-agent/internal/client/client.go' 'MinVersion:\s+tls\.VersionTLS12' 'Primary agent does not enforce TLS 1.2+'
Assert-Text 'tantor-discovery-agent/http_client.go' 'MinVersion:\s+tls\.VersionTLS12' 'Discovery agent does not enforce TLS 1.2+'

Write-Host 'C-06 static TLS/security checks passed.'
