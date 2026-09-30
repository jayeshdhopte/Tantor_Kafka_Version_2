[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

function Read-RepositoryFile([string]$RelativePath) {
    Get-Content -Raw -LiteralPath (Join-Path $root $RelativePath)
}

function Assert-Matches([string]$Text, [string]$Pattern, [string]$Message) {
    if ($Text -notmatch $Pattern) { throw $Message }
}

function Assert-DoesNotMatch([string]$Text, [string]$Pattern, [string]$Message) {
    if ($Text -match $Pattern) { throw $Message }
}

$serverBase = Read-RepositoryFile 'tantor-server/src/main/resources/application.yml'
$serverDevelopment = Read-RepositoryFile 'tantor-server/src/main/resources/application-dev.yml'
$productionCompose = Read-RepositoryFile 'podman-compose.production.yml'
$artifactApplication = Read-RepositoryFile 'tantor-artifact-repository/src/main/java/io/translab/tantor/artifact/ArtifactRepositoryApplication.java'
$keycloakService = Read-RepositoryFile 'tantor-ui/src/services/KeycloakService.ts'
$clusterDeployment = Read-RepositoryFile 'tantor-ui/src/pages/ClusterDeployment.tsx'
$runtimeConfiguration = Read-RepositoryFile 'tantor-ui/src/config/runtimeConfig.ts'
$agentConfiguration = Read-RepositoryFile 'tantor-agent/internal/config/config.go'
$discoveryConfiguration = Read-RepositoryFile 'tantor-discovery-agent/config.go'
$connectDeployer = Read-RepositoryFile 'tantor-agent/internal/deploy/connect/deployer.go'
$schemaDeployer = Read-RepositoryFile 'tantor-agent/internal/deploy/schema/deployer.go'
$ksqlDeployer = Read-RepositoryFile 'tantor-agent/internal/deploy/ksqldb/deployer.go'
$producerUtility = Read-RepositoryFile 'tantor-server/src/main/java/io/translab/tantor/server/ProduceMore.java'
$vmDeploy = Read-RepositoryFile 'tantor-agent/deploy-to-vm.ps1'

Assert-DoesNotMatch $serverBase '(?i)(localhost|127\.0\.0\.1|keycloak\.tantor\.io|monitoring\.tantor\.io|loki-prometheus)' 'Base server configuration contains an environment-specific fallback.'
Assert-Matches $serverDevelopment '(?i)(localhost|127\.0\.0\.1)' 'Development defaults are not isolated in application-dev.yml.'
Assert-DoesNotMatch $artifactApplication '(?i)(localhost|192\.168\.|@Value)' 'Artifact Repository still hardcodes CORS origins.'
Assert-DoesNotMatch $keycloakService '(?i)(VITE_KEYCLOAK|keycloak\.tantor\.io)' 'UI Keycloak configuration is still compile-time or hardcoded.'
Assert-DoesNotMatch $clusterDeployment '(?i)(:8081|VITE_ARTIFACT_REPO_URL)' 'UI still constructs a direct Artifact Repository endpoint.'
Assert-Matches $runtimeConfiguration 'window\.__TANTOR_CONFIG__' 'UI runtime configuration is not consumed.'
Assert-Matches $runtimeConfiguration 'same-origin absolute path' 'UI API paths are not constrained to the deployment origin.'
Assert-Matches $runtimeConfiguration 'resolveRuntimeApiUrl' 'UI runtime API paths are validated but not consumed by request routing.'
Assert-Matches $runtimeConfiguration "rewrite\('/api/v1/artifacts'.*artifactApiBasePath\).*rewrite\('/api'.*apiBasePath\)" 'UI request routing does not consume both runtime API path owners.'
Assert-Matches $keycloakService 'runtimeConfig\.apiBasePath' 'Authenticated fetch does not recognize the configured backend API path.'
Assert-Matches $keycloakService 'runtimeConfig\.artifactApiBasePath' 'Authenticated fetch does not recognize the configured Artifact API path.'

foreach ($required in @(
    'SPRING_PROFILES_ACTIVE:\s*production',
    'TANTOR_CORS_ALLOWED_ORIGINS:\s*\$\{TANTOR_CORS_ALLOWED_ORIGINS:\?',
    'TANTOR_OIDC_ISSUER_URI:\s*\$\{TANTOR_OIDC_ISSUER_URI:\?',
    'TANTOR_OIDC_AUDIENCE:\s*\$\{TANTOR_OIDC_AUDIENCE:\?',
    'TANTOR_MONITORING_MODE:\s*\$\{TANTOR_MONITORING_MODE:\?',
    'TANTOR_KAFKA_SECURITY_MODE:\s*\$\{TANTOR_KAFKA_SECURITY_MODE:\?',
    'TANTOR_REPO_PUBLIC_URL:\s*\$\{TANTOR_REPO_PUBLIC_URL:\?',
    'TANTOR_PUBLIC_ORIGIN:\s*\$\{TANTOR_PUBLIC_ORIGIN:\?',
    'TANTOR_UI_RUNTIME_CONFIG_FILE:\?',
    'TANTOR_UI_NGINX_CONFIG_FILE:\?'
)) {
    Assert-Matches $productionCompose $required "Production Compose is missing required configuration contract: $required"
}
Assert-DoesNotMatch $productionCompose 'TANTOR_(?:OIDC_ISSUER_URI|CORS_ALLOWED_ORIGINS):[^\r\n]*(?:localhost|127\.0\.0\.1|\.invalid)' 'Production Compose contains a local/example endpoint fallback.'

Assert-Matches $agentConfiguration 'environment must be development, sit, uat, or production' 'Primary agent environment validation is missing.'
Assert-Matches $agentConfiguration 'cannot use a loopback host outside development' 'Primary agent production loopback rejection is missing.'
Assert-Matches $discoveryConfiguration 'cannot use a loopback host outside development' 'Discovery agent production loopback rejection is missing.'
Assert-DoesNotMatch $connectDeployer 'localhost:9092' 'Kafka Connect silently falls back to a local broker.'
Assert-DoesNotMatch $schemaDeployer 'localhost:9092' 'Schema Registry silently falls back to a local broker.'
Assert-DoesNotMatch $ksqlDeployer 'localhost:(?:9092|8081)' 'ksqlDB silently falls back to local dependencies.'
Assert-DoesNotMatch $producerUtility '192\.168\.' 'Producer utility contains a developer Kafka address.'
Assert-DoesNotMatch $vmDeploy '\$VmIp\s*=\s*["''][0-9]' 'VM deployment helper contains a target-address fallback.'

$securitySources = (Read-RepositoryFile 'tantor-agent/internal/client/client.go') + (Read-RepositoryFile 'tantor-discovery-agent/http_client.go')
Assert-DoesNotMatch $securitySources 'InsecureSkipVerify\s*:\s*true' 'M-01 reintroduced trust-all TLS behavior.'

Write-Host 'M-01 configuration ownership and hardcoding checks passed.'
