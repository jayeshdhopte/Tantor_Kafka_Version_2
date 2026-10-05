# build.ps1 - Automated Build Script for Tantor Java Backends

$ErrorActionPreference = 'Stop'

$MavenVersion = "3.9.6"
$MavenUrl = "https://archive.apache.org/dist/maven/maven-3/$MavenVersion/binaries/apache-maven-$MavenVersion-bin.zip"
$MavenZip = "$PSScriptRoot\apache-maven.zip"
$MavenDir = "$PSScriptRoot\apache-maven-$MavenVersion"
$MvnCmd = "$MavenDir\bin\mvn.cmd"

# Force JAVA_HOME to the downloaded JDK 21
$env:JAVA_HOME = "$PSScriptRoot\jdk21\jdk-21.0.2"
$candidateJavaHome1 = "C:\Program Files\Java\jdk-21"
$candidateJavaHome2 = "C:\Program Files\Microsoft\jdk-21.0.10.7-hotspot"
$candidateJavaHome3 = "C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot"
if ([string]::IsNullOrWhiteSpace($env:JAVA_HOME) -or -not (Test-Path "$env:JAVA_HOME\bin\java.exe")) {
    if (Test-Path $candidateJavaHome0) {
        $env:JAVA_HOME = $candidateJavaHome0
    } elseif (Test-Path $candidateJavaHome1) {
        $env:JAVA_HOME = $candidateJavaHome1
    } elseif (Test-Path $candidateJavaHome2) {
        $env:JAVA_HOME = $candidateJavaHome2
    } elseif (Test-Path $candidateJavaHome3) {
        $env:JAVA_HOME = $candidateJavaHome3
    }
}
if (![string]::IsNullOrWhiteSpace($env:JAVA_HOME)) {
    $env:Path = "$env:JAVA_HOME\bin;" + $env:Path
}

# 1. Download Maven if not exists
if (-Not (Test-Path $MvnCmd)) {
    Write-Host "Maven not found. Downloading Apache Maven $MavenVersion..." -ForegroundColor Cyan
    Invoke-WebRequest -Uri $MavenUrl -OutFile $MavenZip
    Write-Host "Extracting Maven..." -ForegroundColor Cyan
    Expand-Archive -Path $MavenZip -DestinationPath $PSScriptRoot -Force
    Remove-Item $MavenZip
}

Write-Host "Using Maven at $MvnCmd" -ForegroundColor Green

# 2. Build Artifact Repository
Write-Host "`n=== Building Artifact Repository ===" -ForegroundColor Magenta
cd "$PSScriptRoot\tantor-artifact-repository"
& $MvnCmd clean verify -DskipTests
if ($LASTEXITCODE -ne 0) { throw "Artifact Repository verification failed." }

# 3. Build Management Server
Write-Host "`n=== Building Management Server ===" -ForegroundColor Magenta
cd "$PSScriptRoot\tantor-server"
& $MvnCmd clean verify -DskipTests
if ($LASTEXITCODE -ne 0) { throw "Management Server verification failed." }

# Restore original directory
cd $PSScriptRoot

# 4. Build both Linux agents from the checked-in sources.
$GoCommand = if (Test-Path "$PSScriptRoot\go\bin\go.exe") {
    "$PSScriptRoot\go\bin\go.exe"
} else {
    $goOnPath = Get-Command go -ErrorAction SilentlyContinue
    if ($goOnPath) { $goOnPath.Source } else { $null }
}
if (-not $GoCommand) { Write-Warning "Go 1.22+ is required to build agents. Skipping agent build."; exit 0 }
$previousGoOs = $env:GOOS
$previousGoArch = $env:GOARCH
try {
    $env:GOOS = 'linux'
    $env:GOARCH = 'amd64'
    Write-Host "`n=== Building Tantor Internal Agent (Linux amd64 and arm64) ===" -ForegroundColor Magenta
    Push-Location "$PSScriptRoot\internal-agent\source"
    try {
        foreach ($architecture in @('amd64', 'arm64')) {
            $env:GOARCH = $architecture
            & $GoCommand build -o "$PSScriptRoot\internal-agent\bin\tantor-agent-linux-$architecture" ./cmd/agent
            if ($LASTEXITCODE -ne 0) { throw "Tantor Internal Agent $architecture build failed." }
        }
    } finally { Pop-Location }

    Write-Host "`n=== Building Tantor Discovery Agent (Linux amd64 and arm64) ===" -ForegroundColor Magenta
    Push-Location "$PSScriptRoot\tantor-discovery-agent"
    try {
        foreach ($architecture in @('amd64', 'arm64')) {
            $env:GOARCH = $architecture
            & $GoCommand build -o "tantor-discovery-agent-linux-$architecture" .
            if ($LASTEXITCODE -ne 0) { throw "Tantor Discovery Agent $architecture build failed." }
        }
    } finally { Pop-Location }
} finally {
    $env:GOOS = $previousGoOs
    $env:GOARCH = $previousGoArch
}

Write-Host "`nBuild Complete!" -ForegroundColor Green
Write-Host "To start the Artifact Repository:"
Write-Host "  java -jar tantor-artifact-repository\target\tantor-artifact-repository-1.0.0.jar"
Write-Host "To start the Management Server:"
Write-Host "  java -jar tantor-server\target\tantor-server-1.0.0.jar"
