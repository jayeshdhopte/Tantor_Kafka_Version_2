# start-backend.ps1 - Start Tantor backend services for local development.

[CmdletBinding()]
param(
    [switch]$Restart
)

$ErrorActionPreference = "Stop"

$RootDir = $PSScriptRoot
$RuntimeDir = Join-Path $RootDir ".runtime"
$LogDir = Join-Path $RuntimeDir "logs"
$PidDir = Join-Path $RuntimeDir "pids"

New-Item -ItemType Directory -Force -Path $LogDir, $PidDir | Out-Null

function Import-DotEnv {
    param([string]$Path)

    if (!(Test-Path $Path)) {
        Write-Host "No .env file found." -ForegroundColor Yellow
        return
    }

    Write-Host "Loading environment variables from .env..." -ForegroundColor Cyan
    foreach ($line in Get-Content $Path) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#") -or !$trimmed.Contains("=")) {
            continue
        }

        $name, $value = $trimmed.Split("=", 2)
        $name = $name.Trim()
        $value = $value.Trim().Trim('"').Trim("'")
        if (![string]::IsNullOrWhiteSpace($name)) {
            Set-Item -Path "env:$name" -Value $value
        }
    }
}

function Normalize-PathEnvironment {
    $envVars = [System.Environment]::GetEnvironmentVariables("Process")
    $pathValue = $envVars["Path"]
    if ([string]::IsNullOrWhiteSpace($pathValue)) {
        $pathValue = $envVars["PATH"]
    }
    if (![string]::IsNullOrWhiteSpace($pathValue)) {
        [System.Environment]::SetEnvironmentVariable("PATH", $null, "Process")
        [System.Environment]::SetEnvironmentVariable("Path", $pathValue, "Process")
    }
}

function Resolve-Java {
    $candidateJavaHome = "C:\Program Files\Java\jdk-21"
    if ([string]::IsNullOrWhiteSpace($env:JAVA_HOME) -and (Test-Path $candidateJavaHome)) {
        $env:JAVA_HOME = $candidateJavaHome
    }

    if (![string]::IsNullOrWhiteSpace($env:JAVA_HOME)) {
        $javaFromHome = Join-Path $env:JAVA_HOME "bin\java.exe"
        if (Test-Path $javaFromHome) {
            $env:Path = "$env:JAVA_HOME\bin;$env:Path"
            return $javaFromHome
        }
    }

    $javaOnPath = Get-Command java.exe -ErrorAction SilentlyContinue
    if ($javaOnPath) {
        return $javaOnPath.Source
    }

    throw "Java was not found. Install JDK 21 or set JAVA_HOME before starting the backend."
}

function Get-TantorJavaProcesses {
    param(
        [string]$JarFragment,
        [string]$JarPath
    )

    $fragmentNeedle = $JarFragment.ToLowerInvariant()
    $pathNeedle = $JarPath.ToLowerInvariant()

    try {
        return Get-CimInstance Win32_Process -Filter "Name = 'java.exe'" | Where-Object {
            $commandLine = $_.CommandLine
            if ([string]::IsNullOrWhiteSpace($commandLine)) {
                return $false
            }

            $normalized = $commandLine.ToLowerInvariant()
            return $normalized.Contains($fragmentNeedle) -or $normalized.Contains($pathNeedle)
        }
    } catch {
        Write-Host "Could not inspect existing Java processes: $($_.Exception.Message)" -ForegroundColor Yellow
        return @()
    }
}

function Stop-ServiceIfRunning {
    param([hashtable]$Service)

    $processes = @(Get-TantorJavaProcesses -JarFragment $Service.JarFragment -JarPath $Service.Jar)
    foreach ($process in $processes) {
        Write-Host "Stopping $($Service.DisplayName) (PID $($process.ProcessId))..." -ForegroundColor Yellow
        Stop-Process -Id $process.ProcessId -Force -ErrorAction SilentlyContinue
    }

    if (Test-Path $Service.PidFile) {
        Remove-Item -Path $Service.PidFile -Force
    }
}

Import-DotEnv -Path (Join-Path $RootDir ".env")
if ([string]::IsNullOrWhiteSpace($env:SPRING_PROFILES_ACTIVE)) {
    $env:SPRING_PROFILES_ACTIVE = 'dev'
}
Normalize-PathEnvironment
$JavaExe = Resolve-Java

$services = @(
    @{
        Name = "artifact-repository"
        DisplayName = "Artifact Repository"
        Port = 8081
        JarFragment = "tantor-artifact-repository\target\tantor-artifact-repository-1.0.0.jar"
        Jar = Join-Path $RootDir "tantor-artifact-repository\target\tantor-artifact-repository-1.0.0.jar"
        PidFile = Join-Path $PidDir "artifact-repository.pid"
        StdOut = Join-Path $LogDir "artifact-repository.out.log"
        StdErr = Join-Path $LogDir "artifact-repository.err.log"
    },
    @{
        Name = "management-server"
        DisplayName = "Management Server"
        Port = 8443
        JarFragment = "tantor-server\target\tantor-server-1.0.0.jar"
        Jar = Join-Path $RootDir "tantor-server\target\tantor-server-1.0.0.jar"
        PidFile = Join-Path $PidDir "management-server.pid"
        StdOut = Join-Path $LogDir "management-server.out.log"
        StdErr = Join-Path $LogDir "management-server.err.log"
    }
)

foreach ($service in $services) {
    if (!(Test-Path $service.Jar)) {
        throw "$($service.DisplayName) jar was not found at $($service.Jar). Run .\build.ps1 first."
    }
}

if ($Restart) {
    foreach ($service in $services) {
        Stop-ServiceIfRunning -Service $service
    }
    Start-Sleep -Seconds 2
}

foreach ($service in $services) {
    $running = @(Get-TantorJavaProcesses -JarFragment $service.JarFragment -JarPath $service.Jar)
    if ($running.Count -gt 0) {
        $pids = ($running | ForEach-Object { $_.ProcessId }) -join ", "
        Write-Host "$($service.DisplayName) is already running on/for port $($service.Port) (PID $pids). Skipping." -ForegroundColor Yellow
        continue
    }

    Write-Host "Starting $($service.DisplayName) on port $($service.Port)..." -ForegroundColor Magenta
    $process = Start-Process `
        -FilePath $JavaExe `
        -ArgumentList "-jar `"$($service.Jar)`"" `
        -WorkingDirectory $RootDir `
        -RedirectStandardOutput $service.StdOut `
        -RedirectStandardError $service.StdErr `
        -WindowStyle Hidden `
        -PassThru

    Set-Content -Path $service.PidFile -Value $process.Id
    Write-Host "  PID $($process.Id)" -ForegroundColor Green
    Write-Host "  Logs: $($service.StdOut) and $($service.StdErr)" -ForegroundColor DarkGray
}

Write-Host "`nBackend start command finished." -ForegroundColor Green
Write-Host "Use .\stop-backend.ps1 to stop these background services." -ForegroundColor Cyan
Write-Host "Use .\start-backend.ps1 -Restart to stop any existing Tantor backend services and start fresh." -ForegroundColor Cyan
