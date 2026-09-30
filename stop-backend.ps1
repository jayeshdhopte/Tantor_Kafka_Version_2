# stop-backend.ps1 - Stop Tantor backend services started for local development.

[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$RootDir = $PSScriptRoot
$RuntimeDir = Join-Path $RootDir ".runtime"
$PidDir = Join-Path $RuntimeDir "pids"

$services = @(
    @{
        DisplayName = "Artifact Repository"
        JarFragment = "tantor-artifact-repository\target\tantor-artifact-repository-1.0.0.jar"
        Jar = Join-Path $RootDir "tantor-artifact-repository\target\tantor-artifact-repository-1.0.0.jar"
        PidFile = Join-Path $PidDir "artifact-repository.pid"
    },
    @{
        DisplayName = "Management Server"
        JarFragment = "tantor-server\target\tantor-server-1.0.0.jar"
        Jar = Join-Path $RootDir "tantor-server\target\tantor-server-1.0.0.jar"
        PidFile = Join-Path $PidDir "management-server.pid"
    }
)

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
        Write-Host "Could not inspect Java processes: $($_.Exception.Message)" -ForegroundColor Yellow
        return @()
    }
}

foreach ($service in $services) {
    $processes = @(Get-TantorJavaProcesses -JarFragment $service.JarFragment -JarPath $service.Jar)
    if ($processes.Count -eq 0) {
        Write-Host "$($service.DisplayName) is not running." -ForegroundColor DarkGray
    }

    foreach ($process in $processes) {
        Write-Host "Stopping $($service.DisplayName) (PID $($process.ProcessId))..." -ForegroundColor Yellow
        Stop-Process -Id $process.ProcessId -Force -ErrorAction SilentlyContinue
    }

    if (Test-Path $service.PidFile) {
        Remove-Item -Path $service.PidFile -Force
    }
}

Write-Host "Backend services stopped." -ForegroundColor Green
