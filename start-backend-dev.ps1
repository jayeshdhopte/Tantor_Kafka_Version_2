# start-backend-dev.ps1 - Run Tantor backend services in foreground with labeled, color-coded logs.
# Usage: .\start-backend-dev.ps1
# Press Ctrl+C to stop all services.

[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$RootDir = $PSScriptRoot

# ── Load .env ────────────────────────────────────────────────────────────────
$envFile = Join-Path $RootDir ".env"
if (Test-Path $envFile) {
    Write-Host "Loading environment variables from .env..." -ForegroundColor Cyan
    foreach ($line in Get-Content $envFile) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#") -or !$trimmed.Contains("=")) { continue }
        $name, $value = $trimmed.Split("=", 2)
        $name  = $name.Trim()
        $value = $value.Trim().Trim('"').Trim("'")
        if (![string]::IsNullOrWhiteSpace($name)) { Set-Item -Path "env:$name" -Value $value }
    }
}
if ([string]::IsNullOrWhiteSpace($env:SPRING_PROFILES_ACTIVE)) {
    $env:SPRING_PROFILES_ACTIVE = 'dev'
}

# ── Resolve Java ─────────────────────────────────────────────────────────────
$candidateJavaHome1 = "C:\Program Files\Java\jdk-21"
$candidateJavaHome2 = "C:\Program Files\Microsoft\jdk-21.0.10.7-hotspot"
$candidateJavaHome3 = "C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot"
if ([string]::IsNullOrWhiteSpace($env:JAVA_HOME) -or -not (Test-Path "$env:JAVA_HOME\bin\java.exe")) {
    if (Test-Path $candidateJavaHome1) {
        $env:JAVA_HOME = $candidateJavaHome1
    } elseif (Test-Path $candidateJavaHome2) {
        $env:JAVA_HOME = $candidateJavaHome2
    } elseif (Test-Path $candidateJavaHome3) {
        $env:JAVA_HOME = $candidateJavaHome3
    }
}
if (![string]::IsNullOrWhiteSpace($env:JAVA_HOME)) {
    $env:Path = "$env:JAVA_HOME\bin;$env:Path"
}
$JavaExe = (Get-Command java.exe -ErrorAction SilentlyContinue).Source
if (!$JavaExe) { throw "Java not found. Install JDK 21 or set JAVA_HOME." }

# ── Locate JARs ─────────────────────────────────────────────────────────────
$ArtifactJar = Join-Path $RootDir "tantor-artifact-repository\target\tantor-artifact-repository-1.0.0.jar"
$ServerJar   = Join-Path $RootDir "tantor-server\target\tantor-server-1.0.0.jar"

if (!(Test-Path $ArtifactJar)) { throw "Artifact Repository JAR not found at $ArtifactJar. Run .\build.ps1 first." }
if (!(Test-Path $ServerJar))   { throw "Management Server JAR not found at $ServerJar. Run .\build.ps1 first." }

# ── Stop any already-running Tantor Java processes ───────────────────────────
try {
    $existingJava = Get-CimInstance Win32_Process -Filter "Name = 'java.exe'" -ErrorAction SilentlyContinue |
        Where-Object {
            $cmd = $_.CommandLine
            $cmd -and ($cmd -like "*tantor-artifact-repository*" -or $cmd -like "*tantor-server*")
        }
    foreach ($proc in $existingJava) {
        Write-Host "Stopping existing Tantor process PID $($proc.ProcessId)..." -ForegroundColor Yellow
        Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
    }
    if ($existingJava) { Start-Sleep -Seconds 2 }
} catch {
    Write-Host "Warning: Could not check for existing processes: $($_.Exception.Message)" -ForegroundColor Yellow
}

# ── Banner ───────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "  Tantor Backend -- Development Mode      " -ForegroundColor Cyan
Write-Host "  Press Ctrl+C to stop all services       " -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# ── Start Artifact Repository ────────────────────────────────────────────────
$artifactPsi = New-Object System.Diagnostics.ProcessStartInfo
$artifactPsi.FileName               = $JavaExe
$artifactPsi.Arguments              = "-jar ""$ArtifactJar"""
$artifactPsi.WorkingDirectory       = $RootDir
$artifactPsi.UseShellExecute        = $false
$artifactPsi.RedirectStandardOutput = $true
$artifactPsi.RedirectStandardError  = $true
$artifactPsi.CreateNoWindow         = $true

$artifactProc = New-Object System.Diagnostics.Process
$artifactProc.StartInfo = $artifactPsi
$artifactProc.EnableRaisingEvents = $true

Register-ObjectEvent -InputObject $artifactProc -EventName OutputDataReceived -SourceIdentifier "ARTIFACT_OUT" | Out-Null
Register-ObjectEvent -InputObject $artifactProc -EventName ErrorDataReceived  -SourceIdentifier "ARTIFACT_ERR" | Out-Null

$artifactProc.Start() | Out-Null
$artifactProc.BeginOutputReadLine()
$artifactProc.BeginErrorReadLine()

Write-Host "[ARTIFACT] Tantor Artifact Repository started -- PID $($artifactProc.Id)" -ForegroundColor Magenta

# ── Start Management Server ──────────────────────────────────────────────────
$serverPsi = New-Object System.Diagnostics.ProcessStartInfo
$serverPsi.FileName               = $JavaExe
$serverPsi.Arguments              = "-jar ""$ServerJar"""
$serverPsi.WorkingDirectory       = $RootDir
$serverPsi.UseShellExecute        = $false
$serverPsi.RedirectStandardOutput = $true
$serverPsi.RedirectStandardError  = $true
$serverPsi.CreateNoWindow         = $true

$serverProc = New-Object System.Diagnostics.Process
$serverProc.StartInfo = $serverPsi
$serverProc.EnableRaisingEvents = $true

Register-ObjectEvent -InputObject $serverProc -EventName OutputDataReceived -SourceIdentifier "SERVER_OUT" | Out-Null
Register-ObjectEvent -InputObject $serverProc -EventName ErrorDataReceived  -SourceIdentifier "SERVER_ERR" | Out-Null

$serverProc.Start() | Out-Null
$serverProc.BeginOutputReadLine()
$serverProc.BeginErrorReadLine()

Write-Host "[SERVER  ] Tantor Management Server started -- PID $($serverProc.Id)" -ForegroundColor Green
Write-Host ""

# ── Main loop: drain events and print labelled lines ─────────────────────────
try {
    while ($true) {
        # Drain all queued events (Get-Event can return an array)
        $events = @(Get-Event -ErrorAction SilentlyContinue)
        foreach ($ev in $events) {
            if ($null -eq $ev) { continue }
            $line = $ev.SourceEventArgs.Data
            if (![string]::IsNullOrEmpty($line)) {
                $src = $ev.SourceIdentifier
                if ($src -eq "ARTIFACT_OUT" -or $src -eq "ARTIFACT_ERR") {
                    Write-Host "[ARTIFACT] " -ForegroundColor Magenta -NoNewline
                    Write-Host $line
                }
                elseif ($src -eq "SERVER_OUT" -or $src -eq "SERVER_ERR") {
                    Write-Host "[SERVER  ] " -ForegroundColor Green -NoNewline
                    Write-Host $line
                }
                else {
                    Write-Host $line
                }
            }
            Remove-Event -EventIdentifier $ev.EventIdentifier -ErrorAction SilentlyContinue
        }

        # Check if both processes have exited
        $artifactDone = $artifactProc.HasExited
        $serverDone   = $serverProc.HasExited

        if ($artifactDone -and $serverDone) {
            # Drain any remaining events after exit
            Start-Sleep -Milliseconds 500
            $remaining = @(Get-Event -ErrorAction SilentlyContinue)
            foreach ($ev in $remaining) {
                if ($null -eq $ev) { continue }
                $line = $ev.SourceEventArgs.Data
                if (![string]::IsNullOrEmpty($line)) {
                    $src = $ev.SourceIdentifier
                    if ($src -eq "ARTIFACT_OUT" -or $src -eq "ARTIFACT_ERR") {
                        Write-Host "[ARTIFACT] " -ForegroundColor Magenta -NoNewline
                        Write-Host $line
                    }
                    elseif ($src -eq "SERVER_OUT" -or $src -eq "SERVER_ERR") {
                        Write-Host "[SERVER  ] " -ForegroundColor Green -NoNewline
                        Write-Host $line
                    }
                }
                Remove-Event -EventIdentifier $ev.EventIdentifier -ErrorAction SilentlyContinue
            }

            Write-Host ""
            Write-Host "[ARTIFACT] Exited with code $($artifactProc.ExitCode)" -ForegroundColor Yellow
            Write-Host "[SERVER  ] Exited with code $($serverProc.ExitCode)" -ForegroundColor Yellow
            break
        }

        Start-Sleep -Milliseconds 100
    }
}
finally {
    Write-Host ""
    Write-Host "Shutting down..." -ForegroundColor Yellow

    Unregister-Event -SourceIdentifier "ARTIFACT_OUT" -ErrorAction SilentlyContinue
    Unregister-Event -SourceIdentifier "ARTIFACT_ERR" -ErrorAction SilentlyContinue
    Unregister-Event -SourceIdentifier "SERVER_OUT"   -ErrorAction SilentlyContinue
    Unregister-Event -SourceIdentifier "SERVER_ERR"   -ErrorAction SilentlyContinue

    if ($artifactProc -and !$artifactProc.HasExited) {
        Write-Host "Stopping Artifact Repository (PID $($artifactProc.Id))..." -ForegroundColor Yellow
        try { $artifactProc.Kill() } catch {}
        try { $artifactProc.WaitForExit(5000) | Out-Null } catch {}
    }
    if ($serverProc -and !$serverProc.HasExited) {
        Write-Host "Stopping Management Server (PID $($serverProc.Id))..." -ForegroundColor Yellow
        try { $serverProc.Kill() } catch {}
        try { $serverProc.WaitForExit(5000) | Out-Null } catch {}
    }

    if ($artifactProc) { $artifactProc.Dispose() }
    if ($serverProc)   { $serverProc.Dispose() }

    Write-Host "All services stopped." -ForegroundColor Green
}
