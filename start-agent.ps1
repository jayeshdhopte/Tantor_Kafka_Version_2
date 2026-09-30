# start-agent.ps1 - Automatically downloads Go (if missing), compiles, and starts the Agent

$goVersion = "1.22.4"
$goZip = "go$goVersion.windows-amd64.zip"
$goUrl = "https://go.dev/dl/$goZip"
$goDir = "$PWD\go"

# Check if Go is already available locally
if (!(Test-Path "$goDir\bin\go.exe")) {
    Write-Host "Golang compiler not found on system!" -ForegroundColor Yellow
    Write-Host "Downloading Portable Go $goVersion (no admin rights needed)..." -ForegroundColor Cyan
    Invoke-WebRequest -Uri $goUrl -OutFile $goZip
    
    Write-Host "Extracting Go..." -ForegroundColor Cyan
    Expand-Archive -Path $goZip -DestinationPath . -Force
    Remove-Item $goZip
}

# Add local Go to PATH for this session
$env:PATH = "$goDir\bin;" + $env:PATH

Write-Host "`nDownloading Go Dependencies..." -ForegroundColor Cyan
cd tantor-agent
go mod tidy

Write-Host "Building Tantor Agent..." -ForegroundColor Cyan
go build -o tantor-agent.exe ./cmd/agent/main.go

if (Test-Path "tantor-agent.exe") {
    Write-Host "`nBuild Successful! Starting Agent..." -ForegroundColor Green
    .\tantor-agent.exe -config configs/agent.yaml
} else {
    Write-Host "`nBuild Failed!" -ForegroundColor Red
}
