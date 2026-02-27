# start-dashboard.ps1
# Launches the Streamlit dashboard as a background process that survives terminal closure.
# Usage: .\dashboard\start-dashboard.ps1

$ProjectRoot = Split-Path -Parent $PSScriptRoot
if (-not $ProjectRoot) { $ProjectRoot = (Get-Location).Path }

$Port = 8501
$AppFile = Join-Path $ProjectRoot "dashboard\app.py"

# Kill any existing Streamlit on the same port
$existing = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue |
    Where-Object { $_.State -eq "Listen" } |
    Select-Object -ExpandProperty OwningProcess -Unique

if ($existing) {
    foreach ($pid in $existing) {
        Write-Host "[Dashboard] Killing stale process PID=$pid on port $Port..." -ForegroundColor Yellow
        Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 1
}

# Start Streamlit as a background job (survives terminal closure)
Write-Host "[Dashboard] Starting Streamlit on http://localhost:$Port ..." -ForegroundColor Cyan

$job = Start-Process -FilePath "python" `
    -ArgumentList "-m", "streamlit", "run", $AppFile, "--server.port", $Port, "--server.headless", "true" `
    -WorkingDirectory $ProjectRoot `
    -WindowStyle Hidden `
    -PassThru

Write-Host "[Dashboard] Streamlit started (PID=$($job.Id))" -ForegroundColor Green
Write-Host "[Dashboard] URL: http://localhost:$Port" -ForegroundColor Green
Write-Host "[Dashboard] To stop: Stop-Process -Id $($job.Id)" -ForegroundColor DarkGray

# Wait a moment then open in Chrome
Start-Sleep -Seconds 3
$chrome = "C:\Program Files\Google\Chrome\Application\chrome.exe"
if (Test-Path $chrome) {
    Start-Process $chrome "http://localhost:$Port"
}
