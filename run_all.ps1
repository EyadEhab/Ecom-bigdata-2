param(
    [string]$SparkSubmit = "spark-submit",
    [string]$Python = "python",
    [string]$DataDir = "Dataset",
    [string]$OutputDir = "output",
    [switch]$SkipPhase1,
    [switch]$SkipPhase2,
    [switch]$SkipPhase3
)

$Scripts = "scripts"
$Start = Get-Date

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  E-Commerce Behavioral Analytics" -ForegroundColor Cyan
Write-Host "  Full Pipeline Execution" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

if (-not $SkipPhase1) {
    Write-Host "`n[Phase 1.1] Market Basket Analysis..." -ForegroundColor Yellow
    & $SparkSubmit --master local[*] "$Scripts\phase1_market_basket.py"
    if ($LASTEXITCODE -ne 0) { Write-Host "FAILED" -ForegroundColor Red; exit 1 }

    Write-Host "`n[Phase 1.2] User Affinity Aggregation..." -ForegroundColor Yellow
    & $SparkSubmit --master local[*] "$Scripts\phase1.2_user_affinity.py"
    if ($LASTEXITCODE -ne 0) { Write-Host "FAILED" -ForegroundColor Red; exit 1 }
}

if (-not $SkipPhase2) {
    Write-Host "`n[Phase 2] Ingesting into MongoDB..." -ForegroundColor Yellow
    & $Python "$Scripts\phase2_mongodb_ingest.py"
    if ($LASTEXITCODE -ne 0) { Write-Host "FAILED" -ForegroundColor Red; exit 1 }
}

if (-not $SkipPhase3) {
    Write-Host "`n[Phase 3] Cart Abandonment Recovery..." -ForegroundColor Yellow
    & $SparkSubmit --master local[*] --driver-memory 4g "$Scripts\phase3_cart_abandonment.py"
    if ($LASTEXITCODE -ne 0) { Write-Host "FAILED" -ForegroundColor Red; exit 1 }
}

$Elapsed = (Get-Date) - $Start
Write-Host "`n========================================" -ForegroundColor Green
Write-Host "  Pipeline complete in $($Elapsed.TotalMinutes.ToString('F1')) min" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
