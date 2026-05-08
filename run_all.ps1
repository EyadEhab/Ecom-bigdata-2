param(
    [string]$Container = "ecom-bigdata-2-spark-master-1",
    [string]$MongoContainer = "ecom-bigdata-2-mongodb-1",
    [switch]$SkipPhase1,
    [switch]$SkipPhase2,
    [switch]$SkipPhase3
)

function Run-InContainer {
    param([string]$Cmd)
    cmd /c "docker exec $Container $Cmd"
    if ($LASTEXITCODE -ne 0) { Write-Host "FAILED" -ForegroundColor Red; try { Read-Host "Press Enter to exit" } catch {}; exit 1 }
}

$Start = Get-Date

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  E-Commerce Behavioral Analytics" -ForegroundColor Cyan
Write-Host "  Full Pipeline via Docker" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

if (-not $SkipPhase1) {
    Write-Host "`n[Phase 1.1] Market Basket Analysis..." -ForegroundColor Yellow
    Run-InContainer "/opt/spark/bin/spark-submit --master local[*] /opt/spark/scripts/phase1_market_basket.py"

    Write-Host "`n[Phase 1.2] User Affinity Aggregation..." -ForegroundColor Yellow
    Run-InContainer "/opt/spark/bin/spark-submit --master local[*] /opt/spark/scripts/phase1.2_user_affinity.py"
}

if (-not $SkipPhase2) {
    Write-Host "`n[Phase 2] Ingesting into MongoDB..." -ForegroundColor Yellow
    Run-InContainer "python3 /opt/spark/scripts/phase2_mongodb_ingest.py"
}

if (-not $SkipPhase3) {
    Write-Host "`n[Phase 3] Cart Abandonment Recovery..." -ForegroundColor Yellow
    Run-InContainer "/opt/spark/bin/spark-submit --master local[*] --driver-memory 4g /opt/spark/scripts/phase3_cart_abandonment.py"
}

$Elapsed = (Get-Date) - $Start
Write-Host "`n========================================" -ForegroundColor Green
Write-Host "  Pipeline complete in $($Elapsed.TotalMinutes.ToString('F1')) min" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
try { Read-Host "`nPress Enter to exit" } catch {}
