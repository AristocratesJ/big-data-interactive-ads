Write-Host "🔄 Initiating Full System Restart..." -ForegroundColor Cyan

# 1. Stop all processing
Write-Host "🛑 Stopping Spark Jobs..." -ForegroundColor Yellow
.\scripts\stop_spark_jobs.ps1

Write-Host "🛑 Stopping NiFi Flows..." -ForegroundColor Yellow
.\scripts\stop_nifi_flows.ps1

# 2. Restart Infrastructure
Write-Host "🐳 Restarting Docker Containers..." -ForegroundColor Yellow
docker-compose down
Write-Host "   Waiting for containers to stop..."
Start-Sleep -Seconds 5

Write-Host "🚀 Starting Docker Containers..." -ForegroundColor Green
docker-compose up -d

Write-Host "⏳ Waiting 30s for services to stabilize (HBase, Kafka, Spark)..." -ForegroundColor Cyan
Start-Sleep -Seconds 30

# 3. Restart Processing
Write-Host "🌊 Starting NiFi Flows..." -ForegroundColor Green
.\scripts\run_nifi_flows.ps1

Start-Sleep -Seconds 10

Write-Host "⚡ Starting Spark Jobs..." -ForegroundColor Green
.\scripts\run_spark_jobs.ps1

Write-Host "✅ System Restart Complete!" -ForegroundColor Green
Write-Host "Monitor at:"
Write-Host " - Spark UI: http://localhost:8080"
Write-Host " - NiFi UI:  https://localhost:8443/nifi"
Write-Host " - Kafka UI: http://localhost:8090"
