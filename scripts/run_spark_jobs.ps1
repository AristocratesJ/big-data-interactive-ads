Write-Host "Starting Spark streaming jobs..." -ForegroundColor Cyan

# Install dependencies
Write-Host "`nInstalling Python dependencies..." -ForegroundColor Yellow
docker exec -u root spark-master pip install happybase

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to install dependencies" -ForegroundColor Red
    exit 1
}

Start-Sleep -Seconds 15

# Submit buses job in background
Write-Host "`nStarting buses streaming job..." -ForegroundColor Yellow
docker exec -d -u root spark-master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --total-executor-cores 1 `
    --executor-memory 512M `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 `
    /opt/spark-apps/consume_buses_to_hbase.py

Start-Sleep -Seconds 1

# Submit trolleys job in background
Write-Host "Starting trolleys streaming job..." -ForegroundColor Yellow
docker exec -d -u root spark-master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --total-executor-cores 1 `
    --executor-memory 512M `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 `
    /opt/spark-apps/consume_trolleys_to_hbase.py

Start-Sleep -Seconds 1

# Submit weather job in background
Write-Host "Starting weather streaming job..." -ForegroundColor Yellow
docker exec -d -u root spark-master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --total-executor-cores 1 `
    --executor-memory 512M `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 `
    /opt/spark-apps/consume_weather_to_hbase.py

Start-Sleep -Seconds 1

# Submit air quality job in background
Write-Host "Starting air quality streaming job..." -ForegroundColor Yellow
docker exec -d -u root spark-master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --total-executor-cores 1 `
    --executor-memory 512M `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 `
    /opt/spark-apps/consume_air_quality_to_hbase.py

Start-Sleep -Seconds 1

Write-Host "`nAll streaming jobs submitted!" -ForegroundColor Green
Write-Host "`nMonitor jobs at:" -ForegroundColor Cyan
Write-Host "   - Spark UI: http://localhost:8080" -ForegroundColor White
Write-Host "   - Logs: docker logs -f spark-master" -ForegroundColor White
Write-Host "`nTo stop jobs: .\scripts\stop_spark_jobs.ps1" -ForegroundColor Yellow
