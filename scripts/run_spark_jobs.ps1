Write-Host "Starting Spark streaming jobs..." -ForegroundColor Cyan

function Wait-SparkApp {
    param([int]$ExpectedCount, [int]$TimeoutSeconds = 60)
    $elapsed = 0
    while ($elapsed -lt $TimeoutSeconds) {
        try {
            $response = Invoke-RestMethod -Uri "http://localhost:8080/json/" -ErrorAction Stop
            $currentCount = $response.activeapps.Count
            if ($currentCount -ge $ExpectedCount) {
                Write-Host "  Application $ExpectedCount registered" -ForegroundColor Green
                return $true
            }
            Write-Host "  Waiting... $currentCount of $ExpectedCount ready" -ForegroundColor Gray
        }
        catch {
            Write-Host "  Waiting for Spark Master..." -ForegroundColor Gray
        }
        Start-Sleep -Seconds 2
        $elapsed += 2
    }
    Write-Host "  Timeout waiting for applications" -ForegroundColor Yellow
    return $false
}


Write-Host "`nSetup complete, ready to submit jobs" -ForegroundColor Green

Write-Host "`nStarting buses streaming job..." -ForegroundColor Yellow
docker exec -d -u spark spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 1 --executor-memory 512M --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark-apps/consume_buses_to_hbase.py
Wait-SparkApp -ExpectedCount 1
Write-Host "`nStarting trolleys streaming job..." -ForegroundColor Yellow
docker exec -d -u spark spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 1 --executor-memory 512M --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark-apps/consume_trolleys_to_hbase.py
Wait-SparkApp -ExpectedCount 2

Write-Host "`nStarting weather streaming job..." -ForegroundColor Yellow
docker exec -d -u spark spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 1 --executor-memory 512M --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark-apps/consume_weather_to_hbase.py
Wait-SparkApp -ExpectedCount 3

Write-Host "`nStarting air quality streaming job..." -ForegroundColor Yellow
docker exec -d -u spark spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 1 --executor-memory 512M --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark-apps/consume_air_quality_to_hbase.py
Wait-SparkApp -ExpectedCount 4

Write-Host "`nStarting Twitter sentiment streaming job..." -ForegroundColor Yellow
docker exec -d -u spark spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 1 --executor-memory 512M --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark-apps/consume_sentiment_to_hbase.py
Wait-SparkApp -ExpectedCount 5


Write-Host "`nAll streaming jobs submitted!" -ForegroundColor Green
Write-Host "`nMonitor jobs at:" -ForegroundColor Cyan
Write-Host "   - Spark UI: http://localhost:8080" -ForegroundColor White
Write-Host "   - Spark logs: docker logs -f spark-master" -ForegroundColor White
Write-Host "`nTo stop jobs: .\scripts\stop_spark_jobs.ps1" -ForegroundColor Yellow
