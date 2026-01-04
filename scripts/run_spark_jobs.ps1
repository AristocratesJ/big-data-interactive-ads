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


# Dependencies are now pre-installed in the Docker image.


Write-Host "Setup complete, ready to submit jobs" -ForegroundColor Green

Write-Host "`nStarting buses streaming job..." -ForegroundColor Yellow
docker exec -d -u spark spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 1 --executor-memory 350M --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark-apps/consume_buses_to_hbase.py
Wait-SparkApp -ExpectedCount 1

Write-Host "`nStarting trolleys streaming job..." -ForegroundColor Yellow
docker exec -d -u spark spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 1 --executor-memory 350M --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark-apps/consume_trolleys_to_hbase.py
Wait-SparkApp -ExpectedCount 2

Write-Host "`nStarting weather streaming job..." -ForegroundColor Yellow
docker exec -d -u spark spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 1 --executor-memory 350M --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark-apps/consume_weather_to_hbase.py
Wait-SparkApp -ExpectedCount 3

Write-Host "`nStarting air quality streaming job..." -ForegroundColor Yellow
docker exec -d -u spark spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 1 --executor-memory 350M --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark-apps/consume_air_quality_to_hbase.py
Wait-SparkApp -ExpectedCount 4

Write-Host "`nStarting Twitter sentiment streaming job..." -ForegroundColor Yellow
docker exec -d -u spark spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 1 --executor-memory 350M --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark-apps/consume_sentiment_to_hbase.py
Wait-SparkApp -ExpectedCount 5

Write-Host "`nStarting Ad Campaign Manager..." -ForegroundColor Yellow
docker exec -d -u root spark-master bash -c '/opt/spark/bin/spark-submit --master local[1] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark-apps/ad_campaign_manager.py > /opt/spark-apps/ad_manager.log 2>&1'

Write-Host "`nStarting Hive Archiver Scheduler (Hourly)..." -ForegroundColor Yellow
docker exec -d -u root spark-master bash -c 'while true; do echo \"[$(date)] Running scheduled archive...\"; /opt/spark/bin/spark-submit --master local[*] --packages com.google.protobuf:protobuf-java:2.5.0 /opt/spark-apps/archive_to_hive.py >> /opt/spark-apps/archive.log 2>&1; echo \"[$(date)] Archive complete. Sleeping for 1 hour...\"; sleep 3600; done'

Write-Host "`nAll streaming jobs submitted!" -ForegroundColor Green
Write-Host "`nMonitor jobs at:" -ForegroundColor Cyan
Write-Host "   - Spark UI: http://localhost:8080" -ForegroundColor White
Write-Host "   - Logs: docker logs -f spark-master" -ForegroundColor White
Write-Host "`nTo stop jobs: .\scripts\stop_spark_jobs.ps1" -ForegroundColor Yellow
