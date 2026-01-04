Write-Host "Stopping all Spark streaming jobs..." -ForegroundColor Yellow

# Kill buses job
Write-Host "Stopping buses job..." -ForegroundColor Cyan
docker exec -u root spark-master pkill -9 -f "consume_buses_to_hbase.py"

# Kill trolleys job
Write-Host "Stopping trolleys job..." -ForegroundColor Cyan
docker exec -u root spark-master pkill -9 -f "consume_trolleys_to_hbase.py"

# Kill weather job
Write-Host "Stopping weather job..." -ForegroundColor Cyan
docker exec -u root spark-master pkill -9 -f "consume_weather_to_hbase.py"

# Kill air quality job
Write-Host "Stopping air quality job..." -ForegroundColor Cyan
docker exec -u root spark-master pkill -9 -f "consume_air_quality_to_hbase.py"

# Kill Twitter sentiment job
Write-Host "Stopping Twitter sentiment job..." -ForegroundColor Cyan
docker exec -u root spark-master pkill -9 -f "consume_sentiment_to_hbase.py"

Write-Host "`nAll streaming jobs stopped" -ForegroundColor Green
