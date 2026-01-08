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

# Note Ad Campaign Manager is now a Docker service
Write-Host "Note: Ad Campaign Manager runs as Docker service (ad-campaign-manager)" -ForegroundColor Gray
Write-Host "      To stop: docker-compose stop ad-campaign-manager" -ForegroundColor
Write-Host "      To restart: docker-compose restart ad-campaign-manager" -ForegroundColor Gray

# Note: Archive scheduler is now a Docker service
Write-Host "Note: Archive scheduler runs as Docker service (archive-scheduler)" -ForegroundColor Gray
Write-Host "      To stop: docker-compose stop archive-scheduler" -ForegroundColor Gray
Write-Host "      To restart: docker-compose restart archive-scheduler" -ForegroundColor Gray

Write-Host "`nAll streaming jobs stopped" -ForegroundColor Green
