# Setup script for HBase tables and Kafka topics
# Run from project root directory

Write-Host "=== Starting HBase and Kafka Setup ===" -ForegroundColor Cyan

# Setup HBase Tables
Write-Host "`n[1/5] Creating HBase transport_events table..." -ForegroundColor Yellow
uv run .\hbase\setup_transport_table.py
if ($LASTEXITCODE -ne 0) { Write-Host "Failed!" -ForegroundColor Red; exit 1 }

Write-Host "`n[2/5] Creating HBase weather_forecast table..." -ForegroundColor Yellow
uv run .\hbase\setup_weather_table.py
if ($LASTEXITCODE -ne 0) { Write-Host "Failed!" -ForegroundColor Red; exit 1 }

Write-Host "`n[3/5] Creating HBase air_quality_forecast table..." -ForegroundColor Yellow
uv run .\hbase\setup_air_quality_table.py
if ($LASTEXITCODE -ne 0) { Write-Host "Failed!" -ForegroundColor Red; exit 1 }

Write-Host "`n[4/5] Creating HBase sentiment_tweets table..." -ForegroundColor Yellow
uv run .\hbase\setup_sentiment_table.py
if ($LASTEXITCODE -ne 0) { Write-Host "Failed!" -ForegroundColor Red; exit 1 }

# Setup Kafka Topics
Write-Host "`n[5/5] Creating Kafka topics..." -ForegroundColor Yellow
uv run .\kafka\setup_kafka_topics.py
if ($LASTEXITCODE -ne 0) { Write-Host "Failed!" -ForegroundColor Red; exit 1 }

Write-Host "`n=== Setup Complete! ===" -ForegroundColor Green
