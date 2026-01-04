#!/bin/bash

echo "🔄 Initiating Full System Restart..."

# 1. Stop all processing
echo "🛑 Stopping Spark Jobs..."
./scripts/stop_spark_jobs.sh

echo "🛑 Stopping NiFi Flows..."
./scripts/stop_nifi_flows.sh

# 2. Restart Infrastructure
echo "🐳 Restarting Docker Containers..."
docker-compose down
echo "   Waiting for containers to stop..."
sleep 5

echo "🚀 Starting Docker Containers..."
docker-compose up -d

echo "⏳ Waiting 30s for services to stabilize (HBase, Kafka, Spark)..."
sleep 30

# 3. Restart Processing
echo "🌊 Starting NiFi Flows..."
./scripts/run_nifi_flows.sh

sleep 10

echo "⚡ Starting Spark Jobs..."
./scripts/run_spark_jobs.sh

echo "✅ System Restart Complete!"
echo "Monitor at:"
echo " - Spark UI: http://localhost:8080"
echo " - NiFi UI:  https://localhost:8443/nifi"
echo " - Kafka UI: http://localhost:8090"
