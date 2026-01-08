#!/bin/bash

echo "Stopping all Spark streaming jobs..."

stop_job() {
    JOB_NAME=$1
    SCRIPT_NAME=$2
    echo "Stopping $JOB_NAME job..."
    docker exec -u root spark-master pkill -9 -f "$SCRIPT_NAME"
}

stop_job "buses" "consume_buses_to_hbase.py"
stop_job "trolleys" "consume_trolleys_to_hbase.py"
stop_job "weather" "consume_weather_to_hbase.py"
stop_job "air quality" "consume_air_quality_to_hbase.py"
stop_job "Twitter sentiment" "consume_sentiment_to_hbase.py"

# Note Ad Campaign Manager is now a Docker service
echo "Note: Ad Campaign Manager runs as Docker service (ad-campaign-manager)"
echo "      To stop: docker-compose stop ad-campaign-manager"
echo "      To restart: docker-compose restart ad-campaign-manager"

# Note: Archive scheduler is now a Docker service
echo "Note: Archive scheduler runs as Docker service (archive-scheduler)"
echo "      To stop: docker-compose stop archive-scheduler"
echo "      To restart: docker-compose restart archive-scheduler"

echo -e "\nAll streaming jobs stopped"
