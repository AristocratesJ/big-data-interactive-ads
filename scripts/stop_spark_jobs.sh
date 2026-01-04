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

echo "Stopping Ad Campaign Manager..."
docker exec -u root spark-master pkill -f "ad_campaign_manager.py"

echo "Stopping Hive Archiver Scheduler..."
docker exec -u root spark-master pkill -f "archive_to_hive.py"

echo -e "\nAll streaming jobs stopped"
