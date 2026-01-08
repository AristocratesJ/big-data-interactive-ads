#!/bin/bash

echo "Starting Spark streaming jobs..."

wait_for_spark_app() {
    EXPECTED_COUNT=$1
    TIMEOUT_SECONDS=60
    ELAPSED=0

    while [ $ELAPSED -lt $TIMEOUT_SECONDS ]; do
        CURRENT_COUNT=$(curl -s http://localhost:8080/json/ | jq '.activeapps | length')
        
        # Check if curl failed or returned empty
        if [ -z "$CURRENT_COUNT" ]; then
             echo "  Waiting for Spark Master..."
        elif [ "$CURRENT_COUNT" -ge "$EXPECTED_COUNT" ]; then
            echo "  Application $EXPECTED_COUNT registered"
            return 0
        else
            echo "  Waiting... $CURRENT_COUNT of $EXPECTED_COUNT ready"
        fi

        sleep 2
        ((ELAPSED+=2))
    done

    echo "  Timeout waiting for applications"
    return 1
}


echo "Setup complete, ready to submit jobs"

# Function to submit job
submit_job() {
    SCRIPT=$1
    echo -e "\nStarting $2 streaming job..."
    docker exec -d -u spark spark-master bash -c "/opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --total-executor-cores 1 \
        --executor-memory 512M \
        --driver-memory 512M \
        --conf spark.executor.memoryOverhead=200M \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        /opt/spark-apps/$SCRIPT > /opt/logs/${2}.log 2>&1"
}

submit_job "consume_buses_to_hbase.py" "buses"
wait_for_spark_app 1

submit_job "consume_trolleys_to_hbase.py" "trolleys"
wait_for_spark_app 2

submit_job "consume_weather_to_hbase.py" "weather"
wait_for_spark_app 3

submit_job "consume_air_quality_to_hbase.py" "air_quality"
wait_for_spark_app 4

submit_job "consume_sentiment_to_hbase.py" "sentiment"
wait_for_spark_app 5


echo -e "\nAll streaming jobs submitted!"
echo -e "\nMonitor jobs at:"
echo "   - Spark UI: http://localhost:8080"
echo "   - Logs: docker logs -f spark-master"
echo -e "\nTo stop jobs: ./scripts/stop_spark_jobs.sh"
