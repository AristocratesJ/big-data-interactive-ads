"""
Ad Campaign Manager
Persistent process that analyzes data from HBase and makes ad placement decisions.

Logic:
1. Scan last 10 minutes of Transport, Weather, and Tweets data.
2. Calculate Traffic Congestion Score (based on bus speeds).
3. Calculate Discomfort Score (Weather + Sentiment).
4. Decide where to show ads (Global Score > Threshold).
5. Write decisions to HBase 'ad_decisions' (Audit/History).
6. Push decisions to Kafka 'ad-decisions' (Real-time Delivery) using Spark.
"""

import happybase
import time
from datetime import datetime, timedelta
import math
from collections import defaultdict
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# --- Configuration ---
HBASE_HOST = "hbase"
HBASE_PORT = 9090
KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC = "ad-decisions"
SCAN_WINDOW_MINUTES = 10
LOOP_INTERVAL_SECONDS = 60

# Scoring Weights
WEIGHT_CONGESTION = 0.5
WEIGHT_WEATHER = 0.3
WEIGHT_SENTIMENT = 0.2

# Thresholds
SPEED_CONGESTION_THRESHOLD_KMH = 15.0
SCORE_THRESHOLD_SHOW_AD = 0.6  # 0.0 to 1.0 scale


def haversine_km(lat1, lon1, lat2, lon2):
    """Calculate distance in km between two points."""
    R = 6371  # Earth radius in km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) * math.sin(dlat / 2) +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) * math.sin(dlon / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def get_time_window():
    """Return start and end row keys for the last 10 minutes."""
    now = datetime.now()
    start_time = now - timedelta(minutes=SCAN_WINDOW_MINUTES)
    start_key = start_time.strftime("%Y%m%d_%H%M%S")
    end_key = now.strftime("%Y%m%d_%H%M%S")
    return start_key, end_key, now


def scan_hbase_data(connection, table_name, start_row, end_row):
    """Scan HBase table for a given row key range."""
    table = connection.table(table_name)
    return table.scan(row_start=start_row.encode(), row_stop=end_row.encode())


def calculate_traffic_score(connection, start_key, end_key):
    """
    Calculate traffic congestion score (0.0 - 1.0).
    Logic: Find buses with speed < threshold.
    Returns: score, list of congested locations (lat, lon)
    """
    vehicle_positions = defaultdict(list)
    congested_locations = []

    try:
        scanner = scan_hbase_data(
            connection, "transport_events", start_key, end_key)

        for key, data in scanner:
            vn = data.get(b'info:VehicleNumber', b'').decode('utf-8')
            lat = float(data.get(b'location:Lat', 0.0))
            lon = float(data.get(b'location:Lon', 0.0))

            row_ts_str = key.decode(
                'utf-8').split('_')[0] + key.decode('utf-8').split('_')[1]
            try:
                dt = datetime.strptime(row_ts_str, "%Y%m%d%H%M%S")
            except:
                continue

            vehicle_positions[vn].append((dt, lat, lon))

    except Exception as e:
        print(f"Error scanning transport events: {e}")
        return 0.0, []

    # Calculate speeds
    total_vehicles = len(vehicle_positions)

    if total_vehicles == 0:
        return 0.0, []

    for vn, positions in vehicle_positions.items():
        if len(positions) < 2:
            continue

        positions.sort(key=lambda x: x[0])
        start_pos = positions[0]
        end_pos = positions[-1]

        time_diff_hours = (end_pos[0] - start_pos[0]).total_seconds() / 3600.0
        if time_diff_hours <= 0:
            continue

        dist_km = haversine_km(
            start_pos[1], start_pos[2], end_pos[1], end_pos[2])
        speed_kmh = dist_km / time_diff_hours

        if speed_kmh < SPEED_CONGESTION_THRESHOLD_KMH and speed_kmh > 0.1:
            congested_locations.append((speed_kmh, vn))

    congested_locations.sort(key=lambda x: x[0])
    slowest_vehicle_ids = [vn for speed, vn in congested_locations]

    score = len(slowest_vehicle_ids) / \
        total_vehicles if total_vehicles > 0 else 0.0
    return min(score, 1.0), slowest_vehicle_ids


def calculate_weather_score(connection):
    """
    Calculate weather score (0.0 - 1.0).
    Logic: Rain or Cold = Bad = High Score.
    """
    now = datetime.now()
    key = now.strftime("%Y%m%d_%H")

    try:
        table = connection.table("weather_forecast")
        row = table.row(key.encode())
        if not row:
            return 0.0

        rain = float(row.get(b'precipitation:rain', b'0.0'))
        temp = float(row.get(b'temperature:temp_2m', b'20.0'))

        score = 0.0
        if rain > 0.5:
            score += 0.5
        if temp < 10.0:
            score += 0.5

        return min(score, 1.0)

    except Exception as e:
        print(f"Error getting weather: {e}")
        return 0.0


def calculate_sentiment_score(connection, start_key, end_key):
    """
    Calculate sentiment score (0.0 - 1.0).
    Logic: Ratio of negative tweets.
    """
    try:
        scanner = scan_hbase_data(connection, "tweets", start_key, end_key)
        total_tweets = 0
        negative_score_sum = 0

        for key, data in scanner:
            total_tweets += 1
            score = int(data.get(b'sentiment:score', b'5'))
            if score < 4:
                negative_score_sum += 1

        if total_tweets == 0:
            return 0.0

        return float(negative_score_sum) / total_tweets

    except Exception as e:
        print(f"Error scanning tweets: {e}")
        return 0.0


def main_loop():
    print("🚀 Ad Campaign Manager started (Spark Edition)...")
    print(f"   Window: {SCAN_WINDOW_MINUTES} min")

    # Initialize Spark Session for Kafka writing
    spark = SparkSession.builder \
        .appName("AdCampaignManager") \
        .getOrCreate()

    # Silence Spark logs
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("key", StringType(), True),
        StructField("value", StringType(), True)
    ])

    while True:
        try:
            start_loop = time.time()
            connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)

            # 1. Define window
            start_key, end_key, now_dt = get_time_window()
            print(f"\n[{now_dt}] Analyzing window {start_key} -> {end_key}")

            # 2. Calculate scores
            traffic_score, congested_locs = calculate_traffic_score(
                connection, start_key, end_key)
            weather_score = calculate_weather_score(connection)
            sentiment_score = calculate_sentiment_score(
                connection, start_key, end_key)

            # 3. Global Score
            global_score = (traffic_score * WEIGHT_CONGESTION +
                            weather_score * WEIGHT_WEATHER +
                            sentiment_score * WEIGHT_SENTIMENT)

            print(
                f"   Scores -> Traffic: {traffic_score:.2f} | Weather: {weather_score:.2f} | Sentiment: {sentiment_score:.2f}")
            print(f"   Global Score: {global_score:.2f}")

            # 4. Decision
            show_ad = global_score > SCORE_THRESHOLD_SHOW_AD
            decision_str = "SHOW_AD" if show_ad else "NO_AD"

            if show_ad:
                print(
                    f"   🎯 DECISION: {decision_str}! Targeting {len(congested_locs)} vehicles.")
            else:
                print(f"   DECISION: {decision_str}. Conditions acceptable.")

            # 5. Write Decision to HBase
            decision_key = now_dt.strftime("%Y%m%d_%H%M%S")
            ad_table = connection.table("ad_decisions")
            display_dt = now_dt + timedelta(minutes=5)
            display_ts = display_dt.strftime("%Y-%m-%d %H:%M:%S")

            def enc(val): return str(val).encode('utf-8')

            data = {
                b'metrics:traffic_score': enc(traffic_score),
                b'metrics:weather_score': enc(weather_score),
                b'metrics:sentiment_score': enc(sentiment_score),
                b'metrics:global_score': enc(global_score),
                b'decision:result': enc(decision_str),
                b'timestamps:decision_time': enc(now_dt.strftime("%Y-%m-%d %H:%M:%S")),
                b'timestamps:display_time': enc(display_ts),
            }

            top_targets = []
            campaign_type = ""

            if show_ad and congested_locs:
                top_targets = congested_locs[:5]
                targets_str = str(top_targets)
                data[b'target:locations'] = enc(targets_str)
                data[b'decision:campaign_type'] = b'ESCAPISM_NOW'
                campaign_type = "ESCAPISM_NOW"

            ad_table.put(decision_key.encode(), data)
            connection.close()

            # 6. Push to Kafka (using Spark)
            kafka_payload = {
                "decision_id": decision_key,
                "timestamp": now_dt.isoformat(),
                "display_time": display_ts,
                "decision": decision_str,
                "metrics": {
                    "global": global_score,
                    "traffic": traffic_score,
                    "weather": weather_score,
                    "sentiment": sentiment_score
                },
                "action": {
                    "show_ad": show_ad,
                    "campaign": campaign_type,
                    "targets": top_targets
                }
            }

            payload_json = json.dumps(kafka_payload)

            # Create a single-row DataFrame
            df = spark.createDataFrame(
                [(decision_key, payload_json)],
                schema=schema
            )

            # Write to Kafka
            df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
              .write \
              .format("kafka") \
              .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
              .option("topic", KAFKA_TOPIC) \
              .save()

            # print(f"   Produced to Kafka via Spark: {payload_json}")

            # Sleep
            elapsed = time.time() - start_loop
            sleep_time = max(0, LOOP_INTERVAL_SECONDS - elapsed)
            time.sleep(sleep_time)

        except Exception as e:
            print(f"❌ Error in main loop: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main_loop()
