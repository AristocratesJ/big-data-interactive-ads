"""
Ad Campaign Manager
Persistent process that analyzes data from HBase and makes ad placement decisions.
SKIPS writing records if no source data is found.

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
WEIGHT_CONGESTION = 0.4
WEIGHT_WEATHER = 0.4
WEIGHT_SENTIMENT = 0.2

# Thresholds
SPEED_CONGESTION_THRESHOLD_KMH = 15.0
SCORE_THRESHOLD_SHOW_AD = 0.5  # 0.0 to 1.0 scale


def haversine_km(lat1, lon1, lat2, lon2):
    """Calculate distance in km between two points."""
    R = 6371  # Earth radius in km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) * math.sin(dlat / 2) + math.cos(
        math.radians(lat1)
    ) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) * math.sin(dlon / 2)
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
    Returns: (score, list of congested locations, has_data_bool)
    """
    vehicle_positions = defaultdict(list)
    congested_locations = []

    try:
        scanner = scan_hbase_data(connection, "transport_events", start_key, end_key)

        for key, data in scanner:
            vn = data.get(b"info:VehicleNumber", b"").decode("utf-8")
            lat = float(data.get(b"location:Lat", 0.0))
            lon = float(data.get(b"location:Lon", 0.0))

            row_ts_str = (
                key.decode("utf-8").split("_")[0] + key.decode("utf-8").split("_")[1]
            )
            try:
                dt = datetime.strptime(row_ts_str, "%Y%m%d%H%M%S")
            except Exception:
                continue

            vehicle_positions[vn].append((dt, lat, lon))

    except Exception as e:
        print(f"Error scanning transport events: {e}")
        return 0.0, [], False

    # Calculate speeds
    total_vehicles = len(vehicle_positions)

    if total_vehicles == 0:
        return 0.0, [], False

    for vn, positions in vehicle_positions.items():
        if len(positions) < 2:
            continue

        positions.sort(key=lambda x: x[0])
        start_pos = positions[0]
        end_pos = positions[-1]

        time_diff_hours = (end_pos[0] - start_pos[0]).total_seconds() / 3600.0
        if time_diff_hours <= 0:
            continue

        dist_km = haversine_km(start_pos[1], start_pos[2], end_pos[1], end_pos[2])
        speed_kmh = dist_km / time_diff_hours

        if speed_kmh < SPEED_CONGESTION_THRESHOLD_KMH and speed_kmh > 0.1:
            congested_locations.append((speed_kmh, vn))

    congested_locations.sort(key=lambda x: x[0])
    slowest_vehicle_ids = [vn for _, vn in congested_locations]

    score = len(slowest_vehicle_ids) / total_vehicles if total_vehicles > 0 else 0.0

    print(
        f"   Traffic: {total_vehicles} vehicles, {len(slowest_vehicle_ids)} congested (<{SPEED_CONGESTION_THRESHOLD_KMH}km/h) -> score={score:.2f}"
    )

    return min(score, 1.0), slowest_vehicle_ids, True


def calculate_weather_score(connection):
    """
    Calculate weather discomfort score.
    Returns: (score, has_data_bool)
    """
    now = datetime.now()
    key = now.strftime("%Y%m%d_%H")

    try:
        table = connection.table("weather_forecast")
        row = table.row(key.encode())
        if not row:
            print("   ⚠️  No weather data found for current hour")
            return 0.0, False

        # Track which factors we can calculate
        factors_used = []
        score = 0.0

        # Extract available metrics (with defaults only for calculations)
        temp = row.get(b"temperature:temp_2m")
        feels_like = row.get(b"temperature:apparent_temp")
        rain = row.get(b"precipitation:rain")
        snow = row.get(b"precipitation:snowfall")
        snow_depth = row.get(b"precipitation:snow_depth")
        wind_speed = row.get(b"wind:wind_speed")
        weather_code = row.get(b"conditions:weather_code")

        # 1. Temperature Discomfort (0.0 - 0.4)
        if feels_like is not None or temp is not None:
            # Prefer "feels like" temperature
            effective_temp = float(feels_like) if feels_like else float(temp)

            if effective_temp < 0:
                score += 0.4  # Freezing
            elif effective_temp < 10:
                score += 0.3  # Very cold
            elif effective_temp < 15:
                score += 0.15  # Cold
            elif effective_temp > 30:
                score += 0.3  # Very hot
            elif effective_temp > 25:
                score += 0.15  # Hot
            # 15-25°C adds nothing (comfortable)

            factors_used.append(f"temp={effective_temp:.1f}°C")

        # 2. Precipitation (0.0 - 0.3)
        if rain is not None or snow is not None:
            rain_mm = float(rain) if rain else 0.0
            snow_mm = float(snow) * 100 if snow else 0.0
            total_precip = rain_mm + snow_mm

            if total_precip > 10.0:
                score += 0.3  # Heavy rain/snow
            elif total_precip > 5.0:
                score += 0.2  # Moderate rain
            elif total_precip > 1.0:
                score += 0.1  # Light rain
            elif total_precip > 0.1:
                score += 0.05  # Drizzle

            # Snow is worse than rain (harder to walk)
            if snow_mm > 0.5:
                score += 0.1

            factors_used.append(f"rain={rain_mm:.1f}mm")
            if snow_mm > 0:
                factors_used.append(f"snow={snow_mm:.1f}mm")

        # 3. Snow Depth (0.0 - 0.25)
        if snow_depth is not None:
            depth_m = float(snow_depth)
            depth_cm = depth_m * 100

            if depth_cm > 30:
                score += 0.25  # Heavy accumulation (>30 cm)
            elif depth_cm > 20:
                score += 0.2  # Significant accumulation (20-30 cm)
            elif depth_cm > 10:
                score += 0.15  # Moderate accumulation (10-20 cm)
            elif depth_cm > 5:
                score += 0.1  # Light accumulation (5-10 cm)
            elif depth_cm > 1:
                score += 0.05  # Trace accumulation (1-5 cm)

            factors_used.append(f"snow_depth={depth_cm:.1f}cm")

        # 4. Wind (0.0 - 0.2)
        if wind_speed is not None:
            wind_kmh = float(wind_speed)

            if wind_kmh > 50:
                score += 0.2  # Storm-force
            elif wind_kmh > 30:
                score += 0.15  # Gale
            elif wind_kmh > 20:
                score += 0.1  # Strong wind
            elif wind_kmh > 10:
                score += 0.05  # Breezy

            factors_used.append(f"wind={wind_kmh:.1f}km/h")

        # 5. Weather Condition Code (0.0 - 0.1)
        if weather_code is not None:
            code = int(weather_code)

            severe_codes = {95, 96, 99, 85, 86, 75, 67}
            moderate_codes = {61, 63, 65, 71, 73, 77, 80, 81, 82}

            if code in severe_codes:
                score += 0.1
                factors_used.append(f"severe_weather={code}")
            elif code in moderate_codes:
                score += 0.05
                factors_used.append(f"moderate_weather={code}")

        # 5. Time-of-day modifier
        hour = now.hour
        rush_hour = hour in [7, 8, 9, 16, 17, 18]
        if rush_hour and score > 0:
            score *= 1.2
            factors_used.append("rush_hour_x1.2")

        # Cap at 1.0
        score = min(score, 1.0)

        # Log what we used
        if factors_used:
            print(f"   Weather: {', '.join(factors_used)} -> score={score:.2f}")
        else:
            print("   ⚠️  No usable weather factors found")

        return score, True

    except Exception as e:
        print(f"❌ Error calculating weather score: {e}")
        return 0.0, False


def calculate_sentiment_score(connection, start_key, end_key):
    """
    Calculate sentiment score (0.0 - 1.0).
    Returns: (score, has_data_bool)
    """
    try:
        scanner = scan_hbase_data(connection, "tweets", start_key, end_key)
        total_tweets = 0
        negative_score_sum = 0

        for key, data in scanner:
            total_tweets += 1
            score = int(data.get(b"sentiment:score", b"5"))
            if score < 4:
                negative_score_sum += 1

        if total_tweets == 0:
            print("   Sentiment: No tweets found in window")
            return 0.0, False  # <--- Added False

        ratio = float(negative_score_sum) / total_tweets
        print(
            f"   Sentiment: {total_tweets} tweets, {negative_score_sum} negative (<4) -> score={ratio:.2f}"
        )

        return ratio, True  # <--- Added True

    except Exception as e:
        print(f"Error scanning tweets: {e}")
        return 0.0, False  # <--- Added False


def main_loop():
    print("🚀 Ad Campaign Manager started (Spark Edition)...")
    print(f"   Window: {SCAN_WINDOW_MINUTES} min")

    time.sleep(120)  # Initial wait

    # Initialize Spark Session for Kafka writing
    spark = SparkSession.builder.appName("AdCampaignManager").getOrCreate()

    # Silence Spark logs
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
            StructField("key", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )

    while True:
        try:
            start_loop = time.time()
            connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)

            # 1. Define window
            start_key, end_key, now_dt = get_time_window()
            print(f"\n[{now_dt}] Analyzing window {start_key} -> {end_key}")

            # 2. Calculate scores (Unpacking the new tuples)
            traffic_score, congested_locs, traffic_found = calculate_traffic_score(
                connection, start_key, end_key
            )
            weather_score, weather_found = calculate_weather_score(connection)
            sentiment_score, sentiment_found = calculate_sentiment_score(
                connection, start_key, end_key
            )

            # --- NEW CHECK: Skip if all sources are empty ---
            if not (traffic_found or weather_found or sentiment_found):
                print(
                    "💤 No data found in any source (Transport, Weather, Tweets). Skipping decision..."
                )
                connection.close()
                time.sleep(LOOP_INTERVAL_SECONDS)
                continue
            # ------------------------------------------------

            # 3. Global Score
            global_score = (
                traffic_score * WEIGHT_CONGESTION
                + weather_score * WEIGHT_WEATHER
                + sentiment_score * WEIGHT_SENTIMENT
            )

            print(
                f"   Scores -> Traffic: {traffic_score:.2f} | Weather: {weather_score:.2f} | Sentiment: {sentiment_score:.2f}"
            )
            print(f"   Global Score: {global_score:.2f}")

            # 4. Decision
            show_ad = global_score > SCORE_THRESHOLD_SHOW_AD
            decision_str = "SHOW_AD" if show_ad else "NO_AD"

            if show_ad:
                print(
                    f"   🎯 DECISION: {decision_str}! Targeting {len(congested_locs)} vehicles."
                )
            else:
                print(f"   DECISION: {decision_str}. Conditions acceptable.")

            # 5. Write Decision to HBase
            decision_key = now_dt.strftime("%Y%m%d_%H%M%S")
            ad_table = connection.table("ad_decisions")
            display_dt = now_dt + timedelta(minutes=5)
            display_ts = display_dt.strftime("%Y-%m-%d %H:%M:%S")

            def enc(val):
                return str(val).encode("utf-8")

            data = {
                b"metrics:traffic_score": enc(traffic_score),
                b"metrics:weather_score": enc(weather_score),
                b"metrics:sentiment_score": enc(sentiment_score),
                b"metrics:global_score": enc(global_score),
                b"decision:result": enc(decision_str),
                b"timestamps:decision_time": enc(now_dt.strftime("%Y-%m-%d %H:%M:%S")),
                b"timestamps:display_time": enc(display_ts),
            }

            top_targets = []
            campaign_type = ""

            if show_ad and congested_locs:
                top_targets = congested_locs[:5]
                targets_str = str(top_targets)
                data[b"target:locations"] = enc(targets_str)
                data[b"decision:campaign_type"] = b"ESCAPISM_NOW"
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
                    "sentiment": sentiment_score,
                },
                "action": {
                    "show_ad": show_ad,
                    "campaign": campaign_type,
                    "targets": top_targets,
                },
            }

            payload_json = json.dumps(kafka_payload)

            # Create a single-row DataFrame
            df = spark.createDataFrame([(decision_key, payload_json)], schema=schema)

            # Write to Kafka
            df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format(
                "kafka"
            ).option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option(
                "topic", KAFKA_TOPIC
            ).save()

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
