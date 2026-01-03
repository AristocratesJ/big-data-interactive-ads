"""
Spark Streaming: Weather Forecast from Kafka to HBase

Consumes weather forecast data from Kafka topic 'weather-forecast-raw',
parses the hourly arrays structure, and writes to HBase 'weather_forecast' table.

Data structure from Open-Meteo API:
{
  "hourly": {
    "time": ["2026-01-02T00:00", "2026-01-02T01:00", ...],
    "temperature_2m": [5.2, 4.8, ...],
    "apparent_temperature": [3.1, 2.5, ...],
    ...
  }
}

Row key format: YYYYMMDD_HH (e.g., 20260102_14)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    explode,
    arrays_zip,
    date_format,
    to_timestamp,
    current_timestamp,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    DoubleType,
    IntegerType,
)
import happybase


# Define schema for Open-Meteo weather API response
weather_schema = StructType(
    [
        StructField(
            "hourly",
            StructType(
                [
                    StructField("time", ArrayType(StringType()), True),
                    StructField("temperature_2m", ArrayType(DoubleType()), True),
                    StructField("apparent_temperature", ArrayType(DoubleType()), True),
                    StructField(
                        "precipitation_probability", ArrayType(IntegerType()), True
                    ),
                    StructField("precipitation", ArrayType(DoubleType()), True),
                    StructField("rain", ArrayType(DoubleType()), True),
                    StructField("snowfall", ArrayType(DoubleType()), True),
                    StructField("snow_depth", ArrayType(DoubleType()), True),
                    StructField("weather_code", ArrayType(IntegerType()), True),
                    StructField("cloud_cover", ArrayType(IntegerType()), True),
                    StructField("relative_humidity_2m", ArrayType(IntegerType()), True),
                    StructField("wind_speed_10m", ArrayType(DoubleType()), True),
                    StructField("wind_gusts_10m", ArrayType(DoubleType()), True),
                ]
            ),
            True,
        )
    ]
)


def write_to_hbase(batch_df, batch_id):
    """
    Write each batch to HBase using happybase.

    Args:
        batch_df: DataFrame with weather forecast records
        batch_id: Batch identifier from Spark streaming
    """
    print(f"\n📦 Processing batch {batch_id}...")

    # Collect data to driver
    records = batch_df.collect()

    if not records:
        print(f"⚠️  Batch {batch_id} is empty, skipping...")
        return

    print(f"   Found {len(records)} records to write")

    # Connect to HBase
    connection = happybase.Connection("hbase", port=9090)
    table = connection.table("weather_forecast")

    # Write each record
    for row in records:
        row_key = row["row_key"]

        # Prepare data for HBase (all values as strings)
        data = {
            # temperature family
            b"temperature:temp_2m": str(row["temp_2m"]).encode("utf-8"),
            b"temperature:apparent_temp": str(row["apparent_temp"]).encode("utf-8"),
            # precipitation family
            b"precipitation:precip_probability": str(row["precip_probability"]).encode(
                "utf-8"
            ),
            b"precipitation:precip_total": str(row["precip_total"]).encode("utf-8"),
            b"precipitation:rain": str(row["rain"]).encode("utf-8"),
            b"precipitation:snowfall": str(row["snowfall"]).encode("utf-8"),
            b"precipitation:snow_depth": str(row["snow_depth"]).encode("utf-8"),
            # atmospheric family
            b"atmospheric:weather_code": str(row["weather_code"]).encode("utf-8"),
            b"atmospheric:cloud_cover": str(row["cloud_cover"]).encode("utf-8"),
            b"atmospheric:relative_humidity": str(row["relative_humidity"]).encode(
                "utf-8"
            ),
            # wind family
            b"wind:wind_speed": str(row["wind_speed"]).encode("utf-8"),
            b"wind:wind_gusts": str(row["wind_gusts"]).encode("utf-8"),
            # metadata family
            b"metadata:ingestion_timestamp": row["ingestion_timestamp"].encode("utf-8"),
            b"metadata:forecast_time": row["forecast_time"].encode("utf-8"),
        }

        # Write to HBase
        table.put(row_key.encode("utf-8"), data)

    connection.close()
    print(f"✅ Batch {batch_id} written successfully ({len(records)} records)")


# Initialize Spark Session
spark = (
    SparkSession.builder.appName("Weather Forecast to HBase")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()
)

# Reduce Spark logging verbosity
spark.sparkContext.setLogLevel("ERROR")

print("=" * 70)
print("SPARK STREAMING: Weather Forecast to HBase")
print("=" * 70)
print(f"Reading from Kafka topic: weather-forecast-raw")
print(f"Writing to HBase table: weather_forecast")
print(f"Checkpoint location: /tmp/spark-checkpoint-weather")
print("=" * 70)

# Read from Kafka
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "weather-forecast-raw")
    .option("startingOffsets", "earliest")
    .load()
)

# Extract message value and Kafka timestamp
messages_df = kafka_df.select(
    col("value").cast("string").alias("json_value"), col("timestamp").alias("kafka_ts")
)

# Parse JSON with schema
parsed_df = messages_df.select(
    from_json(col("json_value"), weather_schema).alias("data"), col("kafka_ts")
)

# Explode hourly arrays - zip all arrays together then explode
exploded_df = parsed_df.select(
    explode(
        arrays_zip(
            col("data.hourly.time"),
            col("data.hourly.temperature_2m"),
            col("data.hourly.apparent_temperature"),
            col("data.hourly.precipitation_probability"),
            col("data.hourly.precipitation"),
            col("data.hourly.rain"),
            col("data.hourly.snowfall"),
            col("data.hourly.snow_depth"),
            col("data.hourly.weather_code"),
            col("data.hourly.cloud_cover"),
            col("data.hourly.relative_humidity_2m"),
            col("data.hourly.wind_speed_10m"),
            col("data.hourly.wind_gusts_10m"),
        )
    ).alias("hourly_data"),
    col("kafka_ts"),
)

# Extract fields and create row key
final_df = exploded_df.select(
    # Parse timestamp and create row key: YYYYMMDD_HH
    date_format(
        to_timestamp(col("hourly_data.time"), "yyyy-MM-dd'T'HH:mm"), "yyyyMMdd_HH"
    ).alias("row_key"),
    # Column family: temperature
    col("hourly_data.temperature_2m").alias("temp_2m"),
    col("hourly_data.apparent_temperature").alias("apparent_temp"),
    # Column family: precipitation
    col("hourly_data.precipitation_probability").alias("precip_probability"),
    col("hourly_data.precipitation").alias("precip_total"),
    col("hourly_data.rain").alias("rain"),
    col("hourly_data.snowfall").alias("snowfall"),
    col("hourly_data.snow_depth").alias("snow_depth"),
    # Column family: atmospheric
    col("hourly_data.weather_code").alias("weather_code"),
    col("hourly_data.cloud_cover").alias("cloud_cover"),
    col("hourly_data.relative_humidity_2m").alias("relative_humidity"),
    # Column family: wind
    col("hourly_data.wind_speed_10m").alias("wind_speed"),
    col("hourly_data.wind_gusts_10m").alias("wind_gusts"),
    # Column family: metadata
    date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias(
        "ingestion_timestamp"
    ),
    col("hourly_data.time").alias("forecast_time"),
)

# Write to HBase using foreachBatch
query = (
    final_df.writeStream.foreachBatch(write_to_hbase)
    .option("checkpointLocation", "/tmp/spark-checkpoint-weather")
    .trigger(processingTime="30 seconds")
    .start()
)

print("\n🚀 Streaming query started!")
print("   Press Ctrl+C in terminal to stop (if running interactively)")
print("   Or use stop script to kill the process\n")

# Wait for termination
query.awaitTermination()
