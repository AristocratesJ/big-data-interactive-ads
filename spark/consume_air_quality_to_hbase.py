"""
Spark Streaming: Air Quality Forecast from Kafka to HBase

Consumes air quality forecast data from Kafka topic 'air-quality-raw',
parses the hourly arrays structure, and writes to HBase 'air_quality_forecast' table.

Data structure from Open-Meteo Air Quality API:
{
  "hourly": {
    "time": ["2026-01-03T00:00", "2026-01-03T01:00", ...],
    "pm10": [35.2, 32.1, ...],
    "pm2_5": [18.5, 16.3, ...],
    ...
  }
}

Row key format: YYYYMMDD_HH (e.g., 20260103_14)
"""

import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    arrays_zip,
    col,
    current_timestamp,
    date_format,
    explode,
    from_json,
    to_timestamp,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# Define schema for Open-Meteo Air Quality API response
air_quality_schema = StructType(
    [
        StructField(
            "hourly",
            StructType(
                [
                    StructField("time", ArrayType(StringType()), True),
                    StructField("pm10", ArrayType(DoubleType()), True),
                    StructField("pm2_5", ArrayType(DoubleType()), True),
                    StructField("ozone", ArrayType(DoubleType()), True),
                    StructField("nitrogen_dioxide", ArrayType(DoubleType()), True),
                    StructField("sulphur_dioxide", ArrayType(DoubleType()), True),
                    StructField("ammonia", ArrayType(DoubleType()), True),
                    StructField("uv_index", ArrayType(DoubleType()), True),
                    StructField("uv_index_clear_sky", ArrayType(DoubleType()), True),
                    StructField("alder_pollen", ArrayType(DoubleType()), True),
                    StructField("birch_pollen", ArrayType(DoubleType()), True),
                    StructField("grass_pollen", ArrayType(DoubleType()), True),
                    StructField("mugwort_pollen", ArrayType(DoubleType()), True),
                    StructField("ragweed_pollen", ArrayType(DoubleType()), True),
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
        batch_df: DataFrame with air quality forecast records
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
    table = connection.table("air_quality_forecast")

    # Write each record
    for row in records:
        row_key = row["row_key"]

        # Prepare data for HBase (all values as strings)
        data = {
            # particulates family
            b"particulates:pm10": str(row["pm10"]).encode("utf-8"),
            b"particulates:pm2_5": str(row["pm2_5"]).encode("utf-8"),
            # gases family
            b"gases:ozone": str(row["ozone"]).encode("utf-8"),
            b"gases:nitrogen_dioxide": str(row["nitrogen_dioxide"]).encode("utf-8"),
            b"gases:sulphur_dioxide": str(row["sulphur_dioxide"]).encode("utf-8"),
            b"gases:ammonia": str(row["ammonia"]).encode("utf-8"),
            # uv family
            b"uv:uv_index": str(row["uv_index"]).encode("utf-8"),
            b"uv:uv_index_clear_sky": str(row["uv_index_clear_sky"]).encode("utf-8"),
            # pollen family
            b"pollen:alder": str(row["alder_pollen"]).encode("utf-8"),
            b"pollen:birch": str(row["birch_pollen"]).encode("utf-8"),
            b"pollen:grass": str(row["grass_pollen"]).encode("utf-8"),
            b"pollen:mugwort": str(row["mugwort_pollen"]).encode("utf-8"),
            b"pollen:ragweed": str(row["ragweed_pollen"]).encode("utf-8"),
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
    SparkSession.builder.appName("Air Quality Forecast to HBase")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()
)

# Reduce Spark logging verbosity
spark.sparkContext.setLogLevel("ERROR")

print("=" * 70)
print("SPARK STREAMING: Air Quality Forecast to HBase")
print("=" * 70)
print("Reading from Kafka topic: air-quality-raw")
print("Writing to HBase table: air_quality_forecast")
print("Checkpoint location: /tmp/spark-checkpoint-air-quality")
print("=" * 70)

# Read from Kafka
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "air-quality-raw")
    .option("startingOffsets", "earliest")
    .load()
)

# Extract message value and Kafka timestamp
messages_df = kafka_df.select(
    col("value").cast("string").alias("json_value"), col("timestamp").alias("kafka_ts")
)

# Parse JSON with schema
parsed_df = messages_df.select(
    from_json(col("json_value"), air_quality_schema).alias("data"), col("kafka_ts")
)

# Explode hourly arrays - zip all arrays together then explode
exploded_df = parsed_df.select(
    explode(
        arrays_zip(
            col("data.hourly.time"),
            col("data.hourly.pm10"),
            col("data.hourly.pm2_5"),
            col("data.hourly.ozone"),
            col("data.hourly.nitrogen_dioxide"),
            col("data.hourly.sulphur_dioxide"),
            col("data.hourly.ammonia"),
            col("data.hourly.uv_index"),
            col("data.hourly.uv_index_clear_sky"),
            col("data.hourly.alder_pollen"),
            col("data.hourly.birch_pollen"),
            col("data.hourly.grass_pollen"),
            col("data.hourly.mugwort_pollen"),
            col("data.hourly.ragweed_pollen"),
        )
    ).alias("hourly_data"),
    col("kafka_ts"),
)

# Extract fields and create row key
final_df = exploded_df.select(
    # Parse timestamp and create row key: YYYYMMDD_HH
    date_format(to_timestamp(col("hourly_data.time"), "yyyy-MM-dd'T'HH:mm"), "yyyyMMdd_HH").alias(
        "row_key"
    ),
    # Column family: particulates
    col("hourly_data.pm10").alias("pm10"),
    col("hourly_data.pm2_5").alias("pm2_5"),
    # Column family: gases
    col("hourly_data.ozone").alias("ozone"),
    col("hourly_data.nitrogen_dioxide").alias("nitrogen_dioxide"),
    col("hourly_data.sulphur_dioxide").alias("sulphur_dioxide"),
    col("hourly_data.ammonia").alias("ammonia"),
    # Column family: uv
    col("hourly_data.uv_index").alias("uv_index"),
    col("hourly_data.uv_index_clear_sky").alias("uv_index_clear_sky"),
    # Column family: pollen
    col("hourly_data.alder_pollen").alias("alder_pollen"),
    col("hourly_data.birch_pollen").alias("birch_pollen"),
    col("hourly_data.grass_pollen").alias("grass_pollen"),
    col("hourly_data.mugwort_pollen").alias("mugwort_pollen"),
    col("hourly_data.ragweed_pollen").alias("ragweed_pollen"),
    # Column family: metadata
    date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("ingestion_timestamp"),
    col("hourly_data.time").alias("forecast_time"),
)

# Write to HBase using foreachBatch
query = (
    final_df.writeStream.foreachBatch(write_to_hbase)
    .option("checkpointLocation", "/tmp/spark-checkpoint-air-quality")
    .trigger(processingTime="30 seconds")
    .start()
)

print("\n🚀 Streaming query started!")
print("   Press Ctrl+C in terminal to stop (if running interactively)")
print("   Or use stop script to kill the process\n")

# Wait for termination
query.awaitTermination()
