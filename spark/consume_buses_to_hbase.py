"""
Spark Streaming Job: Consume ZTM Buses from Kafka and Write to HBase

Reads from: ztm-buses-raw Kafka topic
Writes to: transport_events HBase table
"""

import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    explode,
    expr,
    from_json,
    lit,
)
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

# Initialize Spark Session
spark = (
    SparkSession.builder.appName("ZTM Buses to HBase")
    .config("spark.sql.session.timeZone", "Europe/Warsaw")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Define ZTM API JSON Schema
ztm_schema = StructType(
    [
        StructField(
            "result",
            ArrayType(
                StructType(
                    [
                        StructField("Lines", StringType(), True),
                        StructField("Lon", DoubleType(), True),
                        StructField("VehicleNumber", StringType(), True),
                        StructField("Time", StringType(), True),
                        StructField("Lat", DoubleType(), True),
                        StructField("Brigade", StringType(), True),
                    ]
                )
            ),
            True,
        )
    ]
)

print("🚀 Starting Spark Streaming: ZTM Buses → HBase")

# Read from Kafka
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "ztm-buses-raw")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

print("✅ Connected to Kafka topic: ztm-buses-raw")

# Extract Kafka message components (no headers - not configured in NiFi)
messages_df = kafka_df.select(
    col("value").cast("string").alias("json_string"),
    col("timestamp").alias("kafka_timestamp"),
)

# Parse JSON
parsed_df = messages_df.select(
    from_json(col("json_string"), ztm_schema).alias("data"),
    col("kafka_timestamp"),
)

# Explode result array (one row per bus)
exploded_df = parsed_df.select(explode(col("data.result")).alias("bus"), col("kafka_timestamp"))

# Extract fields and create row key
final_df = exploded_df.select(
    # Row key: YYYYMMDD_HHMMSS_vehicleNumber
    concat_ws(
        "_",
        expr("date_format(current_timestamp(), 'yyyyMMdd')"),
        expr("date_format(current_timestamp(), 'HHmmss')"),
        col("bus.VehicleNumber"),
    ).alias("row_key"),
    # Column family: info
    col("bus.Lines").alias("Lines"),
    col("bus.VehicleNumber").alias("VehicleNumber"),
    col("bus.Brigade").alias("Brigade"),
    col("bus.Time").alias("Time"),
    # Column family: location
    col("bus.Lat").cast("string").alias("Lat"),
    col("bus.Lon").cast("string").alias("Lon"),
    # Column family: metadata (using defaults - no headers from NiFi)
    lit("bus").alias("vehicle_type"),
    current_timestamp().alias("ingestion_timestamp"),
)

print("✅ Data transformation pipeline ready")


# Function to write batch to HBase
def write_to_hbase(batch_df, batch_id):
    """Write a micro-batch to HBase"""

    print(f"📦 Processing batch {batch_id}...")

    # Collect rows (careful: only for small batches!)
    rows = batch_df.collect()

    if not rows:
        print(f"⚠️  Batch {batch_id} is empty, skipping...")
        return

    print(f"   Found {len(rows)} records to write")

    # Connect to HBase
    try:
        connection = happybase.Connection("hbase", port=9090)
        table = connection.table("transport_events")

        # Write each row
        for row in rows:
            row_key = row["row_key"]

            # Prepare data for HBase
            data = {
                b"info:Lines": str(row["Lines"] or "").encode("utf-8"),
                b"info:VehicleNumber": str(row["VehicleNumber"] or "").encode("utf-8"),
                b"info:Brigade": str(row["Brigade"] or "").encode("utf-8"),
                b"info:Time": str(row["Time"] or "").encode("utf-8"),
                b"location:Lat": str(row["Lat"] or "").encode("utf-8"),
                b"location:Lon": str(row["Lon"] or "").encode("utf-8"),
                b"metadata:vehicle_type": str(row["vehicle_type"] or "bus").encode("utf-8"),
                b"metadata:ingestion_timestamp": str(row["ingestion_timestamp"] or "").encode(
                    "utf-8"
                ),
            }

            # Write to HBase
            table.put(row_key.encode("utf-8"), data)

        connection.close()
        print(f"✅ Batch {batch_id} written successfully ({len(rows)} records)")

    except Exception as e:
        print(f"❌ Error writing batch {batch_id} to HBase: {e}")
        raise


# Start streaming query
query = (
    final_df.writeStream.foreachBatch(write_to_hbase)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/spark-checkpoint-buses")
    .trigger(processingTime="30 seconds")
    .start()
)

print("🎯 Streaming query started!")
print("   Consuming from: ztm-buses-raw")
print("   Writing to: HBase transport_events")
print("   Checkpoint: /tmp/spark-checkpoint-buses")
print("\n📊 Waiting for data... (Press Ctrl+C to stop)\n")

# Wait for termination
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n🛑 Stopping streaming query...")
    query.stop()
    print("✅ Stopped gracefully")
