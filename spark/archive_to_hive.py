from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
import happybase
import time
from datetime import datetime, timedelta

# --- Configuration ---
HBASE_HOST = 'hbase'
HBASE_PORT = 9090
HIVE_TABLE = "default.ad_decisions_archive"


def get_recent_decisions(minutes=60):
    """
    Fetch decisions from HBase created in the last N minutes.
    """
    connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    table = connection.table('ad_decisions')

    now = datetime.now()
    start_time = now - timedelta(minutes=minutes)

    # Row Key: YYYYMMDD_HHMMSS
    start_key = start_time.strftime("%Y%m%d_%H%M%S")
    end_key = now.strftime("%Y%m%d_%H%M%S")

    print(f"Scanning HBase from {start_key} to {end_key}...")

    rows = []
    # Prefix scan if needed, but range scan is better here
    scanner = table.scan(row_start=start_key.encode(),
                         row_stop=end_key.encode())

    for key, data in scanner:
        # Decode HBase bytes to proper values
        decision_id = key.decode('utf-8')

        # Helper to safely decode
        def get_val(col_name, default=b''):
            val = data.get(col_name, default)
            return val.decode('utf-8')

        row_dict = {
            "decision_id": decision_id,
            "traffic_score": float(get_val(b'metrics:traffic_score', b'0.0')),
            "weather_score": float(get_val(b'metrics:weather_score', b'0.0')),
            "sentiment_score": float(get_val(b'metrics:sentiment_score', b'0.0')),
            "global_score": float(get_val(b'metrics:global_score', b'0.0')),
            "decision_result": get_val(b'decision:result', b'NO_AD'),
            "target_locations": get_val(b'target:locations', b'[]'),
            "decision_time": get_val(b'timestamps:decision_time', b''),
            "display_time": get_val(b'timestamps:display_time', b''),
            # Partition Columns
            "dt": decision_id.split('_')[0],       # YYYYMMDD
            "hr": decision_id.split('_')[1][:2]    # HH
        }
        rows.append(row_dict)

    connection.close()
    return rows


def archive_to_hive():
    # Use HDFS for Hive Warehouse
    spark = SparkSession.builder \
        .appName("AdDecisionsHiveArchiver") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("Fetching data from HBase...")
    data = get_recent_decisions(minutes=60)  # Archive last hour

    if not data:
        print("No new data to archive.")
        return

    print(f"Found {len(data)} records to archive.")

    # Create DataFrame
    df = spark.createDataFrame(data)

    # Ensure types
    df.printSchema()

    # Write to Hive
    # Mode 'append' adds new data
    # Partition by Date and Hour for efficient querying
    print(f"Writing to Hive table: {HIVE_TABLE}")

    df.write \
        .mode("append") \
        .partitionBy("dt", "hr") \
        .format("parquet") \
        .saveAsTable(HIVE_TABLE)

    print("Archive complete!")
    spark.stop()


if __name__ == "__main__":
    archive_to_hive()
