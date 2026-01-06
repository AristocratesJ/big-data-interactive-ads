from pyspark.sql import SparkSession
import happybase
from datetime import datetime, timedelta

# --- Configuration ---
HBASE_HOST = 'hbase'
HBASE_PORT = 9090
HDFS_PATH = "hdfs://namenode:9000/user/archive/ad_decisions"


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

    print(f"Scanning HBase from {start_key} to {end_key}...", flush=True)

    rows = []
    scanner = table.scan(row_start=start_key.encode(),
                         row_stop=end_key.encode())

    for key, data in scanner:
        decision_id = key.decode('utf-8')

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


def archive_to_parquet():
    """
    Simple archiver: HBase -> Parquet on HDFS (no Hive metastore needed)
    """
    print(f"[{datetime.now()}] Starting archive job...", flush=True)
    
    spark = SparkSession.builder \
        .appName("AdDecisionsArchiver") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("Fetching data from HBase...", flush=True)
    data = get_recent_decisions(minutes=5)  # Archive last 5 minutes

    if not data:
        print("No new data to archive.", flush=True)
        spark.stop()
        return

    print(f"Found {len(data)} records to archive.", flush=True)

    # Create DataFrame
    df = spark.createDataFrame(data)
    df.printSchema()

    # Write to HDFS as Parquet with partitioning
    print(f"Writing to HDFS: {HDFS_PATH}", flush=True)

    df.write \
        .mode("append") \
        .partitionBy("dt", "hr") \
        .format("parquet") \
        .save(HDFS_PATH)

    print("Archive complete!", flush=True)
    print(f"Data saved to: {HDFS_PATH}", flush=True)
    spark.stop()


if __name__ == "__main__":
    archive_to_parquet()
