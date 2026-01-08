#!/usr/bin/env python3
"""
Unified Archive Scheduler & Worker

Modes:
1. Scheduler (Default): Runs the orchestration loop, managing retries and service health.
2. Worker (--worker):   The actual Spark job that moves data from HBase to HDFS.

Usage:
  - Scheduler: python3 archive_scheduler.py
  - Worker:    Called automatically by the scheduler via spark-submit
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta

# --- Configuration ---
ARCHIVE_INTERVAL_SECONDS = 60
HBASE_HOST = "hbase"
HBASE_PORT = 9090
HDFS_ROOT = "hdfs://namenode:9000"
TARGET_PATH = f"{HDFS_ROOT}/user/archive/ad_decisions"

# Paths (Assumes script is mounted at /opt/spark-apps/)
SCRIPT_PATH = os.path.abspath(__file__)
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_SQL = "/opt/spark/bin/spark-sql"
LOG_FILE = "/opt/spark-apps/scheduler.log"

# Hive DDL
HIVE_CREATE_TABLE_SQL = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS ad_decisions_archive (
    decision_id STRING,
    traffic_score DOUBLE,
    weather_score DOUBLE,
    sentiment_score DOUBLE,
    global_score DOUBLE,
    decision_result STRING,
    target_locations STRING,
    decision_time STRING,
    display_time STRING
)
PARTITIONED BY (dt STRING, hr STRING)
STORED AS PARQUET
LOCATION '{TARGET_PATH}'
"""

running = True

# ==========================================
#        PART 1: WORKER LOGIC (SPARK)
# ==========================================


def run_worker(start_str, end_str):
    """
    The Spark Job: Fetches data from HBase -> Writes to HDFS Parquet
    """
    # Lazy imports to avoid overhead in Scheduler process
    import happybase
    from pyspark.sql import SparkSession

    print(f"[Worker] Starting archive for window: {start_str} to {end_str}")

    # Initialize Spark
    spark = (
        SparkSession.builder.appName(f"AdArchive_{start_str}")
        .config("spark.hadoop.fs.defaultFS", HDFS_ROOT)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Connect to HBase
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
        table = connection.table("ad_decisions")

        print("[Worker] Scanning HBase...", flush=True)

        # Row Key Scan
        # We assume keys are formatted as YYYYMMDD_HHMMSS
        rows = []
        scanner = table.scan(row_start=start_str.encode(), row_stop=end_str.encode())

        for key, data in scanner:
            decision_id = key.decode("utf-8")

            def get_val(col, default=b""):
                return data.get(col, default).decode("utf-8")

            try:
                row_dict = {
                    "decision_id": decision_id,
                    "traffic_score": float(get_val(b"metrics:traffic_score", b"0.0")),
                    "weather_score": float(get_val(b"metrics:weather_score", b"0.0")),
                    "sentiment_score": float(get_val(b"metrics:sentiment_score", b"0.0")),
                    "global_score": float(get_val(b"metrics:global_score", b"0.0")),
                    "decision_result": get_val(b"decision:result", b"NO_AD"),
                    "target_locations": get_val(b"target:locations", b"[]"),
                    "decision_time": get_val(b"timestamps:decision_time", b""),
                    "display_time": get_val(b"timestamps:display_time", b""),
                    # Partition Columns (derived from ID)
                    "dt": decision_id.split("_")[0],  # YYYYMMDD
                    "hr": decision_id.split("_")[1][:2],  # HH
                }
                rows.append(row_dict)
            except Exception as e:
                print(f"[Worker] Error parsing row {decision_id}: {e}")

        connection.close()

        if not rows:
            print("[Worker] No data found in this time window.", flush=True)
            return

        print(f"[Worker] Writing {len(rows)} records to HDFS...", flush=True)

        df = spark.createDataFrame(rows)

        df.write.mode("append").partitionBy("dt", "hr").format("parquet").save(TARGET_PATH)

        print("[Worker] Write complete!", flush=True)

    except Exception as e:
        print(f"[Worker] Critical Error: {e}", flush=True)
        sys.exit(1)
    finally:
        spark.stop()


# ==========================================
#        PART 2: SCHEDULER LOGIC
# ==========================================


def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def signal_handler(signum, frame):
    global running
    log(f"Received signal {signum}, shutting down...")
    running = False


def wait_for_services():
    log("Waiting for services (HBase)...")
    import socket

    retries = 30
    while retries > 0:
        try:
            # Simple socket check first
            sock = socket.create_connection((HBASE_HOST, HBASE_PORT), timeout=2)
            sock.close()

            # Then check library
            import happybase

            conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
            conn.tables()
            conn.close()
            log("✓ Services are ready")
            return True
        except Exception:
            retries -= 1
            time.sleep(5)
    return False


def setup_hive_table():
    log("Initializing Hive table...")
    cmd = [SPARK_SQL, "-e", HIVE_CREATE_TABLE_SQL]
    # Simple retry loop
    for i in range(5):
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if res.returncode == 0:
                log("✓ Hive table initialized")
                return True
            log(f"Hive setup attempt {i + 1} failed: {res.stderr[-200:]}")
        except Exception as e:
            log(f"Hive setup error: {e}")
        time.sleep(10)
    return False


def refresh_partitions():
    """Runs MSCK REPAIR to make new HDFS files visible in Hive"""
    try:
        res = subprocess.run(
            [SPARK_SQL, "-e", "MSCK REPAIR TABLE ad_decisions_archive"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if res.returncode == 0:
            log("✓ Partitions refreshed")
        else:
            log(f"Partition refresh failed: {res.stderr[-100:]}")
    except Exception as e:
        log(f"Partition refresh error: {e}")


def run_scheduler():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    time.sleep(120)

    log("=== UNIFIED SCHEDULER STARTED ===")

    if not wait_for_services():
        log("CRITICAL: Services not ready.")
        sys.exit(1)

    if not setup_hive_table():
        log("CRITICAL: Hive table setup failed.")
        sys.exit(1)

    last_run_time = datetime.now() - timedelta(seconds=ARCHIVE_INTERVAL_SECONDS)

    while running:
        current_time = datetime.now()

        start_str = last_run_time.strftime("%Y%m%d_%H%M%S")
        end_str = current_time.strftime("%Y%m%d_%H%M%S")

        log(f"--- Starting Job: {start_str} -> {end_str} ---")

        cmd = [
            SPARK_SUBMIT,
            "--master",
            "local[1]",
            SCRIPT_PATH,
            "--worker",
            "--start",
            start_str,
            "--end",
            end_str,
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

            if result.returncode == 0:
                log("✓ Job Success")
                last_run_time = current_time
                refresh_partitions()

                # Sleep logic
                sleep_time = ARCHIVE_INTERVAL_SECONDS
                log(f"Sleeping {sleep_time}s...")
                while sleep_time > 0 and running:
                    time.sleep(1)
                    sleep_time -= 1
            else:
                log(f"✗ Job Failed (Code {result.returncode})")
                log(f"Stderr: {result.stderr[-500:]}")
                time.sleep(10)  # Short retry wait

        except subprocess.TimeoutExpired:
            log("✗ Job Timed Out")
        except Exception as e:
            log(f"✗ Scheduler Error: {e}")
            time.sleep(10)

    log("Scheduler stopped.")


# ==========================================
#        ENTRY POINT
# ==========================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true", help="Run in Spark Worker mode")
    parser.add_argument("--start", help="Start timestamp YYYYMMDD_HHMMSS")
    parser.add_argument("--end", help="End timestamp YYYYMMDD_HHMMSS")

    args = parser.parse_args()

    if args.worker:
        # We are being called by spark-submit
        if not args.start or not args.end:
            print("Error: Worker requires --start and --end")
            sys.exit(1)
        run_worker(args.start, args.end)
    else:
        # We are being called by Docker/User
        run_scheduler()
