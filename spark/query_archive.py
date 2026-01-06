"""
Simple script to query archived ad decisions from HDFS Parquet files.
Usage: spark-submit query_archive.py [--date YYYYMMDD] [--limit N]
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import argparse

HDFS_PATH = "hdfs://namenode:9000/user/archive/ad_decisions"


def query_archive(date_filter=None, limit=20):
    """
    Query archived ad decisions from Parquet files on HDFS.
    """
    spark = SparkSession.builder \
        .appName("QueryAdDecisions") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print(f"Querying Ad Decisions Archive: {HDFS_PATH}")
    print(f"{'='*60}\n")

    try:
        # Read Parquet files
        df = spark.read.parquet(HDFS_PATH)
        
        # Apply date filter if specified
        if date_filter:
            df = df.filter(col("dt") == date_filter)
            print(f"Filtering by date: {date_filter}")
        
        # Show schema
        print("Schema:")
        df.printSchema()
        
        # Show count
        total_count = df.count()
        print(f"Total records: {total_count}\n")
        
        # Show sample data
        print(f"Sample data (limit {limit}):")
        df.select(
            "decision_id", 
            "global_score", 
            "decision_result", 
            "traffic_score",
            "weather_score",
            "sentiment_score",
            "dt", 
            "hr"
        ).orderBy(col("decision_id").desc()).limit(limit).show(truncate=False)
        
        # Show statistics by decision result
        print("\nDecisions by result:")
        df.groupBy("decision_result").count().show()
        
        # Show statistics by date
        print("\nDecisions by date:")
        df.groupBy("dt").count().orderBy("dt").show()
        
        # Show average scores
        print("\nAverage scores:")
        df.selectExpr(
            "round(avg(global_score), 3) as avg_global",
            "round(avg(traffic_score), 3) as avg_traffic",
            "round(avg(weather_score), 3) as avg_weather", 
            "round(avg(sentiment_score), 3) as avg_sentiment"
        ).show()
        
    except Exception as e:
        print(f"Error reading archive: {e}")
        print("Archive may be empty or not yet created.")
    
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query ad decisions archive")
    parser.add_argument("--date", type=str, help="Filter by date (YYYYMMDD)")
    parser.add_argument("--limit", type=int, default=20, help="Number of rows to show")
    args = parser.parse_args()
    
    query_archive(date_filter=args.date, limit=args.limit)
