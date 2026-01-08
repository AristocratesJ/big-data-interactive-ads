"""
Spark Streaming Job: Consume Twitter Sentiment from Kafka and Write to HBase

Reads from: tweets-warsaw-raw Kafka topic
Writes to: tweets HBase table
"""

import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    explode,
    expr,
    from_json,
    size,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Define schema for Twitter API response
author_schema = StructType(
    [
        StructField("userName", StringType(), True),
        StructField("name", StringType(), True),
        StructField("id", StringType(), True),
    ]
)

hashtag_schema = StructType(
    [
        StructField("text", StringType(), True),
        StructField("indices", ArrayType(IntegerType()), True),
    ]
)

entities_schema = StructType(
    [
        StructField("hashtags", ArrayType(hashtag_schema), True),
    ]
)

tweet_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("createdAt", StringType(), True),
        StructField("author", author_schema, True),
        StructField("entities", entities_schema, True),
        StructField("lang", StringType(), True),
        StructField("likeCount", IntegerType(), True),
        StructField("retweetCount", IntegerType(), True),
        StructField("replyCount", IntegerType(), True),
        StructField("viewCount", IntegerType(), True),
    ]
)

twitter_schema = StructType(
    [
        StructField("tweets", ArrayType(tweet_schema), True),
    ]
)


# Initialize Spark Session
spark = (
    SparkSession.builder.appName("Twitter Sentiment to HBase")
    .config("spark.sql.session.timeZone", "Europe/Warsaw")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("🚀 Starting Spark Streaming: Twitter Sentiment → HBase")

# Read from Kafka
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "tweets-warsaw-raw")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

print("✅ Connected to Kafka topic: tweets-warsaw-raw")

# Extract message value and Kafka timestamp
messages_df = kafka_df.select(
    col("value").cast("string").alias("json_value"), col("timestamp").alias("kafka_ts")
)

# Parse JSON with schema
parsed_df = messages_df.select(
    from_json(col("json_value"), twitter_schema).alias("data"), col("kafka_ts")
)

# Explode tweets array to get individual tweets
# Filter out null data.tweets before exploding
tweets_df = parsed_df.filter(col("data.tweets").isNotNull()).select(
    explode(col("data.tweets")).alias("tweet"), col("kafka_ts")
)

# Extract fields from nested structure and create row key
final_df = tweets_df.select(
    # Create row key: YYYYMMDD_HHMMSS_TWEETID (ensures uniqueness)
    expr("concat(date_format(kafka_ts, 'yyyyMMdd_HHmmss'), '_', tweet.id)").alias("row_key"),
    # Tweet info
    col("tweet.id").alias("tweet_id"),
    col("tweet.createdAt").alias("created_at"),
    col("tweet.author.name").alias("author_name"),
    col("tweet.author.userName").alias("username"),
    # Content
    col("tweet.text").alias("text"),
    # Hashtags - extract text from array and join with commas
    when(
        size(col("tweet.entities.hashtags")) > 0,
        expr("concat_ws(',', transform(tweet.entities.hashtags, x -> x.text))"),
    )
    .otherwise("")
    .alias("hashtags"),
    # Engagement metrics
    col("tweet.likeCount").alias("like_count"),
    col("tweet.retweetCount").alias("retweet_count"),
    col("tweet.replyCount").alias("reply_count"),
    col("tweet.viewCount").alias("view_count"),
    # Language
    col("tweet.lang").alias("lang"),
    # Metadata
    date_format(col("kafka_ts"), "yyyy-MM-dd").alias("data_acquisition_date"),
    date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("ingestion_timestamp"),
    date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("analysis_timestamp"),
)

print("✅ Data transformation pipeline ready")


def write_to_hbase(batch_df, batch_id):
    """Write a micro-batch to HBase with simulated sentiment"""

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
        table = connection.table("tweets")

        # Write each row
        for row in rows:
            row_key = row["row_key"]

            # Calculate sentiment
            analyzer = SentimentIntensityAnalyzer()
            text = str(row["text"]) if row["text"] else ""

            # Get compound score usually between -1.0 (most negative) and 1.0 (most positive)
            compound_score = analyzer.polarity_scores(text)["compound"]

            # Map -1.0...1.0 to 0...9 scale
            # -1.0 -> 0
            # 0.0 -> 4.5 -> 4 or 5
            # 1.0 -> 9
            sentiment_score = int((compound_score + 1) * 4.5)
            # Ensure boundaries
            sentiment_score = max(0, min(9, sentiment_score))

            # Helper function for safe string conversion
            def safe_str(value):
                return str(value) if value is not None else ""

            # Prepare data for HBase
            data = {
                # Column family: tweet_info
                b"tweet_info:tweet_id": safe_str(row["tweet_id"]).encode("utf-8"),
                b"tweet_info:created_at": safe_str(row["created_at"]).encode("utf-8"),
                b"tweet_info:author_name": safe_str(row["author_name"]).encode("utf-8"),
                # Column family: content
                b"content:text": safe_str(row["text"]).encode("utf-8"),
                b"content:hashtags": safe_str(row["hashtags"]).encode("utf-8"),
                # Column family: sentiment
                b"sentiment:score": str(sentiment_score).encode("utf-8"),
                b"sentiment:compound_score": str(compound_score).encode("utf-8"),
                b"sentiment:analysis_timestamp": safe_str(row["analysis_timestamp"]).encode(
                    "utf-8"
                ),
                # Column family: metadata
                b"metadata:data_acquisition_date": safe_str(row["data_acquisition_date"]).encode(
                    "utf-8"
                ),
                b"metadata:ingestion_timestamp": safe_str(row["ingestion_timestamp"]).encode(
                    "utf-8"
                ),
                # Column family: engagement
                b"engagement:like_count": safe_str(row["like_count"]).encode("utf-8"),
                b"engagement:retweet_count": safe_str(row["retweet_count"]).encode("utf-8"),
                b"engagement:reply_count": safe_str(row["reply_count"]).encode("utf-8"),
                b"engagement:view_count": safe_str(row["view_count"]).encode("utf-8"),
                # Column family: author
                b"author:username": safe_str(row["username"]).encode("utf-8"),
                b"author:display_name": safe_str(row["author_name"]).encode("utf-8"),
                b"author:language": safe_str(row["lang"]).encode("utf-8"),
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
    .option("checkpointLocation", "/tmp/spark-checkpoint-sentiment")
    .trigger(processingTime="30 seconds")
    .start()
)

print("🎯 Streaming query started!")
print("   Consuming from: tweets-warsaw-raw")
print("   Writing to: HBase tweets")
print("   Checkpoint: /tmp/spark-checkpoint-sentiment")
print("\n📊 Waiting for data... (Press Ctrl+C to stop)\n")

# Wait for termination
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n🛑 Stopping streaming query...")
    query.stop()
    print("✅ Stopped gracefully")
