"""
HBase Table Setup for Twitter Sentiment Data

This script creates the HBase table structure for storing Warsaw tweet data
for sentiment and mood analysis.

Data source: api.twitterapi.io/twitter/tweet/advanced_search
Location: Filtered for Warsaw tweets

Row Key format: {timestamp}_{tweet_id}
  Example: 20260101_143025_1745678901234567890

This format allows:
  - Efficient time-based scans
  - Unique identification per tweet
  - Natural chronological sorting
  - Range queries for time periods

Note: Different row key format than weather/transport tables
      (tweets have exact timestamps, not hourly aggregation)
"""

import os
import sys
from datetime import datetime

import happybase


def create_sentiment_table():
    """
    Create HBase table for tweet sentiment data.

    Column Families:
      - content: text, lang, hashtags (sentiment analysis input)
      - engagement: likeCount, retweetCount, replyCount, viewCount (sentiment indicator)
      - author: userName, location, followers, isVerified (influence/credibility)
      - metadata: tweet_id, url, source, ingestion_timestamp
    """

    # Connect to HBase
    print("=" * 70)
    print("HBase Sentiment Table Setup - Twitter API")
    print("=" * 70)
    print("\nConnecting to HBase Thrift server...")

    # Detect if running in Docker
    is_docker = os.path.exists("/.dockerenv") or os.environ.get("DOCKER_CONTAINER") == "true"
    host = "hbase" if is_docker else "localhost"

    try:
        connection = happybase.Connection(
            host=host,
            port=9090,
            timeout=30000,  # 30 seconds timeout
        )
        print(f"✓ Connected to HBase at {host} successfully")

    except Exception as e:
        print(f"✗ Failed to connect to HBase: {e}")
        print("\nTroubleshooting:")
        print("  1. Check if HBase is running: docker ps | grep hbase")
        print("  2. Verify port 9090 is exposed in docker-compose.yml")
        print("  3. Wait a few more seconds for HBase to fully start")
        sys.exit(1)

    # Table configuration
    table_name = "tweets"

    # Check if table already exists
    print(f"\nChecking if table '{table_name}' exists...")
    existing_tables = [t.decode("utf-8") for t in connection.tables()]

    if table_name in existing_tables:
        print(f"⚠️  Table '{table_name}' already exists")

        # In automated mode (AUTO_SETUP=true), skip without recreating
        if os.environ.get("AUTO_SETUP") == "true":
            print("✓ Table exists, skipping (AUTO_SETUP mode)")
            connection.close()
            return

        response = input("Do you want to delete and recreate it? (yes/no): ")

        if response.lower() in ["yes", "y"]:
            print(f"Disabling table '{table_name}'...")
            connection.disable_table(table_name)
            print(f"Deleting table '{table_name}'...")
            connection.delete_table(table_name)
            print(f"✓ Table '{table_name}' deleted")
        else:
            print("Keeping existing table. Exiting...")
            connection.close()
            return

    # Create table with column families
    print(f"\nCreating table '{table_name}'...")

    families = {
        "tweet_info": dict(max_versions=1, compression="GZ", bloom_filter_type="ROW"),
        "content": dict(max_versions=1, compression="GZ", bloom_filter_type="ROW"),
        "engagement": dict(max_versions=1, compression="GZ", bloom_filter_type="ROW"),
        "author": dict(max_versions=1, compression="GZ", bloom_filter_type="ROW"),
        "sentiment": dict(max_versions=1, compression="GZ", bloom_filter_type="ROW"),
        "metadata": dict(max_versions=1, compression="GZ", bloom_filter_type="ROW"),
    }

    try:
        connection.create_table(table_name, families)
        print(f"✓ Table '{table_name}' created successfully!")

    except Exception as e:
        print(f"✗ Failed to create table: {e}")
        connection.close()
        sys.exit(1)

    # Verify table creation
    print("\nVerifying table structure...")
    table = connection.table(table_name)

    print("\n" + "=" * 70)
    print("TABLE INFORMATION")
    print("=" * 70)
    print(f"Table Name: {table_name}")
    print("Location Filter: Warsaw, Poland")
    print("Row Key Format: YYYYMMDD_HHMMSS_tweet_id")
    print("  Example: 20260101_143025_1745678901234567890")
    print("\nColumn Families:")

    print("\n  1. content: (Sentiment Analysis Input)")
    print("     - text: Tweet text (primary data for sentiment)")
    print("     - lang: Language code (e.g., 'pl', 'en')")
    print("     - hashtags: Comma-separated hashtags")

    print("\n  2. engagement: (Sentiment Indicators)")
    print("     - like_count: Number of likes")
    print("     - retweet_count: Number of retweets")
    print("     - reply_count: Number of replies")
    print("     - view_count: Number of views")
    print("     - quote_count: Number of quotes")

    print("\n  3. author: (Influence & Credibility)")
    print("     - username: Twitter username (@handle)")
    print("     - name: Display name")
    print("     - location: User's location (if available)")
    print("     - followers: Follower count (influence)")
    print("     - is_verified: Blue check mark (boolean)")

    print("\n  4. metadata: (Tracking Info)")
    print("     - tweet_id: Unique Twitter ID")
    print("     - url: Direct link to tweet")
    print("     - created_at: Original tweet timestamp (ISO format)")
    print("     - ingestion_timestamp: When we ingested it")

    # Insert sample record to test
    print("\n" + "=" * 70)
    print("TESTING TABLE WITH SAMPLE DATA")
    print("=" * 70)

    # Sample tweet with negative sentiment about Warsaw weather
    sample_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sample_tweet_id = "1745678901234567890"
    sample_key = f"{sample_timestamp}_{sample_tweet_id}_TEST"

    sample_data = {
        # Content - for sentiment analysis
        b"content:text": b"Znowu pada w Warszawie, caly dzien siedzenie w korku. Tragedia! :( #warszawa #pogoda",
        b"content:lang": b"pl",
        b"content:hashtags": b"warszawa,pogoda",
        # Engagement - popularity/sentiment indicators
        b"engagement:like_count": b"45",
        b"engagement:retweet_count": b"12",
        b"engagement:reply_count": b"8",
        b"engagement:view_count": b"2341",
        b"engagement:quote_count": b"3",
        # Author - influence
        b"author:username": b"jan_kowalski_wawa",
        b"author:name": b"Jan Kowalski",
        b"author:location": b"Warszawa, Polska",
        b"author:followers": b"1250",
        b"author:is_verified": b"false",
        # Metadata
        b"metadata:tweet_id": sample_tweet_id.encode(),
        b"metadata:url": f"https://twitter.com/jan_kowalski_wawa/status/{sample_tweet_id}".encode(),
        b"metadata:created_at": b"2026-01-01T14:30:25.000Z",
        b"metadata:ingestion_timestamp": datetime.now().isoformat().encode(),
    }

    print(f"\nInserting test record with key: {sample_key}")
    print("Sample tweet (negative sentiment about weather in Warsaw):")
    print(f"  Text: {sample_data[b'content:text'].decode()}")
    table.put(sample_key.encode(), sample_data)
    print("✓ Test record inserted")

    # Read back the test record
    print("\nReading test record back...")
    result = table.row(sample_key.encode())

    if result:
        print("✓ Test record retrieved successfully:")
        for key, value in sorted(result.items()):
            print(f"  {key.decode()}: {value.decode()}")

        # Delete test record
        print("\nDeleting test record...")
        table.delete(sample_key.encode())
        print("✓ Test record deleted")
    else:
        print("✗ Failed to retrieve test record")

    # Close connection
    connection.close()
    print("\n" + "=" * 70)
    print("✓ Setup complete! Table is ready for sentiment data ingestion.")
    print("=" * 70)


def main():
    """Main entry point"""
    try:
        create_sentiment_table()
    except KeyboardInterrupt:
        print("\n\n✗ Setup cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
