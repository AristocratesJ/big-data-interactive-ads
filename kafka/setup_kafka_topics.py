"""
Kafka Topics Setup for Big Data Interactive Ads Project

This script creates all Kafka topics needed for the data pipeline:
1. Raw data topics (from NiFi)
2. Processed data topics (from Spark)
3. Decision topics (ad placement decisions)

Architecture:
  NiFi → Kafka (raw) → Spark → HBase
                            → Kafka (processed)
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError
import sys


def create_kafka_topics():
    """
    Create all Kafka topics for the data pipeline.
    """

    print("=" * 70)
    print("Kafka Topics Setup - Big Data Interactive Ads")
    print("=" * 70)

    # Connect to Kafka
    print("\nConnecting to Kafka...")
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:9092",
            client_id="kafka_setup_script",
            request_timeout_ms=10000,
        )
        print("✓ Connected to Kafka successfully")
    except Exception as e:
        print(f"✗ Failed to connect to Kafka: {e}")
        print("\nTroubleshooting:")
        print("  1. Check if Kafka is running: docker ps | grep kafka")
        print("  2. Verify port 9092 is exposed in docker-compose.yml")
        print("  3. Wait a few more seconds for Kafka to fully start")
        sys.exit(1)

    # Define all topics
    topics = [
        # ===== RAW DATA TOPICS (from NiFi) =====
        NewTopic(
            name="ztm-buses-raw",
            num_partitions=3,
            replication_factor=1,
            topic_configs={
                "retention.ms": "86400000",  # 24 hours
                "compression.type": "snappy",
                "cleanup.policy": "delete",
            },
        ),
        NewTopic(
            name="ztm-trolleys-raw",
            num_partitions=3,
            replication_factor=1,
            topic_configs={
                "retention.ms": "86400000",  # 24 hours
                "compression.type": "snappy",
                "cleanup.policy": "delete",
            },
        ),
        NewTopic(
            name="weather-forecast-raw",
            num_partitions=2,
            replication_factor=1,
            topic_configs={
                "retention.ms": "172800000",  # 48 hours (forecasts are less frequent)
                "compression.type": "snappy",
                "cleanup.policy": "delete",
            },
        ),
        NewTopic(
            name="air-quality-raw",
            num_partitions=2,
            replication_factor=1,
            topic_configs={
                "retention.ms": "172800000",  # 48 hours
                "compression.type": "snappy",
                "cleanup.policy": "delete",
            },
        ),
        NewTopic(
            name="tweets-warsaw-raw",
            num_partitions=3,
            replication_factor=1,
            topic_configs={
                "retention.ms": "86400000",  # 24 hours
                "compression.type": "gzip",  # Better for text
                "cleanup.policy": "delete",
            },
        ),
        # ===== PROCESSED DATA TOPICS (from Spark) =====
        NewTopic(
            name="transport-processed",
            num_partitions=3,
            replication_factor=1,
            topic_configs={
                "retention.ms": "86400000",  # 24 hours
                "compression.type": "snappy",
                "cleanup.policy": "delete",
            },
        ),
        NewTopic(
            name="weather-processed",
            num_partitions=2,
            replication_factor=1,
            topic_configs={
                "retention.ms": "172800000",  # 48 hours
                "compression.type": "snappy",
                "cleanup.policy": "delete",
            },
        ),
        NewTopic(
            name="sentiment-processed",
            num_partitions=2,
            replication_factor=1,
            topic_configs={
                "retention.ms": "86400000",  # 24 hours
                "compression.type": "snappy",
                "cleanup.policy": "delete",
            },
        ),
        # ===== DECISION TOPICS (final output) =====
        NewTopic(
            name="ad-placement-decisions",
            num_partitions=1,  # Single partition for ordered decisions
            replication_factor=1,
            topic_configs={
                "retention.ms": "604800000",  # 7 days (keep decisions longer)
                "compression.type": "snappy",
                "cleanup.policy": "delete",
            },
        ),
        # ===== MONITORING TOPICS =====
        NewTopic(
            name="data-quality-alerts",
            num_partitions=1,
            replication_factor=1,
            topic_configs={
                "retention.ms": "604800000",  # 7 days
                "compression.type": "snappy",
                "cleanup.policy": "delete",
            },
        ),
    ]

    # Create topics
    print("\n" + "=" * 70)
    print("Creating Kafka Topics...")
    print("=" * 70)

    created_count = 0
    already_exists_count = 0
    failed_count = 0

    for topic in topics:
        try:
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"✓ Created topic: {topic.name}")
            print(f"  Partitions: {topic.num_partitions}")
            print(
                f"  Retention: {int(topic.topic_configs['retention.ms']) / 3600000:.0f} hours"
            )
            print(f"  Compression: {topic.topic_configs['compression.type']}")
            created_count += 1
        except TopicAlreadyExistsError:
            print(f"⚠️  Topic already exists: {topic.name}")
            already_exists_count += 1
        except KafkaError as e:
            print(f"✗ Failed to create topic {topic.name}: {e}")
            failed_count += 1
        print()

    # List all topics
    print("=" * 70)
    print("Current Kafka Topics:")
    print("=" * 70)

    try:
        all_topics = admin_client.list_topics()
        for topic_name in sorted(all_topics):
            if not topic_name.startswith("__"):  # Skip internal topics
                print(f"  • {topic_name}")
    except Exception as e:
        print(f"✗ Failed to list topics: {e}")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Created: {created_count} topics")
    print(f"Already existed: {already_exists_count} topics")
    print(f"Failed: {failed_count} topics")

    # Close connection
    admin_client.close()
    print("\n" + "=" * 70)
    print("✓ Kafka topics setup complete!")
    print("=" * 70)
    print()


def main():
    """Main entry point"""
    try:
        create_kafka_topics()
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
