"""Kafka Setup Module - Topic Creation Orchestration"""

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent


def is_running_in_docker():
    """Check if script is running inside Docker container."""
    return os.path.exists("/.dockerenv") or os.environ.get("DOCKER_CONTAINER") == "true"


def run_kafka_setup():
    """Run Kafka topics setup."""
    print(f"\n{'=' * 70}")
    print("Creating Kafka Topics")
    print(f"{'=' * 70}")

    try:
        from setup_kafka_topics import create_kafka_topics

        create_kafka_topics()
        print("✓ Kafka topics created successfully")
        return True
    except Exception as e:
        print(f"✗ Kafka setup failed: {e}")
        return False


def check_kafka():
    """Check if Kafka is ready."""
    from kafka.admin import KafkaAdminClient

    try:
        bootstrap_server = "kafka:29092" if is_running_in_docker() else "localhost:9092"
        client = KafkaAdminClient(
            bootstrap_servers=bootstrap_server,
            client_id="setup_check",
            request_timeout_ms=5000,
        )
        client.close()
        return True
    except Exception:
        return False
