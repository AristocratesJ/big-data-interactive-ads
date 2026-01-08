"""HBase Setup Module - Table Creation Orchestration"""

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent


def is_running_in_docker():
    """Check if script is running inside Docker container."""
    return os.path.exists("/.dockerenv") or os.environ.get("DOCKER_CONTAINER") == "true"


def run_hbase_setup():
    """Run all HBase table creation scripts."""
    print(f"\n{'=' * 70}")
    print("Creating HBase Tables")
    print(f"{'=' * 70}")

    # Set environment variable for automated mode (skips prompts)
    os.environ["AUTO_SETUP"] = "true"

    tables = [
        ("Air Quality", "setup_air_quality_table", "create_air_quality_table"),
        ("Weather", "setup_weather_table", "create_weather_table"),
        ("Transport", "setup_transport_table", "create_transport_table"),
        ("Sentiment", "setup_sentiment_table", "create_sentiment_table"),
    ]

    success_count = 0
    for table_name, module_name, func_name in tables:
        print(f"\nCreating {table_name} table...")
        try:
            module = __import__(module_name)
            create_func = getattr(module, func_name)
            create_func()
            print(f"✓ {table_name} table created successfully")
            success_count += 1
        except Exception as e:
            print(f"✗ {table_name} table creation failed: {e}")

    print(f"\n{success_count}/{len(tables)} HBase tables created successfully")

    # Inline creation of ad_decisions table
    try:
        import happybase

        print("\nCreating ad_decisions table...")
        host = "hbase" if is_running_in_docker() else "localhost"
        connection = happybase.Connection(host, port=9090)
        if b"ad_decisions" not in connection.tables():
            connection.create_table(
                "ad_decisions",
                {"metrics": dict(), "decision": dict(), "timestamps": dict(), "target": dict()},
            )
            print("✓ ad_decisions table created successfully")
        else:
            print("⚠️  ad_decisions table already exists")
        connection.close()
    except Exception as e:
        print(f"✗ ad_decisions table creation failed: {e}")

    return success_count == len(tables)


def check_hbase():
    """Check if HBase Thrift server is ready."""
    import happybase

    try:
        host = "hbase" if is_running_in_docker() else "localhost"
        connection = happybase.Connection(host, port=9090, timeout=5000)
        connection.close()
        return True
    except Exception:
        return False
