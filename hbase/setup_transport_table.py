"""
HBase Table Setup for ZTM Transport API (Buses & Trolleys)

This script creates the HBase table structure for storing Warsaw public transport data.
Uses HappyBase library to interact with HBase.

Table: transport_events
Data from: ZTM API (api.um.warszawa.pl)
  - Buses (type=1)
  - Trolleys (type=2)

Row Key format: {timestamp}_{vehicle_number}
  Example: 20260101_002028_1294

This format allows:
  - Efficient time-based scans
  - Unique identification per vehicle per timestamp
  - Natural sorting by time
"""

import happybase
import sys
import os
from datetime import datetime


def create_transport_table():
    """
    Create HBase table for transport events with appropriate column families.

    Column Families:
      - info: General vehicle information (Lines, VehicleNumber, Brigade, Time)
      - location: Geographic data (Lat, Lon)
      - metadata: System metadata (vehicle_type, ingestion_timestamp, data_freshness)
    """

    # Connect to HBase
    print("=" * 70)
    print("HBase Transport Table Setup - ZTM API")
    print("=" * 70)
    print("\nConnecting to HBase Thrift server...")

    # Detect if running in Docker
    is_docker = os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER') == 'true'
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
    table_name = "transport_events"

    # Check if table already exists
    print(f"\nChecking if table '{table_name}' exists...")
    existing_tables = [t.decode("utf-8") for t in connection.tables()]

    if table_name in existing_tables:
        print(f"⚠️  Table '{table_name}' already exists")
        
        # In automated mode (AUTO_SETUP=true), skip without recreating
        if os.environ.get('AUTO_SETUP') == 'true':
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

    # Column family definitions
    # Note: GZ compression is used instead of SNAPPY (not available in this HBase image)
    families = {
        "info": dict(max_versions=1, compression="GZ", bloom_filter_type="ROW"),
        "location": dict(max_versions=1, compression="GZ", bloom_filter_type="ROW"),
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
    print(f"\nColumn Families:")
    print(f"  1. info:")
    print(f"     - Lines (e.g., '26', '161', 'L31')")
    print(f"     - VehicleNumber (e.g., '1294', '10072')")
    print(f"     - Brigade (e.g., '015', '1')")
    print(f"     - Time (e.g., '2026-01-01 00:20:28')")
    print(f"\n  2. location:")
    print(f"     - Lat (e.g., 52.25588)")
    print(f"     - Lon (e.g., 21.055733)")
    print(f"\n  3. metadata:")
    print(f"     - vehicle_type ('bus' or 'trolley')")
    print(f"     - ingestion_timestamp (when data was ingested)")

    # Insert sample record to test
    print("\n" + "=" * 70)
    print("TESTING TABLE WITH SAMPLE DATA")
    print("=" * 70)

    sample_key = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_TEST"
    sample_data = {
        b"info:Lines": b"26",
        b"info:VehicleNumber": b"1294",
        b"info:Brigade": b"015",
        b"info:Time": b"2026-01-01 00:20:28",
        b"location:Lat": b"52.25588",
        b"location:Lon": b"21.055733",
        b"metadata:vehicle_type": b"trolley",
        b"metadata:ingestion_timestamp": datetime.now().isoformat().encode(),
        b"metadata:data_freshness": b"0",
    }

    print(f"\nInserting test record with key: {sample_key}")
    table.put(sample_key.encode(), sample_data)
    print("✓ Test record inserted")

    # Read back the test record
    print("\nReading test record back...")
    result = table.row(sample_key.encode())

    if result:
        print("✓ Test record retrieved successfully:")
        for key, value in result.items():
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
    print("✓ Setup complete! Table is ready for data ingestion.")
    print("=" * 70)
    print()


def main():
    """Main entry point"""
    try:
        create_transport_table()
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
