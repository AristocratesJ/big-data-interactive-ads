"""
HBase Table Setup for Open-Meteo Air Quality Forecast API

This script creates the HBase table structure for storing Warsaw air quality forecast data
that can directly affect human health and mood.

Data source: air-quality-api.open-meteo.com/v1/air-quality
Location: Warsaw (52.2297°N, 21.0122°E)

Row Key format: {date}_{hour}
  Example: 20260101_14 (January 1, 2026, 14:00)

IMPORTANT: Same row key format as weather_forecast table for easy joining in Spark!

This format allows:
  - Efficient time-based queries
  - Hourly granularity
  - Natural joins with weather_forecast table
  - Chronological sorting

Selected Metrics (Mood/Health-Affecting):
  - Particulate Matter (PM2.5, PM10) - breathing, energy levels
  - Ozone (O3) - respiratory issues
  - UV Index - outdoor comfort, sunburn
  - Pollen levels - allergies (major mood impact)
  - NO2, SO2 - respiratory irritation, odor
"""

import happybase
import sys
import os
from datetime import datetime


def create_air_quality_table():
    """
    Create HBase table for air quality forecast data with health/mood-affecting metrics.

    Column Families:
      - particulates: PM10, PM2.5 (breathing quality)
      - gases: O3, NO2, SO2, NH3 (respiratory irritation)
      - uv: UV Index, UV Index Clear Sky (outdoor safety)
      - pollen: Birch, Grass, Ragweed, Alder, Mugwort (allergies)
      - metadata: API source, forecast timestamp, ingestion time
    """

    # Connect to HBase
    print("=" * 70)
    print("HBase Air Quality Table Setup - Open-Meteo Air Quality API")
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
    table_name = "air_quality_forecast"

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

    families = {
        "particulates": dict(max_versions=1, compression="GZ", bloom_filter_type="ROW"),
        "gases": dict(max_versions=1, compression="GZ", bloom_filter_type="ROW"),
        "uv": dict(max_versions=1, compression="GZ", bloom_filter_type="ROW"),
        "pollen": dict(max_versions=1, compression="GZ", bloom_filter_type="ROW"),
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
    print(f"Location: Warsaw (52.2297°N, 21.0122°E)")
    print(f"Row Key Format: YYYYMMDD_HH (e.g., 20260101_14)")
    print(f"\nColumn Families:")

    print(f"\n  1. particulates: (Breathing Quality)")
    print(f"     - pm10: Particulate Matter 10μm (μg/m³)")
    print(f"     - pm2_5: Particulate Matter 2.5μm (μg/m³)")

    print(f"\n  2. gases: (Respiratory Health)")
    print(f"     - ozone: Ozone O3 (μg/m³)")
    print(f"     - nitrogen_dioxide: NO2 (μg/m³)")
    print(f"     - sulphur_dioxide: SO2 (μg/m³)")
    print(f"     - ammonia: NH3 (μg/m³)")

    print(f"\n  3. uv: (Sun Exposure Risk)")
    print(f"     - uv_index: UV Index (0-11+)")
    print(f"     - uv_index_clear_sky: UV Index if clear (0-11+)")

    print(f"\n  4. pollen: (Allergy Impact)")
    print(f"     - birch: Birch pollen (grains/m³)")
    print(f"     - grass: Grass pollen (grains/m³)")
    print(f"     - ragweed: Ragweed pollen (grains/m³)")
    print(f"     - alder: Alder pollen (grains/m³)")
    print(f"     - mugwort: Mugwort pollen (grains/m³)")

    print(f"\n  5. metadata: (Tracking Info)")
    print(f"     - forecast_time: When forecast was generated")
    print(f"     - ingestion_timestamp: When data was ingested")

    # Insert sample record to test
    print("\n" + "=" * 70)
    print("TESTING TABLE WITH SAMPLE DATA")
    print("=" * 70)

    sample_key = f"{datetime.now().strftime('%Y%m%d_%H')}_TEST"
    sample_data = {
        b"particulates:pm10": b"35.2",
        b"particulates:pm2_5": b"18.5",
        b"gases:ozone": b"85.3",
        b"gases:nitrogen_dioxide": b"42.1",
        b"gases:sulphur_dioxide": b"12.5",
        b"gases:ammonia": b"5.2",
        b"uv:uv_index": b"5.2",
        b"uv:uv_index_clear_sky": b"6.8",
        b"pollen:birch": b"25.0",
        b"pollen:grass": b"150.0",
        b"pollen:ragweed": b"8.0",
        b"pollen:alder": b"12.0",
        b"pollen:mugwort": b"3.0",
        b"metadata:forecast_time": b"2026-01-01T12:00:00",
        b"metadata:ingestion_timestamp": datetime.now().isoformat().encode(),
    }

    print(f"\nInserting test record with key: {sample_key}")
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
    print("✓ Setup complete! Table is ready for air quality data ingestion.")
    print("=" * 70)


def main():
    """Main entry point"""
    try:
        create_air_quality_table()
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
