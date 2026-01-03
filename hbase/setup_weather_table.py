"""
HBase Table Setup for Open-Meteo Weather Forecast API

This script creates the HBase table structure for storing Warsaw weather forecast data
that can directly affect human mood.

Data source: api.open-meteo.com/v1/forecast
Location: Warsaw (52.2297°N, 21.0122°E)

Row Key format: {date}_{hour}
  Example: 20260101_14 (January 1, 2026, 14:00)

This format allows:
  - Efficient time-based queries
  - Hourly granularity
  - Natural chronological sorting

Selected Metrics (Mood-Affecting):
  - Temperature & Apparent Temperature (comfort)
  - Precipitation (rain, snow) - outdoor activities
  - Weather Code - overall conditions
  - Cloud Cover - sunshine vs gloom
  - Wind Speed & Gusts - discomfort
  - Relative Humidity - comfort level
"""

import happybase
import sys
from datetime import datetime


def create_weather_table():
    """
    Create HBase table for weather forecast data with mood-affecting metrics.

    Column Families:
      - temperature: Temperature (2m), Apparent Temperature
      - precipitation: Probability, Total Precipitation, Rain, Snowfall
      - atmospheric: Weather Code, Cloud Cover Total, Relative Humidity
      - wind: Wind Speed (10m), Wind Gusts (10m), Wind Direction (10m)
      - metadata: Forecast timestamp, location
    """

    # Connect to HBase
    print("=" * 70)
    print("HBase Weather Table Setup - Open-Meteo Forecast API")
    print("=" * 70)
    print("\nConnecting to HBase Thrift server...")

    try:
        connection = happybase.Connection(
            host="localhost",
            port=9090,
            timeout=30000,  # 30 seconds timeout
        )
        print("✓ Connected to HBase successfully")

    except Exception as e:
        print(f"✗ Failed to connect to HBase: {e}")
        print("\nTroubleshooting:")
        print("  1. Check if HBase is running: docker ps | grep hbase")
        print("  2. Verify port 9090 is exposed in docker-compose.yml")
        print("  3. Wait a few more seconds for HBase to fully start")
        sys.exit(1)

    # Table configuration
    table_name = "weather_forecast"

    # Check if table already exists
    print(f"\nChecking if table '{table_name}' exists...")
    existing_tables = [t.decode("utf-8") for t in connection.tables()]

    if table_name in existing_tables:
        print(f"⚠️  Table '{table_name}' already exists")
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
        "temperature": dict(max_versions=1, compression="GZ", bloom_filter_type="ROW"),
        "precipitation": dict(
            max_versions=1, compression="GZ", bloom_filter_type="ROW"
        ),
        "atmospheric": dict(max_versions=1, compression="GZ", bloom_filter_type="ROW"),
        "wind": dict(max_versions=1, compression="GZ", bloom_filter_type="ROW"),
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

    print(f"\n  1. temperature: (Comfort Level)")
    print(f"     - temp_2m: Temperature at 2m height (°C)")
    print(f"     - apparent_temp: Apparent/feels-like temperature (°C)")

    print(f"\n  2. precipitation: (Outdoor Activity Impact)")
    print(f"     - precip_probability: Probability of precipitation (0-100%)")
    print(f"     - precip_total: Total precipitation (mm)")
    print(f"     - rain: Rain amount (mm)")
    print(f"     - snowfall: Snowfall amount (cm)")
    print(f"     - snow_depth: Snow depth (cm)")

    print(f"\n  3. atmospheric: (General Conditions)")
    print(f"     - weather_code: WMO weather code (0-99)")
    print(f"     - cloud_cover: Total cloud cover (0-100%)")
    print(f"     - relative_humidity: Relative humidity (0-100%)")

    print(f"\n  4. wind: (Discomfort Factors)")
    print(f"     - wind_speed: Wind speed at 10m (km/h)")
    print(f"     - wind_gusts: Wind gusts at 10m (km/h)")

    print(f"\n  5. metadata: (Tracking Info)")
    print(f"     - forecast_time: When forecast was generated")
    print(f"     - ingestion_timestamp: When data was ingested")

    # Insert sample record to test
    print("\n" + "=" * 70)
    print("TESTING TABLE WITH SAMPLE DATA")
    print("=" * 70)

    sample_key = f"{datetime.now().strftime('%Y%m%d_%H')}_TEST"
    sample_data = {
        b"temperature:temp_2m": b"15.5",
        b"temperature:apparent_temp": b"13.2",
        b"precipitation:precip_probability": b"80",
        b"precipitation:precip_total": b"2.5",
        b"precipitation:rain": b"2.5",
        b"precipitation:snowfall": b"0.0",
        b"precipitation:snow_depth": b"0.0",
        b"atmospheric:weather_code": b"61",
        b"atmospheric:cloud_cover": b"85",
        b"atmospheric:relative_humidity": b"75",
        b"wind:wind_speed": b"18.5",
        b"wind:wind_gusts": b"35.0",
        b"wind:wind_direction": b"270",
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
    print("✓ Setup complete! Table is ready for weather data ingestion.")
    print("=" * 70)
    print()


def main():
    """Main entry point"""
    try:
        create_weather_table()
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
