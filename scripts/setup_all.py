"""
Unified Setup Script - Big Data Interactive Ads

Waits for Docker services to be ready, then automatically:
1. Creates Kafka topics
2. Creates all HBase tables
3. Sets NiFi variables from .env
4. Uploads NiFi template

Usage:
    python scripts/setup_all.py
    python scripts/setup_all.py 
"""

import sys
import time
from pathlib import Path

# Add project paths
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "kafka"))
sys.path.insert(0, str(REPO_ROOT / "hbase"))
sys.path.insert(0, str(REPO_ROOT / "nifi"))

from setup_kafka import run_kafka_setup, check_kafka  # noqa: E402
from setup_hbase import run_hbase_setup, check_hbase  # noqa: E402
from setup_nifi import set_nifi_variables, upload_nifi_template, check_nifi  # noqa: E402

def wait_for_service(name: str, check_func, max_retries: int = 30, delay: int = 2):
    """Wait for a service to become ready."""
    print(f"\n{'='*70}")
    print(f"Waiting for {name} to be ready...")
    print(f"{'='*70}")
    
    for attempt in range(1, max_retries + 1):
        try:
            if check_func():
                print(f"✓ {name} is ready!")
                return True
        except Exception:
            pass
        
        print(f"  Attempt {attempt}/{max_retries} - {name} not ready yet, waiting {delay}s...")
        time.sleep(delay)
    
    print(f"✗ {name} failed to become ready after {max_retries * delay}s")
    return False


def main():    
    print("=" * 70)
    print("BIG DATA INTERACTIVE ADS - UNIFIED SETUP")
    print("=" * 70)
    print("\nThis script will automatically set up:")
    print("  ✓ Kafka topics")
    print("  ✓ HBase tables")
    print("  ✓ NiFi variables")
    print("  ✓ NiFi template upload")
    print("\nMake sure Docker services are starting (docker-compose up -d)")
    
    # Wait for all services
    services_ready = True
    services_ready &= wait_for_service("Kafka", check_kafka, max_retries=30, delay=2)
    services_ready &= wait_for_service("HBase", check_hbase, max_retries=30, delay=2)
    services_ready &= wait_for_service("NiFi", check_nifi, max_retries=60, delay=3)
    
    if not services_ready:
        print("\n" + "=" * 70)
        print("✗ SETUP FAILED - Some services are not ready")
        print("=" * 70)
        print("\nTroubleshooting:")
        print("  1. Check Docker containers: docker ps")
        print("  2. Check logs: docker-compose logs -f")
        print("  3. Try: docker-compose down && docker-compose up -d")
        sys.exit(1)
    
    # Run setup steps
    print("\n" + "=" * 70)
    print("All services ready! Starting setup...")
    print("=" * 70)
    
    kafka_ok = run_kafka_setup()
    hbase_ok = run_hbase_setup()
    variables_ok = True
    nifi_ok = True
    variables_ok = set_nifi_variables()
    nifi_ok = upload_nifi_template()
    
    # Final summary
    print("\n" + "=" * 70)
    print("SETUP SUMMARY")
    print("=" * 70)
    print(f"  Kafka Topics: {'✓ SUCCESS' if kafka_ok else '✗ FAILED'}")
    print(f"  HBase Tables: {'✓ SUCCESS' if hbase_ok else '✗ FAILED'}")
    print(f"  NiFi Variables: {'✓ SUCCESS' if variables_ok else '✗ FAILED'}")
    print(f"  NiFi Template: {'✓ SUCCESS' if nifi_ok else '✗ FAILED'}")
    
    if kafka_ok and hbase_ok and nifi_ok:
        print("\n✓ SETUP COMPLETED SUCCESSFULLY!")
        print("\nNext steps:")
        print("  1. Start NiFi flows: ./scripts/run_nifi_flows.ps1")
        print("  2. Start Spark jobs: ./scripts/run_spark_jobs.ps1")
        print("  3. Monitor:")
        print("     - NiFi: https://localhost:8443/nifi (admin/adminadmin123)")
        print("     - Kafka: http://localhost:8090")
        print("     - Spark: http://localhost:8080")
        print("     - HBase: http://localhost:16010")
        
        if variables_ok:
            print("\n📝 NiFi Variables Set:")
            print("   - TWITTER_API_KEY: Available as ${TWITTER_API_KEY}")
            print("\n   Use in processors: ${TWITTER_API_KEY}")
        
        sys.exit(0)
    else:
        print("\n✗ SETUP COMPLETED WITH ERRORS")
        print("  Check error messages above and retry failed steps manually")
        sys.exit(1)


if __name__ == "__main__":
    main()
