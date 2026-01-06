"""
Unified Setup Script - Big Data Interactive Ads

Waits for Docker services to be ready, then automatically:
1. Creates Kafka topics
2. Creates all HBase tables
3. Sets up HDFS directories with proper permissions
4. Sets NiFi variables from .env
5. Uploads NiFi template

Usage:
    python scripts/setup_all.py
    python scripts/setup_all.py 
"""

import sys
import time
import requests
from pathlib import Path

# Add project paths
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "kafka"))
sys.path.insert(0, str(REPO_ROOT / "hbase"))
sys.path.insert(0, str(REPO_ROOT / "nifi"))

from setup_kafka import run_kafka_setup, check_kafka  # noqa: E402
from setup_hbase import run_hbase_setup, check_hbase  # noqa: E402
from setup_nifi import set_nifi_variables, upload_nifi_template, check_nifi  # noqa: E402

# HDFS WebHDFS REST API endpoint (use container name when running inside Docker network)
HDFS_NAMENODE_HOST = "namenode"
HDFS_WEBHDFS_PORT = 9870


def check_hdfs():
    """Check if HDFS namenode is ready using WebHDFS REST API."""
    try:
        # Use WebHDFS REST API to check if HDFS is ready
        url = f"http://{HDFS_NAMENODE_HOST}:{HDFS_WEBHDFS_PORT}/webhdfs/v1/?op=LISTSTATUS"
        response = requests.get(url, timeout=5)
        return response.status_code == 200
    except Exception:
        return False


def setup_hdfs_directories():
    """Create HDFS directories with proper permissions for Spark user using WebHDFS REST API."""
    print("\n" + "-" * 50)
    print("Setting up HDFS directories...")
    print("-" * 50)
    
    base_url = f"http://{HDFS_NAMENODE_HOST}:{HDFS_WEBHDFS_PORT}/webhdfs/v1"
    
    directories = [
        "/user/archive",
        "/user/spark"
    ]
    
    try:
        for dir_path in directories:
            # Create directory using WebHDFS MKDIRS operation
            url = f"{base_url}{dir_path}?op=MKDIRS&permission=777&user.name=root"
            response = requests.put(url, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("boolean", False):
                    print(f"   ✓ Created {dir_path}")
                else:
                    # Directory might already exist
                    print(f"   - {dir_path} (already exists)")
            else:
                print(f"   ✗ Failed to create {dir_path}: {response.status_code}")
        
        # Set permissions using SETPERMISSION operation
        for dir_path in directories:
            url = f"{base_url}{dir_path}?op=SETPERMISSION&permission=777&user.name=root"
            response = requests.put(url, timeout=30)
            if response.status_code == 200:
                print(f"   ✓ Set 777 permissions on {dir_path}")
        
        print("\n✓ HDFS directories ready:")
        print("   - /user/archive (777)")
        print("   - /user/spark (777)")
        return True
        
    except Exception as e:
        print(f"✗ Error setting up HDFS: {e}")
        return False

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
    print("  ✓ HDFS directories")
    print("  ✓ NiFi variables")
    print("  ✓ NiFi template upload")
    print("\nMake sure Docker services are starting (docker-compose up -d)")
    
    # Wait for all services
    services_ready = True
    services_ready &= wait_for_service("Kafka", check_kafka, max_retries=30, delay=2)
    services_ready &= wait_for_service("HBase", check_hbase, max_retries=30, delay=2)
    services_ready &= wait_for_service("HDFS", check_hdfs, max_retries=30, delay=2)
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
    hdfs_ok = setup_hdfs_directories()
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
    print(f"  HDFS Dirs:    {'✓ SUCCESS' if hdfs_ok else '✗ FAILED'}")
    print(f"  NiFi Variables: {'✓ SUCCESS' if variables_ok else '✗ FAILED'}")
    print(f"  NiFi Template: {'✓ SUCCESS' if nifi_ok else '✗ FAILED'}")
    
    if kafka_ok and hbase_ok and hdfs_ok and nifi_ok:
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
