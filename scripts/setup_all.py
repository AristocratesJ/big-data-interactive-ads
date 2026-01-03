"""
Unified Setup Script - Big Data Interactive Ads

Waits for Docker services to be ready, then automatically:
1. Creates Kafka topics
2. Creates all HBase tables
3. Uploads NiFi template (optional)

Usage:
    python scripts/setup_all.py
    python scripts/setup_all.py --skip-nifi  # Skip NiFi template upload
"""

import sys
import time
import os
from pathlib import Path

# Add project paths
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "kafka"))
sys.path.insert(0, str(REPO_ROOT / "hbase"))


def is_running_in_docker():
    """Check if script is running inside Docker container."""
    return os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER') == 'true'


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
        except Exception as e:
            pass
        
        print(f"  Attempt {attempt}/{max_retries} - {name} not ready yet, waiting {delay}s...")
        time.sleep(delay)
    
    print(f"✗ {name} failed to become ready after {max_retries * delay}s")
    return False


def check_kafka():
    """Check if Kafka is ready."""
    from kafka.admin import KafkaAdminClient
    try:
        # When running in Docker, use service name 'kafka' with internal port
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


def check_hbase():
    """Check if HBase Thrift server is ready."""
    import happybase
    try:
        # When running in Docker, use service name 'hbase'
        host = "hbase" if is_running_in_docker() else "localhost"
        connection = happybase.Connection(host, port=9090, timeout=5000)
        connection.close()
        return True
    except Exception:
        return False


def check_nifi():
    """Check if NiFi is ready."""
    import requests
    try:
        # When running in Docker, use service name 'nifi'
        host = "nifi" if is_running_in_docker() else "localhost"
        # NiFi uses self-signed cert, so we disable SSL verification
        response = requests.get(
            f"https://{host}:8443/nifi-api/system-diagnostics",
            verify=False,
            timeout=5,
        )
        return response.status_code == 200
    except Exception:
        return False


def run_kafka_setup():
    """Run Kafka topics setup."""
    print(f"\n{'='*70}")
    print("STEP 1: Creating Kafka Topics")
    print(f"{'='*70}")
    
    try:
        from setup_kafka_topics import create_kafka_topics
        create_kafka_topics()
        print("✓ Kafka topics created successfully")
        return True
    except Exception as e:
        print(f"✗ Kafka setup failed: {e}")
        return False


def run_hbase_setup():
    """Run all HBase table creation scripts."""
    print(f"\n{'='*70}")
    print("STEP 2: Creating HBase Tables")
    print(f"{'='*70}")
    
    # Set environment variable for automated mode (skips prompts)
    os.environ['AUTO_SETUP'] = 'true'
    
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
    return success_count == len(tables)


def upload_nifi_template():
    """Upload NiFi template via REST API."""
    print(f"\n{'='*70}")
    print("STEP 3: Uploading NiFi Template")
    print(f"{'='*70}")
    
    import requests
    import urllib3
    
    # Disable SSL warnings for self-signed cert
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    template_path = REPO_ROOT / "nifi" / "nifi_template_v1.xml"
    if not template_path.exists():
        print(f"✗ Template file not found: {template_path}")
        return False
    
    # Determine host based on environment
    host = "nifi" if is_running_in_docker() else "localhost"
    base_url = f"https://{host}:8443/nifi-api"
    
    # NiFi validates Host header - always use localhost to bypass security check
    request_headers = {"Host": "localhost:8443"}
    
    try:
        # Step 1: Get access token
        print("  Authenticating with NiFi...")
        auth_response = requests.post(
            f"{base_url}/access/token",
            data={"username": "admin", "password": "adminadmin123"},
            headers=request_headers,
            verify=False,
            timeout=10,
        )
        
        if auth_response.status_code != 201:
            print(f"✗ Authentication failed: {auth_response.status_code}")
            print(f"  Response: {auth_response.text[:200]}")
            return False
        
        token = auth_response.text
        headers = {
            "Authorization": f"Bearer {token}",
            "Host": "localhost:8443"  # Keep Host header for all requests
        }
        print("  ✓ Authenticated successfully")
        
        # Step 2: Get root process group ID
        response = requests.get(
            f"{base_url}/flow/process-groups/root",
            headers=headers,
            verify=False,
            timeout=10,
        )
        response.raise_for_status()
        root_id = response.json()["processGroupFlow"]["id"]
        print(f"  Found root process group ID: {root_id}")
        
        # Step 2.5: Check if template already exists and delete it
        print("  Checking for existing templates...")
        try:
            templates_response = requests.get(
                f"{base_url}/flow/templates",
                headers=headers,
                verify=False,
                timeout=10,
            )
            
            if templates_response.status_code == 200:
                existing_templates = templates_response.json().get("templates", [])
                for tmpl in existing_templates:
                    tmpl_name = tmpl["template"]["name"]
                    if "nifi_template" in tmpl_name.lower() or tmpl_name == "nifi_template_v1":
                        tmpl_id = tmpl["id"]
                        print(f"  Found existing template '{tmpl_name}', deleting...")
                        delete_response = requests.delete(
                            f"{base_url}/templates/{tmpl_id}",
                            headers=headers,
                            verify=False,
                            timeout=10,
                        )
                        if delete_response.status_code in (200, 204):
                            print(f"  ✓ Deleted old template")
        except Exception as e:
            print(f"  Warning: Could not check existing templates: {e}")
        
        # Step 2.6: Check if any processors already exist on canvas and delete them
        print("  Checking for existing processors...")
        try:
            processors_response = requests.get(
                f"{base_url}/process-groups/{root_id}/processors",
                headers=headers,
                verify=False,
                timeout=10,
            )
            
            if processors_response.status_code == 200:
                existing_processors = processors_response.json().get("processors", [])
                if existing_processors:
                    print(f"  Found {len(existing_processors)} existing processors, clearing canvas...")
                    for proc in existing_processors:
                        proc_id = proc["id"]
                        
                        # Get current revision
                        get_proc = requests.get(
                            f"{base_url}/processors/{proc_id}",
                            headers=headers,
                            verify=False,
                            timeout=10,
                        )
                        if get_proc.status_code != 200:
                            continue
                        
                        revision = get_proc.json()["revision"]["version"]
                        
                        # Stop processor first
                        requests.put(
                            f"{base_url}/processors/{proc_id}",
                            headers=headers,
                            json={
                                "revision": {"version": revision},
                                "component": {"id": proc_id, "state": "STOPPED"},
                            },
                            verify=False,
                            timeout=10,
                        )
                        
                        # Wait a bit for stop to complete
                        time.sleep(0.5)
                        
                        # Get updated revision after stop
                        get_proc2 = requests.get(
                            f"{base_url}/processors/{proc_id}",
                            headers=headers,
                            verify=False,
                            timeout=10,
                        )
                        if get_proc2.status_code == 200:
                            revision = get_proc2.json()["revision"]["version"]
                        
                        # Delete processor
                        requests.delete(
                            f"{base_url}/processors/{proc_id}?version={revision}",
                            headers=headers,
                            verify=False,
                            timeout=10,
                        )
                    print(f"  ✓ Canvas cleared")
        except Exception as e:
            print(f"  Warning: Could not clear canvas: {e}")
        
        # Step 3: Upload template
        print("  Uploading template...")
        with open(template_path, "rb") as f:
            files = {"template": ("nifi_template_v1.xml", f, "application/xml")}
            response = requests.post(
                f"{base_url}/process-groups/{root_id}/templates/upload",
                headers=headers,
                files=files,
                verify=False,
                timeout=30,
            )
        
        if response.status_code not in (200, 201):
            print(f"✗ Template upload failed: {response.status_code}")
            print(f"  Response: {response.text[:500]}")
            return False
        
        try:
            # NiFi returns XML response, not JSON
            import xml.etree.ElementTree as ET
            root_xml = ET.fromstring(response.text)
            template_id = root_xml.find(".//id").text
            print(f"✓ Template uploaded successfully (ID: {template_id})")
        except Exception as e:
            print(f"✗ Failed to parse upload response: {e}")
            print(f"  Response content: {response.text[:500]}")
            return False
        
        # Step 4: Instantiate template on canvas
        print("  Instantiating template on canvas...")
        instantiate_response = requests.post(
            f"{base_url}/process-groups/{root_id}/template-instance",
            headers=headers,
            json={
                "originX": 100.0,
                "originY": 100.0,
                "templateId": template_id,
            },
            verify=False,
            timeout=30,
        )
        
        if instantiate_response.status_code not in (200, 201):
            print(f"✗ Template instantiation failed: {instantiate_response.status_code}")
            print(f"  Response: {instantiate_response.text[:200]}")
            return False
        
        flow = instantiate_response.json()["flow"]
        print("✓ Template instantiated on canvas")
        
        print(f"\n✓ NiFi template setup complete!")
        print(f"  View at: https://{host}:8443/nifi (admin/adminadmin123)")
        print(f"  To start data ingestion: ./scripts/run_nifi_flows.ps1")
        return True
            
    except Exception as e:
        print(f"✗ NiFi template upload failed: {e}")
        print("\n  You can manually import the template:")
        print("  1. Open https://localhost:8443/nifi")
        print("  2. Click menu (☰) → Upload Template")
        print(f"  3. Select: {template_path}")
        return False


def main():
    """Main setup orchestration."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Unified setup for all services")
    parser.add_argument(
        "--skip-nifi",
        action="store_true",
        help="Skip NiFi template upload (manual import only)",
    )
    args = parser.parse_args()
    
    print("=" * 70)
    print("BIG DATA INTERACTIVE ADS - UNIFIED SETUP")
    print("=" * 70)
    print("\nThis script will automatically set up:")
    print("  ✓ Kafka topics")
    print("  ✓ HBase tables")
    if not args.skip_nifi:
        print("  ✓ NiFi template upload")
    print("\nMake sure Docker services are starting (docker-compose up -d)")
    
    # Wait for all services
    services_ready = True
    services_ready &= wait_for_service("Kafka", check_kafka, max_retries=30, delay=2)
    services_ready &= wait_for_service("HBase", check_hbase, max_retries=30, delay=2)
    
    if not args.skip_nifi:
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
    nifi_ok = True
    
    if not args.skip_nifi:
        nifi_ok = upload_nifi_template()
    
    # Final summary
    print("\n" + "=" * 70)
    print("SETUP SUMMARY")
    print("=" * 70)
    print(f"  Kafka Topics: {'✓ SUCCESS' if kafka_ok else '✗ FAILED'}")
    print(f"  HBase Tables: {'✓ SUCCESS' if hbase_ok else '✗ FAILED'}")
    if not args.skip_nifi:
        print(f"  NiFi Template: {'✓ SUCCESS' if nifi_ok else '✗ FAILED'}")
    
    if kafka_ok and hbase_ok and (nifi_ok or args.skip_nifi):
        print("\n✓ SETUP COMPLETED SUCCESSFULLY!")
        print("\nNext steps:")
        print("  1. Start NiFi flows: ./scripts/run_nifi_flows.ps1")
        print("  2. Start Spark jobs: ./scripts/run_spark_jobs.ps1")
        print("  3. Monitor:")
        print("     - NiFi: https://localhost:8443/nifi (admin/adminadmin123)")
        print("     - Kafka: http://localhost:8090")
        print("     - Spark: http://localhost:8080")
        print("     - HBase: http://localhost:16010")
        sys.exit(0)
    else:
        print("\n✗ SETUP COMPLETED WITH ERRORS")
        print("  Check error messages above and retry failed steps manually")
        sys.exit(1)


if __name__ == "__main__":
    main()
