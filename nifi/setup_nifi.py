"""NiFi Setup Module - Variable Configuration and Template Upload"""

import os
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def is_running_in_docker():
    """Check if script is running inside Docker container."""
    return os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER') == 'true'


def set_nifi_variables():
    """Set NiFi variables from .env file via REST API."""
    print(f"\n{'='*70}")
    print("Setting NiFi Variables from .env")
    print(f"{'='*70}")
    
    import requests
    import urllib3
    
    # Disable SSL warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Load environment variables (try dotenv first, fallback to os.environ)
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=REPO_ROOT / ".env")
    except ImportError:
        # dotenv not available (e.g., in Docker), env vars should be set directly
        pass
    
    twitter_api_key = os.getenv("TWITTER_API_KEY")
    
    if not twitter_api_key:
        print("⚠️  TWITTER_API_KEY not found in .env file")
        print("   Skipping variable setup (you'll need to set it manually in NiFi)")
        return True  # Not a fatal error
    
    # Determine host
    host = "nifi" if is_running_in_docker() else "localhost"
    base_url = f"https://{host}:8443/nifi-api"
    request_headers = {"Host": "localhost:8443"}
    
    try:
        # Authenticate
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
            return False
        
        token = auth_response.text
        headers = {
            "Authorization": f"Bearer {token}",
            "Host": "localhost:8443"
        }
        print("  ✓ Authenticated successfully")
        
        # Get root process group ID
        response = requests.get(
            f"{base_url}/flow/process-groups/root",
            headers=headers,
            verify=False,
            timeout=10,
        )
        response.raise_for_status()
        root_id = response.json()["processGroupFlow"]["id"]
        print(f"  Found root process group: {root_id}")
        
        # Get current variable registry
        var_response = requests.get(
            f"{base_url}/process-groups/{root_id}/variable-registry",
            headers=headers,
            verify=False,
            timeout=10,
        )
        var_response.raise_for_status()
        var_data = var_response.json()
        current_version = var_data["processGroupRevision"]["version"]
        
        # Prepare variables payload
        variables_to_set = [
            {
                "variable": {
                    "name": "TWITTER_API_KEY",
                    "value": twitter_api_key
                }
            }
        ]
        
        payload = {
            "processGroupRevision": {
                "version": current_version
            },
            "variableRegistry": {
                "processGroupId": root_id,
                "variables": variables_to_set
            }
        }
        
        # Set variables
        print("  Setting variables:")
        print(f"    - TWITTER_API_KEY: {'*' * 20}{twitter_api_key[-4:]}")
        
        update_response = requests.put(
            f"{base_url}/process-groups/{root_id}/variable-registry",
            json=payload,
            headers=headers,
            verify=False,
            timeout=10,
        )
        
        if update_response.status_code in (200, 201):
            print("  ✓ Variables set successfully!")
            return True
        else:
            print(f"✗ Failed to set variables: {update_response.status_code}")
            print(f"  Response: {update_response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"✗ Failed to set NiFi variables: {e}")
        print("\n  You can manually set variables in NiFi:")
        print("  1. Open https://localhost:8443/nifi")
        print("  2. Right-click canvas → Variables")
        print("  3. Add: TWITTER_API_KEY = <your key>")
        return False


def upload_nifi_template():
    """Upload NiFi template via REST API."""
    print(f"\n{'='*70}")
    print("Uploading NiFi Template")
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
                            print("  ✓ Deleted old template")
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
                    print("  ✓ Canvas cleared")
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
        
        print("✓ Template instantiated on canvas")
        
        print("\n✓ NiFi template setup complete!")
        print(f"  View at: https://{host}:8443/nifi (admin/adminadmin123)")
        print("  To start data ingestion: ./scripts/run_nifi_flows.ps1")
        return True
            
    except Exception as e:
        print(f"✗ NiFi template upload failed: {e}")
        print("\n  You can manually import the template:")
        print("  1. Open https://localhost:8443/nifi")
        print("  2. Click menu (☰) → Upload Template")
        print(f"  3. Select: {template_path}")
        return False


def check_nifi():
    """Check if NiFi is ready."""
    import requests
    try:
        host = "nifi" if is_running_in_docker() else "localhost"
        response = requests.get(
            f"https://{host}:8443/nifi-api/system-diagnostics",
            verify=False,
            timeout=5,
        )
        return response.status_code == 200
    except Exception:
        return False
