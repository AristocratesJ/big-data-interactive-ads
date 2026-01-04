#!/bin/bash

# Configuration
NIFI_API="https://localhost:8443/nifi-api"
USERNAME="admin"
PASSWORD="adminadmin123"

echo "Starting NiFi data ingestion flows..."

# Check for jq
if ! command -v jq &> /dev/null; then
    echo "Error: jq is not installed. Please install it (e.g., brew install jq or apt-get install jq)"
    exit 1
fi

# Step 1: Authenticate
echo -e "\nAuthenticating with NiFi..."
TOKEN=$(curl -k -s -X POST "$NIFI_API/access/token" \
    -d "username=$USERNAME&password=$PASSWORD")

if [[ $TOKEN == *"error"* ]] || [[ -z "$TOKEN" ]]; then
    echo "Authentication failed. Check credentials or NiFi status."
    exit 1
fi

echo "Authenticated successfully"

# Step 2: Get root process group
echo -e "\nGetting process group..."
ROOT_ID=$(curl -k -s -H "Authorization: Bearer $TOKEN" "$NIFI_API/flow/process-groups/root" | jq -r '.processGroupFlow.id')

if [[ $ROOT_ID == "null" ]]; then
    echo "Failed to get process group."
    exit 1
fi

echo "Found root process group: $ROOT_ID"

# Step 3: Get all processors
echo -e "\nGetting processors..."
PROCESSORS_JSON=$(curl -k -s -H "Authorization: Bearer $TOKEN" "$NIFI_API/process-groups/$ROOT_ID/processors")
COUNT=$(echo "$PROCESSORS_JSON" | jq '.processors | length')

echo "Found $COUNT processors"

# Step 4: Start all processors
echo -e "\nStarting processors..."
SUCCESS_COUNT=0

# Iterate over each processor using jq to extract ID, Name, and Version
for row in $(echo "$PROCESSORS_JSON" | jq -r '.processors[] | @base64'); do
    _jq() {
     echo ${row} | base64 --decode | jq -r ${1}
    }

    PROC_ID=$(_jq '.id')
    PROC_NAME=$(_jq '.component.name')
    REVISION=$(_jq '.revision.version')

    # Construct JSON payload
    BODY=$(jq -n \
                  --arg id "$PROC_ID" \
                  --arg version "$REVISION" \
                  '{revision: {version: $version}, component: {id: $id, state: "RUNNING"}}')

    # Start processor
    RESPONSE=$(curl -k -s -o /dev/null -w "%{http_code}" -X PUT "$NIFI_API/processors/$PROC_ID" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d "$BODY")

    if [ "$RESPONSE" -eq 200 ]; then
        echo "   Started: $PROC_NAME"
        ((SUCCESS_COUNT++))
    else
        echo "   Failed to start $PROC_NAME (HTTP $RESPONSE)"
    fi
done

echo -e "\n$SUCCESS_COUNT/$COUNT processors started successfully"
echo -e "\nMonitor flows at:"
echo "   - NiFi UI: https://localhost:8443/nifi"
echo "   - Kafka UI: http://localhost:8090"
echo -e "\nTo stop flows: ./scripts/stop_nifi_flows.sh"
