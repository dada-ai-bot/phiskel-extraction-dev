#!/bin/bash
set -e

echo "========================================="
echo "  Forge Extraction Service              "
echo "========================================="

# Decrypt certs if provided
if [ -n "$IOT_CERT_BASE64" ]; then
    mkdir -p /workspace/certs
    echo "$IOT_CERT_BASE64" | base64 -d > /workspace/certs/certificate.pem.crt
    echo "$IOT_PRIVATE_KEY_BASE64" | base64 -d > /workspace/certs/private.pem.key
    echo "$IOT_CA_BASE64" | base64 -d > /workspace/certs/AmazonRootCA1.pem
    export IOT_CERT_PATH="/workspace/certs/certificate.pem.crt"
    export IOT_PRIVATE_KEY_PATH="/workspace/certs/private.pem.key"
    export IOT_CA_PATH="/workspace/certs/AmazonRootCA1.pem"
fi

cd /workspace

# Set JOB_TYPE for clarity
export JOB_TYPE=extraction

# Check if we should run extraction immediately (NOSANA_MODE)
if [ "$NOSANA_MODE" = "true" ]; then
    echo "Running in Nosana mode - executing extraction pipeline..."
    
    if [ -n "$FORGE_PAYLOAD" ]; then
        echo "$FORGE_PAYLOAD" | base64 -d > /tmp/forge_payload.json
        echo "Decoded forge payload"
        
        # Extract env vars from payload
        eval $(python3 -c "
import json, os
with open('/tmp/forge_payload.json') as f:
    data = json.load(f)
p = data.get('payload', {})
print(f'export CHAR_NAME=\"{p.get(\"char_name\", \"\")}\"')
print(f'export EXTRACTION_MODEL=\"{p.get(\"extractionModel\", \"\")}\"')
print(f'export QUALITY_LEVEL=\"{p.get(\"qualityLevel\", \"0\")}\"')
print(f'export S3_BUCKET=\"{p.get(\"s3Bucket\", \"phiskel-extraction-data\")}\"')
print(f'export S3_PREFIX=\"{p.get(\"s3Prefix\", \"extraction-jobs\")}\"')
print(f'export AUTHENTICATED_USER_ID=\"{data.get(\"authenticatedUserId\", \"nosana-user\")}\"')
if p.get('vllmMaxModelLen'): print(f'export VLLM_MAX_MODEL_LEN=\"{p[\"vllmMaxModelLen\"]}\"')
if p.get('vllmGpuMemoryUtilization'): print(f'export VLLM_GPU_MEMORY_UTILIZATION=\"{p[\"vllmGpuMemoryUtilization\"]}\"')
if p.get('vllmMaxNumSeqs'): print(f'export VLLM_MAX_NUM_SEQS=\"{p[\"vllmMaxNumSeqs\"]}\"')
specs = p.get('specifications', {})
if specs.get('enableThinking'): print(f'export ENABLE_THINKING=\"{specs[\"enableThinking\"]}\"')
if specs.get('description'): print(f'export USER_DESCRIPTION=\"{specs[\"description\"]}\"')
if specs.get('values'): print(f'export USER_VALUES=\"{specs[\"values\"]}\"')
if specs.get('perspective'): print(f'export USER_PERSPECTIVE=\"{specs[\"perspective\"]}\"')
if specs.get('tasks'): print(f'export USER_TASKS=\"{specs[\"tasks\"]}\"')
")
    fi
    
    # Run extraction and capture output
    set +e  # Don't exit on error, we need to capture output
    OUTPUT=$(./run_extraction.sh 2>&1)
    EXIT_CODE=$?
    set -e
    
    echo "$OUTPUT"
    
    # Parse extraction job ID from output
    EXTRACTION_JOB_ID=$(echo "$OUTPUT" | grep "\[PIPELINE_RESULT_EXTRACTION_JOB_ID\]" | awk '{print $2}')
    
    if [ $EXIT_CODE -eq 0 ] && [ -n "$EXTRACTION_JOB_ID" ]; then
        echo "Extraction completed successfully! Job ID: $EXTRACTION_JOB_ID"
        
        # Callback to API to trigger training (API will publish to IoT)
        if [ -n "$API_CALLBACK_URL" ]; then
            echo "Notifying API of extraction completion..."
            python3 -c "
import json
import urllib.request
import os

url = '$API_CALLBACK_URL'
payload = {
    'userId': os.environ.get('AUTHENTICATED_USER_ID', 'unknown'),
    'characterName': os.environ.get('CHAR_NAME', 'Unknown'),
    'extractionJobId': '$EXTRACTION_JOB_ID',
    'qualityLevel': int(os.environ.get('QUALITY_LEVEL', '0')),
    's3Bucket': os.environ.get('S3_BUCKET', 'phiskel-extraction-data'),
    's3Prefix': os.environ.get('S3_PREFIX', 'extraction-jobs'),
    'status': 'success'
}

req = urllib.request.Request(
    url,
    data=json.dumps(payload).encode('utf-8'),
    headers={'Content-Type': 'application/json'},
    method='POST'
)
try:
    with urllib.request.urlopen(req, timeout=30) as response:
        print(f'API callback successful: {response.status}')
except Exception as e:
    print(f'API callback failed: {e}')
"
        else
            echo "API_CALLBACK_URL not set - skipping API notification"
        fi
    else
        echo "Extraction failed or no job ID captured (exit code: $EXIT_CODE)"
    fi
    
    exit $EXIT_CODE
fi

# Otherwise, start MQTT listener for IoT commands
echo "Starting MQTT listener for IoT commands..."
python3 -m src.main &
MQTT_PID=$!

# Wait for MQTT process
wait $MQTT_PID
