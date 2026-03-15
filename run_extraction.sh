#!/bin/bash
# Forge Extraction Pipeline
# Extracts persona data from novels using NVFP4 models and uploads JSONL to S3
# Triggered via IoT: forge/extraction/new

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$SCRIPT_DIR"
PERSONA_DIR="$ROOT_DIR/persona_datasets"
OUTPUT_DIR="$PERSONA_DIR/output"

# --- Configuration from IoT payload ---
CHAR_NAME="${CHAR_NAME:-}"
EXTRACTION_MODEL="${EXTRACTION_MODEL:-Qwen/Qwen2.5-7B-Instruct-AWQ}"
ENABLE_THINKING="${ENABLE_THINKING:-false}"
USER_DESCRIPTION="${USER_DESCRIPTION:-}"
USER_VALUES="${USER_VALUES:-}"
USER_PERSPECTIVE="${USER_PERSPECTIVE:-}"
USER_TASKS="${USER_TASKS:-}"

# S3 upload config
S3_BUCKET="${S3_BUCKET:-phiskel-extraction-data}"
S3_PREFIX="${S3_PREFIX:-extraction-jobs}"
AUTHENTICATED_USER_ID="${AUTHENTICATED_USER_ID:-nosana-user}"

if [ -z "$CHAR_NAME" ]; then
    echo "Error: CHAR_NAME must be provided via environment variable."
    echo "Usage: CHAR_NAME=\"Character Name\" ./run_extraction.sh"
    exit 1
fi

echo "============================================="
echo "   Forge Extraction Pipeline"
echo "============================================="
echo "Character: $CHAR_NAME"
echo "Extraction Model: $EXTRACTION_MODEL"
echo "S3 Bucket: $S3_BUCKET"
echo "============================================="

# =============================================
# PHASE 0: Initial Cleanup
# =============================================
echo "============================================="
echo "PHASE 0: Initial Cleanup"
echo "============================================="

echo "Cleaning up any leftover files from previous runs..."

if [ -d "$OUTPUT_DIR" ] && [ "$(ls -A $OUTPUT_DIR 2>/dev/null)" ]; then
    rm -f "$OUTPUT_DIR"/*
    echo " + Deleted files in persona_datasets/output"
else
    echo " + No files to delete in persona_datasets/output"
fi

# =============================================
# PHASE 1: Start vLLM with NVFP4 Model
# =============================================
echo "============================================="
echo "PHASE 1: Starting vLLM with NVFP4 Model"
echo "============================================="

# If vLLM is running, stop it first
if [ -f /tmp/vllm.pid ]; then
    RUNNING_PID=$(cat /tmp/vllm.pid)
    echo "vLLM is running (PID: $RUNNING_PID). Stopping..."
    kill $RUNNING_PID || true
    rm /tmp/vllm.pid
    tail --pid=$RUNNING_PID -f /dev/null 2>/dev/null || true
    echo "vLLM stopped."
fi

# Start vLLM with the extraction model
export VLLM_MODEL="$EXTRACTION_MODEL"
export VLLM_GPU_MEMORY_UTILIZATION="${VLLM_GPU_MEMORY_UTILIZATION:-0.9}"
export VLLM_MAX_MODEL_LEN="${VLLM_MAX_MODEL_LEN:-16384}"
export VLLM_MAX_NUM_SEQS="${VLLM_MAX_NUM_SEQS:-32}"

echo "Starting vLLM with model: $VLLM_MODEL (GPU: $VLLM_GPU_MEMORY_UTILIZATION, MaxLen: $VLLM_MAX_MODEL_LEN, MaxSeqs: $VLLM_MAX_NUM_SEQS)..."

# Start vLLM in background (tee output to both log file and stdout for debugging)
python3 -m vllm.entrypoints.openai.api_server \
    --model "$VLLM_MODEL" \
    --served-model-name "$VLLM_MODEL" \
    --port 8000 \
    --gpu-memory-utilization "$VLLM_GPU_MEMORY_UTILIZATION" \
    --max-model-len "$VLLM_MAX_MODEL_LEN" \
    --swap-space 1 \
    --enforce-eager \
    --kv-cache-dtype fp8 \
    --max-num-seqs "$VLLM_MAX_NUM_SEQS" \
    --dtype half \
    2>&1 | tee /var/log/vllm.log &

VLLM_PID=$!
echo $VLLM_PID > /tmp/vllm.pid
echo "vLLM started with PID: $VLLM_PID"

# Wait for vLLM to be ready (stream logs every 10 seconds)
echo "Waiting for vLLM to be ready..."
LAST_LINE_COUNT=0
for i in {1..300}; do
    # Print new vLLM log lines every 10 seconds for visibility
    if [ $((i % 10)) -eq 0 ] && [ -f /var/log/vllm.log ]; then
        CURRENT_LINE_COUNT=$(wc -l < /var/log/vllm.log)
        if [ $CURRENT_LINE_COUNT -gt $LAST_LINE_COUNT ]; then
            echo "[vLLM log progress - line $LAST_LINE_COUNT to $CURRENT_LINE_COUNT:]"
            tail -n $((CURRENT_LINE_COUNT - LAST_LINE_COUNT)) /var/log/vllm.log | sed 's/^/  /'
            LAST_LINE_COUNT=$CURRENT_LINE_COUNT
        fi
    fi
    
    if curl -s http://localhost:8000/health > /dev/null 2>&1; then
        echo "vLLM is ready! Running warmup request..."
        # Final log dump if there was any output
        if [ -f /var/log/vllm.log ]; then
            FINAL_LINE_COUNT=$(wc -l < /var/log/vllm.log)
            if [ $FINAL_LINE_COUNT -gt $LAST_LINE_COUNT ]; then
                echo "[Final vLLM startup logs:]"
                tail -n $((FINAL_LINE_COUNT - LAST_LINE_COUNT)) /var/log/vllm.log | sed 's/^/  /'
            fi
        fi
        # Warmup request
        curl -s -X POST http://localhost:8000/v1/chat/completions \
            -H "Content-Type: application/json" \
            -d "{\"model\": \"$VLLM_MODEL\", \"messages\": [{\"role\": \"user\", \"content\": \"Hello\"}], \"max_tokens\": 10}" \
            > /dev/null 2>&1
        echo "Warmup complete! vLLM is fully ready."
        break
    fi
    if [ $i -eq 300 ]; then
        echo "ERROR: vLLM failed to start within 300 seconds"
        echo "============================================="
        echo "vLLM STDERR LOGS:"
        echo "============================================="
        if [ -f /var/log/vllm.log ]; then
            cat /var/log/vllm.log
        else
            echo "No vLLM log file found at /var/log/vllm.log"
        fi
        echo "============================================="
        exit 1
    fi
    sleep 1
done

# =============================================
# PHASE 2: Extract Training Data
# =============================================
echo "============================================="
echo "PHASE 2: Extracting Training Data"
echo "============================================="

cd "$PERSONA_DIR" || exit 1

# Run extraction script
EXTRACTION_ARGS="-c \"$CHAR_NAME\""
if [ "$ENABLE_THINKING" = "true" ]; then
    EXTRACTION_ARGS="$EXTRACTION_ARGS --thinking"
fi

echo "Running extraction for $CHAR_NAME..."
python3 extract_persona_dataset_TEST_MARK_III.py $EXTRACTION_ARGS || exit 1

echo " + Extraction complete!"
echo " + Generated training files in output/"

# =============================================
# PHASE 3: Upload JSONL to S3
# =============================================
echo "============================================="
echo "PHASE 3: Uploading JSONL to S3"
echo "============================================="

cd "$ROOT_DIR" || exit 1

# Generate job ID
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
CHAR_SLUG=$(echo "$CHAR_NAME" | tr '[:upper:]' '[:lower:]' | tr ' ' '_' | tr -cd '[:alnum:]_')
EXTRACTION_JOB_ID="${CHAR_SLUG}-extract-${TIMESTAMP}"

echo "Extraction Job ID: $EXTRACTION_JOB_ID"
echo "Uploading to S3: s3://$S3_BUCKET/$S3_PREFIX/$EXTRACTION_JOB_ID/"

# Call Python uploader
python3 -m src.upload_extraction \
    --output-dir "$OUTPUT_DIR" \
    --job-id "$EXTRACTION_JOB_ID" \
    --bucket "$S3_BUCKET" \
    --prefix "$S3_PREFIX" \
    --user-id "$AUTHENTICATED_USER_ID" \
    --char-name "$CHAR_NAME" \
    --model-name "$EXTRACTION_MODEL"

if [ $? -eq 0 ]; then
    echo " + JSONL files uploaded successfully!"
    echo "[EXTRACTION_COMPLETE] s3://$S3_BUCKET/$S3_PREFIX/$EXTRACTION_JOB_ID/"
    echo "[PIPELINE_RESULT_EXTRACTION_JOB_ID] $EXTRACTION_JOB_ID"
else
    echo " !!Error: Failed to upload JSONL files to S3"
    exit 1
fi

# =============================================
# PHASE 4: Cleanup
# =============================================
echo "============================================="
echo "PHASE 4: Cleanup"
echo "============================================="

echo "Cleaning up temporary files..."

# Archive logs
LOGS_DIR="$ROOT_DIR/logs/extraction_jobs/$EXTRACTION_JOB_ID"
mkdir -p "$LOGS_DIR"

if [ -f /var/log/vllm.log ]; then
    cp /var/log/vllm.log "$LOGS_DIR/"
    echo " + Archived vLLM logs"
fi

# Clean output directory
if [ -d "$OUTPUT_DIR" ] && [ "$(ls -A $OUTPUT_DIR 2>/dev/null)" ]; then
    rm -f "$OUTPUT_DIR"/*
    echo " + Cleaned output directory"
fi

# Stop vLLM
if [ -f /tmp/vllm.pid ]; then
    VLLM_PID=$(cat /tmp/vllm.pid)
    echo "Stopping vLLM (PID: $VLLM_PID)..."
    kill $VLLM_PID || true
    rm -f /tmp/vllm.pid
    echo "vLLM stopped."
fi

echo "============================================="
echo "Extraction Pipeline Complete!"
echo "============================================="
echo "Job ID: $EXTRACTION_JOB_ID"
echo "Logs saved to: $LOGS_DIR"
echo "============================================="
