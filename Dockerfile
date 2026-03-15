# Forge Extraction Service Dockerfile
# Target: RTX 5090 (Blackwell, sm_120) with NVFP4 quantization
# Purpose: Extract persona data from novels using NVFP4 models, upload JSONL to S3
FROM nvidia/cuda:12.8.0-cudnn-devel-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && apt-get install -y \
    python3.12 \
    python3.12-dev \
    python3.12-venv \
    git \
    wget \
    curl \
    build-essential \
    libssl-dev \
    ca-certificates

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.12 1
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.12

WORKDIR /workspace

# Install PyTorch nightly with cu128 (required for Blackwell sm_120 support)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip3 install --pre torch torchvision torchaudio \
    --index-url https://download.pytorch.org/whl/nightly/cu128

# Install vLLM 0.15.1 (critical for NVFP4 MoE kernel fixes on sm_120)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip3 install vllm==0.15.1

# Install latest transformers from source (required for Qwen3.5 architecture support)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip3 install git+https://github.com/huggingface/transformers.git

# Install AWS SDK for S3 uploads
RUN --mount=type=cache,target=/root/.cache/pip \
    pip3 install boto3

# Copy and install extraction requirements
COPY requirements.txt /workspace/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip3 install -r requirements.txt

# Copy application code
COPY src/ /workspace/src/
COPY executors/ /workspace/executors/
COPY persona_datasets/ /workspace/persona_datasets/
COPY config/ /workspace/config/
COPY entrypoint.sh run_extraction.sh /workspace/
COPY certs/ /workspace/certs/

# Create necessary directories
RUN mkdir -p /workspace/persona_datasets/novels \
    /workspace/persona_datasets/output \
    /workspace/logs/extraction_jobs

# Fix permissions
RUN chmod +x /workspace/run_extraction.sh /workspace/entrypoint.sh && \
    sed -i 's/\r$//' /workspace/run_extraction.sh /workspace/entrypoint.sh

# Enable runtime LoRA loading via REST API (/v1/load_lora_adapter)
ENV VLLM_ALLOW_RUNTIME_LORA_UPDATING=True

# Use V0 engine for stable multi-LoRA support
ENV VLLM_USE_V1=0

ENV PYTHONPATH=/workspace
EXPOSE 8000

ENTRYPOINT ["/workspace/entrypoint.sh"]
