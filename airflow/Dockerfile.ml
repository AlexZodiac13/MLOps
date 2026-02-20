FROM apache/airflow:2.8.1-python3.10

USER root
# Install system dependencies required for ML libraries and compilation
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libgomp1 \
    git \
    cmake \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies for ML (including torch with CUDA 12.1 support)
# We install torch first to ensure the CUDA version is correct
RUN pip install --no-cache-dir torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121

# Install other ML dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Install llama.cpp for GGUF conversion
WORKDIR /opt
RUN git clone https://github.com/ggerganov/llama.cpp && \
    cd llama.cpp && \
    cmake -B build -DGGML_CUDA=ON && \
    cmake --build build --config Release -j$(nproc)

# Create directory separately to avoid permission issues
RUN mkdir -p /opt/airflow/ml
