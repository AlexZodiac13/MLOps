FROM apache/airflow:2.10.0-python3.10

USER root
# Install system dependencies required for ML libraries and compilation
RUN apt-get update &&     apt-get install -y --no-install-recommends     build-essential     libgomp1     git     cmake     && apt-get clean     && rm -rf /var/lib/apt/lists/*

USER airflow

# Install PyTorch (heavy layer, rarely changes)
RUN pip install --no-cache-dir torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121

# Install other dependencies
COPY requirements.txt /requirements.txt
# Install requirements directly. 
# We explicitly upgrade to ensure compatible versions are selected based on the new constraints.
RUN pip install --no-cache-dir --upgrade -r /requirements.txt

# Install llama.cpp
USER root
WORKDIR /opt
RUN git clone https://github.com/ggerganov/llama.cpp &&     cd llama.cpp &&     cmake -B build -DGGML_CUDA=OFF &&     cmake --build build --config Release -j$(nproc)

# Fix permissions
RUN chown -R airflow:root /opt/llama.cpp

USER airflow
RUN mkdir -p /opt/airflow/ml
