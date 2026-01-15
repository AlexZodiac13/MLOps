#!/bin/bash
set -ex

echo "Starting initialization script..."
echo "User: $(whoami)"
echo "Path: $PATH"

# Explicitly use /usr/bin/python3
PYTHON_BIN="/usr/bin/python3"

echo "Using Python binary: $PYTHON_BIN"
$PYTHON_BIN --version

echo "Installing python packages..."

# Ensure wheel is installed
$PYTHON_BIN -m pip install --no-cache-dir wheel

# Install packages
# Added --prefer-binary to avoid compilation issues
$PYTHON_BIN -m pip install --no-cache-dir --prefer-binary \
    mlflow==2.17.2 \
    boto3==1.37.22 \
    psycopg2-binary==2.9.10 \
    scikit-learn==1.3.2 \
    pandas==2.0.3 \
    aniso8601==9.0.1

echo "Verifying installation..."
$PYTHON_BIN -c "import mlflow; print(f'MLflow: {mlflow.__version__}')"
$PYTHON_BIN -c "import boto3; print(f'Boto3: {boto3.__version__}')"
$PYTHON_BIN -c "import pandas; print(f'Pandas: {pandas.__version__}')"

echo "Initialization script complete."


