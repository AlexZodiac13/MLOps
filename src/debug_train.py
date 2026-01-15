"""
Debug script
"""
import sys
import os

print("DEBUG_TRAIN: STARTING SIMPLE SCRIPT")
print(f"DEBUG_TRAIN: Python version: {sys.version}")
print(f"DEBUG_TRAIN: CWD: {os.getcwd()}")
try:
    print(f"DEBUG_TRAIN: LS: {os.listdir('.')}")
except Exception as e:
    print(f"DEBUG_TRAIN: LS failed: {e}")

try:
    import pyspark
    print(f"DEBUG_TRAIN: PySpark ver: {pyspark.__version__}")
except Exception as e:
    print(f"DEBUG_TRAIN: PySpark import failed: {e}")

try:
    import mlflow
    print(f"DEBUG_TRAIN: MLflow ver: {mlflow.__version__}")
except Exception as e:
    print(f"DEBUG_TRAIN: MLflow import failed: {e}")

print("DEBUG_TRAIN: ENDING SIMPLE SCRIPT")
sys.exit(0)
