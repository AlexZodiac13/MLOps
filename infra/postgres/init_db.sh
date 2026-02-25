#!/bin/bash
set -e

# This script runs inside the official postgres image at first container init
# It creates the mlflow_db database used by MLflow.

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
  CREATE DATABASE mlflow_db;
EOSQL
