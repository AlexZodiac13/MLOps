#!/usr/bin/env bash
set -euo pipefail

compose_file="docker-compose.task8.yml"

echo "[1/5] Build & start stack"
docker compose -f "$compose_file" up -d --build

echo "[2/5] Wait for Kafka & MLflow endpoints"
echo "- Kafka UI:   http://localhost:8282"
echo "- MLflow UI:  http://localhost:5000"
echo "- Airflow UI: http://localhost:8088"

echo "[3/5] Register model in MLflow (alias @champion)"
docker compose -f "$compose_file" --profile init up --build mlflow-register

echo "[4/5] Show spark-infer last logs (should be running)"
docker compose -f "$compose_file" ps
docker compose -f "$compose_file" logs --tail=50 spark-infer || true

echo "[5/5] Quick load + evaluation (optional)"
echo "Run load:"
echo "  docker compose -f $compose_file run --rm -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 -e KAFKA_SECURITY_PROTOCOL=PLAINTEXT spark-master python3 /opt/app/src/producer.py --data-path /opt/app/data/test_data.npy --tps 50 --duration-s 10"
echo "Run eval:"
echo "  docker compose -f $compose_file run --rm -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 -e KAFKA_SECURITY_PROTOCOL=PLAINTEXT spark-master python3 /opt/app/src/evaluate_predictions.py --limit 200 --timeout-s 30"

echo "Done."
