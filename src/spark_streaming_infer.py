from __future__ import annotations

import os
import time
from typing import Any

import numpy as np
import pandas as pd
import torch
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
  col,
  current_timestamp,
  from_json,
  lit,
  struct,
  to_json,
  pandas_udf,
)
from pyspark.sql.types import ArrayType, DoubleType, StructField, StructType

import mlflow
import mlflow.pytorch


def _env(name: str, default: str) -> str:
  return os.getenv(name, default)


def load_champion_weights() -> dict[str, np.ndarray]:
  tracking_uri = _env('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
  model_name = _env('MLFLOW_MODEL_NAME', 'mnist')
  model_alias = _env('MLFLOW_MODEL_ALIAS', 'champion')

  mlflow.set_tracking_uri(tracking_uri)
  model_uri = f"models:/{model_name}@{model_alias}"

  model = mlflow.pytorch.load_model(model_uri)
  state = model.state_dict()
  weights: dict[str, np.ndarray] = {}
  for k, v in state.items():
    weights[k] = v.detach().cpu().numpy()
  return weights


def load_champion_weights_with_retry() -> dict[str, np.ndarray]:
  retry_s = float(_env('MODEL_RETRY_SECONDS', '10'))
  while True:
    try:
      return load_champion_weights()
    except Exception as e:
      print(f'Failed to load champion model from MLflow: {e}. Retrying in {retry_s}s...')
      time.sleep(retry_s)


_MODEL: Any | None = None


def make_predict_udf(broadcast_weights):
  from model import MNISTClassifier

  @pandas_udf('double')
  def predict_udf(x_series: pd.Series) -> pd.Series:
    global _MODEL
    if _MODEL is None:
      model = MNISTClassifier()
      state = {k: torch.tensor(v) for k, v in broadcast_weights.value.items()}
      model.load_state_dict(state)
      model.eval()
      _MODEL = model

    # x_series: each element is a list[float]
    xs = x_series.to_list()
    if not xs:
      return pd.Series([], dtype='float64')

    X = np.asarray(xs, dtype=np.float32).reshape(-1, 1, 28, 28)
    with torch.no_grad():
      out = _MODEL(torch.from_numpy(X))
      pred = torch.argmax(out, dim=1).cpu().numpy().astype(np.float64)
    return pd.Series(pred)

  return predict_udf


def main() -> None:
  kafka_bootstrap = _env('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
  input_topic = _env('KAFKA_INPUT_TOPIC', 'inputs')
  output_topic = _env('KAFKA_OUTPUT_TOPIC', 'predictions')
  consumer_group = _env('KAFKA_CONSUMER_GROUP', 'spark-infer')
  checkpoint = _env('SPARK_CHECKPOINT', '/tmp/spark-checkpoints/mnist-infer')
  run_for_s = os.getenv('RUN_FOR_SECONDS')
  run_for_seconds = int(run_for_s) if run_for_s else None

  spark = (
    SparkSession.builder.appName('task8-spark-streaming-infer')
    .config('spark.sql.execution.arrow.pyspark.enabled', 'true')
    .getOrCreate()
  )

  spark.sparkContext.setLogLevel(_env('SPARK_LOG_LEVEL', 'WARN'))

  weights = load_champion_weights_with_retry()
  b_weights = spark.sparkContext.broadcast(weights)
  predict_udf = make_predict_udf(b_weights)

  schema = StructType(
    [
      StructField('X', ArrayType(DoubleType()), nullable=False),
      StructField('y', DoubleType(), nullable=True),
      StructField('ts', DoubleType(), nullable=True),
    ]
  )

  raw = (
    spark.readStream.format('kafka')
    .option('kafka.bootstrap.servers', kafka_bootstrap)
    .option('subscribe', input_topic)
    .option('startingOffsets', _env('KAFKA_STARTING_OFFSETS', 'latest'))
    .option('kafka.group.id', consumer_group)
    .load()
  )

  parsed = (
    raw.selectExpr('CAST(value AS STRING) as value_str')
    .select(from_json(col('value_str'), schema).alias('j'))
    .select('j.*')
    .withColumn('processed_ts', current_timestamp())
  )

  out_df = (
    parsed.withColumn('pred', predict_udf(col('X')))
    .withColumn('gt', col('y').cast('double'))
    .withColumn('model', lit(_env('MLFLOW_MODEL_NAME', 'mnist')))
  )

  out_value = to_json(
    struct(
      col('pred'),
      col('gt'),
      col('ts'),
      col('processed_ts'),
      col('model'),
    )
  ).alias('value')

  query = (
    out_df.select(out_value)
    .selectExpr('CAST(value AS STRING) AS value')
    .writeStream.format('kafka')
    .option('kafka.bootstrap.servers', kafka_bootstrap)
    .option('topic', output_topic)
    .option('checkpointLocation', checkpoint)
    .outputMode('append')
    .start()
  )

  if run_for_seconds is None:
    query.awaitTermination()
  else:
    query.awaitTermination(run_for_seconds)
    query.stop()
    spark.stop()


if __name__ == '__main__':
  main()
