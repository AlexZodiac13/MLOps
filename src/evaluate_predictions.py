from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime

import typer
from kafka import KafkaConsumer  # type: ignore

import cfg


@dataclass
class Metrics:
  n: int
  correct: int
  avg_latency_ms: float


def main(
  limit: int = typer.Option(1000, help='How many prediction messages to read.'),
  timeout_s: int = typer.Option(60, help='Stop if no messages within timeout.'),
  group_id: str = typer.Option('eval-predictions', help='Kafka consumer group id.'),
  kafka_user: str = typer.Option('consumer'),
  kafka_pass: str = typer.Option('consumer'),
):
  consumer = KafkaConsumer(
    cfg.kafka_output_topic,
    **cfg.kafka_client_kwargs(kafka_user, kafka_pass),
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id=group_id,
    auto_offset_reset='earliest',
    consumer_timeout_ms=timeout_s * 1000,
  )

  n = 0
  correct = 0
  latencies_ms: list[float] = []
  t0 = time.time()

  try:
    for msg in consumer:
      n += 1
      v = msg.value
      pred = float(v.get('pred'))
      gt = float(v.get('gt'))
      if pred == gt:
        correct += 1

      # processed_ts comes as ISO-like string from Spark JSON.
      processed_ts = v.get('processed_ts')
      event_ts = v.get('ts')
      if processed_ts is not None and event_ts is not None:
        try:
          dt = datetime.fromisoformat(processed_ts.replace('Z', '+00:00'))
          latency_ms = (dt.timestamp() - float(event_ts)) * 1000.0
          if latency_ms >= 0:
            latencies_ms.append(latency_ms)
        except Exception:
          pass

      if n >= limit:
        break
  finally:
    consumer.close()

  elapsed = time.time() - t0
  accuracy = (correct / n) if n else 0.0
  avg_lat = (sum(latencies_ms) / len(latencies_ms)) if latencies_ms else 0.0
  throughput = (n / elapsed) if elapsed > 0 else 0.0

  print(
    json.dumps(
      {
        'n': n,
        'correct': correct,
        'accuracy': accuracy,
        'avg_latency_ms': avg_lat,
        'throughput_msg_per_s': throughput,
      },
      ensure_ascii=False,
      indent=2,
    )
  )


if __name__ == '__main__':
  typer.run(main)
