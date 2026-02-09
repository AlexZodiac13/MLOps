from __future__ import annotations

import csv
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import typer
from kafka import KafkaConsumer, KafkaProducer, TopicPartition  # type: ignore

import cfg


@dataclass
class LagSample:
  ts: float
  total_lag: int


def read_group_lag(bootstrap: str, group: str, topic: str) -> int:
  """Total lag for (group, topic) computed as sum(end_offset - committed_offset)."""
  consumer = KafkaConsumer(
    bootstrap_servers=[bootstrap],
    security_protocol='PLAINTEXT',
    group_id=group,
    enable_auto_commit=False,
  )
  try:
    parts = consumer.partitions_for_topic(topic) or set()
    if not parts:
      return 0
    tps = [TopicPartition(topic, p) for p in sorted(parts)]
    end = consumer.end_offsets(tps)
    total = 0
    for tp in tps:
      committed = consumer.committed(tp)
      committed_offset = int(committed) if committed is not None else 0
      total += max(int(end.get(tp, 0)) - committed_offset, 0)
    return total
  finally:
    consumer.close()


def produce_load(
  producer: KafkaProducer,
  X: np.ndarray,
  y: np.ndarray,
  topic: str,
  *,
  tps: float,
  duration_s: float,
) -> None:
  if tps <= 0:
    raise typer.BadParameter('tps must be > 0')
  period_s = 1.0 / tps
  start_ts = time.perf_counter()
  i = 0
  while True:
    now = time.perf_counter()
    if (now - start_ts) >= duration_s:
      break
    idx = np.random.randint(0, X.shape[0])
    producer.send(
      topic,
      {
        'X': X[idx].tolist(),
        'y': float(y[idx]),
        'ts': time.time(),
      },
    )
    i += 1
    target = start_ts + i * period_s
    sleep_for = target - time.perf_counter()
    if sleep_for > 0:
      time.sleep(sleep_for)
  producer.flush(timeout=10)


def resolve_test_data_path() -> Path:
  candidates = [
    Path('data/test_data.npy'),
    Path('/opt/app/data/test_data.npy'),
    (Path(__file__).resolve().parent.parent / 'data' / 'test_data.npy'),
  ]
  for candidate in candidates:
    if candidate.exists():
      return candidate
  raise FileNotFoundError(
    'test_data.npy not found. Tried: ' + ', '.join(str(p) for p in candidates)
  )


def main(
  tps_list: str = typer.Option('50,100,200,300,400', help='Comma-separated TPS values to test.'),
  duration_s: int = typer.Option(30, help='How long to run each TPS step.'),
  warmup_s: int = typer.Option(10, help='Warm-up time before sampling lag.'),
  sample_every_s: float = typer.Option(2.0, help='Sampling interval.'),
  consumer_group: str = typer.Option('spark-infer', help='Consumer group used by Spark streaming job.'),
  kafka_bootstrap: str = typer.Option('kafka:29092', help='Kafka bootstrap server.'),
  input_topic: str = typer.Option('inputs', help='Input topic.'),
  output_csv: Optional[Path] = typer.Option(Path('reports/task8_lag_results.csv'), help='Where to write results (optional).'),
):
  tps_values = [float(x.strip()) for x in tps_list.split(',') if x.strip()]
  if not tps_values:
    raise typer.BadParameter('tps_list is empty')

  if output_csv is not None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)

  # Load retrospective data
  data_path = resolve_test_data_path()
  data = np.load(data_path)
  X = data[:, 1:]
  y = data[:, 0]

  producer = KafkaProducer(
    bootstrap_servers=[kafka_bootstrap],
    security_protocol='PLAINTEXT',
    value_serializer=lambda m: json.dumps(m).encode('utf-8'),
    linger_ms=20,
    batch_size=65536,
  )

  rows: list[dict[str, str]] = []
  try:
    for tps in tps_values:
      print(f'=== TPS={tps} ===')
      # Warm-up phase (produce, no sampling)
      produce_load(producer, X, y, input_topic, tps=tps, duration_s=warmup_s)

      start = time.time()
      samples: list[LagSample] = []
      # Sampling phase: produce load continuously while sampling lag.
      # To keep logic simple, we interleave produce+sample by producing in bursts.
      burst_s = min(sample_every_s, 1.0)
      while time.time() - start < duration_s:
        produce_load(producer, X, y, input_topic, tps=tps, duration_s=burst_s)
        lag = read_group_lag(kafka_bootstrap, consumer_group, input_topic)
        samples.append(LagSample(ts=time.time(), total_lag=lag))
        print(f'lag={lag}')
        remaining_sleep = sample_every_s - burst_s
        if remaining_sleep > 0:
          time.sleep(remaining_sleep)

      max_lag = max((s.total_lag for s in samples), default=0)
      end_lag = samples[-1].total_lag if samples else 0
      rows.append(
        {
          'tps': str(tps),
          'duration_s': str(duration_s),
          'warmup_s': str(warmup_s),
          'max_total_lag': str(max_lag),
          'end_total_lag': str(end_lag),
        }
      )
  finally:
    producer.close()

  # Always print summary to logs (Airflow-friendly)
  print('=== SUMMARY ===')
  print(
    json.dumps(
      rows,
      ensure_ascii=False,
      indent=2,
    )
  )

  if output_csv is not None:
    with output_csv.open('w', newline='') as f:
      writer = csv.DictWriter(
        f,
        fieldnames=['tps', 'duration_s', 'warmup_s', 'max_total_lag', 'end_total_lag'],
      )
      writer.writeheader()
      writer.writerows(rows)
    print(f'Wrote: {output_csv}')


if __name__ == '__main__':
  typer.run(main)
