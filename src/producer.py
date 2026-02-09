from __future__ import annotations

import time
import random
import json
from pathlib import Path
from typing import Generator, Optional

import typer
import numpy as np
from numpy.typing import NDArray
from kafka import KafkaProducer # type: ignore

import cfg

def load_data(data_path: Path) -> tuple[NDArray, NDArray]:
  data = np.load(data_path)
  X = data[:, 1:]
  y = data[:, 0]
  return X, y


def unlimited() -> Generator[int, None, None]:
  index = 0            
  while True:
      yield index
      index += 1


def main(
  data_path: Path = typer.Option(Path('data/test_data.npy')),
  kafka_user: str = typer.Option('producer'),
  kafka_pass: str = typer.Option('cat281983'),
  limit: Optional[int] = typer.Option(None),
  tps: float = typer.Option(1.0, help='Target transactions per second.'),
  duration_s: Optional[float] = typer.Option(None, help='How long to run (seconds). If omitted, runs until limit/unlimited.'),
):
  
  X, y = load_data(data_path)
  
  if tps <= 0:
    raise typer.BadParameter('tps must be > 0')

  producer = KafkaProducer(
    **cfg.kafka_client_kwargs(kafka_user, kafka_pass),
    value_serializer=lambda m: json.dumps(m).encode('ascii'),
    linger_ms=20,
    batch_size=65536,
  )

  try:
    show_must_go_on = range(limit) if limit is not None and limit > 0 else unlimited()

    start_ts = time.perf_counter()
    next_flush = start_ts
    period_s = 1.0 / tps

    for i in show_must_go_on:
      now = time.perf_counter()
      if duration_s is not None and (now - start_ts) >= duration_s:
        break

      index = random.randint(0, X.shape[0]-1)
      producer.send(
        topic=cfg.kafka_input_topic,
        value={
          'X': X[index].tolist(),
          'y': y[index],
          'ts': time.time(),
        }
      )

      # Pace to target TPS.
      target = start_ts + (i + 1) * period_s
      sleep_for = target - time.perf_counter()
      if sleep_for > 0:
        time.sleep(sleep_for)

      # Flush periodically so latency doesn't grow unbounded.
      if time.perf_counter() - next_flush >= 1.0:
        producer.flush(timeout=5)
        next_flush = time.perf_counter()

      if i % max(int(tps), 1) == 0:
        print(i)

  finally:
    producer.close()


if __name__ == '__main__':
  typer.run(main)