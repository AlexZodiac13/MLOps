import json
from pathlib import Path
from datetime import datetime

import torch
import typer
import numpy as np
from kafka import KafkaConsumer, KafkaProducer # type: ignore

import cfg
from model import MNISTClassifier


def load_model(model_path: Path) -> MNISTClassifier:
  model = MNISTClassifier()
  model.load_state_dict(
    torch.load(model_path, weights_only=True)
  )
  model.eval()
  
  return model


def predict(model: MNISTClassifier, values: dict) -> tuple[float, float]:
  y = float(values['y'])
  X = torch.Tensor(values['X']).float().reshape(1, 1, 28, 28)
  pred = float(np.argmax(model(X).detach().numpy()))
  return pred, y


def kafka_init(kafka_user: str, kafka_pass: str) -> KafkaConsumer:
  return KafkaConsumer(
    cfg.kafka_input_topic,
    **cfg.kafka_client_kwargs(kafka_user, kafka_pass),
    value_deserializer=lambda m: json.loads(m.decode('ascii')),
    group_id='consume-to-predict',
    auto_offset_reset='earliest'
  )
 

def main(
  model_path: Path = typer.Option(Path('data/MNISTClassifier.pt')),
  kafka_user: str = typer.Option('consumer'),
  kafka_pass: str = typer.Option('cat281983'),
) -> None:
  
  model = load_model(model_path)

  consumer = kafka_init(kafka_user, kafka_pass)

  try:
    for msg in consumer:
      pred, gt = predict(model, msg.value)
      
      print(f'pred={pred}, gt={gt}')
  finally:
    consumer.close()


if __name__ == '__main__':
  typer.run(main)