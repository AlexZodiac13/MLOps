import os
from pathlib import Path

import mlflow
import mlflow.pytorch
import torch
import typer

from model import MNISTClassifier


def _env(name: str, default: str) -> str:
  return os.getenv(name, default)


def load_local_model(model_path: Path) -> MNISTClassifier:
  model = MNISTClassifier()
  model.load_state_dict(torch.load(model_path, weights_only=True))
  model.eval()
  return model


def main(
  model_path: Path = typer.Option(Path('data/MNISTClassifier.pt')),
  model_name: str = typer.Option('mnist'),
  alias: str = typer.Option('champion'),
  experiment_name: str = typer.Option('task8'),
):
  mlflow.set_tracking_uri(_env('MLFLOW_TRACKING_URI', 'http://mlflow:5000'))
  mlflow.set_experiment(experiment_name)

  model = load_local_model(model_path)

  with mlflow.start_run(run_name='register-mnist') as run:
    mlflow.log_param('model_name', model_name)
    mlflow.pytorch.log_model(
      pytorch_model=model,
      artifact_path='model',
      registered_model_name=model_name,
    )

    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='{model_name}'")
    version = max((int(v.version) for v in versions), default=None)
    if version is None:
      raise RuntimeError('Model version was not created')

    # Set alias (MLflow model alias API)
    client.set_registered_model_alias(model_name, alias, str(version))
    print(f'Registered {model_name} v{version} as @{alias}')


if __name__ == '__main__':
  typer.run(main)
