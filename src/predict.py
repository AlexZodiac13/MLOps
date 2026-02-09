from pathlib import Path

import torch
import typer
import numpy as np
from numpy.typing import NDArray

from model import MNISTClassifier


def load_model(model_path: Path) -> MNISTClassifier:
  model = MNISTClassifier()
  model.load_state_dict(
    torch.load(model_path, weights_only=True)
  )
  model.eval()
  
  return model


def load_data(data_path: Path) -> tuple[torch.Tensor, torch.Tensor]:
  data = np.load(data_path)
  X = torch.tensor(data[:, 1:]).float().reshape(data.shape[0], 1, 1, 28, 28)
  y = torch.tensor(data[:, 0]).long()

  return X, y


def main(
  data_path: Path = typer.Option(Path('data/test_data.npy')),
  model_path: Path = typer.Option(Path('data/MNISTClassifier.pt')),
) -> None:
  
  X, y = load_data(data_path)
  model = load_model(model_path)

  pred = model(X[0])

  print(f'predicted={np.argmax(pred.detach().numpy())}, ground_truth={int(y[0])}')


if __name__ == '__main__':
  typer.run(main)
  
  