import torch
from torch import nn

class MNISTClassifier(nn.Module):
  def __init__(self):
    super().__init__()
    self.model = nn.Sequential(
      nn.Conv2d(1, 8, kernel_size=3),
      nn.ReLU(),
      nn.Conv2d(8, 16, kernel_size=3),
      nn.ReLU(),
      nn.Flatten(),
      nn.LazyLinear(10), # 10 classes in total.
    )

  def forward(self, x: torch.Tensor):
    return self.model(x)