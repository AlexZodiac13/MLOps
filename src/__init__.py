"""
URL Classification MLOps Package

Модули для обучения, валидации и A/B тестирования моделей классификации URL
"""

__version__ = "1.0.0"
__author__ = "MLOps Team"

from .common import (
    load_and_prepare_data,
    create_base_pipeline,
    evaluate_model,
    bootstrap_metrics,
    statistical_comparison,
    save_model_to_mlflow,
    load_model_from_mlflow,
    transition_model_stage,
    set_model_alias,
    setup_mlflow
)

__all__ = [
    "load_and_prepare_data",
    "create_base_pipeline", 
    "evaluate_model",
    "bootstrap_metrics",
    "statistical_comparison",
    "save_model_to_mlflow",
    "load_model_from_mlflow",
    "transition_model_stage",
    "set_model_alias", 
    "setup_mlflow"
]
