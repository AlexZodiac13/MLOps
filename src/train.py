"""
Скрипт для обучения базовой модели и регистрации в MLflow
"""

import os
import argparse

from dotenv import load_dotenv

from common import (
    setup_mlflow,
    load_and_prepare_data,
    create_base_pipeline,
    evaluate_model,
    save_model_to_mlflow,
    set_model_alias,
)

load_dotenv()  # Загрузка переменных окружения из .env файла

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")


def train_base_model(
    model_name="url_classifier",
    n_estimators=35,
    max_depth=9,
    sample_frac=0.1,
    register_as_prod=True,
):
    """
    Обучает базовую модель и регистрирует её в MLflow

    Parameters
    ----------
    model_name : str, default="url_classifier"
        Имя модели для регистрации
    n_estimators : int, default=35
        Количество деревьев
    max_depth : int, default=9
        Максимальная глубина
    sample_frac : float, default=0.1
        Доля данных для использования
    register_as_prod : bool, default=True
        Регистрировать ли как Production модель

    Returns
    -------
    model : sklearn estimator
        Обученная модель
    metrics : dict
        Метрики качества модели
    """
    print("=" * 60)
    print("ОБУЧЕНИЕ БАЗОВОЙ МОДЕЛИ")
    print("=" * 60)

    # Настройка MLflow
    setup_mlflow(tracking_uri=MLFLOW_TRACKING_URI)

    # Загрузка и подготовка данных
    X_train, X_test, y_train, y_test = load_and_prepare_data(sample_frac=sample_frac)

    # Создание и обучение модели
    print("\nСоздание pipeline...")
    model = create_base_pipeline(n_estimators=n_estimators, max_depth=max_depth)

    print("Обучение модели...")
    model.fit(X_train, y_train)

    # Оценка модели
    print("Оценка качества модели...")
    metrics, y_pred = evaluate_model(model, X_test, y_test)

    print("\nРезультаты:")
    for metric_name, value in metrics.items():
        print(f"  {metric_name}: {value:.4f}")

    # Сохранение в MLflow
    print(f"\nСохранение модели '{model_name}' в MLflow...")
    description = f"Базовая модель RandomForest (n_estimators={n_estimators}, max_depth={max_depth})"

    model_info = save_model_to_mlflow(
        model=model,
        model_name=model_name,
        metrics=metrics,
        register_model=register_as_prod,
        description=description,
    )

    if register_as_prod:
        print(f"Модель зарегистрирована как версия {model_info['version']}")
        # Устанавливаем алиас "champion" для production модели
        set_model_alias(
            model_name=model_name,
            version=model_info["version"],
            alias="champion",
            description="Базовая производственная модель",
        )
    else:
        print("Модель сохранена в MLflow")

    print("\n" + "=" * 60)
    print("ОБУЧЕНИЕ ЗАВЕРШЕНО УСПЕШНО")
    print("=" * 60)

    return model, metrics


def main():
    """Главная функция для запуска из командной строки"""
    parser = argparse.ArgumentParser(description="Обучение базовой модели")
    parser.add_argument("--model-name", default="url_classifier",help="Имя модели в MLflow",)
    parser.add_argument("--n-estimators", type=int, default=35,help="Количество деревьев в RandomForest",)
    parser.add_argument("--max-depth", type=int, default=9, help="Максимальная глубина деревьев")
    parser.add_argument("--sample-frac", type=float, default=0.1, help="Доля данных для использования")
    parser.add_argument("--no-prod", action="store_true", help="Не регистрировать как Production модель")

    args = parser.parse_args()

    train_base_model(
        model_name=args.model_name,
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        sample_frac=args.sample_frac,
        register_as_prod=not args.no_prod,
    )


if __name__ == "__main__":
    main()
