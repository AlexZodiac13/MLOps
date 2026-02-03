"""
Скрипт для A/B тестирования моделей с оптимизацией гиперпараметров
"""

import os
import argparse
import numpy as np
from sklearn.model_selection import RandomizedSearchCV

from dotenv import load_dotenv
from common import (
    setup_mlflow,
    load_and_prepare_data,
    create_base_pipeline,
    evaluate_model,
    bootstrap_metrics,
    statistical_comparison,
    save_model_to_mlflow,
    load_model_from_mlflow,
    set_model_alias,
)

load_dotenv()  # Загрузка переменных окружения из .env файла

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")


def optimize_hyperparameters(
    X_train, y_train, n_iter=50, cv=3, random_state=42, verbose=True
):
    """
    Оптимизация гиперпараметров с помощью RandomizedSearchCV

    Parameters
    ----------
    X_train : array-like
        Обучающие признаки
    y_train : array-like
        Обучающие метки
    n_iter : int, default=50
        Количество итераций поиска
    cv : int, default=5
        Количество фолдов для кросс-валидации
    random_state : int, default=42
        Зерно для воспроизводимости
    verbose : bool, default=True
        Показывать прогресс

    Returns
    -------
    best_model : sklearn estimator
        Лучшая модель
    best_params : dict
        Лучшие параметры
    cv_score : float
        Оценка кросс-валидации
    """
    print("Поиск оптимальных гиперпараметров...")

    # Базовый pipeline
    base_pipeline = create_base_pipeline()

    # Расширенное пространство поиска
    param_grid = {
        "classifier__n_estimators": [30, 40],
        "classifier__max_depth": [8, 10],
        "classifier__min_samples_split": [2, 5],
        "classifier__min_samples_leaf": [1, 2],
        "classifier__max_features": ["sqrt", "log2", None],
    }

    # Случайный поиск
    random_search = RandomizedSearchCV(
        base_pipeline,
        param_grid,
        n_iter=n_iter,
        cv=cv,
        scoring="f1",
        n_jobs=-1,
        verbose=1 if verbose else 0,
        refit=True,
        random_state=random_state,
    )

    # Обучение
    random_search.fit(X_train, y_train)

    print(f"Поиск завершен!")
    print(f"Лучший CV F1-score: {random_search.best_score_:.4f}")
    print(f"Лучшие параметры: {random_search.best_params_}")

    return (
        random_search.best_estimator_,
        random_search.best_params_,
        random_search.best_score_,
    )


def ab_test_models(
    production_model,
    candidate_model,
    X_test,
    y_test,
    bootstrap_iterations=100,
    alpha=0.01,
):
    """
    A/B тестирование двух моделей

    Parameters
    ----------
    production_model : sklearn estimator
        Текущая производственная модель
    candidate_model : sklearn estimator
        Модель-кандидат
    X_test : array-like
        Тестовые признаки
    y_test : array-like
        Тестовые метки
    bootstrap_iterations : int, default=100
        Количество итераций bootstrap
    alpha : float, default=0.01
        Уровень значимости

    Returns
    -------
    results : dict
        Словарь с результатами сравнения
    """
    print("\nA/B ТЕСТИРОВАНИЕ МОДЕЛЕЙ")
    print("=" * 50)

    # Оценка производственной модели
    print("Оценка производственной модели...")
    prod_metrics, prod_predictions = evaluate_model(production_model, X_test, y_test)

    print("Метрики производственной модели:")
    for metric_name, value in prod_metrics.items():
        print(f"  {metric_name}: {value:.4f}")

    # Оценка модели-кандидата
    print("\nОценка модели-кандидата...")
    cand_metrics, cand_predictions = evaluate_model(candidate_model, X_test, y_test)

    print("Метрики модели-кандидата:")
    for metric_name, value in cand_metrics.items():
        print(f"  {metric_name}: {value:.4f}")

    # Bootstrap анализ
    print(f"\nBootstrap анализ ({bootstrap_iterations} итераций)...")

    print("Bootstrap для производственной модели...")
    prod_bootstrap = bootstrap_metrics(y_test, prod_predictions, bootstrap_iterations)

    print("Bootstrap для модели-кандидата...")
    cand_bootstrap = bootstrap_metrics(y_test, cand_predictions, bootstrap_iterations)

    # Статистическое сравнение
    print(f"\nСтатистическое сравнение (α = {alpha})...")
    comparison_results = statistical_comparison(prod_bootstrap, cand_bootstrap, alpha)

    print("\nРезультаты t-теста:")
    for metric, results in comparison_results.items():
        print(f"\n{metric}-score:")
        print(f"  Production: {results['base_mean']:.4f}")
        print(f"  Candidate:  {results['candidate_mean']:.4f}")
        print(f"  Улучшение:  {results['improvement']:+.4f}")
        print(f"  p-value:    {results['p_value']:.6f}")
        print(f"  Cohen's d:  {results['effect_size']:.4f}")

        if results["is_significant"]:
            print(f"  ЗНАЧИМОЕ улучшение при α={alpha}")
        else:
            print(f"  Незначимое различие при α={alpha}")

    # Общее решение
    f1_significant = comparison_results["F1"]["is_significant"]
    f1_improvement = comparison_results["F1"]["improvement"] > 0

    should_deploy = f1_significant and f1_improvement

    print(f"\n{'='*50}")
    print(f"ИТОГОВОЕ РЕШЕНИЕ:")
    if should_deploy:
        print("РАЗВЕРНУТЬ новую модель в Production")
        print("   Модель-кандидат показала статистически значимое улучшение")
    else:
        print("ОСТАВИТЬ текущую модель в Production")
        if not f1_improvement:
            print("   Модель-кандидат не показала улучшения")
        else:
            print("   Улучшение статистически незначимо")
    print(f"{'='*50}")

    return {
        "should_deploy": should_deploy,
        "production_metrics": prod_metrics,
        "candidate_metrics": cand_metrics,
        "comparison_results": comparison_results,
        "production_bootstrap": prod_bootstrap,
        "candidate_bootstrap": cand_bootstrap,
    }


def run_ab_test(
    model_name="url_classifier",
    n_iter=50,
    cv=5,
    sample_frac=0.1,
    bootstrap_iterations=100,
    alpha=0.01,
    auto_deploy=False,
):
    """
    Выполняет полный цикл A/B тестирования

    Parameters
    ----------
    model_name : str, default="url_classifier"
        Имя модели в MLflow
    n_iter : int, default=50
        Количество итераций поиска гиперпараметров
    cv : int, default=5
        Количество фолдов кросс-валидации
    sample_frac : float, default=0.1
        Доля данных для использования
    bootstrap_iterations : int, default=100
        Количество итераций bootstrap
    alpha : float, default=0.01
        Уровень значимости
    auto_deploy : bool, default=False
        Автоматически разворачивать лучшую модель

    Returns
    -------
    ab_results : dict
        Результаты A/B тестирования
    """
    print("=" * 60)
    print("A/B ТЕСТИРОВАНИЕ МОДЕЛЕЙ")
    print("=" * 60)

    # Настройка MLflow
    setup_mlflow(tracking_uri=MLFLOW_TRACKING_URI)

    # Загрузка данных
    X_train, X_test, y_train, y_test = load_and_prepare_data(sample_frac=sample_frac)

    # Загрузка производственной модели
    print(f"Загрузка производственной модели '{model_name}'...")
    production_model = load_model_from_mlflow(model_name, alias="champion")

    if production_model is None:
        print("Производственная модель не найдена!")
        print("Сначала запустите train.py для создания базовой модели")
        return None

    # Оптимизация гиперпараметров для модели-кандидата
    print("\nОбучение модели-кандидата...")
    candidate_model, best_params, cv_score = optimize_hyperparameters(
        X_train, y_train, n_iter=n_iter, cv=cv
    )

    # A/B тестирование
    ab_results = ab_test_models(
        production_model,
        candidate_model,
        X_test,
        y_test,
        bootstrap_iterations=bootstrap_iterations,
        alpha=alpha,
    )

    # Сохранение модели-кандидата в MLflow
    print(f"\nСохранение модели-кандидата в MLflow...")
    candidate_description = (
        f"Оптимизированная модель (CV F1: {cv_score:.4f}). Параметры: {best_params}"
    )

    candidate_info = save_model_to_mlflow(
        model=candidate_model,
        model_name=model_name,
        metrics=ab_results["candidate_metrics"],
        register_model=True,
        description=candidate_description,
    )

    print(f"Модель-кандидат сохранена как версия {candidate_info['version']}")

    # Развертывание новой модели (если нужно)
    if ab_results["should_deploy"]:
        if auto_deploy:
            print(f"\nАвтоматическое развертывание новой модели...")
            set_model_alias(
                model_name=model_name,
                version=candidate_info["version"],
                alias="champion",
                description=f"Автоматически развернута после A/B теста. "
                f"F1 улучшение: {ab_results['comparison_results']['F1']['improvement']:+.4f}",
            )
            print("Новая модель будет развернута в Production!")
        else:
            print(
                f"\nРекомендация: разверните модель версии {candidate_info['version']} в Production"
            )
            print(f"   Команда: mlflow models serve -m 'models:{model_name}@champion'")

    print("\n" + "=" * 60)
    print("A/B ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print("=" * 60)

    return ab_results


def main():
    """Главная функция для запуска из командной строки"""
    parser = argparse.ArgumentParser(description="A/B тестирование моделей")
    parser.add_argument("--model-name", default="url_classifier", help="Имя модели в MLflow")
    parser.add_argument("--n-iter", type=int, default=50, help="Количество итераций поиска гиперпараметров")
    parser.add_argument("--cv", type=int, default=5, help="Количество фолдов кросс-валидации")
    parser.add_argument("--sample-frac", type=float, default=0.1, help="Доля данных для использования")
    parser.add_argument("--bootstrap-iterations", type=int, default=100, help="Количество итераций bootstrap",)
    parser.add_argument("--alpha", type=float, default=0.01, help="Уровень значимости для статистических тестов",)
    parser.add_argument("--auto-deploy", action="store_true", help="Автоматически разворачивать лучшую модель",)

    args = parser.parse_args()

    run_ab_test(
        model_name=args.model_name,
        n_iter=args.n_iter,
        cv=args.cv,
        sample_frac=args.sample_frac,
        bootstrap_iterations=args.bootstrap_iterations,
        alpha=args.alpha,
        auto_deploy=args.auto_deploy,
    )


if __name__ == "__main__":
    main()
