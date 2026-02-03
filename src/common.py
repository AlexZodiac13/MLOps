"""
Общие функции для работы с MLflow и данными
"""
import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_recall_fscore_support, f1_score, precision_score, recall_score, roc_auc_score
from scipy.stats import ttest_ind
import warnings

warnings.simplefilter(action='ignore', category=(FutureWarning, UserWarning))


def load_and_prepare_data(sample_frac=0.1, test_size=0.3, random_state=42):
    """
    Загружает и подготавливает данные для обучения
    
    Parameters
    ----------
    sample_frac : float, default=0.1
        Доля от исходного датасета для использования
    test_size : float, default=0.3
        Доля тестовой выборки
    random_state : int, default=42
        Зерно для воспроизводимости
        
    Returns
    -------
    X_train, X_test, y_train, y_test : tuple
        Разделенные данные для обучения и тестирования
    """
    print("Загрузка данных...")
    url = "https://github.com/faizann24/Using-machine-learning-to-detect-malicious-URLs/raw/master/data/data.csv"
    df = pd.read_csv(url)
    
    print(f"Исходный размер датасета: {len(df)}")
    
    # Сэмплирование для ускорения
    df_sample = df.sample(frac=sample_frac, random_state=random_state)
    print(f"Размер выборки: {len(df_sample)}")
    
    # Кодирование меток
    df_sample["label_enc"] = df_sample.label.map({"bad": 1, "good": 0})
    
    # Разделение данных
    X = df_sample["url"]
    y = df_sample["label_enc"].values
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    
    print(f"Размер обучающей выборки: {len(X_train)}")
    print(f"Размер тестовой выборки: {len(X_test)}")
    
    return X_train, X_test, y_train, y_test


def create_base_pipeline(n_estimators=35, max_depth=9, random_state=42):
    """
    Создает базовый pipeline для классификации URL
    
    Parameters
    ----------
    n_estimators : int, default=35
        Количество деревьев
    max_depth : int, default=9
        Максимальная глубина
    random_state : int, default=42
        Зерно для воспроизводимости
        
    Returns
    -------
    pipeline : sklearn.pipeline.Pipeline
        Готовый pipeline для обучения
    """
    url_vectorizer = CountVectorizer(
        analyzer="char",
        ngram_range=(1, 1),
    )
    
    rf_clf = RandomForestClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        random_state=random_state,
        n_jobs=-1,
    )
    
    pipeline = Pipeline(steps=[
        ("vectorizer", url_vectorizer),
        ("classifier", rf_clf),
    ])
    
    return pipeline


def evaluate_model(model, X_test, y_test):
    """
    Оценивает качество модели
    
    Parameters
    ----------
    model : sklearn estimator
        Обученная модель
    X_test : array-like
        Тестовые признаки
    y_test : array-like
        Тестовые метки
        
    Returns
    -------
    metrics : dict
        Словарь с метриками качества модели
    y_pred : array-like
        Предсказания модели
    """
    y_pred = model.predict(X_test)
    
    P, R, F1, _ = precision_recall_fscore_support(y_test, y_pred, average="binary")
    auc = roc_auc_score(y_test, y_pred)
    
    metrics = {
        "precision": P,
        "recall": R,
        "f1_score": F1,
        "auc": auc
    }
    
    return metrics, y_pred


def bootstrap_metrics(y_test, y_pred, n_iterations=100, random_state=42):
    """
    Выполняет bootstrap-анализ метрик
    
    Parameters
    ----------
    y_test : array-like
        Истинные метки
    y_pred : array-like
        Предсказания модели
    n_iterations : int, default=100
        Количество итераций bootstrap
    random_state : int, default=42
        Зерно для воспроизводимости
        
    Returns
    -------
    scores : pandas.DataFrame
        DataFrame с метриками для каждой итерации
    """
    np.random.seed(random_state)
    
    df_bootstrap = pd.DataFrame({
        "y_test": y_test,
        "y_pred": y_pred,
    })
    
    scores = []
    
    for i in range(n_iterations):
        sample = df_bootstrap.sample(frac=1.0, replace=True)
        
        metrics = {
            "F1": f1_score(sample["y_test"], sample["y_pred"]),
            "P": precision_score(sample["y_test"], sample["y_pred"]),
            "R": recall_score(sample["y_test"], sample["y_pred"]),
            "AUC": roc_auc_score(sample["y_test"], sample["y_pred"])
        }
        scores.append(metrics)
    
    return pd.DataFrame(scores)


def statistical_comparison(scores_base, scores_candidate, alpha=0.01, metrics=['F1', 'P']):
    """
    Статистическое сравнение двух моделей с помощью t-теста
    
    Parameters
    ----------
    scores_base : pandas.DataFrame
        Метрики базовой модели
    scores_candidate : pandas.DataFrame
        Метрики модели-кандидата
    alpha : float, default=0.01
        Уровень значимости
    metrics : list, default=['F1', 'P']
        Список метрик для сравнения
        
    Returns
    -------
    results : dict
        Словарь с результатами статистического сравнения
    """
    results = {}

    for metric in metrics:
        t_stat, pvalue = ttest_ind(scores_base[metric], scores_candidate[metric])
        
        # Размер эффекта (Cohen's d)
        pooled_std = np.sqrt((scores_base[metric].var() + scores_candidate[metric].var()) / 2)
        effect_size = abs(scores_candidate[metric].mean() - scores_base[metric].mean()) / pooled_std
        
        is_significant = pvalue < alpha
        
        results[metric] = {
            't_statistic': t_stat,
            'p_value': pvalue,
            'effect_size': effect_size,
            'is_significant': is_significant,
            'base_mean': scores_base[metric].mean(),
            'candidate_mean': scores_candidate[metric].mean(),
            'improvement': scores_candidate[metric].mean() - scores_base[metric].mean()
        }
    
    return results


def save_model_to_mlflow(model, model_name, metrics, register_model=False, description=""):
    """
    Сохраняет модель в MLflow
    
    Parameters
    ----------
    model : sklearn estimator
        Обученная модель
    model_name : str
        Имя модели для регистрации
    metrics : dict
        Словарь с метриками
    register_model : bool, default=False
        Регистрировать ли модель в Model Registry
    description : str, default=""
        Описание модели
        
    Returns
    -------
    model_info : dict
        Информация о сохраненной модели (run_id, model_uri)
    """
    with mlflow.start_run() as run:
        # Логируем метрики
        for metric_name, value in metrics.items():
            mlflow.log_metric(metric_name, value)
        
        # Сохраняем модель без автоматической регистрации
        mlflow.sklearn.log_model(model, "model")
        
        run_id = run.info.run_id
        model_uri = f"runs:/{run_id}/model"
    
    model_info = {
        "run_id": run_id,
        "model_uri": model_uri
    }
    
    # Регистрируем модель в Model Registry если требуется
    if register_model:
        client = mlflow.tracking.MlflowClient()
        
        # Создаем или получаем зарегистрированную модель
        try:
            client.get_registered_model(model_name)
        except Exception:
            client.create_registered_model(model_name)
        
        # Регистрируем новую версию
        model_details = mlflow.register_model(model_uri, model_name)
        
        # Обновляем описание
        if description:
            client.update_model_version(
                name=model_name,
                version=model_details.version,
                description=description
            )
        
        model_info["version"] = model_details.version
        model_info["model_details"] = model_details
    
    return model_info


def load_model_from_mlflow(model_name, alias="champion"):
    """
    Загружает модель из MLflow Model Registry по алиасу
    
    Parameters
    ----------
    model_name : str
        Имя модели в реестре
    alias : str, default="champion"
        Алиас модели для загрузки
        
    Returns
    -------
    model : sklearn estimator or None
        Загруженная модель или None в случае ошибки
    """
    client = mlflow.tracking.MlflowClient()
    
    try:
        # Пытаемся получить модель по алиасу
        try:
            model_uri = f"models:/{model_name}@{alias}"
            model = mlflow.sklearn.load_model(model_uri)
            print(f"Модель '{model_name}' с алиасом '{alias}' успешно загружена")
            return model
        except Exception:
            # Fallback: ищем модель по тегу alias
            print(f"Поиск модели '{model_name}' по тегу alias='{alias}'")
            model_versions = client.get_latest_versions(model_name)
            
            for version in model_versions:
                if hasattr(version, 'tags') and version.tags.get('alias') == alias:
                    model_uri = f"models:/{model_name}/{version.version}"
                    model = mlflow.sklearn.load_model(model_uri)
                    print(f"Модель '{model_name}' версии {version.version} с тегом alias='{alias}' загружена")
                    return model
            
            print(f"Модель '{model_name}' с алиасом '{alias}' не найдена")
            return None
            
    except Exception as e:
        print(f"Ошибка при загрузке модели: {e}")
        return None


def set_model_alias(model_name, version, alias, description=""):
    """
    Устанавливает алиас для модели
    
    Parameters
    ----------
    model_name : str
        Имя модели
    version : str
        Версия модели
    alias : str
        Алиас для модели (например, "champion", "challenger")
    description : str, default=""
        Описание изменения
    """
    client = mlflow.tracking.MlflowClient()
    
    try:
        # Проверяем доступность метода set_registered_model_alias
        if hasattr(client, 'set_registered_model_alias'):
            client.set_registered_model_alias(
                name=model_name,
                alias=alias,
                version=version
            )
        else:
            # Для старых версий MLflow используем тег
            client.set_model_version_tag(model_name, version, "alias", alias)
    except Exception as e:
        print(f"Ошибка установки алиаса '{alias}': {e}")
        # Fallback через теги
        client.set_model_version_tag(model_name, version, "alias", alias)
    
    if description:
        client.update_model_version(
            name=model_name,
            version=version,
            description=description
        )
    
    print(f"Модель {model_name} версии {version} получила алиас '{alias}'")


def transition_model_stage(model_name, version, stage, description=""):
    """
    Функция для обратной совместимости - переводит стадии в алиасы
    
    Parameters
    ----------
    model_name : str
        Имя модели
    version : str
        Версия модели
    stage : str
        Стадия модели (Production, Staging, Archived)
    description : str, default=""
        Описание перехода
    """
    # Преобразуем стадии в алиасы для обратной совместимости
    stage_to_alias = {
        "Production": "champion",
        "Staging": "challenger", 
        "Archived": "archived"
    }
    
    alias = stage_to_alias.get(stage, stage.lower())
    set_model_alias(model_name, version, alias, description)


def setup_mlflow(tracking_uri="http://localhost:5000", experiment_name="url_classification"):
    """
    Настройка MLflow
    
    Parameters
    ----------
    tracking_uri : str, default="http://localhost:5000"
        URI для MLflow Tracking Server
    experiment_name : str, default="url_classification"
        Имя эксперимента
        
    Returns
    -------
    experiment_id : str
        Идентификатор эксперимента
    """
    mlflow.set_tracking_uri(tracking_uri)
    
    try:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment is None:
            experiment_id = mlflow.create_experiment(experiment_name)
            print(f"Создан новый эксперимент: {experiment_name}")
        else:
            experiment_id = experiment.experiment_id
            print(f"Используется существующий эксперимент: {experiment_name}")
        
        mlflow.set_experiment(experiment_name)
        return experiment_id
    except Exception as e:
        print(f"Ошибка при настройке MLflow: {e}")
        # Используем локальное хранилище как fallback
        mlflow.set_tracking_uri("file:./mlruns")
        mlflow.set_experiment(experiment_name)
        print("Используется локальное хранилище MLflow")
