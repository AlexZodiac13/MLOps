import subprocess
from datetime import datetime
import pandas as pd
from feast import FeatureStore
from feast.data_source import PushMode

def run_demo():
    # Инициализация Feature Store с текущей директорией в качестве пути к репозиторию
    store = FeatureStore(repo_path=".")

    # Применение изменений в репозитории Feature Store (создание таблиц, обновление схем и т.д.)
    print("\n--- Run feast apply ---")
    subprocess.run(["feast", "apply"])

    # Получение исторических данных для тренировки модели
    # Использует временные метки из entity_df
    print("\n--- Historical features for training ---")
    fetch_historical_features_entity_df(store, for_batch_scoring=False)

    # Получение исторических данных для пакетного скоринга
    # Использует текущее время для всех записей
    print("\n--- Historical features for batch scoring ---")
    fetch_historical_features_entity_df(store, for_batch_scoring=True)

    # Материализация признаков в онлайн-хранилище
    # Обновляет онлайн-хранилище последними значениями признаков до текущего момента
    print("\n--- Load features into online store ---")
    store.materialize_incremental(end_date=datetime.now())

    # Получение онлайн-признаков напрямую из Feature Views
    print("\n--- Online features ---")
    fetch_online_features(store)

    # Получение онлайн-признаков через Feature Service
    # Использует предопределенный набор признаков из driver_activity_v1
    print("\n--- Online features retrieved (instead) through a feature service---")
    fetch_online_features(store, source="feature_service")

    # Получение онлайн-признаков через Feature Service с push source
    # Использует driver_activity_v3, который работает с push source
    print(
        "\n--- Online features retrieved (using feature service v3, which uses a feature view with a push source---"
    )
    fetch_online_features(store, source="push")

    # Симуляция потокового события: добавление новых данных
    # Создает одну запись с текущим временем и отправляет её в онлайн и офлайн хранилища
    print("\n--- Simulate a stream event ingestion of the hourly stats df ---")
    event_df = pd.DataFrame.from_dict(
        {
            "driver_id": [1001],
            "event_timestamp": [datetime.now()],
            "created": [datetime.now()],
            "conv_rate": [1.0],
            "acc_rate": [1.0],
            "avg_daily_trips": [1000],
        }
    )
    print(event_df)
    # Отправка данных в онлайн и офлайн хранилища через push source
    store.push("driver_stats_push_source", event_df, to=PushMode.ONLINE_AND_OFFLINE)

    # Проверка обновленных онлайн-признаков после push-события
    print("\n--- Online features again with updated values from a stream push---")
    fetch_online_features(store, source="push")

    # Очистка ресурсов Feature Store
    print("\n--- Run feast teardown ---")
    subprocess.run(["feast", "teardown"])


def fetch_historical_features_entity_df(store: FeatureStore, for_batch_scoring: bool):
    """
    Получение исторических признаков для заданного набора сущностей
    
    Args:
        store: Экземпляр Feature Store
        for_batch_scoring: Если True, использует текущее время вместо исторических временных меток
    """
    # Создание DataFrame с данными сущностей и временными метками
    entity_df = pd.DataFrame.from_dict(
        {
            # ID водителей для получения их признаков
            "driver_id": [1001, 1002, 1003],
            # Временные метки для каждого события
            "event_timestamp": [
                datetime(2021, 4, 12, 10, 59, 42),
                datetime(2021, 4, 12, 8, 12, 10),
                datetime(2021, 4, 12, 16, 40, 26),
            ],
            # Метки (лейблы) для обучения - не обрабатываются в Feature Store
            "label_driver_reported_satisfaction": [1, 5, 3],
            # Значения для on-demand трансформаций
            "val_to_add": [1, 2, 3],
            "val_to_add_2": [10, 20, 30],
        }
    )
    
    # Для пакетного скоринга используем текущее время
    if for_batch_scoring:
        entity_df["event_timestamp"] = pd.to_datetime("now", utc=True)

    # Получение исторических признаков из Feature Store
    training_df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "driver_hourly_stats:conv_rate",  # Базовые статистики водителя
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips",
            "transformed_conv_rate:conv_rate_plus_val1",  # Трансформированные признаки
            "transformed_conv_rate:conv_rate_plus_val2",
        ],
    ).to_df()
    print(training_df.head())


def fetch_online_features(store, source: str = ""):
    """
    Получение онлайн-признаков для заданных сущностей
    
    Args:
        store: Экземпляр Feature Store
        source: Тип источника данных ("feature_service", "push" или пустая строка для прямого доступа)
    """
    # Определение сущностей для запроса
    entity_rows = [
        # Данные для каждой сущности с дополнительными значениями для трансформаций
        {
            "driver_id": 1001,
            "val_to_add": 1000,
            "val_to_add_2": 2000,
        },
        {
            "driver_id": 1002,
            "val_to_add": 1001,
            "val_to_add_2": 2002,
        },
    ]

    # Выбор способа получения признаков в зависимости от source
    if source == "feature_service":
        # Использование Feature Service v1
        features_to_fetch = store.get_feature_service("driver_activity_v1")
    elif source == "push":
        # Использование Feature Service v3 с push source
        features_to_fetch = store.get_feature_service("driver_activity_v3")
    else:
        # Прямой запрос конкретных признаков
        features_to_fetch = [
            "driver_hourly_stats:acc_rate",
            "transformed_conv_rate:conv_rate_plus_val1",
            "transformed_conv_rate:conv_rate_plus_val2",
        ]

    # Получение онлайн-признаков и вывод результатов
    returned_features = store.get_online_features(
        features=features_to_fetch,
        entity_rows=entity_rows,
    ).to_dict()
    for key, value in sorted(returned_features.items()):
        print(key, " : ", value)


if __name__ == "__main__":
    run_demo()