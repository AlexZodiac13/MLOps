# Это пример файла с определением признаков (feature definition)

import os

from datetime import timedelta

import numpy as np
import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    PushSource,
    RequestSource,
)
from feast.feature_logging import LoggingConfig
from feast.infra.offline_stores.file_source import FileLoggingDestination
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64

REPO_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(REPO_PATH, "data")

# Определяем сущность для водителя. Сущность можно рассматривать как первичный ключ,
# который используется для получения признаков
driver = Entity(name="driver", join_keys=["driver_id"])

# Читаем данные из parquet файлов. Parquet удобен для локальной разработки.
# Для промышленного использования можно использовать любое хранилище данных,
# например BigQuery. Подробнее смотрите в документации Feast
driver_stats_source = FileSource(
    name="driver_hourly_stats_source",
    path=os.path.join(DATA_PATH, "driver_stats.parquet"),
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Наши parquet файлы содержат примеры данных, включающие столбец driver_id,
# временные метки и три столбца с признаками. Здесь мы определяем Feature View,
# который позволит нам передавать эти данные в нашу модель онлайн
driver_stats_fv = FeatureView(
    # Уникальное имя этого представления признаков. Два представления признаков
    # в одном проекте не могут иметь одинаковое имя
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    # Список признаков, определенных ниже, действует как схема для материализации
    # признаков в хранилище, а также используется как ссылки при извлечении
    # для создания обучающего набора данных или предоставления признаков
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64, description="Среднее количество поездок в день"),
    ],
    online=True,
    source=driver_stats_source,
    # Теги - это определенные пользователем пары ключ/значение,
    # которые прикрепляются к каждому представлению признаков
    tags={"team": "driver_performance"},
)

# Определяем источник данных запроса, который кодирует признаки/информацию,
# доступную только во время запроса (например, часть пользовательского HTTP-запроса)
input_request = RequestSource(
    name="vals_to_add",
    schema=[
        Field(name="val_to_add", dtype=Int64),
        Field(name="val_to_add_2", dtype=Int64),
    ],
)

# Определяем представление признаков по требованию, которое может генерировать
# новые признаки на основе существующих представлений и признаков из RequestSource
@on_demand_feature_view(
    sources=[driver_stats_fv, input_request],
    schema=[
        Field(name="conv_rate_plus_val1", dtype=Float64),
        Field(name="conv_rate_plus_val2", dtype=Float64),
    ],
)
def transformed_conv_rate(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_val1"] = inputs["conv_rate"] + inputs["val_to_add"]
    df["conv_rate_plus_val2"] = inputs["conv_rate"] + inputs["val_to_add_2"]
    return df


# FeatureService группирует признаки в версию модели
driver_activity_v1 = FeatureService(
    name="driver_activity_v1",
    features=[
        driver_stats_fv[["conv_rate"]],  # Выбирает подмножество признаков из представления
        transformed_conv_rate,  # Выбирает все признаки из представления
    ],
    logging_config=LoggingConfig(
        destination=FileLoggingDestination(path=DATA_PATH)
    ),
)
driver_activity_v2 = FeatureService(
    name="driver_activity_v2", features=[driver_stats_fv, transformed_conv_rate]
)

# Определяет способ отправки данных (доступных офлайн, онлайн или обоих типов) в Feast
driver_stats_push_source = PushSource(
    name="driver_stats_push_source",
    batch_source=driver_stats_source,
)

# Определяет слегка измененную версию представления признаков, описанного выше,
# где источник был изменен на push source. Это позволяет напрямую отправлять
# свежие признаки в онлайн-хранилище для этого представления признаков
driver_stats_fresh_fv = FeatureView(
    name="driver_hourly_stats_fresh",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    online=True,
    source=driver_stats_push_source,  # Изменено по сравнению с предыдущей версией
    tags={"team": "driver_performance"},
)


# Определяем представление признаков по требованию, которое может генерировать
# новые признаки на основе существующих представлений и признаков из RequestSource
@on_demand_feature_view(
    sources=[driver_stats_fresh_fv, input_request],  # использует свежую версию Feature View
    schema=[
        Field(name="conv_rate_plus_val1", dtype=Float64),
        Field(name="conv_rate_plus_val2", dtype=Float64),
    ],
)
def transformed_conv_rate_fresh(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_val1"] = inputs["conv_rate"] + inputs["val_to_add"]
    df["conv_rate_plus_val2"] = inputs["conv_rate"] + inputs["val_to_add_2"]
    return df


driver_activity_v3 = FeatureService(
    name="driver_activity_v3",
    features=[driver_stats_fresh_fv, transformed_conv_rate_fresh],
)

# Добавлено!
@on_demand_feature_view(
    sources=[driver_stats_fv],  # Используем существующий Feature View
    schema=[
        Field(name="combined_rating", dtype=Float64),
        Field(name="performance_score", dtype=Float64),
    ],
)
def driver_performance_metrics(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()

    # Рассчитываем комбинированный рейтинг как взвешенную сумму
    # conv_rate имеет вес 0.6, acc_rate имеет вес 0.4
    df["combined_rating"] = (inputs["conv_rate"] * 0.6 + inputs["acc_rate"] * 0.4)

    # Рассчитываем показатель эффективности на основе среднего количества поездок
    # и комбинированного рейтинга
    df["performance_score"] = (df["combined_rating"] * np.log1p(inputs["avg_daily_trips"]))

    return df

# Обновляем существующий FeatureService, добавляя новые метрики
driver_activity_v4 = FeatureService(
    name="driver_activity_v4",
    features=[
        driver_stats_fv,
        driver_performance_metrics  # Добавляем новые метрики в сервис
    ]
)
