#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для генерации демонстрационных данных для обучения модели обнаружения мошенничества.
Создает CSV-файл с синтетическими данными о транзакциях и загружает его в S3-хранилище.
"""

from datetime import datetime, timedelta
import os
import sys
import numpy as np
import pandas as pd
import boto3
from dotenv import load_dotenv
from sklearn.preprocessing import StandardScaler

# pylint: disable=broad-exception-caught

# Загружаем переменные окружения
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
else:
    print("Файл .env не найден!")
    sys.exit(1)

# Проверяем наличие необходимых переменных окружения
required_vars = ["S3_ENDPOINT_URL", "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_BUCKET_NAME"]
missing_vars = [var for var in required_vars if os.getenv(var) is None]
if missing_vars:
    print(f"Не найдены необходимые переменные окружения: {', '.join(missing_vars)}")
    sys.exit(1)

# Константы для генерации данных
NUM_SAMPLES = 100000
FRAUD_RATE = 0.05  # 5% транзакций будут мошенническими
SEED = 42


def generate_data():
    """Генерирует синтетические данные для обучения модели обнаружения мошенничества."""
    np.random.seed(SEED)

    # Генерируем базовые признаки для обычных транзакций
    n_normal = int(NUM_SAMPLES * (1 - FRAUD_RATE))
    n_fraud = NUM_SAMPLES - n_normal

    # Создаем признаки для нормальных транзакций
    normal_amounts = np.random.lognormal(mean=5.0, sigma=1.0, size=n_normal)
    normal_features = {
        "amount": normal_amounts,
        "hour_of_day": np.random.randint(0, 24, n_normal),
        "day_of_week": np.random.randint(0, 7, n_normal),
        "distance_from_home": np.random.exponential(scale=30.0, size=n_normal),
        "distance_from_last_transaction": np.random.exponential(scale=20.0, size=n_normal),
        "ratio_to_median_purchase_price": np.random.normal(loc=1.0, scale=0.5, size=n_normal),
        "repeat_retailer": np.random.randint(0, 2, n_normal),
        "used_chip": np.random.binomial(1, 0.9, n_normal),
        "used_pin_number": np.random.binomial(1, 0.8, n_normal),
        "online_order": np.random.binomial(1, 0.4, n_normal),
        "fraud": np.zeros(n_normal, dtype=int),
    }

    # Создаем признаки для мошеннических транзакций (немного другие распределения)
    fraud_amounts = np.random.lognormal(mean=6.0, sigma=2.0, size=n_fraud)
    fraud_features = {
        "amount": fraud_amounts,
        "hour_of_day": np.random.choice(range(0, 24), n_fraud),  # чаще ночью
        "day_of_week": np.random.randint(0, 7, n_fraud),
        "distance_from_home": np.random.exponential(
            scale=100.0, size=n_fraud
        ),  # обычно дальше от дома
        "distance_from_last_transaction": np.random.exponential(
            scale=100.0, size=n_fraud
        ),  # большее расстояние между транзакциями
        "ratio_to_median_purchase_price": np.random.normal(
            loc=3.0, scale=1.0, size=n_fraud
        ),  # более необычный размер
        "repeat_retailer": np.random.binomial(1, 0.2, n_fraud),  # реже в повторных магазинах
        "used_chip": np.random.binomial(1, 0.2, n_fraud),  # реже используется чип
        "used_pin_number": np.random.binomial(1, 0.2, n_fraud),  # реже используется PIN
        "online_order": np.random.binomial(1, 0.8, n_fraud),  # чаще онлайн-заказы
        "fraud": np.ones(n_fraud, dtype=int),
    }

    # Объединяем данные и перемешиваем
    data = {
        k: np.concatenate([normal_features[k], fraud_features[k]]) for k in normal_features.keys()
    }
    df = pd.DataFrame(data)

    # Перемешиваем данные
    df = df.sample(frac=1.0, random_state=SEED).reset_index(drop=True)

    # Добавляем временную метку
    now = datetime.now()
    df["timestamp"] = [
        now - timedelta(minutes=np.random.randint(1, 60 * 24 * 30)) for _ in range(len(df))
    ]

    # Добавляем ID транзакции
    df["transaction_id"] = [f"tx_{i:08d}" for i in range(len(df))]

    # Переупорядочиваем столбцы
    columns_order = [
        "transaction_id",
        "timestamp",
        "amount",
        "hour_of_day",
        "day_of_week",
        "distance_from_home",
        "distance_from_last_transaction",
        "ratio_to_median_purchase_price",
        "repeat_retailer",
        "used_chip",
        "used_pin_number",
        "online_order",
        "fraud",
    ]
    return df[columns_order]


def upload_to_s3(df, filename="fraud_data.csv"):
    """Загружает данные в формате CSV в S3-хранилище."""
    # Создаем клиента S3
    s3_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
    )

    # Временный файл для загрузки
    input_data_path = os.path.join("data", "input_data", filename)
    df.to_csv(input_data_path, index=False)

    # Загружаем файл в S3
    bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_key = f"input_data/{filename}"

    try:
        s3_client.upload_file(input_data_path, bucket_name, s3_key)
        print(f"Данные успешно загружены в s3://{bucket_name}/{s3_key}")
        return True
    except Exception as e:
        print(f"Ошибка при загрузке данных в S3: {e}")
        return False


def main():
    """Генерирует данные, разделяет их на обучающую и тестовую выборки и загружает в S3."""
    print("Генерация синтетических данных для модели обнаружения мошенничества...")
    df = generate_data()

    # Разделяем данные на обучающую и тестовую выборки (80% / 20%)
    train_df = df.sample(frac=0.8, random_state=SEED)
    test_df = df.drop(train_df.index)

    # Получаем текущую дату для именования файлов
    date_str = datetime.now().strftime("%Y%m%d")

    # Загружаем обучающую выборку
    train_filename = "train.csv"

    try:
        upload_to_s3(train_df, train_filename)
        print(f"Обучающая выборка ({len(train_df)} записей) успешно загружена.")
    except Exception as e:
        print(f"Ошибка при загрузке обучающей выборки: {e}")

    print(
        f"Всего сгенерировано {len(df)} записей, из них {df['fraud'].sum()} "
        f"мошеннических транзакций ({df['fraud'].mean()*100:.2f}%)."
    )


if __name__ == "__main__":
    main()
