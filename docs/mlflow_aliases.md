# Обновление MLflow: использование алиасов и конфигурации через аргументы командной строки

## 1. Переход от стадий к алиасам в MLflow

В последних версиях MLflow была введена новая концепция - алиасы моделей, которая заменяет более старую систему стадий (Staging, Production и т.д.). Алиасы предоставляют более гибкий механизм для управления версиями моделей.

### Основные отличия:

| Параметр | Стадии (старый подход) | Алиасы (новый подход) |
|----------|------------------------|----------------------|
| Количество меток | Фиксированный набор (None, Staging, Production, Archived) | Неограниченное количество произвольных меток |
| Гибкость | Одна модель для каждой стадии | Несколько моделей с разными алиасами |
| Назначение | Технические стадии | Бизнес-ориентированные метки (champion, challenger, latest и т.д.) |
| Метод назначения | `transition_model_version_stage()` | `set_registered_model_alias()` |

### Внесенные изменения:

1. **Функция получения лучшей модели**:
   ```python
   # Вместо поиска модели по стадии 'Production'
   model_versions = client.get_latest_versions(model_name)
   champion_version = None
   
   for version in model_versions:
       if hasattr(version, 'aliases') and "champion" in version.aliases:
           champion_version = version
           break
       elif hasattr(version, 'tags') and version.tags.get('alias') == "champion":
           champion_version = version
           break
   ```

2. **Функция установки алиаса**:
   ```python
   # Вместо перевода модели в Production
   try:
       if hasattr(client, 'set_registered_model_alias'):
           client.set_registered_model_alias(model_name, "champion", new_version)
       else:
           # Для старых версий MLflow используем тег
           client.set_model_version_tag(model_name, new_version, "alias", "champion")
   except Exception as e:
       print(f"Ошибка установки алиаса 'champion': {str(e)}")
       client.set_model_version_tag(model_name, new_version, "alias", "champion")
   ```

## 2. Передача конфигурации через аргументы командной строки

Ранее конфигурация для подключения к S3 хранилась в переменных окружения, что создавало зависимость от того, как именно настроена среда выполнения. Чтобы сделать скрипт более гибким и не зависящим от окружения, конфигурация теперь передается через аргументы командной строки.

### Изменения в функции `create_spark_session`:

```python
def create_spark_session(s3_config=None):
    """
    Create and configure a Spark session.
    
    Parameters
    ----------
    s3_config : dict, optional
        Dictionary containing S3 configuration parameters
        (endpoint_url, access_key, secret_key)
    
    Returns
    -------
    SparkSession
        Configured Spark session
    """
    # Создаем базовый Builder
    builder = (SparkSession
               .builder
               .appName("FraudDetectionModel"))
    
    # Если передана конфигурация S3, добавляем настройки
    if s3_config and all(k in s3_config for k in ['endpoint_url', 'access_key', 'secret_key']):
        builder = (builder
                  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                  .config("spark.hadoop.fs.s3a.endpoint", s3_config['endpoint_url'])
                  .config("spark.hadoop.fs.s3a.access.key", s3_config['access_key'])
                  .config("spark.hadoop.fs.s3a.secret.key", s3_config['secret_key'])
                  .config("spark.hadoop.fs.s3a.path.style.access", "true")
                  .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true"))
    
    # Создаем и возвращаем сессию Spark
    return builder.getOrCreate()
```

### Новые аргументы командной строки:

```python
# S3 параметры
parser.add_argument("--s3-endpoint-url", help="S3 endpoint URL")
parser.add_argument("--s3-access-key", help="S3 access key")
parser.add_argument("--s3-secret-key", help="S3 secret key")
```

### Обновление DAG:

```python
# Запуск PySpark задания для обучения модели
train_model = DataprocCreatePysparkJobOperator(
    task_id="train_fraud_detection_model",
    main_python_file_uri=f"{S3_SRC_BUCKET}fraud_detection_model.py",
    connection_id=YC_SA_CONNECTION.conn_id,
    args=[
        "--input", f"{S3_INPUT_DATA_BUCKET}",
        "--output", f"{S3_OUTPUT_MODEL_BUCKET}fraud_model_{datetime.now().strftime('%Y%m%d')}",
        "--model-type", "rf",
        "--tracking-uri", MLFLOW_TRACKING_URI,
        "--experiment-name", MLFLOW_EXPERIMENT_NAME,
        "--auto-register",  # Включаем автоматическую регистрацию лучшей модели
        "--s3-endpoint-url", S3_ENDPOINT_URL,
        "--s3-access-key", S3_ACCESS_KEY,
        "--s3-secret-key", S3_SECRET_KEY,
        "--run-name", f"fraud_detection_training_{datetime.now().strftime('%Y%m%d_%H%M')}"
    ],
)
```

## 3. Преимущества внесенных изменений

### Преимущества использования алиасов:

1. **Гибкость в управлении моделями**: Можно создавать произвольные алиасы для разных целей и сценариев использования.
2. **Улучшенная семантика**: Алиасы 'champion' и 'challenger' более естественно описывают роли моделей, чем технические 'Production' и 'Staging'.
3. **Поддержка A/B тестирования**: Можно легко поддерживать несколько активных версий моделей.
4. **Совместимость с последними версиями MLflow**: Использование современных API и концепций.

### Преимущества передачи конфигурации через аргументы:

1. **Независимость от окружения**: Скрипт не зависит от предварительно настроенных переменных окружения.
2. **Лучшая изоляция**: Каждый запуск может использовать свои собственные параметры подключения.
3. **Явная конфигурация**: Все параметры явно указаны в вызове, что улучшает отслеживаемость и отладку.
4. **Гибкость при тестировании**: Легко переключаться между различными конфигурациями и окружениями.

## 4. Обратная совместимость

Для обеспечения обратной совместимости со старыми версиями MLflow добавлена проверка наличия методов и атрибутов:

```python
if hasattr(client, 'set_registered_model_alias'):
    client.set_registered_model_alias(model_name, "champion", new_version)
else:
    # Для старых версий MLflow используем тег
    client.set_model_version_tag(model_name, new_version, "alias", "champion")
```

Это позволяет коду работать как с новыми, так и со старыми версиями MLflow, используя оптимальный доступный механизм.

## 5. Обновление формата входных данных

Обновлена логика чтения данных для поддержки CSV формата вместо parquet:

```python
df = spark.read.csv(input_path, header=True, inferSchema=True)
```

И соответствующие названия колонок с 'isFraud' на 'fraud'.

## 6. Как использовать обновленный код

Для запуска обновленного скрипта из командной строки:

```bash
python fraud_detection_model.py \
    --input s3a://bucket/path/to/data.csv \
    --output s3a://bucket/path/to/model \
    --model-type rf \
    --tracking-uri http://mlflow-server:5000 \
    --experiment-name fraud_detection \
    --auto-register \
    --s3-endpoint-url https://storage.yandexcloud.net \
    --s3-access-key YOUR_ACCESS_KEY \
    --s3-secret-key YOUR_SECRET_KEY \
    --run-name "my_training_run"
``` 