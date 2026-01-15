"""
Script: fraud_detection_model.py
Description: PySpark script for training a fraud detection model and logging to MLflow.
"""

import os
import sys
import traceback
import argparse
import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# pylint: disable=broad-exception-caught

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
    print("DEBUG: Начинаем создание Spark сессии")
    try:
        # Создаем базовый Builder
        builder = (SparkSession
            .builder
            .appName("FraudDetectionModel")
        )

        # Если передана конфигурация S3, добавляем настройки
        if s3_config and all(k in s3_config for k in ['endpoint_url', 'access_key', 'secret_key']):
            print(f"DEBUG: Настраиваем S3 с endpoint_url: {s3_config['endpoint_url']}")
            builder = (builder
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.endpoint", s3_config['endpoint_url'])
                .config("spark.hadoop.fs.s3a.access.key", s3_config['access_key'])
                .config("spark.hadoop.fs.s3a.secret.key", s3_config['secret_key'])
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
            )

        print("DEBUG: Spark сессия успешно сконфигурирована")
        # Создаем и возвращаем сессию Spark
        return builder
    except Exception as e:
        print(f"ERROR: Ошибка создания Spark сессии: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        raise


def load_data(spark, input_path):
    """
    Load and prepare the fraud detection dataset.

    Parameters
    ----------
    spark : SparkSession
        Spark session
    input_path : str
        Path to the input data

    Returns
    -------
    tuple
        (train_df, test_df) - Spark DataFrames for training and testing
    """
    print(f"DEBUG: Начинаем загрузку данных из: {input_path}")
    try:
        # Load the data
        print(f"DEBUG: Чтение CSV файла из {input_path}")
        df = spark.read.csv(input_path, header=True, inferSchema=True)

        # Print schema and basic statistics
        print("Dataset Schema:")
        df.printSchema()
        print(f"Total records: {df.count()}")

        # Проверим первые несколько строк
        print("DEBUG: Первые 5 строк данных:")
        df.show(5, truncate=False)

        # Split the data into training and testing sets
        print("DEBUG: Разделение на обучающую и тестовую выборки")
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        print(f"Training set size: {train_df.count()}")
        print(f"Testing set size: {test_df.count()}")

        return train_df, test_df
    except Exception as e:
        print(f"ERROR: Ошибка загрузки данных: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        raise


def prepare_features(train_df, test_df):
    """
    Prepare features for model training.

    Parameters
    ----------
    train_df : DataFrame
        Training DataFrame
    test_df : DataFrame
        Testing DataFrame

    Returns
    -------
    tuple
        (train_df, test_df, feature_cols) - Prepared DataFrames and feature column names
    """
    print("DEBUG: Начинаем подготовку признаков")
    try:
        # Получаем типы столбцов
        print("DEBUG: Проверяем типы столбцов")
        dtypes = dict(train_df.dtypes)
        print(f"DEBUG: Типы данных: {dtypes}")

        # Исключаем строковые столбцы и целевую переменную 'fraud'
        feature_cols = [col for col in train_df.columns
                        if col != 'fraud' and dtypes[col] != 'string']
        print(f"DEBUG: Выбрано {len(feature_cols)} числовых признаков: {feature_cols}")

        # Проверим наличие нулевых значений
        print("DEBUG: Проверка наличия нулевых значений в обучающей выборке")
        for col in train_df.columns:
            null_count = train_df.filter(train_df[col].isNull()).count()
            if null_count > 0:
                print(f"WARNING: Колонка '{col}' содержит {null_count} нулевых значений")

        return train_df, test_df, feature_cols
    except Exception as e:
        print(f"ERROR: Ошибка подготовки признаков: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        raise


def train_model(train_df, test_df, feature_cols, model_type="rf", run_name="fraud_detection_model"):
    """
    Train a fraud detection model and log metrics to MLflow.

    Parameters
    ----------
    train_df : DataFrame
        Training DataFrame
    test_df : DataFrame
        Testing DataFrame
    feature_cols : list
        List of feature column names
    model_type : str
        Model type to train ('rf' for Random Forest, 'lr' for Logistic Regression)
    run_name : str
        Name for the MLflow run

    Returns
    -------
    tuple
        (best_model, metrics) - Best model and its performance metrics
    """
    print(f"DEBUG: Начинаем обучение модели типа {model_type}, run_name: {run_name}")
    try:
        # Create feature vector
        print("DEBUG: Создание преобразователя признаков")
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )

        # Select model based on type
        print("DEBUG: Создание классификатора")
        classifier = RandomForestClassifier(
            labelCol="fraud",
            featuresCol="features",
            numTrees=10,
            maxDepth=5
        )
        param_grid = (ParamGridBuilder()
            .addGrid(classifier.numTrees, [10, 20])
            .addGrid(classifier.maxDepth, [5, 10])
            .build()
        )
        print(f"DEBUG: Сконфигурирована сетка параметров с {len(param_grid)} комбинациями")

        # Create pipeline
        print("DEBUG: Создание пайплайна")
        pipeline = Pipeline(stages=[assembler, scaler, classifier])

        # Create evaluators
        print("DEBUG: Создание оценщиков")
        evaluator_auc = BinaryClassificationEvaluator(
            labelCol="fraud",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        evaluator_acc = MulticlassClassificationEvaluator(
            labelCol="fraud",
            predictionCol="prediction",
            metricName="accuracy"
        )
        evaluator_f1 = MulticlassClassificationEvaluator(
            labelCol="fraud",
            predictionCol="prediction",
            metricName="f1"
        )

        # Create cross-validator
        print("DEBUG: Создание кросс-валидатора")
        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator_auc,
            numFolds=3
        )

        # Start MLflow run
        print(f"DEBUG: Начинаем MLflow run: {run_name}")
        with mlflow.start_run(run_name=run_name) as run:
            run_id = run.info.run_id
            print(f"MLflow Run ID: {run_id}")

            # Log model parameters
            print("DEBUG: Логируем параметры в MLflow")
            mlflow.log_param("numTrees_options", [10, 20])
            mlflow.log_param("maxDepth_options", [5, 10])

            # Train the model
            print("DEBUG: Обучаем модель...")
            cv_model = cv.fit(train_df)
            print("DEBUG: Модель успешно обучена")
            best_model = cv_model.bestModel
            print("DEBUG: Получили лучшую модель")

            # Make predictions on test data
            print("DEBUG: Делаем предсказания на тестовых данных")
            predictions = best_model.transform(test_df)
            print("DEBUG: Предсказания получены")

            # Calculate metrics
            print("DEBUG: Рассчитываем метрики")
            auc = evaluator_auc.evaluate(predictions)
            accuracy = evaluator_acc.evaluate(predictions)
            f1 = evaluator_f1.evaluate(predictions)

            # Log metrics
            print("DEBUG: Логируем метрики в MLflow")
            mlflow.log_metric("auc", auc)
            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_metric("f1", f1)

            # Log best model parameters
            print("DEBUG: Получаем и логируем параметры лучшей модели")
            rf_model = best_model.stages[-1]
            # Получаем параметры модели как свойства, а не методы
            print("DEBUG: Получаем numTrees и maxDepth как свойства")
            try:
                num_trees = rf_model.getNumTrees
                max_depth = rf_model.getMaxDepth()
                print(f"DEBUG: numTrees={num_trees}, maxDepth={max_depth}")
                mlflow.log_param("best_numTrees", num_trees)
                mlflow.log_param("best_maxDepth", max_depth)
            except Exception as e:
                print(f"WARNING: Ошибка при получении параметров модели: {str(e)}")
                print("DEBUG: Проверяем доступные атрибуты модели:")
                for attr in dir(rf_model):
                    if not attr.startswith('_'):
                        print(f"DEBUG: Атрибут: {attr}")
                # Продолжаем выполнение даже если не удалось получить параметры

            # Log the model
            print("DEBUG: Сохраняем модель в MLflow")
            mlflow.spark.log_model(best_model, "model")

            # Print metrics
            print(f"AUC: {auc}")
            print(f"Accuracy: {accuracy}")
            print(f"F1 Score: {f1}")

            metrics = {
                "run_id": run_id,
                "auc": auc,
                "accuracy": accuracy,
                "f1": f1
            }

            return best_model, metrics
    except Exception as e:
        print(f"ERROR: Ошибка обучения модели: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        raise


def save_model(model, output_path):
    """
    Save the trained model to the specified path.

    Parameters
    ----------
    model : PipelineModel
        Trained model
    output_path : str
        Path to save the model
    """
    print(f"DEBUG: Сохраняем модель в: {output_path}")
    try:
        model.write().overwrite().save(output_path)
        print(f"Model saved to: {output_path}")
    except Exception as e:
        print(f"ERROR: Ошибка сохранения модели: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        raise


def get_best_model_metrics(experiment_name):
    """
    Получает метрики лучшей модели из MLflow с алиасом 'champion'

    Parameters
    ----------
    experiment_name : str
        Имя эксперимента MLflow

    Returns
    -------
    dict
        Метрики лучшей модели или None, если модели нет
    """
    print(f"DEBUG: Получаем метрики лучшей модели для эксперимента '{experiment_name}'")
    client = MlflowClient()

    # Получаем ID эксперимента
    try:
        print(f"DEBUG: Ищем эксперимент {experiment_name}")
        experiment = client.get_experiment_by_name(experiment_name)
        if not experiment:
            print(f"Эксперимент '{experiment_name}' не найден")
            return None
        print(f"DEBUG: Эксперимент найден, ID: {experiment.experiment_id}")
    except Exception as e:
        print(f"ERROR: Ошибка при получении эксперимента: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        return None

    try:
        # Пытаемся получить модель по алиасу 'champion'
        model_name = f"{experiment_name}_model"
        print(f"DEBUG: Ищем зарегистрированную модель '{model_name}'")

        # Проверяем, существует ли зарегистрированная модель
        try:
            registered_model = client.get_registered_model(model_name)
            print(f"Модель '{model_name}' зарегистрирована")
            print(f"Модель '{model_name}' имеет {len(registered_model.latest_versions)} версий")
        except Exception as e:
            print(f"DEBUG: Модель '{model_name}' еще не зарегистрирована: {str(e)}")
            return None

        # Получаем версии модели и проверяем наличие алиаса 'champion'
        print("DEBUG: Получаем последние версии модели")
        model_versions = client.get_latest_versions(model_name)
        champion_version = None

        print(f"DEBUG: Найдено {len(model_versions)} версий модели")
        for version in model_versions:
            print(f"DEBUG: Проверяем версию {version.version}")
            # Проверяем наличие атрибута 'aliases' или используем тег
            if hasattr(version, 'aliases') and "champion" in version.aliases:
                print(f"DEBUG: Найден 'champion' в aliases: {version.aliases}")
                champion_version = version
                break
            elif hasattr(version, 'tags') and version.tags.get('alias') == "champion":
                print(f"DEBUG: Найден 'champion' в тегах: {version.tags}")
                champion_version = version
                break
            else:
                print(f"DEBUG: Версия {version.version} не имеет алиаса 'champion'")
                if hasattr(version, 'aliases'):
                    print(f"DEBUG: Aliases: {version.aliases}")
                if hasattr(version, 'tags'):
                    print(f"DEBUG: Tags: {version.tags}")

        if not champion_version:
            print("Модель с алиасом 'champion' не найдена")
            return None

        # Получаем Run ID чемпиона
        champion_run_id = champion_version.run_id
        print(f"DEBUG: Run ID для 'champion': {champion_run_id}")

        # Получаем метрики из прогона
        print(f"DEBUG: Получаем метрики для run_id: {champion_run_id}")
        run = client.get_run(champion_run_id)
        metrics = {
            "run_id": champion_run_id,
            "auc": run.data.metrics["auc"],
            "accuracy": run.data.metrics["accuracy"],
            "f1": run.data.metrics["f1"]
        }

        print(
            f"Текущая лучшая модель (champion): "
            f"версия {champion_version.version}, Run ID: {champion_run_id}"
        )
        print(
            f"Метрики: AUC={metrics['auc']:.4f}, "
            f"Accuracy={metrics['accuracy']:.4f}, "
            f"F1={metrics['f1']:.4f}"
        )

        return metrics
    except Exception as e:
        print(f"ERROR: Ошибка при получении лучшей модели: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        return None


def compare_and_register_model(new_metrics, experiment_name):
    """
    Сравнивает новую модель с лучшей в MLflow и регистрирует, если она лучше

    Parameters
    ----------
    new_metrics : dict
        Метрики новой модели
    experiment_name : str
        Имя эксперимента MLflow

    Returns
    -------
    bool
        True, если новая модель была зарегистрирована как лучшая
    """
    print(f"DEBUG: Сравниваем и регистрируем модель для эксперимента {experiment_name}")
    client = MlflowClient()

    # Получаем метрики лучшей модели
    print("DEBUG: Получаем метрики лучшей модели")
    best_metrics = get_best_model_metrics(experiment_name)

    # Имя модели
    model_name = f"{experiment_name}_model"
    print(f"DEBUG: Имя модели: {model_name}")

    # Создаем или получаем регистрированную модель
    try:
        print(f"DEBUG: Проверяем существует ли модель {model_name}")
        client.get_registered_model(model_name)
        print(f"Модель '{model_name}' уже зарегистрирована")
    except Exception as e:
        print(f"DEBUG: Создаем новую модель: {str(e)}")
        client.create_registered_model(model_name)
        print(f"Создана новая регистрированная модель '{model_name}'")

    # Регистрируем новую модель как новую версию
    run_id = new_metrics["run_id"]
    model_uri = f"runs:/{run_id}/model"
    print(f"DEBUG: Регистрируем модель из {model_uri}")
    model_details = mlflow.register_model(model_uri, model_name)
    new_version = model_details.version
    print(f"DEBUG: Зарегистрирована новая версия: {new_version}")

    # Решаем, должна ли новая модель стать 'champion'
    should_promote = False

    if not best_metrics:
        should_promote = True
        print("Это первая регистрируемая модель, она будет назначена как 'champion'")
    else:
        # Сравниваем на основе AUC (можно изменить критерий сравнения)
        print(f"DEBUG: Сравниваем метрики - текущий AUC: {best_metrics['auc']}, новый AUC: {new_metrics['auc']}")
        if new_metrics["auc"] > best_metrics["auc"]:
            should_promote = True
            improvement = (new_metrics["auc"] - best_metrics["auc"]) / best_metrics["auc"] * 100
            print(
                f"Новая модель лучше на {improvement:.2f}% по AUC. Установка в качестве 'champion'"
            )
        else:
            print(
                f"Новая модель не превосходит текущую 'champion' модель по AUC. "
                f"Текущий AUC: {best_metrics['auc']:.4f}, новый AUC: {new_metrics['auc']:.4f}"
            )

    # Если новая модель лучше, устанавливаем ее как 'champion'
    if should_promote:
        # Устанавливаем алиас 'champion' для новой версии
        try:
            # Проверяем доступность метода set_registered_model_alias
            print("DEBUG: Пытаемся установить алиас 'champion'")
            if hasattr(client, 'set_registered_model_alias'):
                print("DEBUG: Используем set_registered_model_alias")
                client.set_registered_model_alias(model_name, "champion", new_version)
            else:
                # Для старых версий MLflow используем тег
                print("DEBUG: Используем set_model_version_tag")
                client.set_model_version_tag(model_name, new_version, "alias", "champion")
        except Exception as e:
            print(f"ERROR: Ошибка установки алиаса 'champion': {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            # Продолжаем выполнение и используем тег как запасной вариант
            print("DEBUG: Используем set_model_version_tag (запасной вариант)")
            client.set_model_version_tag(model_name, new_version, "alias", "champion")

        print(f"Версия {new_version} модели '{model_name}' установлена как 'champion'")
        return True

    # Если модель не лучше, устанавливаем алиас 'challenger'
    try:
        print("DEBUG: Пытаемся установить алиас 'challenger'")
        # Проверяем доступность метода set_registered_model_alias
        if hasattr(client, 'set_registered_model_alias'):
            print("DEBUG: Используем set_registered_model_alias")
            client.set_registered_model_alias(model_name, "challenger", new_version)
        else:
            # Для старых версий MLflow используем тег
            print("DEBUG: Используем set_model_version_tag")
            client.set_model_version_tag(model_name, new_version, "alias", "challenger")
    except Exception as e:
        print(f"ERROR: Ошибка установки алиаса 'challenger': {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        # Продолжаем выполнение и используем тег как запасной вариант
        print("DEBUG: Используем set_model_version_tag (запасной вариант)")
        client.set_model_version_tag(model_name, new_version, "alias", "challenger")

    print(f"Версия {new_version} модели '{model_name}' установлена как 'challenger'")
    return False


def main():
    """
    Main function to run the fraud detection model training.
    """
    print("DEBUG: Скрипт запущен, начинаем инициализацию")
    parser = argparse.ArgumentParser(description="Fraud Detection Model Training")
    # Основные параметры
    parser.add_argument("--input", required=True, help="Input data path")
    parser.add_argument("--output", required=True, help="Output model path")
    parser.add_argument("--model-type", default="rf", help="Model type (rf or lr)")

    # MLflow параметры
    parser.add_argument("--tracking-uri", help="MLflow tracking URI")
    parser.add_argument("--experiment-name", default="fraud_detection", help="MLflow exp name")
    parser.add_argument("--auto-register", action="store_true", help="Automatically register")
    parser.add_argument("--run-name", default=None, help="Name for the MLflow run")

    # Отключение проверки Git для MLflow
    os.environ['GIT_PYTHON_REFRESH'] = 'quiet'

    # S3 параметры
    parser.add_argument("--s3-endpoint-url", help="S3 endpoint URL")
    parser.add_argument("--s3-access-key", help="S3 access key")
    parser.add_argument("--s3-secret-key", help="S3 secret key")

    args = parser.parse_args()
    print(f"DEBUG: Аргументы командной строки: {args}")

    # Настраиваем S3 конфигурацию
    s3_config = None
    if args.s3_endpoint_url and args.s3_access_key and args.s3_secret_key:
        print("DEBUG: Настраиваем S3 конфигурацию")
        s3_config = {
            'endpoint_url': args.s3_endpoint_url,
            'access_key': args.s3_access_key,
            'secret_key': args.s3_secret_key
        }
        # Устанавливаем переменные окружения для MLflow
        os.environ['AWS_ACCESS_KEY_ID'] = args.s3_access_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = args.s3_secret_key
        os.environ['MLFLOW_S3_ENDPOINT_URL'] = args.s3_endpoint_url
        print("DEBUG: Переменные окружения для S3 установлены")

    # Set MLflow tracking URI if provided
    if args.tracking_uri:
        print(f"DEBUG: Устанавливаем MLflow tracking URI: {args.tracking_uri}")
        mlflow.set_tracking_uri(args.tracking_uri)

    # Create or set the experiment
    print(f"DEBUG: Устанавливаем MLflow эксперимент: {args.experiment_name}")
    mlflow.set_experiment(args.experiment_name)

    # Create Spark session
    print("DEBUG: Создаем Spark сессию")
    spark = create_spark_session(s3_config).getOrCreate()
    print("DEBUG: Spark сессия создана")

    try:
        # Load and prepare data
        print("DEBUG: Загружаем данные")
        train_df, test_df = load_data(spark, args.input)

        print("DEBUG: Подготавливаем признаки")
        train_df, test_df, feature_cols = prepare_features(train_df, test_df)

        # Generate run name if not provided
        run_name = (
            args.run_name or f"fraud_detection_{args.model_type}_{os.path.basename(args.input)}"
        )
        print(f"DEBUG: Run name: {run_name}")

        # Train the model
        print("DEBUG: Обучаем модель")
        model, metrics = train_model(train_df, test_df, feature_cols, args.model_type, run_name)

        # Save the model locally
        print("DEBUG: Сохраняем модель")
        save_model(model, args.output)

        # Register model if requested
        if args.auto_register:
            print("DEBUG: Сравниваем и регистрируем модель")
            compare_and_register_model(metrics, args.experiment_name)

        print("Training completed successfully!")

    except Exception as e:
        print(f"ERROR: Ошибка во время обучения: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)
    finally:
        # Stop Spark session
        print("DEBUG: Останавливаем Spark сессию")
        spark.stop()
        print("DEBUG: Скрипт завершен")


if __name__ == "__main__":
    main()
