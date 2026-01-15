#!/usr/bin/env python3
"""
Скрипт очистки транзакционных данных и выгрузки в S3.
Шаги:
  1) Инициализация Spark (с локальными JAR'ами в SPARK_JARS)
  2) Получение списка файлов в HDFS_INPUT_DIR
  3) По одному: читаем, чистим, append в HDFS_OUTPUT_DIR, удаляем исходный файл
  4) Финальное hadoop distcp HDFS_OUTPUT_DIR -> S3_OUTPUT_PATH
"""
import os
import sys
import subprocess
from pyspark.sql import SparkSession, functions as F, types as T

SPARK_JARS = "/usr/lib/spark/jars/hadoop-aws-3.3.4.jar,/usr/lib/spark/jars/aws-java-sdk-bundle-1.11.1026.jar"
# SPARK_PACKAGES removed — используем локальные JAR'ы, скачанные Ansible'ом

def clean_df(df_in):
    return df_in.withColumn(
        'tx_datetime',
        F.to_timestamp(
            F.regexp_replace('tx_datetime', ' 24:00:00$', ' 23:59:59'),
            'yyyy-MM-dd HH:mm:ss'
        )
    ).filter(
        (F.col('tx_fraud').isNotNull()) &
        (F.col('tx_amount') > 0) & (F.col('tx_amount') < 1e7) &
        (F.col('tx_time_seconds') >= 0) &
        (F.col('tx_time_days') >= 0) &
        (F.col('tx_datetime').isNotNull())
    ).dropDuplicates(['transaction_id'])

def list_hdfs_files(hdfs_input_dir):
    r = subprocess.run(['hdfs', 'dfs', '-ls', hdfs_input_dir], capture_output=True, text=True)
    files = []
    if r.returncode != 0:
        print('Не удалось получить список файлов в HDFS:', r.stderr[:300], file=sys.stderr)
        return files
    for ln in r.stdout.strip().splitlines():
        parts = ln.split()
        if len(parts) >= 8:
            files.append(parts[-1])
    return files

def main():
    # Параметры (подставляются .replace)
    HDFS_NAMENODE = '{HDFS_NAMENODE}'
    HDFS_INPUT_DIR = '{HDFS_INPUT_DIR}'
    HDFS_OUTPUT_DIR = '{HDFS_OUTPUT_DIR}'
    S3_OUTPUT_PATH = '{S3_OUTPUT_PATH}'
    S3_ACCESS_KEY = '{S3_ACCESS_KEY}'
    S3_SECRET_KEY = '{S3_SECRET_KEY}'

    # Инициализация Spark с оптимизированными настройками для s2.small (16GB RAM)
    # 16GB Node RAM -> ~12GB Available for YARN -> 1 Executor per node
    # Executor: 8g heap + 2g overhead = 10g total. Leaves ~2-6g for OS/Hadoop.
    spark = SparkSession.builder \
        .appName("clean_transactions") \
        .config('spark.jars', SPARK_JARS) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", "storage.yandexcloud.net") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.memoryOverhead", "2g") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "512m") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    schema = T.StructType([
        T.StructField('transaction_id', T.IntegerType(), nullable=False),
        T.StructField('tx_datetime', T.StringType(), nullable=False),
        T.StructField('customer_id', T.IntegerType(), nullable=False),
        T.StructField('terminal_id', T.IntegerType(), nullable=False),
        T.StructField('tx_amount', T.DoubleType(), nullable=False),
        T.StructField('tx_time_seconds', T.IntegerType(), nullable=False),
        T.StructField('tx_time_days', T.IntegerType(), nullable=False),
        T.StructField('tx_fraud', T.IntegerType(), nullable=False),
        T.StructField('tx_fraud_scenario', T.IntegerType(), nullable=False),
    ])

    files = list_hdfs_files(HDFS_INPUT_DIR)
    print(f'Files to process: {len(files)}')
    if not files:
        print('No files found in HDFS input dir, exiting')
        spark.stop()
        return

    # Очистим выходной каталог заранее (чтобы итог был заменой)
    subprocess.run(['hdfs','dfs','-rm','-r','-skipTrash', HDFS_OUTPUT_DIR], capture_output=True)

    processed = 0
    total_raw = 0
    total_clean = 0
    for src in files:
        try:
            print('Processing', src)
            if src.startswith('/'):
                reader = f'hdfs://{HDFS_NAMENODE}' + src
            else:
                reader = src
            df_raw = spark.read.option('header','false').option('sep',',').schema(schema).csv(reader)
            cnt_raw = df_raw.count()
            df_clean = clean_df(df_raw)
            cnt_clean = df_clean.count()
            # Используем coalesce(200) вместо 10, чтобы избежать OOM на больших данных (120GB)
            # 120GB / 200 = ~600MB на файл, что безопасно для 8GB heap
            df_clean.coalesce(200).write.format('parquet').mode('append').option('compression','snappy').save(f'hdfs://{HDFS_NAMENODE}{HDFS_OUTPUT_DIR}')
            # Только после успешной записи удаляем исходный файл
            subprocess.run(['hdfs','dfs','-rm', src], capture_output=True)
            print(f'{src}: {cnt_raw:,} -> {cnt_clean:,} processed')
            processed += 1
            total_raw += cnt_raw
            total_clean += cnt_clean
        except Exception as e:
            print(f'Error processing {src}: {str(e)[:400]}', file=sys.stderr)
            # не удаляем исходник при ошибке; переходим к следующему
            continue

    print('Processing complete')
    print(f'Files processed: {processed}/{len(files)}')
    print(f'Rows: {total_raw:,} -> {total_clean:,}')

    # Финальное distcp в S3
    try:
        print('Starting distcp to S3...')
        rc = subprocess.run(['hadoop','distcp','-overwrite','-m','64', f'hdfs://{HDFS_NAMENODE}{HDFS_OUTPUT_DIR}', S3_OUTPUT_PATH], capture_output=True, text=True, timeout=7200)
        if rc.returncode == 0:
            print('✓ distcp to S3 completed successfully')
        else:
            print('⚠ distcp failed:', rc.stderr[:400])
    except Exception as e:
        print('⚠ distcp exception:', str(e)[:400])

    spark.stop()

if __name__ == '__main__':
    main()