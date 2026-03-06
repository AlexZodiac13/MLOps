# Архитектура проекта для презентации

## 1. Общая схема

```mermaid
%%{init: {
  "theme": "base",
  "themeVariables": {
  "background": "#fcfaf6",
  "primaryColor": "#f6d8a8",
    "primaryTextColor": "#1f2937",
  "primaryBorderColor": "#a16207",
  "lineColor": "#334155",
  "secondaryColor": "#d7eafe",
  "tertiaryColor": "#d7f5df",
  "fontFamily": "Trebuchet MS",
  "clusterBkg": "#ffffff",
  "clusterBorder": "#cbd5e1"
  }
}}%%
flowchart LR
  user([Пользователь Telegram])

    subgraph runtime[Продуктовый контур]
    direction TB
    bot[Telegram Bot\naiogram]
    model[ML API\nFastAPI + llama.cpp]
    appdb[(PostgreSQL)]
    end

    subgraph mlops[MLOps контур]
    direction TB
    airflow[Airflow]
    train[Train / Test / Export]
    mlflow[MLflow]
    s3[(S3 / MinIO)]
    end

    subgraph infra[Инфраструктура и деплой]
    direction TB
        gha[GitHub Actions]
        tf[Terraform]
    runtimeenv[Docker Compose / Kubernetes]
    cloud[Yandex Cloud]
    end

  user --> bot
  bot -->|разбор текста| model
  bot -->|хранение| appdb
  bot -->|планирование и отправка| user

  airflow --> train --> mlflow
  train --> s3
    mlflow --> s3
  s3 -->|GGUF-модель| model

    gha --> tf
  gha --> runtimeenv
  tf --> cloud
  cloud --> runtimeenv
  runtimeenv --> runtime
  cloud --> mlops

  classDef runtime fill:#fde7b0,stroke:#a16207,color:#1f2937,stroke-width:2px;
  classDef mlops fill:#dbeafe,stroke:#1d4ed8,color:#1f2937,stroke-width:2px;
  classDef infra fill:#dcfce7,stroke:#15803d,color:#1f2937,stroke-width:2px;
  classDef data fill:#fee2e2,stroke:#b91c1c,color:#1f2937,stroke-width:2px;
  classDef actor fill:#fff,color:#111827,stroke:#64748b,stroke-width:2px;

  class bot,model runtime;
  class airflow,train,mlflow mlops;
  class gha,tf,runtimeenv,cloud infra;
    class appdb,s3 data;
  class user actor;
```

## 2. Что говорить по этой схеме

Проект состоит из трех уровней:

- Продуктовый контур: бот принимает сообщение, обращается к ML API для извлечения даты и текста, затем сохраняет напоминание в PostgreSQL.
- MLOps контур: Airflow запускает обучение, тестирование и экспорт модели в GGUF, а MLflow и S3 хранят метрики и артефакты; ML API загружает готовую модель из S3.
- Инфраструктурный контур: GitHub Actions автоматизирует тесты и деплой, Terraform и Ansible поднимают среду в Yandex Cloud, а Kubernetes или Docker Compose запускают сервисы.

## 3. Схема пайплайна обучения и доставки

```mermaid
%%{init: {
  "theme": "base",
  "themeVariables": {
    "background": "#fbfcfe",
    "primaryColor": "#d9defe",
    "primaryTextColor": "#111827",
    "primaryBorderColor": "#4338ca",
    "lineColor": "#334155",
    "fontFamily": "Trebuchet MS",
    "clusterBkg": "#ffffff",
    "clusterBorder": "#cbd5e1"
  }
}}%%
flowchart LR
    code[Репозиторий\nкод + датасеты]
    tests[CI-проверки]
    train[Airflow\nобучение модели]
    artifacts[MLflow + S3]
  storage[S3 / MinIO\nхранение GGUF]
    build[Сборка образов]
    registry[Docker Hub]
    infra[Terraform\nYandex Cloud]
    deploy[Docker Compose / Kubernetes]
  prod[Прод-сервис\nBot + Model + DB]

    code --> tests --> build
    code --> train --> artifacts
  train --> storage
  storage -->|модель| build
    build --> registry --> deploy --> prod
    infra --> deploy

    classDef ci fill:#e9d5ff,stroke:#7e22ce,color:#111827,stroke-width:2px;
    classDef train fill:#bae6fd,stroke:#0369a1,color:#111827,stroke-width:2px;
    classDef prod fill:#fcd34d,stroke:#b45309,color:#111827,stroke-width:2px;
    classDef infra fill:#bbf7d0,stroke:#15803d,color:#111827,stroke-width:2px;

    class tests,build,registry ci;
  class train,artifacts,storage train;
    class prod prod;
    class infra,deploy infra;
```

## 4. Как разложить по слайдам

Слайд 1. Идея проекта

- Telegram-бот для напоминаний.
- Пользователь пишет естественным языком.
- LLM извлекает дату, время и текст напоминания.

Слайд 2. Архитектура продукта

- Показать первую схему.
- Сделать акцент на связке Bot → Model API → PostgreSQL.

Слайд 3. MLOps пайплайн

- Показать Airflow, MLflow, S3 и экспорт GGUF.
- Подчеркнуть, что модель хранится в S3 и может переобучаться и выкатываться отдельно от бота.

Слайд 4. Инфраструктура и CI/CD

- Показать вторую схему.
- Отдельно выделить Terraform, Ansible, GitHub Actions и два варианта деплоя: VM и Kubernetes.

Слайд 5. Ценность проекта

- Автоматизация напоминаний для пользователя.
- Воспроизводимый ML-пайплайн.
- Инфраструктура как код.
- Наблюдаемость и мониторинг.

## 5. Короткий текст для защиты

Этот проект объединяет продуктовую часть и MLOps-практики в одной системе. Пользователь взаимодействует с Telegram-ботом, который обращается к ML API для извлечения структуры напоминания и сохраняет результат в PostgreSQL. Сама модель проходит через MLOps-контур: Airflow запускает обучение и экспорт, MLflow хранит результаты экспериментов, а готовая GGUF-модель сохраняется в S3-совместимом хранилище, откуда ее забирает ML API. Поверх этого настроены CI/CD и инфраструктура в Yandex Cloud, поэтому систему можно воспроизводимо развернуть как через Docker Compose, так и в Kubernetes.