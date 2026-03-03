import os

k8s_dir = "/home/zodiac/stadygit/MLOps/infra/k8s"
os.makedirs(k8s_dir, exist_ok=True)

manifests = {}

manifests["00-config.yaml"] = """apiVersion: v1
kind: Namespace
metadata:
  name: mlops
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: infra-env
  namespace: mlops
data:
  AIRFLOW__CORE__EXECUTOR: CeleryExecutor
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
  AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres:5432/airflow
  AIRFLOW__CELERY__BROKER_URL: redis://redis:6379/1
  AIRFLOW__CORE__FERNET_KEY: 46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho=
  AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
  AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
  AIRFLOW__WEBSERVER__SECRET_KEY: 'secret_key_secret_key_secret_key_secret_key_secret_key_secret_key'
  AIRFLOW__WEBSERVER__BASE_URL: 'http://owgrant.home/airflow'
  MLFLOW_TRACKING_URI: http://mlflow:5000
---
apiVersion: v1
kind: Secret
metadata:
  name: infra-secrets
  namespace: mlops
type: Opaque
stringData:
  AWS_ACCESS_KEY_ID: "YOUR_AWS_KEY"
  AWS_SECRET_ACCESS_KEY: "YOUR_AWS_SECRET"
  AWS_DEFAULT_REGION: "us-east-1"
  MLFLOW_S3_ENDPOINT_URL: "https://storage.yandexcloud.net"
  MLFLOW_S3_BUCKET: "YOUR_BUCKET"
  HF_TOKEN: "YOUR_HF_TOKEN"
  GIT_REPO_URL: "YOUR_GIT_REPO"
  GIT_BRANCH: "main"
  POSTGRES_USER: "airflow"
  POSTGRES_PASSWORD: "airflow"
  POSTGRES_DB: "airflow"
"""

manifests["01-databases.yaml"] = """apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-init-script
  namespace: mlops
data:
  01-create-mlflow.sh: |
    #!/bin/bash
    set -e
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
      CREATE DATABASE mlflow_db;
    EOSQL
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: mlops
spec:
  ports:
    - port: 5432
  selector:
    app: postgres
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
  namespace: mlops
spec:
  serviceName: "postgres"
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
        - name: postgres
          image: postgres:14
          envFrom:
            - secretRef:
                name: infra-secrets
          ports:
            - containerPort: 5432
          volumeMounts:
            - name: postgres-data
              mountPath: /var/lib/postgresql/data
            - name: init-script
              mountPath: /docker-entrypoint-initdb.d/01-create-mlflow.sh
              subPath: 01-create-mlflow.sh
      volumes:
        - name: init-script
          configMap:
            name: postgres-init-script
            defaultMode: 0755
  volumeClaimTemplates:
    - metadata:
        name: postgres-data
      spec:
        accessModes: [ "ReadWriteOnce" ]
        resources:
          requests:
            storage: 20Gi
---
apiVersion: v1
kind: Service
metadata:
  name: redis
  namespace: mlops
spec:
  ports:
    - port: 6379
  selector:
    app: redis
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
  namespace: mlops
spec:
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
        - name: redis
          image: redis:latest
          ports:
            - containerPort: 6379
          livenessProbe:
            exec:
              command: ["redis-cli", "ping"]
            initialDelaySeconds: 10
            periodSeconds: 10
"""

manifests["02-mlflow.yaml"] = """apiVersion: v1
kind: Service
metadata:
  name: mlflow
  namespace: mlops
spec:
  ports:
    - port: 5000
  selector:
    app: mlflow
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mlflow
  namespace: mlops
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mlflow
  template:
    metadata:
      labels:
        app: mlflow
    spec:
      containers:
        - name: mlflow
          image: DOCKER_USERNAME/mlops-airflow:latest
          command: ["/bin/bash", "-c"]
          args:
            - >
              mlflow server
              --backend-store-uri postgresql://airflow:airflow@postgres:5432/mlflow_db
              --default-artifact-root s3://$(MLFLOW_S3_BUCKET)/ml-artifacts
              --host 0.0.0.0
              --port 5000
              --workers 4
              --serve-artifacts
              --allowed-hosts '*'
          ports:
            - containerPort: 5000
          envFrom:
            - configMapRef:
                name: infra-env
            - secretRef:
                name: infra-secrets
          env:
            - name: GUNICORN_CMD_ARGS
              value: "--bind=0.0.0.0:5000 --forwarded-allow-ips='*'"
"""

manifests["03-airflow.yaml"] = """apiVersion: batch/v1
kind: Job
metadata:
  name: airflow-init
  namespace: mlops
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: airflow-init
          image: DOCKER_USERNAME/mlops-airflow:latest
          command: ["version"]
          envFrom:
            - configMapRef:
                name: infra-env
            - secretRef:
                name: infra-secrets
          env:
            - name: _AIRFLOW_DB_UPGRADE
              value: 'true'
            - name: _AIRFLOW_WWW_USER_CREATE
              value: 'true'
            - name: _AIRFLOW_WWW_USER_USERNAME
              value: "airflow"
            - name: _AIRFLOW_WWW_USER_PASSWORD
              value: "airflow"
---
apiVersion: v1
kind: Service
metadata:
  name: airflow-webserver
  namespace: mlops
spec:
  ports:
    - port: 8080
      targetPort: 8080
  selector:
    app: airflow-webserver
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-webserver
  namespace: mlops
spec:
  replicas: 1
  selector:
    matchLabels:
      app: airflow-webserver
  template:
    metadata:
      labels:
        app: airflow-webserver
    spec:
      containers:
        - name: webserver
          image: DOCKER_USERNAME/mlops-airflow:latest
          command: ["webserver"]
          ports:
            - containerPort: 8080
          envFrom:
            - configMapRef:
                name: infra-env
            - secretRef:
                name: infra-secrets
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-scheduler
  namespace: mlops
spec:
  replicas: 1
  selector:
    matchLabels:
      app: airflow-scheduler
  template:
    metadata:
      labels:
        app: airflow-scheduler
    spec:
      containers:
        - name: scheduler
          image: DOCKER_USERNAME/mlops-airflow:latest
          command: ["scheduler"]
          envFrom:
            - configMapRef:
                name: infra-env
            - secretRef:
                name: infra-secrets
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-worker
  namespace: mlops
spec:
  replicas: 1
  selector:
    matchLabels:
      app: airflow-worker
  template:
    metadata:
      labels:
        app: airflow-worker
    spec:
      containers:
        - name: worker
          image: DOCKER_USERNAME/mlops-airflow:latest
          command: ["celery", "worker"]
          envFrom:
            - configMapRef:
                name: infra-env
            - secretRef:
                name: infra-secrets
"""

manifests["04-nginx.yaml"] = """apiVersion: v1
kind: ConfigMap
metadata:
  name: nginx-conf
  namespace: mlops
data:
  nginx.conf: |
    events {}
    http {
      server {
        listen 80;
        location /airflow/ {
          proxy_pass http://airflow-webserver:8080/;
          proxy_set_header Host $http_host;
        }
        location /mlflow/ {
          proxy_pass http://mlflow:5000/;
          proxy_set_header Host $http_host;
        }
      }
    }
---
apiVersion: v1
kind: Service
metadata:
  name: nginx
  namespace: mlops
  # Использование LoadBalancer для внешнего доступа к Nginx в Yandex k8s
  type: LoadBalancer 
spec:
  ports:
    - port: 80
      targetPort: 80
  selector:
    app: nginx
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx
  namespace: mlops
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
        - name: nginx
          image: nginx:latest
          ports:
            - containerPort: 80
          volumeMounts:
            - name: nginx-config
              mountPath: /etc/nginx/nginx.conf
              subPath: nginx.conf
      volumes:
        - name: nginx-config
          configMap:
            name: nginx-conf
"""

for filename, content in manifests.items():
    with open(os.path.join(k8s_dir, filename), "w") as f:
        f.write(content.strip() + "\\n")

print("Generated manifests successfully.")
