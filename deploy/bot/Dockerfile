FROM python:3.11-slim

WORKDIR /app

RUN sed -i 's/deb.debian.org/mirror.yandex.ru/g' /etc/apt/sources.list.d/debian.sources || true && \
    apt-get update -o Acquire::Retries=3 && \
    apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        tzdata && \
    rm -rf /var/lib/apt/lists/*


COPY src/requirements.txt ./src/requirements.txt
RUN pip install --no-cache-dir -r ./src/requirements.txt -i https://pypi.org/simple/ --extra-index-url https://mirrors.aliyun.com/pypi/simple/

# COPY .env .env

COPY src/ ./src/

CMD ["python", "./src/main.py"]
