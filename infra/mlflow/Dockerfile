FROM python:3.11.4-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip wheel setuptools
RUN pip install --no-cache-dir -r requirements.txt