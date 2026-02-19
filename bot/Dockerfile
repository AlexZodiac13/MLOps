# Используем официальный образ Python (3.9-slim)
FROM python:3.9-slim

# Задаем переменные окружения для корректной работы локали
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

# Обновляем списки пакетов и устанавливаем системные зависимости (gcc, libffi-dev)
RUN apt-get update && \
    apt-get install -y gcc libffi-dev && \
    rm -rf /var/lib/apt/lists/*

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файл зависимостей и обновляем pip
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Загружаем spaCy-модель для русского языка
RUN python -m spacy download ru_core_news_sm

# Копируем весь исходный код, включая папку config
COPY reminder_bot.py reminder_bot.py

# Запускаем бота
CMD ["python", "reminder_bot.py"]
