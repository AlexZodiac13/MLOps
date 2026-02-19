#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Telegram‑бот‑напоминалка с поддержкой одноразовых и повторяющихся напоминаний,
обрабатывающая естественный язык с учётом склонений для дней недели и поддержкой
ежемесячного повторения (цифрового и порядкового).

Примеры запросов:
  • "завтра в 12:15 купить хлеба"
  • "через 5 минут проверить бота"
  • "каждый понедельник в 8-00 Завтрак"
  • "каждый день в 8-00 Медитация"
  • "каждое 15 число в 10:00 оплатить счета"
  • "каждое 1-е число месяца в 12:00 заплатить за аренду"
  • "8-15 сделать зарядку"

Если время не указано, используется время получения.
Новые пользователи получают таймзону по умолчанию ("Europe/Moscow").
"""

import asyncio
import logging
import re
from datetime import datetime, timedelta

import pytz
import configparser
import asyncpg
import spacy
from dateparser.search import search_dates
from dateutil.relativedelta import relativedelta
import calendar

from urllib.parse import quote, unquote
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from timezonefinder import TimezoneFinder
from datetime import datetime

TIMEZONE_LIST = [
    ("UTC−12", "Etc/GMT+12"),
    ("UTC−9", "Etc/GMT+9"),
    ("UTC−8", "Etc/GMT+8"),
    ("UTC−7", "Etc/GMT+7"),
    ("UTC−6", "Etc/GMT+6"),
    ("UTC−5", "Etc/GMT+5"),
    ("UTC−4", "Etc/GMT+4"),
    ("UTC−3", "Etc/GMT+3"),
    ("UTC−2", "Etc/GMT+2"),
    ("UTC−1", "Etc/GMT+1"),
    ("UTC±0", "Etc/GMT+0"),
    ("UTC+1", "Etc/GMT-1"),
    ("UTC+2", "Etc/GMT-2"),
    ("UTC+3 (Москва)", "Europe/Moscow"),
    ("UTC+4", "Etc/GMT-4"),
    ("UTC+5", "Etc/GMT-5"),
    ("UTC+6", "Etc/GMT-6"),
    ("UTC+7", "Etc/GMT+7"),
    ("UTC+8", "Etc/GMT-8"),
    ("UTC+9", "Etc/GMT-9"),
    ("UTC+10", "Etc/GMT-10"),
    ("UTC+11", "Etc/GMT-11"),
    ("UTC+12", "Etc/GMT-12"),
]

# Инициализация TimezoneFinder
tf = TimezoneFinder()

def get_timezone_choice_keyboard():
    """Клавиатура с двумя кнопками: определить по геолокации или выбрать вручную"""
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("📍 Определить автоматически"))
    kb.row(KeyboardButton("🕒 Выбрать вручную"))
    return kb

def get_location_request_keyboard():
    """Клавиатура с кнопкой запроса геолокации"""
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("📍 Отправить геолокацию", request_location=True))
    return kb

def get_timezone_manual_keyboard():
    """Клавиатура со списком таймзон"""
    kb = InlineKeyboardMarkup(row_width=3)
    for label, tz in TIMEZONE_LIST:
        kb.insert(InlineKeyboardButton(text=label, callback_data=f"tz:{tz}"))
    return kb

async def get_timezone_by_location(latitude: float, longitude: float) -> str:
    """Определяет таймзону по координатам.
    
    Args:
        latitude: Широта (-90 до 90)
        longitude: Долгота (-180 до 180)
        
    Returns:
        str: Название таймзоны или Europe/Moscow в случае ошибки
    """
    try:
        # Валидация координат
        if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
            logging.error(f"Некорректные координаты: lat={latitude}, lng={longitude}")
            return "Europe/Moscow"
            
        # Пробуем получить таймзону по точным координатам
        timezone_str = tf.timezone_at(lat=latitude, lng=longitude)
        if timezone_str:
            logging.info(f"Таймзона определена точно: {timezone_str} для координат ({latitude}, {longitude})")
            return timezone_str
            
        # Если точное определение не удалось, ищем ближайшую таймзону
        timezone_str = tf.closest_timezone_at(lat=latitude, lng=longitude)
        if timezone_str:
            logging.info(f"Найдена ближайшая таймзона: {timezone_str} для координат ({latitude}, {longitude})")
            return timezone_str
            
        # Если и это не помогло, используем умный фолбэк на основе долготы
        # Примерно определяем UTC смещение по долготе (15 градусов = 1 час)
        utc_offset = round(longitude / 15)
        if utc_offset > 0:
            fallback = f"Etc/GMT-{min(utc_offset, 12)}"
        else:
            fallback = f"Etc/GMT+{min(abs(utc_offset), 12)}"
            
        logging.warning(f"Используем фолбэк таймзону {fallback} для координат ({latitude}, {longitude})")
        return fallback
            
    except Exception as e:
        logging.error(f"Ошибка определения таймзоны: {e}, координаты: ({latitude}, {longitude})")
        return "Europe/Moscow"

logging.basicConfig(level=logging.INFO)

# Загружаем конфигурацию
config = configparser.ConfigParser()
config.read("config/config.ini")
BOT_TOKEN = config.get("bot", "token")
ADMIN_ID = int(config.get("bot", "admin_id"))
PG_DSN = config.get("postgres", "dns")
# Флаг включения сбора сообщений (можно выключить в config.ini)
COLLECT_MESSAGES = config.getboolean("logging", "collect_messages", fallback=True)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

BOT_USERNAME = ""
# DB_PATH = "config/reminder.db"
nlp = spacy.load("ru_core_news_sm")

pg_pool = None  # глобальный пул соединений

# Словарь вариантов склонения для дней недели
WEEKDAYS_VARIANTS = {
    "понедельник":   ["понедельник", "понедельника", "понедельнику", "понедельником", "понедельнике"],
    "вторник":       ["вторник", "вторника", "вторнику", "вторником", "вторнике"],
    "среда":         ["среда", "среды", "среде", "средой", "среде", "среду"],
    "четверг":       ["четверг", "четверга", "четвергу", "четвергом", "четверге"],
    "пятница":       ["пятница", "пятницы", "пятнице", "пятницей", "пятнице", "пятницу"],
    "суббота":       ["суббота", "субботы", "субботе", "субботой", "субботе", "субботу"],
    "воскресенье":   ["воскресенье", "воскресенья", "воскресенью", "воскресеньем", "воскресенье"]
}


async def init_db():
    global pg_pool
    pg_pool = await asyncpg.create_pool(PG_DSN)
    async with pg_pool.acquire() as conn:
        # Создаем основные таблицы
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reminders (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                user_reminder_id INTEGER,
                chat_id BIGINT,
                reminder_text TEXT,
                scheduled_time TIMESTAMPTZ,
                recurrence_type TEXT,
                recurrence_value TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                chat_id BIGINT,
                timezone TEXT
            )
        """)
        
        # Безопасно добавляем новые столбцы если их еще нет
        try:
            await conn.execute("""
                ALTER TABLE users 
                ADD COLUMN IF NOT EXISTS latitude FLOAT,
                ADD COLUMN IF NOT EXISTS longitude FLOAT,
                ADD COLUMN IF NOT EXISTS last_location_update TIMESTAMPTZ DEFAULT now()
            """)
            logging.info("Columns for geolocation were successfully added to users table")
        except Exception as e:
            logging.error(f"Error adding geolocation columns: {e}")
        # Таблица для хранения собранных сообщений пользователей
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_messages (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                chat_id BIGINT,
                message_text TEXT,
                message_type TEXT,
                created_at TIMESTAMPTZ DEFAULT now()
            )
        """)


async def log_user_message(message: types.Message):
    """Сохраняет сообщение пользователя в таблицу user_messages.
    Не сохраняет команды (начинающиеся с '/'). Уважает флаг COLLECT_MESSAGES.
    """
    if not COLLECT_MESSAGES:
        return
    try:
        text = message.text or getattr(message, "caption", "") or ""
        # Не логируем команды
        if text and text.strip().startswith("/"):
            return
        msg_type = getattr(message, "content_type", "text")
        user_id = message.from_user.id if message.from_user else None
        chat_id = message.chat.id if message.chat else None
        async with pg_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO user_messages (user_id, chat_id, message_text, message_type) VALUES ($1, $2, $3, $4)",
                user_id, chat_id, text, msg_type
            )
    except Exception as e:
        logging.error(f"Не удалось сохранить сообщение пользователя: {e}")


def parse_recurrence(text: str):
    """
    Анализирует текст запроса для определения повторения напоминания.
    Поддерживаемые варианты:
    • Ежедневное: "каждый день" или "каждые N дня".
    • Еженедельное: шаблон "(каждый|каждая|каждое) <день недели>" (с учетом склонений).
    • Ежемесячное: "каждое X число", где X – цифра или порядковое числительное.
    """
    text_lower = text.lower()
    if not any(x in text_lower for x in ["каждый", "каждые", "каждое", "каждую"]):
        return None

    # Ежедневное повторение
    if re.search(r"(?i)\b(каждый|каждая|каждое|каждую)\s+день\b", text_lower):
        return {"type": "daily", "interval": 1}

    m = re.search(r"(?i)\bкаждые\s+(\d+)\s+дня\b", text_lower)
    if m:
        return {"type": "daily", "interval": int(m.group(1))}

    # Еженедельное повторение
    for canonical, variants in WEEKDAYS_VARIANTS.items():
        weekday_pattern = "|".join(variants)
        m = re.search(rf"(?i)\b(каждый|каждая|каждое|каждую)\s+({weekday_pattern})\b", text_lower)
        if m:
            weekdays = {
                "понедельник": 0, "вторник": 1, "среда": 2,
                "четверг": 3, "пятница": 4, "суббота": 5, "воскресенье": 6
            }
            return {"type": "weekly", "weekday": weekdays[canonical]}

    # Ежемесячное повторение
    m = re.search(r"(?i)\bкаждое\s+(\d{1,2}|[а-яё]+)\s+число\b", text_lower)
    if m:
        value = m.group(1)
        if value.isdigit():
            day = int(value)
        else:
            ordinal_map = {
                "первое": 1, "первого": 1,
                "второе": 2, "второго": 2,
                "третье": 3, "третьего": 3,
                "четвертое": 4, "четвертого": 4,
                "пятое": 5, "пятого": 5,
                "шестое": 6, "шестого": 6,
                "седьмое": 7, "седьмого": 7,
                "восьмое": 8, "восьмого": 8,
                "девятое": 9, "девятого": 9,
                "десятое": 10, "десятого": 10,
            }
            day = ordinal_map.get(value, 1)
        return {"type": "monthly", "day": day}

    return None


def parse_reminder(text: str, user_timezone: str):
    """
    Извлекает время напоминания, описание и параметры повторения из запроса.
    
    Шаги:
      1. Заменяет форматы вида "8-15" на "8:15".
      2. Определяет параметры повторения (recurrence) сразу.
      3. Если в запросе имеется явный формат даты (DD.MM или DD.MM.YYYY), дата извлекается вручную с учётом времени.
      4. Если явный формат не найден, применяется search_dates для парсинга даты.
      5. Если дата не извлечена, производится явное извлечение времени (формат "в HH:MM") с использованием сегодняшней даты.
      6. Для еженедельного повторения корректируется дата с установкой следующего требуемого дня недели.
      7. Для ежемесячного повторения применяется специальная логика: с учётом указанного дня месяца и времени, плюс переход в следующий месяц, если дата уже прошла.
    """
    text = re.sub(r'(\d{1,2})-(\d{2})', r'\1:\2', text)
    now = datetime.now(pytz.timezone(user_timezone))
    original_text = text.strip()

    # Определяем параметры повторения
    recurrence = parse_recurrence(text)

    # Если повтор найден, удаляем соответствующие фразы для парсинга
    if recurrence:
        if recurrence["type"] == "daily":
            original_text = re.sub(r"(?i)\b(каждый|каждая|каждое|каждую)\s+день\b", "", original_text).strip()
            original_text = re.sub(r"(?i)\bкаждые\s+\d+\s+дня\b", "", original_text).strip()
        elif recurrence["type"] == "weekly":
            # Собираем все склонения дней недели
            weekday_variants = []
            for variants in WEEKDAYS_VARIANTS.values():
                weekday_variants.extend(variants)
            weekday_pattern = "|".join(weekday_variants)
            every_forms = "каждый|каждая|каждое|каждую|каждой"
            original_text = re.sub(
                rf"(?i)\b({every_forms})\s+({weekday_pattern})\b", "", original_text
            ).strip()
            # Удаляем отдельно стоящее "каждый/каждая/каждое/каждую/каждой" в начале строки (если вдруг осталось)
            original_text = re.sub(rf"(?i)^\s*({every_forms})\b", "", original_text).strip()
        elif recurrence["type"] == "monthly":
            original_text = re.sub(r"(?i)\bкаждое\s+(\d{1,2}|[а-яё]+)\s+число\b", "", original_text).strip()

    # Удаляем лишние пробелы после обработки
    original_text = re.sub(r"\s+", " ", original_text).strip()

    # --- Новый блок явного парсинга даты (DD.MM или DD.MM.YYYY) ---
    date_match = re.search(r'\b(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\b', original_text)
    if date_match:
        day = int(date_match.group(1))
        month = int(date_match.group(2))
        tz = pytz.timezone(user_timezone)
        if date_match.group(3):
            year = int(date_match.group(3))
            if year < 100:
                year += 2000
        else:
            year = now.year
        # Парсим время, если указано
        time_match = re.search(r'(?i)\bв\s*(\d{1,2})[:](\d{2})\b', original_text)
        if time_match:
            hour = int(time_match.group(1))
            minute = int(time_match.group(2))
        else:
            hour = now.hour
            minute = now.minute
        try:
            candidate = tz.localize(datetime(year, month, day, hour, minute))
        except Exception:
            candidate = now
        if candidate <= now:
            try:
                candidate = tz.localize(datetime(year + 1, month, day, hour, minute))
            except Exception:
                candidate = now
        # Удаляем найденную дату и время из текста для оставшейся части
        original_text = re.sub(r'\b(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\b', "", original_text, count=1)
        original_text = re.sub(r'(?i)\bв\s*\d{1,2}[:]\d{2}\b', "", original_text, count=1).strip()
        scheduled_time = candidate
        reminder_text = original_text or "Без описания"
        return scheduled_time.astimezone(pytz.utc), reminder_text, recurrence
    # --- Конец нового блока ---

    # --- Новый блок: "в <день недели>" без "каждый" ---
    weekday_map = {
        "понедельник": 0, "вторник": 1, "среда": 2, "четверг": 3,
        "пятница": 4, "суббота": 5, "воскресенье": 6
    }
    weekday_variants = []
    for canonical, variants in WEEKDAYS_VARIANTS.items():
        for v in variants:
            weekday_variants.append((v, weekday_map[canonical]))
    weekday_pattern = "|".join([re.escape(v[0]) for v in weekday_variants])
    weekday_match = re.search(rf'(?i)\bв\s+({weekday_pattern})\b', original_text)
    if weekday_match:
        found_weekday = weekday_match.group(1).lower()
        # Найти номер дня недели
        for v, idx in weekday_variants:
            if v == found_weekday:
                target_weekday = idx
                break
        else:
            target_weekday = None
        if target_weekday is not None:
            tz = pytz.timezone(user_timezone)
            candidate = now
            days_ahead = (target_weekday - now.weekday() + 7) % 7
            if days_ahead == 0:
                days_ahead = 7
            candidate = candidate + timedelta(days=days_ahead)
            # Парсим время, если указано
            time_match = re.search(r'(?i)\bв\s*(\d{1,2})[:\-](\d{2})\b', original_text)
            if time_match:
                hour = int(time_match.group(1))
                minute = int(time_match.group(2))
            else:
                hour = now.hour
                minute = now.minute
            candidate = candidate.replace(hour=hour, minute=minute, second=0, microsecond=0)
            candidate = tz.localize(candidate.replace(tzinfo=None))
            # Удаляем найденный день недели и время из текста
            original_text = re.sub(rf'\bв\s*{found_weekday}\b', '', original_text, count=1, flags=re.IGNORECASE)
            original_text = re.sub(r'(?i)\bв\s*\d{{1,2}}[:\-]\d{{2}}\b', '', original_text, count=1).strip()
            scheduled_time = candidate
            reminder_text = original_text or "Без описания"
            return scheduled_time.astimezone(pytz.utc), reminder_text, recurrence

    # Если явный формат даты не найден – пробуем search_dates
    found_dates = search_dates(
        original_text,
        settings={
            'RELATIVE_BASE': now,
            'PREFER_DATES_FROM': 'future',
            'DATE_ORDER': 'DMY'
        }
    )
    if found_dates:
        found_str, dt = found_dates[0]
        if dt.tzinfo is None:
            dt = pytz.timezone(user_timezone).localize(dt)
        # Если время не указано (00:00), а в тексте нет явного времени — подставляем текущее время
        if dt.hour == 0 and dt.minute == 0 and not re.search(r'(?i)\bв\s*\d{1,2}[:\-]\d{2}\b', original_text):
            now = datetime.now(pytz.timezone(user_timezone))
            dt = dt.replace(hour=now.hour, minute=now.minute, second=0, microsecond=0)
        scheduled_time = dt
        reminder_text = original_text.replace(found_str, "").strip() or "Без описания"
    else:
        # Фолбэк – пытаемся извлечь только время
        time_match = re.search(r'(?i)\bв\s*(\d{1,2})[:](\d{2})\b', original_text)
        if time_match:
            hour = int(time_match.group(1))
            minute = int(time_match.group(2))
            candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if candidate <= now:
                candidate += timedelta(days=1)
            scheduled_time = candidate
            reminder_text = re.sub(r'(?i)\bв\s*\d{1,2}[:]\d{2}\b', '', original_text).strip() or "Без описания"
        else:
            scheduled_time = now
            reminder_text = original_text

    # Обработка еженедельного повторения: корректировка даты до нужного дня недели
    if recurrence and recurrence.get("type") == "weekly":
        target_weekday = recurrence["weekday"]
        # Если время не указано (т.е. scheduled_time совпадает с now по времени), выставляем текущее время
        if scheduled_time.hour == 0 and scheduled_time.minute == 0 and (
            not re.search(r'(?i)\bв\s*\d{1,2}[:\-]\d{2}\b', text)
        ):
            scheduled_time = scheduled_time.replace(hour=now.hour, minute=now.minute, second=0, microsecond=0)
        days_ahead = (target_weekday - scheduled_time.weekday() + 7) % 7
        if days_ahead == 0 and scheduled_time <= now:
            days_ahead = 7
        scheduled_time += timedelta(days=days_ahead)

    # Обработка ежемесячного повторения: устанавливаем дату с указанным днем месяца.
    if recurrence and recurrence.get("type") == "monthly":
        # Извлекаем время из исходного текста (если задано)
        tz = pytz.timezone(user_timezone)
        time_match = re.search(r'(?i)\bв\s*(\d{1,2})[:](\d{2})\b', text)
        if time_match:
            hour = int(time_match.group(1))
            minute = int(time_match.group(2))
        else:
            hour, minute = scheduled_time.hour, scheduled_time.minute

        now_tz = datetime.now(tz)
        target_day = recurrence.get("day")
        try:
            candidate = now_tz.replace(day=target_day, hour=hour, minute=minute, second=0, microsecond=0)
        except ValueError:
            last_day = calendar.monthrange(now_tz.year, now_tz.month)[1]
            candidate = now_tz.replace(day=last_day, hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= now_tz:
            candidate += relativedelta(months=1)
            try:
                candidate = candidate.replace(day=target_day, hour=hour, minute=minute, second=0, microsecond=0)
            except ValueError:
                last_day = calendar.monthrange(candidate.year, candidate.month)[1]
                candidate = candidate.replace(day=last_day, hour=hour, minute=minute, second=0, microsecond=0)
        scheduled_time = candidate

    # Для ежедневного повторения, если время уже прошло – корректируем до будущего момента
    if recurrence and recurrence.get("type") == "daily" and scheduled_time <= now:
        interval = int(recurrence.get("interval", 1))
        while scheduled_time <= now:
            scheduled_time += timedelta(days=interval)

    scheduled_time_utc = scheduled_time.astimezone(pytz.utc)
    return scheduled_time_utc, reminder_text, recurrence


def compute_next_time(old_time: datetime, recurrence: dict):
    now = datetime.now(pytz.utc)
    new_time = old_time
    if recurrence["type"] == "daily":
        interval = int(recurrence.get("interval", 1))
        new_time += timedelta(days=interval)
        while new_time <= now:
            new_time += timedelta(days=interval)
    elif recurrence["type"] == "weekly":
        new_time += timedelta(weeks=1)
        while new_time <= now:
            new_time += timedelta(weeks=1)
    elif recurrence["type"] == "monthly":
        new_time += relativedelta(months=1)
        while new_time <= now:
            new_time += relativedelta(months=1)
    return new_time


# ...existing code...
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import timedelta

# Глобальный словарь для хранения текста напоминаний на короткое время
REMINDER_TEXT_CACHE = {}
BATCH_SIZE = 20  # Количество напоминаний в одной пачке
BATCH_PAUSE = 1  # Пауза между пачками в секундах

async def reminder_checker():
    global REMINDER_TEXT_CACHE
    while True:
        now = datetime.now(pytz.utc)
        async with pg_pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM reminders WHERE scheduled_time <= $1", now)
            rows = list(rows)
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i+BATCH_SIZE]
                tasks = []
                for row in batch:
                    try:
                        # Сохраняем текст напоминания в кэш по id
                        REMINDER_TEXT_CACHE[row['id']] = row['reminder_text']
                        # Создаём клавиатуру с кнопками
                        kb = InlineKeyboardMarkup(row_width=3)
                        kb.add(
                            InlineKeyboardButton(
                                text="5m",
                                callback_data=f"snooze:5:{row['id']}"
                            ),
                            InlineKeyboardButton(
                                text="15m",
                                callback_data=f"snooze:15:{row['id']}"
                            ),
                            InlineKeyboardButton(
                                text="⭐",
                                callback_data=f"donate:{row['id']}"
                            )
                        )
                        # Добавляем задачу отправки сообщения
                        tasks.append(
                            bot.send_message(
                                row["chat_id"],
                                f"⏰ Напоминание: {row['reminder_text']}",
                                reply_markup=kb
                            )
                        )

                        # Если напоминание повторяющееся — вычисляем следующее время и сохраняем снова
                        if row["recurrence_type"]:
                            recurrence = {}
                            if row["recurrence_type"] == "daily":
                                recurrence = {"type": "daily", "interval": int(row["recurrence_value"])}
                            elif row["recurrence_type"] == "weekly":
                                recurrence = {"type": "weekly", "weekday": int(row["recurrence_value"])}
                            elif row["recurrence_type"] == "monthly":
                                recurrence = {"type": "monthly", "day": int(row["recurrence_value"])}
                            next_time = compute_next_time(row["scheduled_time"], recurrence)
                            await conn.execute(
                                """
                                INSERT INTO reminders (user_id, user_reminder_id, chat_id, reminder_text, scheduled_time, recurrence_type, recurrence_value)
                                VALUES ($1, $2, $3, $4, $5, $6, $7)
                                """,
                                row["user_id"],
                                row["user_reminder_id"],
                                row["chat_id"],
                                row["reminder_text"],
                                next_time,
                                row["recurrence_type"],
                                row["recurrence_value"]
                            )

                        # Удаляем напоминание из БД
                        await conn.execute("DELETE FROM reminders WHERE id = $1", row["id"])
                    except Exception as e:
                        logging.error(f"Ошибка при отправке напоминания: {e}")
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                if i + BATCH_SIZE < len(rows):
                    await asyncio.sleep(BATCH_PAUSE)
        await asyncio.sleep(15)


def get_reply_keyboard(user_registered: bool):
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    if user_registered:
        keyboard.add("📝 Мои напоминания", "ℹ️ Помощь")
    else:
        keyboard.add("🚀 Начать")
    return keyboard


@dp.message_handler(commands=['start'], chat_type=types.ChatType.PRIVATE)
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT timezone FROM users WHERE user_id = $1", user_id)
    
    if not row:
        await message.reply(
            "Добро пожаловать! Давайте настроим ваш часовой пояс, "
            "чтобы напоминания приходили вовремя.\n\n"
            "Выберите способ настройки:",
            reply_markup=get_timezone_choice_keyboard()
        )
    else:
        kb = get_reply_keyboard(True)
        await message.reply(
            "С возвращением! Чем могу помочь?",
            reply_markup=kb
        )


@dp.message_handler(commands=['time'], chat_type=types.ChatType.PRIVATE)
async def cmd_time(message: types.Message):
    user_id = message.from_user.id
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT timezone FROM users WHERE user_id = $1", user_id)
    
    if not row:
        await message.reply(
            "⚠️ Часовой пояс не установлен. Используйте /start для настройки."
        )
        return

    tz = pytz.timezone(row['timezone'])
    now = datetime.now(tz)
    
    # Получаем смещение от UTC в часах
    offset = now.utcoffset().total_seconds() / 3600
    offset_str = f"+{offset:g}" if offset >= 0 else f"{offset:g}"
    
    # Форматируем вывод
    await message.reply(
        f"🕒 Ваше текущее время: {now.strftime('%H:%M:%S')}\n"
        f"📅 Дата: {now.strftime('%d.%m.%Y')}\n"
        f"🌍 Часовой пояс: {row['timezone']} (UTC{offset_str})"
    )

@dp.message_handler(commands=['help'], chat_type=types.ChatType.PRIVATE)
async def cmd_help(message: types.Message):
    help_text = (
        "📝 Помощь по боту\n\n"
        "Этот бот помогает вам создавать напоминания с использованием естественного языка. Вы можете задать как одноразовое, так и повторяющееся напоминание – ежедневное, еженедельное или ежемесячное.\n\n"
        "Примеры запросов:\n\n"
        "◼ Одноразовые напоминания:\n"
        "  • \"завтра в 12:15 купить хлеба\"\n"
        "  • \"через 5 минут проверить бота\"\n"
        "  • \"8:15 сделать зарядку\"\n\n"
        "◼ Повторяющиеся напоминания:\n"
        "  • \"каждый понедельник в 8-00 Завтрак\"\n"
        "     → Будет установлено напоминание на ближайший понедельник в 8:00.\n"
        "  • \"каждый день в 8-00 Медитация\"\n"
        "     → Ежедневное напоминание в 8:00 (если заданное время уже прошло – с завтрашнего дня).\n"
        "  • \"каждое 15 число в 10:00 оплатить счета\"\n"
        "     → Напоминание будет запускаться каждый месяц в 10:00 в 15-е число. Если 15-е уже прошло в текущем месяце, будет установлено на 15-е следующего месяца.\n"
        "  • \"каждое 1-е число месяца в 12:00 заплатить за аренду\"\n"
        "     → Напоминание запланируется на 1-е число каждого месяца в 12:00. Если эта дата уже прошла – установится 1-е числа следующего месяца.\n\n"
        "◼ Управление напоминаниями:\n"
        "  • /myreminders – выводит список ваших активных напоминаний (отсортированных по дате). Для каждого напоминания отображается его номер, время и описание; справа расположена кнопка удаления (❌).\n"
        "     Нажмите на кнопку удаления для удаления конкретного напоминания.\n"
        "  • /settimezone – Смена часового пояса\n"
        "  • /time – Показать текущее время и часовой пояс\n\n"
    )
    await message.reply(help_text)


@dp.message_handler(commands=['settimezone'], chat_type=types.ChatType.PRIVATE)
async def cmd_settimezone(message: types.Message):
    text = (
        "Как бы вы хотели установить часовой пояс?\n\n"
        "📍 Автоматически - я определю ваш часовой пояс по геолокации\n"
        "🕒 Вручную - вы выберете из списка доступных часовых поясов"
    )
    await message.reply(text, reply_markup=get_timezone_choice_keyboard())

@dp.message_handler(lambda message: message.text == "📍 Определить автоматически", chat_type=types.ChatType.PRIVATE)
async def process_timezone_auto(message: types.Message):
    text = (
        "📍 Для определения вашего часового пояса, пожалуйста, "
        "отправьте вашу геолокацию, нажав на кнопку ниже.\n\n"
        "❓ Это нужно только один раз - для настройки правильного "
        "времени напоминаний."
    )
    await message.reply(
        text,
        reply_markup=get_location_request_keyboard()
    )

@dp.message_handler(lambda message: message.text == "🕒 Выбрать вручную", chat_type=types.ChatType.PRIVATE)
async def process_timezone_manual(message: types.Message):
    # Сначала убираем временную reply-клавиатуру выбора метода
    await message.reply(
        "Хорошо — сейчас покажу список таймзон.",
        reply_markup=ReplyKeyboardRemove()
    )
    # Затем отправляем inline-клавиатуру со списком таймзон
    await message.reply(
        "Выберите ваш часовой пояс из списка:",
        reply_markup=get_timezone_manual_keyboard()
    )

@dp.message_handler(content_types=['location'])
async def handle_location(message: types.Message):
    try:
        # Определяем таймзону по координатам
        timezone_str = await get_timezone_by_location(
            message.location.latitude,
            message.location.longitude
        )
        
        # Сохраняем таймзону
        async with pg_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO users (user_id, chat_id, timezone) 
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id) DO UPDATE SET timezone = $3
                """,
                message.from_user.id,
                message.chat.id,
                timezone_str
            )
        
        # Отправляем подтверждение
        confirm_text = (
            f"✅ Отлично! Я определил ваш часовой пояс как {timezone_str}.\n"
            f"Текущее время в вашем часовом поясе: "
            f"{datetime.now(pytz.timezone(timezone_str)).strftime('%H:%M')}\n\n"
            f"Теперь вы можете создавать напоминания, а я буду отправлять их точно в указанное время."
        )
        await message.reply(
            confirm_text,
            reply_markup=get_reply_keyboard(True)
        )
    except Exception as e:
        logging.error(f"Ошибка при определении таймзоны: {e}")
        await message.reply(
            "Извините, произошла ошибка при определении часового пояса. "
            "Пожалуйста, выберите часовой пояс вручную:",
            reply_markup=get_timezone_manual_keyboard()
        )


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("tz:"))
async def timezone_callback_handler(callback_query: types.CallbackQuery):
    tz = callback_query.data.split(":", 1)[1]
    if tz not in pytz.all_timezones:
        await callback_query.answer("Ошибка: неизвестная таймзона.", show_alert=True)
        return
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id, chat_id, timezone) VALUES ($1, $2, $3) "
            "ON CONFLICT (user_id) DO UPDATE SET chat_id = EXCLUDED.chat_id, timezone = EXCLUDED.timezone",
            callback_query.from_user.id, callback_query.message.chat.id, tz
        )
    await callback_query.answer("Таймзона установлена!")
    await callback_query.message.edit_text(f"Таймзона установлена: {tz}")
    # Показываем help сразу после выбора таймзоны
    await cmd_help(callback_query.message)
    # Отправляем короткое подтверждение с основной reply-клавиатурой
    try:
        await callback_query.message.reply(
            "✅ Готово — основная клавиатура доступна ниже.",
            reply_markup=get_reply_keyboard(True)
        )
    except Exception:
        # Игнорируем ошибки отправки клавиатуры
        pass


@dp.message_handler(commands=['stats'], chat_type=types.ChatType.PRIVATE)
async def cmd_stats(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    async with pg_pool.acquire() as conn:
        users_count = await conn.fetchval("SELECT COUNT(*) FROM users")
        reminders_count = await conn.fetchval("SELECT COUNT(*) FROM reminders")
    await message.reply(f"Статистика:\nПользователей: {users_count}\nНапоминаний: {reminders_count}")


@dp.message_handler(commands=['broadcast'], chat_type=types.ChatType.PRIVATE)
async def cmd_broadcast(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("Использование: /broadcast <сообщение>")
        return
    broadcast_message = parts[1]
    async with pg_pool.acquire() as conn:
        users = await conn.fetch("SELECT user_id FROM users")
    count = 0
    for user in users:
        try:
            await bot.send_message(user["user_id"], f"Рассылка от администратора:\n{broadcast_message}")
            count += 1
        except Exception as e:
            logging.error(f"Ошибка рассылки пользователю {user['user_id']}: {e}")
    await message.reply(f"Сообщение отправлено {count} пользователям.")


@dp.message_handler(commands=['admin_reminders'], chat_type=types.ChatType.PRIVATE)
async def cmd_admin_reminders(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    async with pg_pool.acquire() as conn:
        reminders = await conn.fetch("SELECT * FROM reminders")
    if not reminders:
        await message.reply("Нет активных напоминаний.")
        return
    lines = []
    for r in reminders:
        line = (f"ID: {r['id']} | Пользователь №:{r['user_id']}-{r.get('user_reminder_id','-')} | "
                f"Чат: {r['chat_id']} | Текст: {r['reminder_text']} | Время: {r['scheduled_time']}")
        if r["recurrence_type"]:
            line += f" | Повтор: {r['recurrence_type']} ({r['recurrence_value']})"
        lines.append(line)
    full_text = "\n".join(lines)
    if len(full_text) > 4096:
        full_text = full_text[:4090] + "..."
    await message.reply(full_text)


@dp.message_handler(commands=['myreminders'], chat_type=types.ChatType.PRIVATE)
async def cmd_myreminders(message: types.Message):
    user_id = message.from_user.id
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT timezone FROM users WHERE user_id = $1", user_id)
        user_tz = row["timezone"] if row is not None else "Europe/Moscow"
        reminders = await conn.fetch("SELECT * FROM reminders WHERE user_id = $1 ORDER BY scheduled_time ASC", user_id)
    if not reminders:
        await message.reply("У вас нет активных напоминаний.")
        return
    kb = InlineKeyboardMarkup(row_width=2)
    text_lines = []
    for r in reminders:
        # Формируем строку для основного текста с временем, датой и текстом напоминания
        scheduled_dt = r["scheduled_time"]
        local_time = scheduled_dt.astimezone(pytz.timezone(user_tz)).strftime('%Y-%m-%d %H:%M:%S')
        text_lines.append(f"№{r['user_reminder_id']} | {local_time} | {r['reminder_text']}")

        # Добавляем кнопки: текст напоминания (без времени) и маленькая кнопка удаления
        kb.add(
            InlineKeyboardButton(
                text=f"№{r['user_reminder_id']} | {r['reminder_text']}",
                callback_data="noop"  # Не требует обработки
            ),
            InlineKeyboardButton(
                text="❌",  # Маленькая кнопка удаления
                callback_data=f"delete:{r['user_reminder_id']}"
            )
        )
    full_text = "\n".join(text_lines)
    await message.reply(full_text, reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("snooze:"))
async def snooze_reminder_handler(callback_query: types.CallbackQuery):
    # Формат callback_data: snooze:<minutes>:<reminder_id>
    try:
        global REMINDER_TEXT_CACHE
        parts = callback_query.data.split(":")
        minutes = int(parts[1])
        reminder_id = int(parts[2])
        user_id = callback_query.from_user.id

        # Получаем текст напоминания из кэша
        reminder_text = REMINDER_TEXT_CACHE.get(reminder_id)
        if not reminder_text:
            await callback_query.answer("Не удалось найти текст напоминания.", show_alert=True)
            return

        new_time = datetime.now(pytz.utc) + timedelta(minutes=minutes)

        async with pg_pool.acquire() as conn:
            max_row = await conn.fetchval(
                "SELECT COALESCE(MAX(user_reminder_id), 0) FROM reminders WHERE user_id = $1", user_id
            )
            new_user_reminder_id = max_row + 1

            chat_id = callback_query.message.chat.id

            await conn.execute(
                """
                INSERT INTO reminders (user_id, user_reminder_id, chat_id, reminder_text, scheduled_time, recurrence_type, recurrence_value)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                user_id,
                new_user_reminder_id,
                chat_id,
                reminder_text,
                new_time,
                None,
                None
            )

        # Очищаем кэш для этого напоминания
        REMINDER_TEXT_CACHE.pop(reminder_id, None)

        await callback_query.answer(f"Напоминание отложено на {minutes} минут.")
        await callback_query.message.edit_reply_markup()
        # Отправляем отдельное сообщение-подтверждение
        local_time = new_time.astimezone(pytz.timezone("Europe/Moscow")).strftime('%Y-%m-%d %H:%M:%S')
        await bot.send_message(chat_id, f"⏰ Новое напоминание перенесено на {local_time}")
    except Exception as e:
        logging.error(f"Ошибка в snooze_reminder_handler: {e}")
        await callback_query.answer("Произошла ошибка. Попробуйте позже.", show_alert=True)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("donate:"))
async def donate_handler(callback_query: types.CallbackQuery):
    # Заглушка для кнопки доната
    await callback_query.answer("Спасибо за поддержку! ⭐", show_alert=True)

# ...esting code...

@dp.callback_query_handler(lambda c: c.data and c.data == "view_my_reminders")
async def process_view_reminders(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id

    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT timezone FROM users WHERE user_id = $1", user_id)
        user_tz = row["timezone"] if row is not None else "Europe/Moscow"
        reminders = await conn.fetch(
            "SELECT * FROM reminders WHERE user_id = $1 ORDER BY scheduled_time ASC", 
            user_id
        )

    if not reminders:
        text = "У вас нет активных напоминаний."
        kb = None
    else:
        kb = InlineKeyboardMarkup(row_width=2)
        text_lines = []
        for r in reminders:
            # Формируем строку для основного текста с временем, датой и текстом напоминания
            scheduled_dt = r["scheduled_time"]
            local_time = scheduled_dt.astimezone(pytz.timezone(user_tz)).strftime('%Y-%m-%d %H:%M:%S')
            text_lines.append(f"№{r['user_reminder_id']} | {local_time} | {r['reminder_text']}")

            # Добавляем кнопки: текст напоминания (без времени) и маленькая кнопка удаления
            kb.add(
                InlineKeyboardButton(
                    text=f"№{r['user_reminder_id']} | {r['reminder_text']}",
                    callback_data="noop"  # Не требует обработки
                ),
                InlineKeyboardButton(
                    text="❌",  # Маленькая кнопка удаления
                    callback_data=f"delete:{r['user_reminder_id']}"
                )
            )
        text = "\n".join(text_lines)

    await callback_query.message.edit_text(text, reply_markup=kb)
    await callback_query.answer("Ваши напоминания:")


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("delete:"))
async def delete_reminder_handler(callback_query: types.CallbackQuery):
    reminder_id = int(callback_query.data.split(":", 1)[1])
    user_id = callback_query.from_user.id
    async with pg_pool.acquire() as conn:
        # Удаляем напоминание из БД
        await conn.execute(
            "DELETE FROM reminders WHERE user_id = $1 AND user_reminder_id = $2",
            user_id, reminder_id
        )
        # Получаем обновлённый список напоминаний
        reminders = await conn.fetch(
            "SELECT * FROM reminders WHERE user_id = $1 ORDER BY scheduled_time ASC", 
            user_id
        )
        user_row = await conn.fetchrow("SELECT timezone FROM users WHERE user_id = $1", user_id)
        user_tz = user_row["timezone"] if user_row is not None else "Europe/Moscow"
        
    if reminders:
        kb = InlineKeyboardMarkup(row_width=2)
        text_lines = []
        for r in reminders:
            local_time = r["scheduled_time"].astimezone(pytz.timezone(user_tz)).strftime('%Y-%m-%d %H:%M:%S')
            text_lines.append(f"№{r['user_reminder_id']} | {local_time} | {r['reminder_text']}")
            kb.add(
                InlineKeyboardButton(
                    text=f"№{r['user_reminder_id']} | {r['reminder_text']}",
                    callback_data="noop"  # Для отображения, без обработки
                ),
                InlineKeyboardButton(
                    text="❌",
                    callback_data=f"delete:{r['user_reminder_id']}"
                )
            )
        text = "\n".join(text_lines)
    else:
        kb = None
        text = "У вас нет активных напоминаний."
        
    await callback_query.message.edit_text(text, reply_markup=kb)
    await callback_query.answer("Напоминание удалено.")


@dp.message_handler(lambda message: message.chat.type in [types.ChatType.GROUP, types.ChatType.SUPERGROUP])
async def group_messages_handler(message: types.Message):
    """
    Обрабатывает сообщения в групповых чатах.
    Если бот упомянут (например, @YourBotName), сообщение интерпретируется как запрос на напоминание.
    Если пользователь не зарегистрирован, для таймзоны используется "Europe/Moscow".
    """
    # Логируем все входящие сообщения в группы (за исключением команд)
    await log_user_message(message)
    if not message.text:
        return
    bot_mentioned = False
    if message.entities:
        for entity in message.entities:
            if entity.type == "mention":
                mentioned = message.text[entity.offset: entity.offset + entity.length]
                if mentioned.lower() == f"@{BOT_USERNAME.lower()}":
                    bot_mentioned = True
                    break
    if not bot_mentioned:
        return
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT timezone FROM users WHERE user_id = $1", message.from_user.id)
        user_tz = row["timezone"] if row is not None else "Europe/Moscow"
    scheduled_time, reminder_text, recurrence = parse_reminder(message.text, user_tz)
    recurrence_type = recurrence["type"] if recurrence else None
    recurrence_value = ""
    if recurrence:
        if recurrence["type"] == "daily":
            recurrence_value = str(recurrence.get("interval", 1))
        elif recurrence["type"] == "weekly":
            recurrence_value = str(recurrence.get("weekday", 0))
        elif recurrence["type"] == "monthly":
            recurrence_value = str(recurrence.get("day", ""))
    async with pg_pool.acquire() as conn:
        max_row = await conn.fetchval("SELECT COALESCE(MAX(user_reminder_id), 0) FROM reminders WHERE user_id = $1", message.from_user.id)
        new_user_reminder_id = max_row + 1
        await conn.execute(
            "INSERT INTO reminders (user_id, user_reminder_id, chat_id, reminder_text, scheduled_time, recurrence_type, recurrence_value) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            message.from_user.id, new_user_reminder_id, message.chat.id, reminder_text, scheduled_time, recurrence_type, recurrence_value
        )
    local_time = scheduled_time.astimezone(pytz.timezone(user_tz)).strftime('%Y-%m-%d %H:%M:%S')
    view_kb = InlineKeyboardMarkup().add(InlineKeyboardButton(text="Просмотреть мои напоминания", callback_data="view_my_reminders"))
    await message.reply(f"Напоминание установлено на {local_time} по вашей таймзоне.", reply_markup=view_kb)


@dp.message_handler(
    lambda message: (
        message.chat.type == types.ChatType.PRIVATE
        and not message.text.startswith('/')
        and message.text not in ["📝 Мои напоминания", "ℹ️ Помощь", "🚀 Начать"]
    )
)
async def private_reminder_handler(message: types.Message):  
    """
    Все сообщения из ЛС (без команды "/") обрабатываются как запросы на создание напоминания.
    """
    # Логируем личные сообщения (без команд) для анализа
    await log_user_message(message)
    user_id = message.from_user.id
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT timezone FROM users WHERE user_id = $1", user_id)
        user_tz = row["timezone"] if row is not None else "Europe/Moscow"
    try:
        scheduled_time, reminder_text, recurrence = parse_reminder(message.text, user_tz)
    except Exception as e:
        await message.reply("❗ Не удалось распознать напоминание. Попробуйте другой формат или напишите, например: \"завтра в 12:00 купить хлеб\".")
        logging.error(f"Ошибка парсинга напоминания: {e}")
        return

    recurrence_type = recurrence["type"] if recurrence else None
    recurrence_value = ""
    if recurrence:
        if recurrence["type"] == "daily":
            recurrence_value = str(recurrence.get("interval", 1))
        elif recurrence["type"] == "weekly":
            recurrence_value = str(recurrence.get("weekday", 0))
        elif recurrence["type"] == "monthly":
            recurrence_value = str(recurrence.get("day", ""))

    async with pg_pool.acquire() as conn:
        max_row = await conn.fetchval("SELECT COALESCE(MAX(user_reminder_id), 0) FROM reminders WHERE user_id = $1", user_id)
        new_user_reminder_id = max_row + 1
        await conn.execute(
            "INSERT INTO reminders (user_id, user_reminder_id, chat_id, reminder_text, scheduled_time, recurrence_type, recurrence_value) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            user_id, new_user_reminder_id, message.chat.id, reminder_text, scheduled_time, recurrence_type, recurrence_value
        )
    local_time = scheduled_time.astimezone(pytz.timezone(user_tz)).strftime('%Y-%m-%d %H:%M:%S')
    view_kb = InlineKeyboardMarkup().add(InlineKeyboardButton(text="📝 Мои напоминания", callback_data="view_my_reminders"))
    await message.reply(
        f"✅ Напоминание установлено на <b>{local_time}</b> по вашей таймзоне.\n\n<b>Текст:</b> {reminder_text}",
        reply_markup=view_kb,
        parse_mode="HTML"
    )


@dp.message_handler(lambda m: m.text == "📝 Мои напоминания", chat_type=types.ChatType.PRIVATE)
async def btn_myreminders(message: types.Message):
    await cmd_myreminders(message)

@dp.message_handler(lambda m: m.text == "ℹ️ Помощь", chat_type=types.ChatType.PRIVATE)
async def btn_help(message: types.Message):
    await cmd_help(message)

@dp.message_handler(lambda m: m.text == "🚀 Начать", chat_type=types.ChatType.PRIVATE)
async def btn_start(message: types.Message):
    await cmd_start(message)


async def on_startup(_):
    """
    При старте бота:
      - Получаем username бота.
      - Инициализируем базу данных.
      - Запускаем фоновую задачу проверки напоминаний.
    """
    global BOT_USERNAME
    me = await bot.get_me()
    BOT_USERNAME = me.username
    logging.info(f"Бот @{BOT_USERNAME} запущен.")
    await init_db()
    asyncio.create_task(reminder_checker())


@dp.message_handler(commands=['export_messages'], chat_type=types.ChatType.PRIVATE)
async def cmd_export_messages(message: types.Message):
    """Экспорт собранных сообщений в CSV (только для администратора)."""
    if message.from_user.id != ADMIN_ID:
        return
    try:
        async with pg_pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, user_id, chat_id, message_text, message_type, created_at FROM user_messages ORDER BY created_at ASC")
        import io, csv
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["id", "user_id", "chat_id", "message_text", "message_type", "created_at"])
        for r in rows:
            writer.writerow([r["id"], r["user_id"], r["chat_id"], r["message_text"], r["message_type"], r["created_at"].isoformat() if r["created_at"] else ""])
            csv_bytes = ("\ufeff" + output.getvalue()).encode("utf-8")
        bio = io.BytesIO(csv_bytes)
        bio.seek(0)
        from aiogram.types import InputFile
        await bot.send_document(message.from_user.id, InputFile(bio, filename="user_messages.csv"))
        await message.reply(f"Экспортировано {len(rows)} записей.")
    except Exception as e:
        logging.error(f"Ошибка экспорта сообщений: {e}")
        await message.reply("Ошибка при экспорте сообщений.")


if __name__ == '__main__':
    executor.start_polling(dp, on_startup=on_startup)