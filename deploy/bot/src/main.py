import asyncio
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# Загрузка переменных окружения из .env
load_dotenv()
import os

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
# фильтры команд/текста
# no filters used, commands checked manually
# в aiogram 3.x BotBlocked отсутствует, используем TelegramForbiddenError
from aiogram.exceptions import TelegramForbiddenError as BotBlocked
import aiohttp

import db
import reminders
from timezonefinder import TimezoneFinder

# список зон, используется при ручном выборе
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
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📍 Определить автоматически")],
            [KeyboardButton(text="🕒 Выбрать вручную")]
        ],
        resize_keyboard=True
    )
    return kb


def get_location_request_keyboard():
    """Клавиатура с кнопкой запроса геолокации"""
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📍 Отправить геолокацию", request_location=True)]],
        resize_keyboard=True
    )
    return kb


def get_timezone_manual_keyboard():
    """Клавиатура со списком таймзон"""
    # Формируем inline_keyboard как список списков (по 3 кнопки в строке)
    buttons = [InlineKeyboardButton(text=label, callback_data=f"tz:{tz}") for label, tz in TIMEZONE_LIST]
    inline_keyboard = [buttons[i:i+3] for i in range(0, len(buttons), 3)]
    kb = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)
    return kb


def get_main_reply_keyboard():
    """Persistent reply keyboard with main actions: My reminders and Help."""
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🗒️ Мои напоминания"), KeyboardButton(text="ℹ️ Помощь")]
        ],
        resize_keyboard=True
    )
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

# Read required environment variables directly
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN must be set in environment")

ML_API_URL = os.environ.get("MODEL_API_URL") or os.environ.get("ML_API_URL")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0") or 0)
DEFAULT_TZ = os.environ.get("TIMEZONE") or os.environ.get("DEFAULT_TZ", "UTC")

bot = Bot(token=TELEGRAM_TOKEN)
# aiogram 3.x: Dispatcher no longer takes bot as argument
# бот передаётся в start_polling
from aiogram import Dispatcher

dp = Dispatcher()

# ---------- вспомогательные функции ----------

async def call_ml(text: str, context_dt: datetime) -> dict:
    async with aiohttp.ClientSession() as sess:
        async with sess.post(ML_API_URL,
                             json={"input": text,
                                   "context_date": context_dt.isoformat()}) as resp:
            resp.raise_for_status()
            return await resp.json()


def human_dt(dt: datetime, tz: str) -> str:
    loc = reminders.to_local(dt, tz)
    return loc.strftime("%Y-%m-%d %H:%M")


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID

# ---------- хендлеры ----------

async def cmd_start(m: types.Message):
    # при старте сохраняем пользователя с дефолтной зоной, затем спрашиваем выбор
    tz = DEFAULT_TZ
    await db.ensure_user(m.from_user.id, tz)
    await m.answer(
        "Привет! Я бот‑напоминалка. Чтобы я мог правильно рассчитывать время, "
        "укажите вашу временную зону.",
        reply_markup=get_timezone_choice_keyboard()
    )
    # Показываем постоянную клавиатуру действий
    await m.answer("", reply_markup=get_main_reply_keyboard())

async def cmd_settz(m: types.Message):
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("Укажите зону, например: /settz Europe/Moscow")
        return
    tzname = parts[1].strip()
    try:
        ZoneInfo(tzname)  # проверка
    except Exception:
        await m.answer("Неправильная зона.")
        return
    await db.ensure_user(m.from_user.id, tzname)
    await m.answer(f"Временная зона установлена на {tzname}")

# tz выбирается через кнопки
async def tz_auto(m: types.Message):
    await m.answer("Пожалуйста, отправьте свою геолокацию.",
                   reply_markup=get_location_request_keyboard())

async def tz_manual(m: types.Message):
    await m.answer("Выберите временную зону:", reply_markup=get_timezone_manual_keyboard())

async def tz_chosen(c: types.CallbackQuery):
    tz = c.data.split(":", 1)[1]
    await db.ensure_user(c.from_user.id, tz)
    await c.answer(f"Временная зона установлена на {tz}")
    # Отправить help после выбора таймзоны
    await c.message.answer(
        "Просто отправьте мне текст напоминания, а модель попытается разобрать дату/время.\n"
        "Команды:\n"
        "/myreminders – мои напоминания\n"
        "/settz <Zone> – установить зону\n"
        "/help – это сообщение\n\n"
        "Админ: /stats, /broadcast <текст>, /admin_reminders, /collect_messages\n"
        "\nПримеры:\n"
        "каждую среду в 10:00 полить цветы\n"
        "Завтра в 20:00 купить молоко",
        reply_markup=get_main_reply_keyboard()
    )

async def handle_location(m: types.Message):
    lat = m.location.latitude
    lng = m.location.longitude
    tz = await get_timezone_by_location(lat, lng)
    await db.ensure_user(m.from_user.id, tz)
    await m.answer(f"Таймзону определена: {tz}")

async def cmd_help(m: types.Message):
    await m.answer(
        "Просто отправьте мне текст напоминания, а модель попытается "
        "разобрать дату/время.\n"
        "Команды:\n"
        "/myreminders – мои напоминания\n"
        "/settz <Zone> – установить зону\n"
        "/help – это сообщение\n\n"
        "Админ: /stats, /broadcast <текст>, /admin_reminders, /collect_messages\n"
        "\nПримеры:\n"
        "каждую среду в 10:00 полить цветы\n"
        "Завтра в 20:00 купить молоко",
        reply_markup=get_main_reply_keyboard()
    )

async def cmd_myreminders(m: types.Message):
    tz = await db.get_user_tz(m.from_user.id) or DEFAULT_TZ
    rows = await db.list_user_reminders(m.from_user.id)
    if not rows:
        await m.answer("У вас нет напоминаний.")
        return
    # Отправляем каждое напоминание отдельным сообщением с кнопкой удаления
    for r in rows:
        txt = f"№{r['user_reminder_id']} | {human_dt(r['utc_dt'], tz)} | {r['text']}"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌", callback_data=f"del:{r['user_reminder_id']}")]
        ])
        await m.answer(txt, reply_markup=kb)

async def cb_delete(c: types.CallbackQuery):
    _, rid = c.data.split(":", 1)
    await db.delete_reminder(c.from_user.id, int(rid))
    await c.answer("Удалено.")
    # Если удаление инициировано из сообщения-напоминания — удалим это сообщение
    try:
        await c.message.delete()
    except Exception:
        # Если не удалось удалить (например, это общий список), попытаемся обновить список
        try:
            tz = await db.get_user_tz(c.from_user.id) or DEFAULT_TZ
            rows = await db.list_user_reminders(c.from_user.id)
            if not rows:
                await c.message.edit_text("У вас нет напоминаний.")
                return
            # Отправим новый единичный список как отдельные сообщения
            # (редактирование сложного общего формата может быть неудобно)
            for r in rows:
                txt = f"№{r['user_reminder_id']} | {human_dt(r['utc_dt'], tz)} | {r['text']}"
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌", callback_data=f"del:{r['user_reminder_id']}")]
                ])
                await c.message.answer(txt, reply_markup=kb)
        except Exception:
            pass

async def cmd_stats(m: types.Message):
    if not is_admin(m.from_user.id):
        return
    s = await db.stats()
    await m.answer(f"Пользователей: {s['users']}\n"
                   f"Напоминаний: {s['reminders']}")

async def cmd_broadcast(m: types.Message):
    if not is_admin(m.from_user.id):
        return
    text = m.get_args()
    if not text:
        await m.answer("Укажите сообщение.")
        return
    rows = await db.pool.fetch("SELECT user_id FROM users")
    for row in rows:
        try:
            await bot.send_message(row["user_id"], text)
        except BotBlocked:
            pass
    await m.answer("Рассылка завершена.")

async def cmd_admin_reminders(m: types.Message):
    if not is_admin(m.from_user.id):
        return
    rows = await db.all_reminders()
    msgs = []
    for r in rows:
        msgs.append(f"{r['id']} /usr#{r['user_id']} rid#{r['user_reminder_id']} "
                    f"{r['utc_dt']} {r['text']} rep={r['repeat_interval']}")
    await m.answer("\n".join(msgs) or "Нет.")

async def cmd_collect_messages(m: types.Message):
    if not is_admin(m.from_user.id):
        return
    rows = await db.collect_messages()
    text = "\n".join(f"{r['user_id']} @{r['received_at']}: {r['text']}" for r in rows)
    await m.answer(text or "Сообщений нет.")

# explicit registration of handlers (filters provided as lambdas or kwargs)


dp.message.register(cmd_start, lambda m: m.text and m.text.startswith('/start'))
dp.message.register(cmd_settz, lambda m: m.text and m.text.startswith('/settz'))
dp.message.register(cmd_help, lambda m: m.text and m.text.startswith('/help'))
dp.message.register(cmd_myreminders, lambda m: m.text and m.text.startswith('/myreminders'))
dp.message.register(cmd_stats, lambda m: m.text and m.text.startswith('/stats'))
dp.message.register(cmd_broadcast, lambda m: m.text and m.text.startswith('/broadcast'))
dp.message.register(cmd_admin_reminders, lambda m: m.text and m.text.startswith('/admin_reminders'))
dp.message.register(cmd_collect_messages, lambda m: m.text and m.text.startswith('/collect_messages'))

# Обработчики для кнопок ReplyKeyboard после выбора таймзоны
dp.message.register(cmd_myreminders, lambda m: m.text in ("Мои напоминания", "🗒️ Мои напоминания"))
dp.message.register(cmd_help, lambda m: m.text in ("Помощь", "ℹ️ Помощь"))

dp.message.register(tz_auto, lambda m: m.text == "📍 Определить автоматически")
dp.message.register(tz_manual, lambda m: m.text == "🕒 Выбрать вручную")
dp.message.register(handle_location, lambda m: m.content_type == types.ContentType.LOCATION)

dp.callback_query.register(tz_chosen, lambda c: c.data and c.data.startswith("tz:"))
dp.callback_query.register(cb_delete, lambda c: c.data and c.data.startswith("del:"))


async def cb_show_myreminders(c: types.CallbackQuery):
    await c.answer()
    tz = await db.get_user_tz(c.from_user.id) or DEFAULT_TZ
    rows = await db.list_user_reminders(c.from_user.id)
    if not rows:
        await c.message.answer("У вас нет напоминаний.")
        return
    lines = []
    kb_rows = []
    for r in rows:
        lines.append(f"№{r['user_reminder_id']} | {human_dt(r['utc_dt'], tz)} | {r['text']}")
        kb_rows.append([InlineKeyboardButton(text="❌", callback_data=f"del:{r['user_reminder_id']}")])
    text = "\n".join(lines)
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await c.message.answer(text, reply_markup=kb)

dp.callback_query.register(cb_show_myreminders, lambda c: c.data and c.data == "myreminders")

# Обработка обычных текстовых сообщений (не команда, не спец-кнопка)
# регистрация `handle_text` перенесена ниже, после определения функции

async def handle_text(m: types.Message):
    await db.store_message(m.from_user.id, m.text)
    tz = await db.get_user_tz(m.from_user.id) or DEFAULT_TZ
    context_dt = m.date.replace(tzinfo=timezone.utc)
    parsed = await call_ml(m.text, context_dt)
    logging.info(f"ML raw response for user {m.from_user.id}: {parsed}")
    out = parsed["output"]
    if "text" not in out:
        logging.warning(f"ML output missing 'text' field for user {m.from_user.id}; using original message")
    logging.info(f"Parsed fields: {out}")
    date_str = out.get("date")
    time_str = out.get("time")
    repeat = out.get("repeat")
    utc_dt = reminders.normalize_parsed(context_dt, date_str,
                                        time_str, repeat, tz)
    logging.info(f"Computed utc_dt (stored): {utc_dt.isoformat()} for tz {tz}")
    # конвертируем в UTC с учётом зоны пользователя
    # utc_dt уже в UTC
    reminder_text = out.get("text") or m.text
    user_rid = await db.add_reminder(m.from_user.id, reminder_text, utc_dt, repeat)
    human = human_dt(utc_dt, tz)
    # Для сообщения подтверждения показываем время с секундами
    human_with_seconds = reminders.to_local(utc_dt, tz).strftime("%Y-%m-%d %H:%M:%S")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мои напоминания", callback_data="myreminders")]
    ])
    await m.answer(
        f"✅ Напоминание установлено на {human_with_seconds} по вашей таймзоне.\n\n"
        f"Текст: {reminder_text}",
        reply_markup=kb
    )

# Обработка обычных текстовых сообщений (не команда, не спец-кнопка)
dp.message.register(
    handle_text,
    lambda m: m.text and not m.text.startswith("/") and m.text not in ["📍 Определить автоматически", "🕒 Выбрать вручную"]
)

# ---------- фоновая задача ----------

async def scheduler():
    while True:
        now = datetime.now(timezone.utc)
        due = await db.due_reminders(now)
        for r in due:
            uid = r["user_id"]
            tz = await db.get_user_tz(uid) or DEFAULT_TZ
            text = r["text"]
            try:
                await bot.send_message(uid, f"🔔 {text}")
            except BotBlocked:  # TelegramForbiddenError
                pass
            if r["repeat_interval"]:
                new_dt = reminders.next_occurrence(r["utc_dt"], r["repeat_interval"])
                await db.update_reminder_time(r["id"], new_dt)
            else:
                await db.remove_reminder_by_id(r["id"])
        await asyncio.sleep(15)

# ---------- запуск ----------

async def main():
    await db.init_db()
    asyncio.create_task(scheduler())
    # передаём инстанс бота в polling
    await dp.start_polling(bot)

if __name__ == "__main__":
    import uvloop
    uvloop.install()
    asyncio.run(main())
