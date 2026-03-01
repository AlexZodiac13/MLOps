
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
import calendar
import logging


def to_utc(local_dt: datetime, tz_str: str) -> datetime:
    tz = ZoneInfo(tz_str)
    if local_dt.tzinfo is None:
        local_dt = local_dt.replace(tzinfo=tz)
    else:
        local_dt = local_dt.astimezone(tz)
    return local_dt.astimezone(ZoneInfo("UTC"))


def to_local(utc_dt: datetime, tz_str: str) -> datetime:
    # ensure utc_dt is timezone-aware UTC
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    return utc_dt.astimezone(ZoneInfo(tz_str))


def normalize_parsed(context_dt: datetime, date_str: str | None,
                     time_str: str | None, repeat: str | None,
                     tz_str: str = "UTC") -> datetime:
    """
    Возвращает UTC datetime, корректируя пропуски и смещения.
    context_dt - время сообщения (UTC).
    date_str/time_str -- результат парсера в локальном времени пользователя (tz_str).
    Алгоритм: строим локальную datetime в зоне пользователя, с учётом date/time,
    корректируем для repeat, затем переводим в UTC.
    """
    # context_dt должен быть UTC-aware
    if context_dt.tzinfo is None:
        context_dt = context_dt.replace(tzinfo=timezone.utc)

    # локальный контекст (в зоне пользователя)
    local_ctx = context_dt.astimezone(ZoneInfo(tz_str))
    base_local = local_ctx

    if date_str:
        try:
            y, m, d = map(int, date_str.split("-"))
            # guard against invalid day (model may return out-of-range day)
            last_day = calendar.monthrange(y, m)[1]
            if d > last_day or d < 1:
                logging.warning(f"normalize_parsed: received invalid date {date_str}, clamping day to {last_day}")
                d = min(max(1, d), last_day)
            try:
                base_local = base_local.replace(year=y, month=m, day=d)
            except ValueError as e:
                logging.exception(f"normalize_parsed: failed to set date {y}-{m}-{d}: {e}. Falling back to context date")
                base_local = local_ctx
        except Exception as e:
            logging.exception(f"normalize_parsed: can't parse date_str={date_str}: {e}")
    if time_str:
        h, mi = map(int, time_str.split(":"))
        base_local = base_local.replace(hour=h, minute=mi, second=0, microsecond=0)

    # если ни date ни time – получается context_dt (в локальном представлении)
    if repeat and base_local <= local_ctx:
        if repeat == "daily":
            base_local += timedelta(days=1)
        elif repeat == "weekly":
            base_local += timedelta(weeks=1)
        elif repeat == "monthly":
            base_local += relativedelta(months=1)
    elif not repeat and base_local <= local_ctx:
        # единовременное напоминание в прошлом – срабатывает мгновенно
        base_local = local_ctx

    # конвертируем локальное время в UTC для хранения
    return base_local.astimezone(ZoneInfo("UTC"))


def next_occurrence(utc_dt: datetime, repeat: str) -> datetime:
    if repeat == "daily":
        return utc_dt + timedelta(days=1)
    elif repeat == "weekly":
        return utc_dt + timedelta(weeks=1)
    elif repeat == "monthly":
        return utc_dt + relativedelta(months=1)
    else:
        return utc_dt
