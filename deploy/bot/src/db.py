import asyncpg
from datetime import datetime
from typing import Optional, List, Dict
import os
from dotenv import load_dotenv

# Загрузка переменных окружения из .env
load_dotenv()

# Read DB settings from environment directly
DB_DSN = os.environ.get("DB_DSN")
if not DB_DSN:
    raise RuntimeError("DB_DSN must be set in environment")

DEFAULT_TZ = os.environ.get("TIMEZONE") or os.environ.get("DEFAULT_TZ", "UTC")

pool: asyncpg.pool.Pool | None = None

async def init_db():
    global pool
    pool = await asyncpg.create_pool(dsn=DB_DSN)
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                timezone TEXT NOT NULL DEFAULT '{DEFAULT_TZ}'
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reminders (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                user_reminder_id INT NOT NULL,
                text TEXT NOT NULL,
                utc_dt TIMESTAMPTZ NOT NULL,
                repeat_interval TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                text TEXT NOT NULL,
                received_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )

async def ensure_user(user_id: int, timezone: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (user_id, timezone)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET timezone = EXCLUDED.timezone
            """, user_id, timezone
        )

async def get_user_tz(user_id: int) -> str | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT timezone FROM users WHERE user_id=$1", user_id)
        return row["timezone"] if row else None

async def add_reminder(user_id:int, text:str, utc_dt:datetime,
                       repeat:Optional[str]) -> int:
    async with pool.acquire() as conn:
        # вычисляем следующий user_reminder_id
        row = await conn.fetchrow(
            "SELECT COALESCE(MAX(user_reminder_id),0)+1 AS next FROM reminders WHERE user_id=$1", user_id
        )
        user_rid = row["next"]
        await conn.execute(
            """
            INSERT INTO reminders (user_id,user_reminder_id,text,utc_dt,repeat_interval)
            VALUES ($1,$2,$3,$4,$5)
            """, user_id, user_rid, text, utc_dt, repeat
        )
        return user_rid

async def list_user_reminders(user_id:int) -> List[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM reminders WHERE user_id=$1 ORDER BY user_reminder_id", user_id
        )

async def delete_reminder(user_id:int, user_rid:int):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM reminders WHERE user_id=$1 AND user_reminder_id=$2",
            user_id, user_rid
        )

async def due_reminders(before: datetime) -> List[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM reminders WHERE utc_dt <= $1", before
        )

async def update_reminder_time(rem_id:int, new_dt:datetime):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE reminders SET utc_dt=$1 WHERE id=$2", new_dt, rem_id
        )

async def remove_reminder_by_id(rem_id:int):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM reminders WHERE id=$1", rem_id)

async def stats() -> Dict[str,int]:
    async with pool.acquire() as conn:
        users = await conn.fetchval("SELECT COUNT(*) FROM users")
        rems  = await conn.fetchval("SELECT COUNT(*) FROM reminders")
        return {"users": users, "reminders": rems}

async def all_reminders() -> List[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM reminders ORDER BY utc_dt")

async def store_message(user_id:int, text:str):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO messages (user_id,text) VALUES ($1,$2)", user_id, text
        )

async def collect_messages() -> List[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM messages ORDER BY received_at")
