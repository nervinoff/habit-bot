import os
import sqlite3
from typing import Iterable


DB_PATH = os.getenv("HABIT_DB", "habits.db")


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    schema = """
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS web_users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS web_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        refresh_token TEXT UNIQUE NOT NULL,
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES web_users(id)
    );
    CREATE TABLE IF NOT EXISTS web_user_links (
        web_user_id INTEGER PRIMARY KEY,
        telegram_user_id INTEGER NOT NULL,
        FOREIGN KEY(web_user_id) REFERENCES web_users(id)
    );
    CREATE TABLE IF NOT EXISTS link_codes (
        code TEXT PRIMARY KEY,
        telegram_user_id INTEGER NOT NULL,
        expires_at TEXT NOT NULL,
        used INTEGER DEFAULT 0
    );
    """
    with db() as conn:
        conn.executescript(schema)


def fetch_one(query: str, params: Iterable = ()) -> sqlite3.Row | None:
    with db() as conn:
        return conn.execute(query, params).fetchone()


def fetch_all(query: str, params: Iterable = ()) -> list[sqlite3.Row]:
    with db() as conn:
        return conn.execute(query, params).fetchall()


def execute(query: str, params: Iterable = ()) -> int:
    with db() as conn:
        cur = conn.execute(query, params)
        return cur.lastrowid
