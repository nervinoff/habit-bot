import asyncio
import logging
import os
import random
import sqlite3
from datetime import date, datetime, time, timedelta
from typing import Optional

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage


DB_PATH = os.getenv("HABIT_DB", "habits.db")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("habit-bot")


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with db() as conn:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                tz_offset_min INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS habits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT,
                reminder_time TEXT,
                is_active INTEGER DEFAULT 1,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            );
            CREATE TABLE IF NOT EXISTS checkins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                habit_id INTEGER NOT NULL,
                checkin_date TEXT NOT NULL,
                UNIQUE(habit_id, checkin_date),
                FOREIGN KEY(habit_id) REFERENCES habits(id)
            );
            CREATE TABLE IF NOT EXISTS shares (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                habit_id INTEGER NOT NULL,
                owner_id INTEGER NOT NULL,
                viewer_id INTEGER NOT NULL,
                UNIQUE(habit_id, viewer_id),
                FOREIGN KEY(habit_id) REFERENCES habits(id)
            );
            CREATE TABLE IF NOT EXISTS challenges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT,
                goal_per_member INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                FOREIGN KEY(owner_id) REFERENCES users(user_id)
            );
            CREATE TABLE IF NOT EXISTS challenge_members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                challenge_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                role TEXT DEFAULT 'member',
                UNIQUE(challenge_id, user_id),
                FOREIGN KEY(challenge_id) REFERENCES challenges(id),
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            );
            CREATE TABLE IF NOT EXISTS challenge_checkins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                challenge_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                checkin_date TEXT NOT NULL,
                UNIQUE(challenge_id, user_id, checkin_date),
                FOREIGN KEY(challenge_id) REFERENCES challenges(id),
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            );
            CREATE TABLE IF NOT EXISTS habit_skips (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                habit_id INTEGER NOT NULL,
                skip_date TEXT NOT NULL,
                UNIQUE(habit_id, skip_date),
                FOREIGN KEY(habit_id) REFERENCES habits(id)
            );
            CREATE TABLE IF NOT EXISTS link_codes (
                code TEXT PRIMARY KEY,
                telegram_user_id INTEGER NOT NULL,
                expires_at TEXT NOT NULL,
                used INTEGER DEFAULT 0
            );
            """
        )


def upsert_user(user: types.User) -> None:
    with db() as conn:
        conn.execute(
            """
            INSERT INTO users (user_id, username, first_name)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
              username=excluded.username,
              first_name=excluded.first_name
            """,
            (user.id, user.username, user.first_name),
        )


def parse_date(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_time(s: str) -> Optional[time]:
    try:
        return datetime.strptime(s, "%H:%M").time()
    except ValueError:
        return None


def format_habit_row(row: sqlite3.Row) -> str:
    return f"#{row['id']} {row['name']}"


class AddHabit(StatesGroup):
    name = State()
    start_date = State()
    end_date = State()
    reminder_time = State()


class AddChallenge(StatesGroup):
    name = State()
    start_date = State()
    end_date = State()
    goal = State()


class ShareHabit(StatesGroup):
    habit_id = State()
    target = State()


class SetReminder(StatesGroup):
    habit_id = State()
    time = State()


class SetTimezone(StatesGroup):
    offset = State()


class CalendarSelect(StatesGroup):
    habit_id = State()
    month = State()

class HabitActionSelect(StatesGroup):
    habit_id = State()


router = Router()


def get_today_summary(user_id: int) -> tuple[int, int]:
    today = date.today().isoformat()
    with db() as conn:
        total = conn.execute(
            "SELECT COUNT(*) as c FROM habits WHERE user_id=? AND is_active=1",
            (user_id,),
        ).fetchone()["c"]
        skipped = conn.execute(
            """
            SELECT COUNT(DISTINCT h.id) as c
            FROM habit_skips s
            JOIN habits h ON h.id = s.habit_id
            WHERE h.user_id=? AND h.is_active=1 AND s.skip_date=?
            """,
            (user_id, today),
        ).fetchone()["c"]
        done = conn.execute(
            """
            SELECT COUNT(*) as c
            FROM checkins c
            JOIN habits h ON h.id = c.habit_id
            WHERE h.user_id=? AND c.checkin_date=?
            """,
            (user_id, today),
        ).fetchone()["c"]
    return max(total - skipped, 0), done


def get_today_status(user_id: int) -> list[tuple[int, str, str]]:
    today = date.today().isoformat()
    with db() as conn:
        habits = conn.execute(
            "SELECT id, name FROM habits WHERE user_id=? AND is_active=1 ORDER BY id DESC",
            (user_id,),
        ).fetchall()
        done_rows = conn.execute(
            """
            SELECT c.habit_id
            FROM checkins c
            JOIN habits h ON h.id = c.habit_id
            WHERE h.user_id=? AND c.checkin_date=?
            """,
            (user_id, today),
        ).fetchall()
        skipped_rows = conn.execute(
            """
            SELECT s.habit_id
            FROM habit_skips s
            JOIN habits h ON h.id = s.habit_id
            WHERE h.user_id=? AND s.skip_date=?
            """,
            (user_id, today),
        ).fetchall()
    done_ids = {r["habit_id"] for r in done_rows}
    skipped_ids = {r["habit_id"] for r in skipped_rows}
    result = []
    for h in habits:
        status = "none"
        if h["id"] in done_ids:
            status = "done"
        elif h["id"] in skipped_ids:
            status = "skipped"
        result.append((h["id"], h["name"], status))
    return result


def progress_bar(done: int, total: int, size: int = 10) -> str:
    if total <= 0:
        return "░" * size
    filled = min(max(int(round(done / total * size)), 0), size)
    return "█" * filled + "░" * (size - filled)


@router.message(F.text == "⬅️ Назад", StateFilter("*"))
async def back_any_state(message: types.Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Меню:", reply_markup=main_menu_kb())


def main_menu_kb() -> types.ReplyKeyboardMarkup:
    row = [
        types.KeyboardButton(text="Сегодня"),
        types.KeyboardButton(text="Привычки"),
    ]
    if WEBAPP_URL:
        row.append(types.KeyboardButton(text="Открыть Mini App", web_app=types.WebAppInfo(url=WEBAPP_URL)))
    return types.ReplyKeyboardMarkup(
        keyboard=[
            row,
            [types.KeyboardButton(text="Друзья"), types.KeyboardButton(text="Челленджи")],
            [types.KeyboardButton(text="Настройки")],
        ],
        resize_keyboard=True,
    )


def inline_kb_from_rows(rows: list[sqlite3.Row], prefix: str) -> types.InlineKeyboardMarkup:
    buttons = []
    for r in rows:
        buttons.append(
            [
                types.InlineKeyboardButton(
                    text=f"#{r['id']} {r['name']}",
                    callback_data=f"{prefix}:{r['id']}",
                )
            ]
        )
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def inline_kb_checkin_day(habit_id: int) -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(
                    text="Сегодня", callback_data=f"checkin:{habit_id}:0"
                ),
                types.InlineKeyboardButton(
                    text="Вчера", callback_data=f"checkin:{habit_id}:-1"
                ),
                types.InlineKeyboardButton(
                    text="Пропуск", callback_data=f"skip:{habit_id}:0"
                ),
            ]
        ]
    )


def inline_kb_today_list(items: list[tuple[int, str, str]]) -> types.InlineKeyboardMarkup:
    buttons = []
    for habit_id, name, status in items:
        if status == "done":
            buttons.append(
                [
                    types.InlineKeyboardButton(
                        text=f"✅ {name}", callback_data="noop"
                    )
                ]
            )
            continue
        if status == "skipped":
            buttons.append(
                [
                    types.InlineKeyboardButton(
                        text=f"⏭️ {name}", callback_data="noop"
                    )
                ]
            )
            continue
        buttons.append(
            [
                types.InlineKeyboardButton(
                    text=f"✅ {name}", callback_data=f"checkin:{habit_id}:0"
                ),
                types.InlineKeyboardButton(
                    text="⏭️", callback_data=f"skip:{habit_id}:0"
                ),
            ]
        )
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def inline_kb_habits_today(habits: list[sqlite3.Row]) -> types.InlineKeyboardMarkup:
    buttons = []
    for h in habits:
        buttons.append(
            [
                types.InlineKeyboardButton(
                    text=f"✅ {h['name']}", callback_data=f"checkin:{h['id']}:0"
                ),
                types.InlineKeyboardButton(
                    text="⏭️", callback_data=f"skip:{h['id']}:0"
                ),
            ]
        )
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def get_user_habits(user_id: int) -> list[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            "SELECT id, name FROM habits WHERE user_id=? AND is_active=1 ORDER BY id DESC",
            (user_id,),
        ).fetchall()


def get_accessible_habits(user_id: int) -> list[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            """
            SELECT DISTINCT h.id, h.name
            FROM habits h
            LEFT JOIN shares s ON s.habit_id = h.id
            WHERE h.is_active=1 AND (h.user_id=? OR s.viewer_id=?)
            ORDER BY h.id DESC
            """,
            (user_id, user_id),
        ).fetchall()


def get_friend_habits(user_id: int) -> list[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            """
            SELECT DISTINCT h.id, h.name
            FROM shares s
            JOIN habits h ON h.id = s.habit_id
            WHERE s.viewer_id=? AND h.is_active=1
            ORDER BY h.id DESC
            """,
            (user_id,),
        ).fetchall()


def inline_kb_delete_habits(rows: list[sqlite3.Row]) -> types.InlineKeyboardMarkup:
    buttons = []
    for r in rows:
        buttons.append(
            [
                types.InlineKeyboardButton(
                    text=f"🗑️ {r['name']}", callback_data=f"delete:{r['id']}"
                )
            ]
        )
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def inline_kb_habit_actions(habit_id: int) -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(
                    text="✅ Отметить", callback_data=f"checkin:{habit_id}:0"
                ),
                types.InlineKeyboardButton(
                    text="⏭️ Пропуск", callback_data=f"skip:{habit_id}:0"
                ),
            ],
            [
                types.InlineKeyboardButton(
                    text="📅 Календарь", callback_data=f"calendar:{habit_id}"
                ),
                types.InlineKeyboardButton(
                    text="📊 Статистика", callback_data=f"stats:{habit_id}"
                ),
            ],
            [
                types.InlineKeyboardButton(
                    text="🗑️ Удалить", callback_data=f"delete:{habit_id}"
                )
            ],
        ]
    )


@router.message(Command("start"))
async def start(message: types.Message) -> None:
    upsert_user(message.from_user)
    total, done = get_today_summary(message.from_user.id)
    bar = progress_bar(done, total)
    text = (
        "Привет! Я помогу отслеживать привычки.\n"
        f"Сегодня: {done}/{total} выполнено\n"
        f"{bar}"
    )
    await message.answer(text, reply_markup=main_menu_kb())
    items = get_today_status(message.from_user.id)
    if items:
        await message.answer(
            "Сегодняшние привычки (✅ отметить, ⏭️ пропуск):",
            reply_markup=inline_kb_today_list(items),
        )
    if total == 0:
        kb = types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="➕ Новая привычка")],
                [types.KeyboardButton(text="Позже")],
            ],
            resize_keyboard=True,
            one_time_keyboard=True,
        )
        await message.answer("Давай создадим первую привычку?", reply_markup=kb)


@router.message(Command("menu"))
async def menu(message: types.Message) -> None:
    await message.answer("Меню:", reply_markup=main_menu_kb())


@router.message(Command("my_id"))
async def my_id(message: types.Message) -> None:
    upsert_user(message.from_user)
    await message.answer(f"Твой Telegram user_id: {message.from_user.id}")


@router.message(Command("link_web"))
async def link_web(message: types.Message) -> None:
    upsert_user(message.from_user)
    code = f"{random.randint(100000, 999999)}"
    expires_at = (datetime.utcnow() + timedelta(minutes=10)).isoformat()
    with db() as conn:
        conn.execute(
            "UPDATE link_codes SET used=1 WHERE telegram_user_id=? AND used=0",
            (message.from_user.id,),
        )
        conn.execute(
            "INSERT OR REPLACE INTO link_codes (code, telegram_user_id, expires_at, used) VALUES (?, ?, ?, 0)",
            (code, message.from_user.id, expires_at),
        )
    await message.answer(
        "Код для привязки веб‑аккаунта:\n"
        f"{code}\n"
        "Действителен 10 минут."
    )


@router.message(F.text == "Сегодня")
async def menu_home(message: types.Message) -> None:
    total, done = get_today_summary(message.from_user.id)
    remaining = max(total - done, 0)
    bar = progress_bar(done, total)
    await message.answer(
        "Сегодня\n"
        f"Сегодня: {done}/{total} выполнено\n"
        f"{bar}\n"
        f"Осталось: {remaining}",
        reply_markup=main_menu_kb(),
    )
    items = get_today_status(message.from_user.id)
    if items:
        await message.answer(
            "Сегодняшние привычки (✅ отметить, ⏭️ пропуск):",
            reply_markup=inline_kb_today_list(items),
        )


@router.message(F.text == "Позже")
async def menu_later(message: types.Message) -> None:
    await message.answer("Хорошо, позже.", reply_markup=main_menu_kb())


@router.message(F.text == "🏠 Домой")
async def menu_home_legacy(message: types.Message) -> None:
    await menu_home(message)


@router.message(F.text == "Привычки")
async def menu_habits_root(message: types.Message) -> None:
    kb = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="➕ Новая привычка")],
            [types.KeyboardButton(text="📌 Выбрать привычку")],
            [types.KeyboardButton(text="✅ Отметить")],
            [types.KeyboardButton(text="📅 Календарь")],
            [types.KeyboardButton(text="📊 Статистика")],
            [types.KeyboardButton(text="🗑️ Удалить")],
            [types.KeyboardButton(text="⬅️ Назад")],
        ],
        resize_keyboard=True,
    )
    await message.answer("Привычки:", reply_markup=kb)


@router.message(F.text == "Друзья")
async def menu_friends_root(message: types.Message) -> None:
    kb = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="🤝 Дать доступ")],
            [types.KeyboardButton(text="👀 Смотреть друзей")],
            [types.KeyboardButton(text="⬅️ Назад")],
        ],
        resize_keyboard=True,
    )
    await message.answer("Друзья:", reply_markup=kb)


@router.message(F.text == "Челленджи")
async def menu_challenges_root(message: types.Message) -> None:
    await menu_challenges(message)


@router.message(F.text == "Настройки")
async def menu_settings_root(message: types.Message) -> None:
    await menu_settings(message)


@router.message(Command("set_tz"))
async def set_tz(message: types.Message, command: CommandObject) -> None:
    upsert_user(message.from_user)
    if not command.args:
        await message.answer("Пример: /set_tz +3 или /set_tz -5")
        return
    try:
        offset_hours = int(command.args.strip())
    except ValueError:
        await message.answer("Нужен целый часовой сдвиг, например +3 или -5.")
        return
    with db() as conn:
        conn.execute(
            "UPDATE users SET tz_offset_min=? WHERE user_id=?",
            (offset_hours * 60, message.from_user.id),
        )
    await message.answer("Готово! Часовой пояс сохранен.")


@router.message(Command("add_habit"))
async def add_habit_start(message: types.Message, state: FSMContext) -> None:
    upsert_user(message.from_user)
    await state.set_state(AddHabit.name)
    await message.answer("Название привычки?", reply_markup=types.ReplyKeyboardRemove())


@router.message(AddHabit.name)
async def add_habit_name(message: types.Message, state: FSMContext) -> None:
    await state.update_data(name=message.text.strip())
    await state.set_state(AddHabit.start_date)
    kb = types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text="Сегодня")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await message.answer(
        "Дата начала в формате YYYY-MM-DD (например 2026-02-04) или 'Сегодня'",
        reply_markup=kb,
    )


@router.message(AddHabit.start_date)
async def add_habit_start_date(message: types.Message, state: FSMContext) -> None:
    text = message.text.strip().lower()
    if text in {"today", "сегодня"}:
        d = date.today()
    else:
        d = parse_date(text)
    if not d:
        await message.answer("Формат даты: YYYY-MM-DD. Попробуй снова.")
        return
    await state.update_data(start_date=d.isoformat())
    await state.set_state(AddHabit.end_date)
    kb = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="Нет")],
            [types.KeyboardButton(text="Сегодня")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await message.answer(
        "Дата окончания? (YYYY-MM-DD, 'Сегодня' или 'Нет')",
        reply_markup=kb,
    )


@router.message(AddHabit.end_date)
async def add_habit_end_date(message: types.Message, state: FSMContext) -> None:
    text = message.text.strip().lower()
    end_date = None
    if text in {"today", "сегодня"}:
        end_date = date.today().isoformat()
    elif text not in {"нет", "no", "none", "-"}:
        d = parse_date(text)
        if not d:
            await message.answer("Формат даты: YYYY-MM-DD или 'нет'.")
            return
        end_date = d.isoformat()
    await state.update_data(end_date=end_date)
    await state.set_state(AddHabit.reminder_time)
    kb = types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text="Нет")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await message.answer(
        "Время напоминания? (HH:MM или 'Нет')",
        reply_markup=kb,
    )


@router.message(AddHabit.reminder_time)
async def add_habit_reminder(message: types.Message, state: FSMContext) -> None:
    text = message.text.strip().lower()
    reminder = None
    if text not in {"нет", "no", "none", "-", "off"}:
        t = parse_time(text)
        if not t:
            await message.answer("Формат времени: HH:MM или 'нет'.")
            return
        reminder = t.strftime("%H:%M")

    data = await state.get_data()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO habits (user_id, name, start_date, end_date, reminder_time)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                message.from_user.id,
                data["name"],
                data["start_date"],
                data["end_date"],
                reminder,
            ),
        )
        habit_id = cur.lastrowid
    await state.clear()
    await message.answer(
        "Привычка создана!\n"
        f"ID: {habit_id}\n"
        f"Название: {data['name']}\n"
        f"Старт: {data['start_date']}\n"
        f"Конец: {data['end_date'] or '—'}\n"
        f"Напоминание: {reminder or '—'}\n"
        "Используй меню, чтобы открыть список.",
        reply_markup=main_menu_kb(),
    )


@router.message(Command("list"))
async def list_habits(message: types.Message) -> None:
    upsert_user(message.from_user)
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM habits WHERE user_id=? AND is_active=1 ORDER BY id DESC",
            (message.from_user.id,),
        ).fetchall()
    if not rows:
        await message.answer("Пока нет привычек. Создай через /add_habit")
        return
    text = "\n\n".join(format_habit_row(r) for r in rows)
    await message.answer(text)


@router.message(Command("checkin"))
async def checkin(message: types.Message, command: CommandObject) -> None:
    upsert_user(message.from_user)
    if not command.args:
        await message.answer("Пример: /checkin 3")
        return
    try:
        habit_id = int(command.args.strip())
    except ValueError:
        await message.answer("Нужен числовой id привычки.")
        return
    today = date.today().isoformat()
    with db() as conn:
        habit = conn.execute(
            "SELECT * FROM habits WHERE id=? AND user_id=?",
            (habit_id, message.from_user.id),
        ).fetchone()
        if not habit:
            await message.answer("Привычка не найдена.")
            return
        try:
            conn.execute(
                "INSERT INTO checkins (habit_id, checkin_date) VALUES (?, ?)",
                (habit_id, today),
            )
        except sqlite3.IntegrityError:
            await message.answer("Сегодня уже отмечено.")
            return
    await message.answer("Отметка сохранена!")


def fetch_habit_with_access(user_id: int, habit_id: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        habit = conn.execute(
            """
            SELECT h.*
            FROM habits h
            WHERE h.id=? AND (h.user_id=? OR EXISTS (
                SELECT 1 FROM shares s WHERE s.habit_id=h.id AND s.viewer_id=?
            ))
            """,
            (habit_id, user_id, user_id),
        ).fetchone()
    return habit


@router.message(Command("calendar"))
async def calendar(message: types.Message, command: CommandObject) -> None:
    upsert_user(message.from_user)
    if not command.args:
        await message.answer("Пример: /calendar 3 2026-02")
        return
    parts = command.args.split()
    try:
        habit_id = int(parts[0])
    except ValueError:
        await message.answer("Нужен числовой id привычки.")
        return

    month = date.today().strftime("%Y-%m")
    if len(parts) > 1:
        month = parts[1]
    try:
        month_date = datetime.strptime(month, "%Y-%m").date()
    except ValueError:
        await message.answer("Месяц в формате YYYY-MM.")
        return

    start = month_date.replace(day=1)
    next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    habit = fetch_habit_with_access(message.from_user.id, habit_id)
    if not habit:
        await message.answer("Привычка не найдена.")
        return
    with db() as conn:
        rows = conn.execute(
            """
            SELECT checkin_date FROM checkins
            WHERE habit_id=? AND checkin_date>=? AND checkin_date<? 
            ORDER BY checkin_date
            """,
            (habit_id, start.isoformat(), next_month.isoformat()),
        ).fetchall()
        skip_rows = conn.execute(
            """
            SELECT skip_date FROM habit_skips
            WHERE habit_id=? AND skip_date>=? AND skip_date<?
            ORDER BY skip_date
            """,
            (habit_id, start.isoformat(), next_month.isoformat()),
        ).fetchall()
    marked = {r["checkin_date"][-2:] for r in rows}
    skipped = {r["skip_date"][-2:] for r in skip_rows}
    days_in_month = (next_month - start).days
    lines = []
    for d in range(1, days_in_month + 1):
        dd = f"{d:02d}"
        if dd in marked:
            mark = "✅"
        elif dd in skipped:
            mark = "⏭️"
        else:
            mark = "⬜"
        lines.append(f"{dd}{mark}")
    await message.answer(f"{habit['name']} — {month}\n" + " ".join(lines))


def calc_streak(dates: list[date], today: date) -> int:
    if not dates:
        return 0
    s = set(dates)
    streak = 0
    cur = today
    while cur in s:
        streak += 1
        cur -= timedelta(days=1)
    return streak


async def stats_by_id(message: types.Message, habit_id: int) -> None:
    habit = fetch_habit_with_access(message.from_user.id, habit_id)
    if not habit:
        await message.answer("Привычка не найдена.")
        return
    with db() as conn:
        rows = conn.execute(
            "SELECT checkin_date FROM checkins WHERE habit_id=? ORDER BY checkin_date",
            (habit_id,),
        ).fetchall()
        skip_rows = conn.execute(
            "SELECT skip_date FROM habit_skips WHERE habit_id=? ORDER BY skip_date",
            (habit_id,),
        ).fetchall()
    dates = [datetime.strptime(r["checkin_date"], "%Y-%m-%d").date() for r in rows]
    skipped_dates = [datetime.strptime(r["skip_date"], "%Y-%m-%d").date() for r in skip_rows]
    total = len(dates)
    today = date.today()
    start = datetime.strptime(habit["start_date"], "%Y-%m-%d").date()
    end = None
    if habit["end_date"]:
        end = datetime.strptime(habit["end_date"], "%Y-%m-%d").date()
    effective_end = min(today, end) if end else today
    days_elapsed = max((effective_end - start).days + 1, 0)
    skipped_in_range = sum(1 for d in skipped_dates if start <= d <= effective_end)
    days_effective = max(days_elapsed - skipped_in_range, 0)
    completion = (total / days_effective * 100) if days_effective > 0 else 0

    month_start = today.replace(day=1)
    month_end = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    month_days = max((min(month_end, effective_end) - max(month_start, start)).days, 0)
    skipped_month = sum(1 for d in skipped_dates if month_start <= d < month_end)
    month_effective = max(month_days - skipped_month, 0)
    month_total = sum(1 for d in dates if month_start <= d < month_end)
    month_completion = (month_total / month_effective * 100) if month_effective > 0 else 0

    streak = calc_streak(dates, today)
    await message.answer(
        f"{habit['name']}\n"
        f"Всего отметок: {total}\n"
        f"Стрик: {streak} дней\n"
        f"Выполнение с начала: {completion:.0f}%\n"
        f"Выполнение за месяц: {month_completion:.0f}%"
    )


@router.message(Command("stats"))
async def stats(message: types.Message, command: CommandObject) -> None:
    upsert_user(message.from_user)
    if not command.args:
        await message.answer("Пример: /stats 3")
        return
    try:
        habit_id = int(command.args.strip())
    except ValueError:
        await message.answer("Нужен числовой id привычки.")
        return
    await stats_by_id(message, habit_id)


@router.message(Command("set_reminder"))
async def set_reminder(message: types.Message, command: CommandObject) -> None:
    upsert_user(message.from_user)
    if not command.args:
        await message.answer("Пример: /set_reminder 3 09:30 или /set_reminder 3 off")
        return
    parts = command.args.split()
    if len(parts) < 2:
        await message.answer("Пример: /set_reminder 3 09:30 или /set_reminder 3 off")
        return
    try:
        habit_id = int(parts[0])
    except ValueError:
        await message.answer("Нужен числовой id привычки.")
        return
    raw = parts[1].lower()
    reminder = None
    if raw not in {"off", "нет", "none", "-"}:
        t = parse_time(raw)
        if not t:
            await message.answer("Формат времени: HH:MM")
            return
        reminder = t.strftime("%H:%M")
    with db() as conn:
        res = conn.execute(
            "UPDATE habits SET reminder_time=? WHERE id=? AND user_id=?",
            (reminder, habit_id, message.from_user.id),
        )
        if res.rowcount == 0:
            await message.answer("Привычка не найдена.")
            return
    await message.answer("Напоминание обновлено.")


@router.message(Command("share"))
async def share(message: types.Message, command: CommandObject) -> None:
    upsert_user(message.from_user)
    if not command.args:
        await message.answer("Пример: /share 3 @username или /share 3 123456")
        return
    parts = command.args.split()
    if len(parts) < 2:
        await message.answer("Пример: /share 3 @username или /share 3 123456")
        return
    try:
        habit_id = int(parts[0])
    except ValueError:
        await message.answer("Нужен числовой id привычки.")
        return
    target = parts[1].lstrip("@")
    with db() as conn:
        habit = conn.execute(
            "SELECT * FROM habits WHERE id=? AND user_id=?",
            (habit_id, message.from_user.id),
        ).fetchone()
        if not habit:
            await message.answer("Привычка не найдена.")
            return
        user = None
        if target.isdigit():
            user = conn.execute(
                "SELECT * FROM users WHERE user_id=?",
                (int(target),),
            ).fetchone()
        else:
            user = conn.execute(
                "SELECT * FROM users WHERE username=?",
                (target,),
            ).fetchone()
        if not user:
            await message.answer("Пользователь не найден. Пусть сначала напишет боту /start.")
            return
        try:
            conn.execute(
                "INSERT INTO shares (habit_id, owner_id, viewer_id) VALUES (?, ?, ?)",
                (habit_id, message.from_user.id, user["user_id"]),
            )
        except sqlite3.IntegrityError:
            await message.answer("Доступ уже выдан.")
            return
    await message.answer("Готово! Доступ выдан.")


@router.message(Command("friends"))
async def friends(message: types.Message) -> None:
    upsert_user(message.from_user)
    with db() as conn:
        rows = conn.execute(
            """
            SELECT h.*, u.username, u.first_name
            FROM shares s
            JOIN habits h ON h.id = s.habit_id
            JOIN users u ON u.user_id = s.owner_id
            WHERE s.viewer_id=?
            ORDER BY h.id DESC
            """,
            (message.from_user.id,),
        ).fetchall()
    if not rows:
        await message.answer("Пока нет доступов от друзей.")
        return
    lines = []
    for r in rows:
        owner = r["username"] or r["first_name"] or str(r["user_id"])
        lines.append(f"@{owner}: #{r['id']} {r['name']} (старт {r['start_date']})")
    await message.answer("\n".join(lines))


@router.message(Command("create_challenge"))
async def create_challenge_start(message: types.Message, state: FSMContext) -> None:
    upsert_user(message.from_user)
    await state.set_state(AddChallenge.name)
    await message.answer(
        "Название челленджа/совместной привычки?",
        reply_markup=types.ReplyKeyboardRemove(),
    )


@router.message(AddChallenge.name)
async def create_challenge_name(message: types.Message, state: FSMContext) -> None:
    await state.update_data(name=message.text.strip())
    await state.set_state(AddChallenge.start_date)
    kb = types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text="Сегодня")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await message.answer(
        "Дата начала в формате YYYY-MM-DD (или 'Сегодня')",
        reply_markup=kb,
    )


@router.message(AddChallenge.start_date)
async def create_challenge_start_date(message: types.Message, state: FSMContext) -> None:
    text = message.text.strip().lower()
    if text in {"today", "сегодня"}:
        d = date.today()
    else:
        d = parse_date(text)
    if not d:
        await message.answer("Формат даты: YYYY-MM-DD. Попробуй снова.")
        return
    await state.update_data(start_date=d.isoformat())
    await state.set_state(AddChallenge.end_date)
    kb = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="Нет")],
            [types.KeyboardButton(text="Сегодня")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await message.answer(
        "Дата окончания? (YYYY-MM-DD, 'Сегодня' или 'Нет')",
        reply_markup=kb,
    )


@router.message(AddChallenge.end_date)
async def create_challenge_end_date(message: types.Message, state: FSMContext) -> None:
    text = message.text.strip().lower()
    end_date = None
    if text in {"today", "сегодня"}:
        end_date = date.today().isoformat()
    elif text not in {"нет", "no", "none", "-"}:
        d = parse_date(text)
        if not d:
            await message.answer("Формат даты: YYYY-MM-DD или 'нет'.")
            return
        end_date = d.isoformat()
    await state.update_data(end_date=end_date)
    await state.set_state(AddChallenge.goal)
    kb = types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text="Нет")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await message.answer(
        "Цель на участника в днях? (число или 'Нет')",
        reply_markup=kb,
    )


@router.message(AddChallenge.goal)
async def create_challenge_goal(message: types.Message, state: FSMContext) -> None:
    text = message.text.strip().lower()
    goal = 0
    if text not in {"нет", "no", "none", "-"}:
        if not text.isdigit():
            await message.answer("Нужна цифра или 'нет'.")
            return
        goal = int(text)
    data = await state.get_data()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO challenges (owner_id, name, start_date, end_date, goal_per_member)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                message.from_user.id,
                data["name"],
                data["start_date"],
                data["end_date"],
                goal,
            ),
        )
        challenge_id = cur.lastrowid
        conn.execute(
            "INSERT INTO challenge_members (challenge_id, user_id, role) VALUES (?, ?, ?)",
            (challenge_id, message.from_user.id, "owner"),
        )
    await state.clear()
    await message.answer(
        "Челлендж создан!\n"
        f"ID: {challenge_id}\n"
        "Пригласи друзей: пусть введут /join_challenge "
        f"{challenge_id}",
        reply_markup=main_menu_kb(),
    )


@router.message(Command("join_challenge"))
async def join_challenge(message: types.Message, command: CommandObject) -> None:
    upsert_user(message.from_user)
    if not command.args:
        await message.answer("Пример: /join_challenge 2")
        return
    try:
        challenge_id = int(command.args.strip())
    except ValueError:
        await message.answer("Нужен числовой id челленджа.")
        return
    with db() as conn:
        challenge = conn.execute(
            "SELECT * FROM challenges WHERE id=? AND is_active=1",
            (challenge_id,),
        ).fetchone()
        if not challenge:
            await message.answer("Челлендж не найден.")
            return
        try:
            conn.execute(
                "INSERT INTO challenge_members (challenge_id, user_id) VALUES (?, ?)",
                (challenge_id, message.from_user.id),
            )
        except sqlite3.IntegrityError:
            await message.answer("Ты уже в этом челлендже.")
            return
    await message.answer("Готово! Ты в челлендже.")


@router.message(Command("challenges"))
async def list_challenges(message: types.Message) -> None:
    upsert_user(message.from_user)
    with db() as conn:
        rows = conn.execute(
            """
            SELECT c.*, m.role
            FROM challenge_members m
            JOIN challenges c ON c.id = m.challenge_id
            WHERE m.user_id=?
            ORDER BY c.id DESC
            """,
            (message.from_user.id,),
        ).fetchall()
    if not rows:
        await message.answer("Пока нет челленджей.")
        return
    lines = []
    for r in rows:
        role = " (owner)" if r["role"] == "owner" else ""
        lines.append(f"#{r['id']} {r['name']}{role} — старт {r['start_date']}")
    await message.answer("\n".join(lines))


@router.message(Command("challenge_checkin"))
async def challenge_checkin(message: types.Message, command: CommandObject) -> None:
    upsert_user(message.from_user)
    if not command.args:
        await message.answer("Пример: /challenge_checkin 2")
        return
    try:
        challenge_id = int(command.args.strip())
    except ValueError:
        await message.answer("Нужен числовой id челленджа.")
        return
    today = date.today().isoformat()
    with db() as conn:
        member = conn.execute(
            """
            SELECT * FROM challenge_members
            WHERE challenge_id=? AND user_id=?
            """,
            (challenge_id, message.from_user.id),
        ).fetchone()
        if not member:
            await message.answer("Ты не участник этого челленджа.")
            return
        try:
            conn.execute(
                """
                INSERT INTO challenge_checkins (challenge_id, user_id, checkin_date)
                VALUES (?, ?, ?)
                """,
                (challenge_id, message.from_user.id, today),
            )
        except sqlite3.IntegrityError:
            await message.answer("Сегодня уже отмечено.")
            return
    await message.answer("Отметка в челлендже сохранена!")


@router.message(Command("challenge_stats"))
async def challenge_stats(message: types.Message, command: CommandObject) -> None:
    upsert_user(message.from_user)
    if not command.args:
        await message.answer("Пример: /challenge_stats 2")
        return
    try:
        challenge_id = int(command.args.strip())
    except ValueError:
        await message.answer("Нужен числовой id челленджа.")
        return
    with db() as conn:
        challenge = conn.execute(
            "SELECT * FROM challenges WHERE id=?",
            (challenge_id,),
        ).fetchone()
        if not challenge:
            await message.answer("Челлендж не найден.")
            return
        member = conn.execute(
            """
            SELECT * FROM challenge_members
            WHERE challenge_id=? AND user_id=?
            """,
            (challenge_id, message.from_user.id),
        ).fetchone()
        if not member:
            await message.answer("Ты не участник этого челленджа.")
            return
        rows = conn.execute(
            """
            SELECT u.username, u.first_name, m.user_id, COUNT(c.id) as total
            FROM challenge_members m
            JOIN users u ON u.user_id = m.user_id
            LEFT JOIN challenge_checkins c
              ON c.challenge_id = m.challenge_id AND c.user_id = m.user_id
            WHERE m.challenge_id=?
            GROUP BY m.user_id
            ORDER BY total DESC
            """,
            (challenge_id,),
        ).fetchall()
    lines = [f"{challenge['name']} — участники:"]
    for r in rows:
        name = r["username"] or r["first_name"] or str(r["user_id"])
        lines.append(f"@{name}: {r['total']}")
    await message.answer("\n".join(lines))


@router.message(F.text == "➕ Новая привычка")
async def menu_add_habit(message: types.Message, state: FSMContext) -> None:
    await add_habit_start(message, state)


@router.message(F.text == "📋 Мои привычки")
async def menu_list_habits(message: types.Message) -> None:
    upsert_user(message.from_user)
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM habits WHERE user_id=? AND is_active=1 ORDER BY id DESC",
            (message.from_user.id,),
        ).fetchall()
    if not rows:
        await message.answer("Пока нет привычек. Создай через меню.")
        return
    text = "\n\n".join(format_habit_row(r) for r in rows)
    await message.answer(text)


@router.message(F.text == "📋 Список")
async def menu_list_habits_short(message: types.Message) -> None:
    await menu_list_habits(message)


@router.message(F.text == "📌 Выбрать привычку")
async def menu_pick_habit_actions(message: types.Message) -> None:
    upsert_user(message.from_user)
    rows = get_user_habits(message.from_user.id)
    if not rows:
        await message.answer("Пока нет привычек. Создай через меню.")
        return
    await message.answer(
        "Выбери привычку:",
        reply_markup=inline_kb_from_rows(rows, "habit_action"),
    )


@router.message(F.text == "✅ Отметить")
async def menu_checkin(message: types.Message) -> None:
    upsert_user(message.from_user)
    rows = get_user_habits(message.from_user.id)
    if not rows:
        await message.answer("Пока нет привычек. Создай через меню.")
        return
    await message.answer(
        "Что отметить сегодня?",
        reply_markup=inline_kb_from_rows(rows, "checkin_pick"),
    )


@router.message(F.text == "🗑️ Удалить")
async def menu_delete_habit(message: types.Message) -> None:
    upsert_user(message.from_user)
    rows = get_user_habits(message.from_user.id)
    if not rows:
        await message.answer("Пока нет привычек. Создай через меню.")
        return
    await message.answer(
        "Выбери привычку для удаления.",
        reply_markup=inline_kb_delete_habits(rows),
    )


@router.message(F.text == "📅 Календарь")
async def menu_calendar(message: types.Message, state: FSMContext) -> None:
    upsert_user(message.from_user)
    own = get_user_habits(message.from_user.id)
    friends = get_friend_habits(message.from_user.id)
    if not own and not friends:
        await message.answer("Пока нет привычек. Создай через меню.")
        return
    if own:
        await message.answer(
            "Мои привычки:",
            reply_markup=inline_kb_from_rows(own, "calendar"),
        )
    if friends:
        await message.answer(
            "Привычки друзей:",
            reply_markup=inline_kb_from_rows(friends, "calendar"),
        )
    await state.set_state(CalendarSelect.habit_id)


@router.message(F.text == "📊 Статистика")
async def menu_stats(message: types.Message) -> None:
    upsert_user(message.from_user)
    own = get_user_habits(message.from_user.id)
    friends = get_friend_habits(message.from_user.id)
    if not own and not friends:
        await message.answer("Пока нет привычек. Создай через меню.")
        return
    if own:
        await message.answer(
            "Мои привычки:",
            reply_markup=inline_kb_from_rows(own, "stats"),
        )
    if friends:
        await message.answer(
            "Привычки друзей:",
            reply_markup=inline_kb_from_rows(friends, "stats"),
        )


@router.message(F.text == "👥 Доступы")
async def menu_access(message: types.Message) -> None:
    await menu_friends_root(message)


@router.message(F.text == "🤝 Дать доступ")
async def menu_share_start(message: types.Message, state: FSMContext) -> None:
    upsert_user(message.from_user)
    rows = get_user_habits(message.from_user.id)
    if not rows:
        await message.answer("Пока нет привычек. Создай через меню.")
        return
    await message.answer(
        "Выбери привычку для доступа.",
        reply_markup=inline_kb_from_rows(rows, "share"),
    )
    await state.set_state(ShareHabit.habit_id)


@router.message(F.text == "👀 Смотреть друзей")
async def menu_friends(message: types.Message) -> None:
    await friends(message)


@router.message(F.text == "🏁 Челленджи")
async def menu_challenges(message: types.Message) -> None:
    kb = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="🏁 Создать челлендж")],
            [types.KeyboardButton(text="📋 Мои челленджи")],
            [types.KeyboardButton(text="✅ Отметиться в челлендже")],
            [types.KeyboardButton(text="📊 Статистика челленджа")],
            [types.KeyboardButton(text="⬅️ Назад")],
        ],
        resize_keyboard=True,
    )
    await message.answer(
        "Челленджи\n"
        "Создать — начать совместную привычку\n"
        "Отметиться — отметить участие сегодня",
        reply_markup=kb,
    )




@router.message(F.text == "🏁 Создать челлендж")
async def menu_create_challenge(message: types.Message, state: FSMContext) -> None:
    await create_challenge_start(message, state)


@router.message(F.text == "📋 Мои челленджи")
async def menu_list_challenges(message: types.Message) -> None:
    await list_challenges(message)


@router.message(F.text == "✅ Отметиться в челлендже")
async def menu_challenge_checkin(message: types.Message) -> None:
    upsert_user(message.from_user)
    with db() as conn:
        rows = conn.execute(
            """
            SELECT c.id, c.name
            FROM challenge_members m
            JOIN challenges c ON c.id = m.challenge_id
            WHERE m.user_id=?
            ORDER BY c.id DESC
            """,
            (message.from_user.id,),
        ).fetchall()
    if not rows:
        await message.answer("Пока нет челленджей.")
        return
    await message.answer(
        "Выбери челлендж для отметки.",
        reply_markup=inline_kb_from_rows(rows, "challenge_checkin"),
    )


@router.message(F.text == "📊 Статистика челленджа")
async def menu_challenge_stats(message: types.Message) -> None:
    upsert_user(message.from_user)
    with db() as conn:
        rows = conn.execute(
            """
            SELECT c.id, c.name
            FROM challenge_members m
            JOIN challenges c ON c.id = m.challenge_id
            WHERE m.user_id=?
            ORDER BY c.id DESC
            """,
            (message.from_user.id,),
        ).fetchall()
    if not rows:
        await message.answer("Пока нет челленджей.")
        return
    await message.answer(
        "Выбери челлендж для статистики.",
        reply_markup=inline_kb_from_rows(rows, "challenge_stats"),
    )


@router.message(F.text == "⚙️ Настройки")
async def menu_settings(message: types.Message) -> None:
    kb = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="🕒 Часовой пояс")],
            [types.KeyboardButton(text="⏰ Напоминания")],
            [types.KeyboardButton(text="⬅️ Назад")],
        ],
        resize_keyboard=True,
    )
    await message.answer(
        "Настройки\n"
        "Часовой пояс — для правильных напоминаний\n"
        "Напоминания — включить/выключить время",
        reply_markup=kb,
    )




@router.message(F.text == "🕒 Часовой пояс")
async def menu_timezone(message: types.Message, state: FSMContext) -> None:
    kb = types.ReplyKeyboardMarkup(
        keyboard=[
            [
                types.KeyboardButton(text="-5"),
                types.KeyboardButton(text="-3"),
                types.KeyboardButton(text="0"),
                types.KeyboardButton(text="+1"),
            ],
            [
                types.KeyboardButton(text="+3"),
                types.KeyboardButton(text="+5"),
                types.KeyboardButton(text="+7"),
                types.KeyboardButton(text="Ввести вручную"),
            ],
            [types.KeyboardButton(text="⬅️ Назад")],
        ],
        resize_keyboard=True,
    )
    await state.set_state(SetTimezone.offset)
    await message.answer("Выбери часовой пояс (UTC).", reply_markup=kb)


@router.message(F.text == "⏰ Напоминания")
async def menu_reminders(message: types.Message, state: FSMContext) -> None:
    rows = get_user_habits(message.from_user.id)
    if not rows:
        await message.answer("Пока нет привычек. Создай через меню.")
        return
    await message.answer(
        "Выбери привычку для напоминания.",
        reply_markup=inline_kb_from_rows(rows, "reminder"),
    )
    await state.set_state(SetReminder.habit_id)


@router.message(SetTimezone.offset)
async def set_timezone_value(message: types.Message, state: FSMContext) -> None:
    text = message.text.strip()
    if text.lower() == "ввести вручную":
        await message.answer("Введи часовой пояс, например +3 или -5.")
        return
    try:
        offset_hours = int(text)
    except ValueError:
        await message.answer("Нужен целый часовой сдвиг, например +3 или -5.")
        return
    with db() as conn:
        conn.execute(
            "UPDATE users SET tz_offset_min=? WHERE user_id=?",
            (offset_hours * 60, message.from_user.id),
        )
    await state.clear()
    await message.answer("Готово! Часовой пояс сохранен.", reply_markup=main_menu_kb())


@router.message(SetReminder.time)
async def set_reminder_time(message: types.Message, state: FSMContext) -> None:
    text = message.text.strip().lower()
    data = await state.get_data()
    habit_id = data.get("habit_id")
    if not habit_id:
        await state.clear()
        await message.answer("Не удалось найти привычку.", reply_markup=main_menu_kb())
        return
    reminder = None
    if text not in {"нет", "off", "выключить"}:
        t = parse_time(text)
        if not t:
            await message.answer("Формат времени: HH:MM")
            return
        reminder = t.strftime("%H:%M")
    with db() as conn:
        res = conn.execute(
            "UPDATE habits SET reminder_time=? WHERE id=? AND user_id=?",
            (reminder, habit_id, message.from_user.id),
        )
        if res.rowcount == 0:
            await message.answer("Привычка не найдена.")
            return
    await state.clear()
    await message.answer("Напоминание обновлено.", reply_markup=main_menu_kb())


@router.message(ShareHabit.target)
async def share_target(message: types.Message, state: FSMContext) -> None:
    text = message.text.strip().lstrip("@")
    data = await state.get_data()
    habit_id = data.get("habit_id")
    if not habit_id:
        await state.clear()
        await message.answer("Не удалось найти привычку.", reply_markup=main_menu_kb())
        return
    with db() as conn:
        user = None
        if text.isdigit():
            user = conn.execute(
                "SELECT * FROM users WHERE user_id=?",
                (int(text),),
            ).fetchone()
        else:
            user = conn.execute(
                "SELECT * FROM users WHERE username=?",
                (text,),
            ).fetchone()
        if not user:
            await message.answer("Пользователь не найден. Пусть сначала напишет боту /start.")
            return
        try:
            conn.execute(
                "INSERT INTO shares (habit_id, owner_id, viewer_id) VALUES (?, ?, ?)",
                (habit_id, message.from_user.id, user["user_id"]),
            )
        except sqlite3.IntegrityError:
            await message.answer("Доступ уже выдан.")
            return
    await state.clear()
    await message.answer("Готово! Доступ выдан.", reply_markup=main_menu_kb())


@router.callback_query(F.data.startswith("checkin_pick:"))
async def cb_checkin_pick(call: types.CallbackQuery) -> None:
    habit_id = int(call.data.split(":")[1])
    await call.message.answer(
        "Когда отметить?",
        reply_markup=inline_kb_checkin_day(habit_id),
    )
    await call.answer()


@router.callback_query(F.data.startswith("checkin:"))
async def cb_checkin(call: types.CallbackQuery) -> None:
    parts = call.data.split(":")
    habit_id = int(parts[1])
    offset_days = int(parts[2]) if len(parts) > 2 else 0
    day = date.today() + timedelta(days=offset_days)
    day_str = day.isoformat()
    with db() as conn:
        habit = conn.execute(
            "SELECT * FROM habits WHERE id=? AND user_id=?",
            (habit_id, call.from_user.id),
        ).fetchone()
        if not habit:
            await call.message.answer("Привычка не найдена.")
            await call.answer()
            return
        try:
            conn.execute(
                "DELETE FROM habit_skips WHERE habit_id=? AND skip_date=?",
                (habit_id, day_str),
            )
            conn.execute(
                "INSERT INTO checkins (habit_id, checkin_date) VALUES (?, ?)",
                (habit_id, day_str),
            )
        except sqlite3.IntegrityError:
            label = "Сегодня" if offset_days == 0 else "Вчера"
            await call.message.answer(f"{label} уже отмечено.")
            await call.answer()
            return
    label = "Сегодня" if offset_days == 0 else "Вчера"
    await call.message.answer(f"{label} отмечено!", reply_markup=main_menu_kb())
    await call.answer()


@router.callback_query(F.data.startswith("skip:"))
async def cb_skip(call: types.CallbackQuery) -> None:
    parts = call.data.split(":")
    habit_id = int(parts[1])
    offset_days = int(parts[2]) if len(parts) > 2 else 0
    day = date.today() + timedelta(days=offset_days)
    day_str = day.isoformat()
    with db() as conn:
        habit = conn.execute(
            "SELECT * FROM habits WHERE id=? AND user_id=?",
            (habit_id, call.from_user.id),
        ).fetchone()
        if not habit:
            await call.message.answer("Привычка не найдена.")
            await call.answer()
            return
        already = conn.execute(
            "SELECT 1 FROM checkins WHERE habit_id=? AND checkin_date=?",
            (habit_id, day_str),
        ).fetchone()
        if already:
            await call.message.answer("Уже отмечено. Пропуск не нужен.")
            await call.answer()
            return
        try:
            conn.execute(
                "INSERT INTO habit_skips (habit_id, skip_date) VALUES (?, ?)",
                (habit_id, day_str),
            )
        except sqlite3.IntegrityError:
            await call.message.answer("Пропуск уже установлен.")
            await call.answer()
            return
    await call.message.answer("Пропуск сохранен.", reply_markup=main_menu_kb())
    await call.answer()


@router.callback_query(F.data.startswith("stats:"))
async def cb_stats(call: types.CallbackQuery) -> None:
    habit_id = int(call.data.split(":")[1])
    await stats_by_id(call.message, habit_id)
    await call.answer()


@router.callback_query(F.data.startswith("calendar:"))
async def cb_calendar(call: types.CallbackQuery, state: FSMContext) -> None:
    habit_id = int(call.data.split(":")[1])
    await state.clear()
    kb = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="Этот месяц")],
            [types.KeyboardButton(text="Другой месяц")],
            [types.KeyboardButton(text="⬅️ Назад")],
        ],
        resize_keyboard=True,
    )
    await state.update_data(habit_id=habit_id)
    await state.set_state(CalendarSelect.month)
    await call.message.answer("Какой месяц показать?", reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("habit_action:"))
async def cb_habit_action(call: types.CallbackQuery) -> None:
    habit_id = int(call.data.split(":")[1])
    await call.message.answer(
        "Действие с привычкой:",
        reply_markup=inline_kb_habit_actions(habit_id),
    )
    await call.answer()


@router.message(CalendarSelect.month)
async def calendar_month_choice(message: types.Message, state: FSMContext) -> None:
    text = message.text.strip().lower()
    data = await state.get_data()
    habit_id = data.get("habit_id")
    if text == "этот месяц":
        await state.clear()
        fake_command = CommandObject(command="calendar", args=f"{habit_id} {date.today():%Y-%m}")
        await calendar(message, fake_command)
        return
    if text == "другой месяц":
        await message.answer("Введи месяц в формате YYYY-MM.")
        return
    try:
        month_date = datetime.strptime(message.text.strip(), "%Y-%m").date()
    except ValueError:
        await message.answer("Месяц в формате YYYY-MM.")
        return
    await state.clear()
    fake_command = CommandObject(command="calendar", args=f"{habit_id} {month_date.strftime('%Y-%m')}")
    await calendar(message, fake_command)


@router.callback_query(F.data.startswith("reminder:"))
async def cb_reminder(call: types.CallbackQuery, state: FSMContext) -> None:
    habit_id = int(call.data.split(":")[1])
    await state.update_data(habit_id=habit_id)
    await state.set_state(SetReminder.time)
    kb = types.ReplyKeyboardMarkup(
        keyboard=[
            [
                types.KeyboardButton(text="09:00"),
                types.KeyboardButton(text="12:00"),
                types.KeyboardButton(text="18:00"),
                types.KeyboardButton(text="21:00"),
            ],
            [types.KeyboardButton(text="Выключить")],
            [types.KeyboardButton(text="⬅️ Назад")],
        ],
        resize_keyboard=True,
    )
    await call.message.answer("Во сколько напоминать?", reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("share:"))
async def cb_share(call: types.CallbackQuery, state: FSMContext) -> None:
    habit_id = int(call.data.split(":")[1])
    await state.update_data(habit_id=habit_id)
    await state.set_state(ShareHabit.target)
    await call.message.answer(
        "Кому дать доступ? Введи @username или user_id.",
        reply_markup=types.ReplyKeyboardRemove(),
    )
    await call.answer()


@router.callback_query(F.data.startswith("challenge_checkin:"))
async def cb_challenge_checkin(call: types.CallbackQuery) -> None:
    challenge_id = int(call.data.split(":")[1])
    fake_command = CommandObject(command="challenge_checkin", args=str(challenge_id))
    await challenge_checkin(call.message, fake_command)
    await call.answer()


@router.callback_query(F.data.startswith("challenge_stats:"))
async def cb_challenge_stats(call: types.CallbackQuery) -> None:
    challenge_id = int(call.data.split(":")[1])
    fake_command = CommandObject(command="challenge_stats", args=str(challenge_id))
    await challenge_stats(call.message, fake_command)
    await call.answer()

@router.callback_query(F.data.startswith("delete:"))
async def cb_delete_habit(call: types.CallbackQuery) -> None:
    habit_id = int(call.data.split(":")[1])
    with db() as conn:
        res = conn.execute(
            "UPDATE habits SET is_active=0 WHERE id=? AND user_id=?",
            (habit_id, call.from_user.id),
        )
        if res.rowcount == 0:
            await call.message.answer("Привычка не найдена.")
            await call.answer()
            return
    await call.message.answer("Привычка удалена.", reply_markup=main_menu_kb())
    await call.answer()


@router.callback_query(F.data == "noop")
async def cb_noop(call: types.CallbackQuery) -> None:
    await call.answer("Уже отмечено.")


async def reminder_loop(bot: Bot) -> None:
    await asyncio.sleep(2)
    while True:
        try:
            now_utc = datetime.utcnow()
            with db() as conn:
                rows = conn.execute(
                    """
                    SELECT h.id, h.name, h.user_id, h.reminder_time, u.tz_offset_min
                    FROM habits h
                    JOIN users u ON u.user_id = h.user_id
                    WHERE h.is_active=1 AND h.reminder_time IS NOT NULL
                    """
                ).fetchall()
            for r in rows:
                local_now = now_utc + timedelta(minutes=r["tz_offset_min"] or 0)
                if local_now.strftime("%H:%M") == r["reminder_time"]:
                    await bot.send_message(
                        r["user_id"],
                        f"Напоминание: отметь привычку «{r['name']}»",
                        reply_markup=inline_kb_checkin_day(r["id"]),
                    )
        except Exception:
            logger.exception("Reminder loop error")
        await asyncio.sleep(60)


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")
    init_db()
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    asyncio.create_task(reminder_loop(bot))
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
