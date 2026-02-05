from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request, status, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel, EmailStr

from backend.auth import (
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    verify_password,
)
from backend.db import execute, fetch_all, fetch_one, init_db


app = FastAPI(title="Habit Web API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")


class RegisterIn(BaseModel):
    email: EmailStr
    password: str


class LoginIn(BaseModel):
    email: EmailStr
    password: str


class TokenOut(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class LinkTelegramIn(BaseModel):
    telegram_user_id: int


class LinkCodeIn(BaseModel):
    code: str


class HabitCreateIn(BaseModel):
    name: str
    start_date: date
    end_date: Optional[date] = None
    reminder_time: Optional[str] = None


class CheckinIn(BaseModel):
    day: Optional[date] = None


def get_current_user_id(token: str = Depends(oauth2_scheme)) -> int:
    payload = decode_token(token)
    if not payload or payload.get("type") != "access":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    return int(payload["sub"])


@app.on_event("startup")
def on_startup() -> None:
    init_db()


@app.post("/auth/register", response_model=TokenOut)
def register(data: RegisterIn) -> TokenOut:
    existing = fetch_one("SELECT id FROM web_users WHERE email=?", (data.email,))
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    password_hash = hash_password(data.password)
    now = datetime.utcnow().isoformat()
    user_id = execute(
        "INSERT INTO web_users (email, password_hash, created_at) VALUES (?, ?, ?)",
        (data.email, password_hash, now),
    )
    access = create_access_token(user_id)
    refresh, exp = create_refresh_token(user_id)
    execute(
        "INSERT INTO web_sessions (user_id, refresh_token, created_at, expires_at) VALUES (?, ?, ?, ?)",
        (user_id, refresh, now, exp.isoformat()),
    )
    return TokenOut(access_token=access, refresh_token=refresh)


@app.post("/auth/login", response_model=TokenOut)
def login(data: LoginIn) -> TokenOut:
    user = fetch_one("SELECT id, password_hash FROM web_users WHERE email=?", (data.email,))
    if not user:
        password_hash = hash_password(data.password)
        now = datetime.utcnow().isoformat()
        user_id = execute(
            "INSERT INTO web_users (email, password_hash, created_at) VALUES (?, ?, ?)",
            (data.email, password_hash, now),
        )
        access = create_access_token(user_id)
        refresh, exp = create_refresh_token(user_id)
        execute(
            "INSERT INTO web_sessions (user_id, refresh_token, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (user_id, refresh, now, exp.isoformat()),
        )
        return TokenOut(access_token=access, refresh_token=refresh)
    if not verify_password(data.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    now = datetime.utcnow().isoformat()
    access = create_access_token(user["id"])
    refresh, exp = create_refresh_token(user["id"])
    execute(
        "INSERT INTO web_sessions (user_id, refresh_token, created_at, expires_at) VALUES (?, ?, ?, ?)",
        (user["id"], refresh, now, exp.isoformat()),
    )
    return TokenOut(access_token=access, refresh_token=refresh)


@app.post("/auth/refresh", response_model=TokenOut)
async def refresh_token(request: Request) -> TokenOut:
    body = await request.json()
    if not body or "refresh_token" not in body:
        raise HTTPException(status_code=400, detail="refresh_token required")
    refresh = body["refresh_token"]
    payload = decode_token(refresh)
    if not payload or payload.get("type") != "refresh":
        raise HTTPException(status_code=401, detail="Invalid refresh token")
    session = fetch_one("SELECT id FROM web_sessions WHERE refresh_token=?", (refresh,))
    if not session:
        raise HTTPException(status_code=401, detail="Session not found")
    user_id = int(payload["sub"])
    access = create_access_token(user_id)
    new_refresh, exp = create_refresh_token(user_id)
    execute("DELETE FROM web_sessions WHERE refresh_token=?", (refresh,))
    execute(
        "INSERT INTO web_sessions (user_id, refresh_token, created_at, expires_at) VALUES (?, ?, ?, ?)",
        (user_id, new_refresh, datetime.utcnow().isoformat(), exp.isoformat()),
    )
    return TokenOut(access_token=access, refresh_token=new_refresh)


@app.post("/auth/logout")
async def logout(request: Request) -> dict:
    body = await request.json()
    if not body or "refresh_token" not in body:
        raise HTTPException(status_code=400, detail="refresh_token required")
    execute("DELETE FROM web_sessions WHERE refresh_token=?", (body["refresh_token"],))
    return {"ok": True}


@app.get("/auth/me")
def me(user_id: int = Depends(get_current_user_id)) -> dict:
    user = fetch_one("SELECT id, email, created_at FROM web_users WHERE id=?", (user_id,))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    link = fetch_one(
        "SELECT telegram_user_id FROM web_user_links WHERE web_user_id=?",
        (user_id,),
    )
    return {
        "id": user["id"],
        "email": user["email"],
        "created_at": user["created_at"],
        "telegram_user_id": link["telegram_user_id"] if link else None,
    }


@app.post("/link/telegram")
def link_telegram(data: LinkTelegramIn, user_id: int = Depends(get_current_user_id)) -> dict:
    execute(
        "INSERT OR REPLACE INTO web_user_links (web_user_id, telegram_user_id) VALUES (?, ?)",
        (user_id, data.telegram_user_id),
    )
    return {"ok": True}


@app.post("/link/telegram/code")
def link_telegram_code(data: LinkCodeIn, user_id: int = Depends(get_current_user_id)) -> dict:
    row = fetch_one(
        "SELECT code, telegram_user_id, expires_at, used FROM link_codes WHERE code=?",
        (data.code,),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Code not found")
    if row["used"]:
        raise HTTPException(status_code=400, detail="Code already used")
    if datetime.fromisoformat(row["expires_at"]) < datetime.utcnow():
        raise HTTPException(status_code=400, detail="Code expired")
    execute(
        "INSERT OR REPLACE INTO web_user_links (web_user_id, telegram_user_id) VALUES (?, ?)",
        (user_id, row["telegram_user_id"]),
    )
    execute("UPDATE link_codes SET used=1 WHERE code=?", (data.code,))
    return {"ok": True}


@app.post("/link/resolve")
def link_resolve(data: LinkCodeIn) -> dict:
    row = fetch_one(
        "SELECT code, telegram_user_id, expires_at, used FROM link_codes WHERE code=?",
        (data.code,),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Code not found")
    if row["used"]:
        raise HTTPException(status_code=400, detail="Code already used")
    if datetime.fromisoformat(row["expires_at"]) < datetime.utcnow():
        raise HTTPException(status_code=400, detail="Code expired")
    execute("UPDATE link_codes SET used=1 WHERE code=?", (data.code,))
    return {"telegram_user_id": row["telegram_user_id"]}


def get_telegram_id_from_request(
    telegram_user_id: int | None = Query(default=None),
    x_telegram_id: int | None = Header(default=None, alias="X-Telegram-Id"),
) -> int:
    tg = telegram_user_id or x_telegram_id
    if not tg:
        raise HTTPException(status_code=400, detail="telegram_user_id required")
    return int(tg)


@app.get("/habits")
def list_habits(telegram_id: int = Depends(get_telegram_id_from_request)) -> list[dict]:
    rows = fetch_all(
        "SELECT id, name, start_date, end_date, reminder_time FROM habits WHERE user_id=? AND is_active=1 ORDER BY id DESC",
        (telegram_id,),
    )
    return [dict(r) for r in rows]


@app.get("/friends/habits")
def list_friend_habits(telegram_id: int = Depends(get_telegram_id_from_request)) -> list[dict]:
    rows = fetch_all(
        """
        SELECT h.id, h.name, h.start_date, h.end_date, h.reminder_time
        FROM shares s
        JOIN habits h ON h.id = s.habit_id
        WHERE s.viewer_id=? AND h.is_active=1
        ORDER BY h.id DESC
        """,
        (telegram_id,),
    )
    return [dict(r) for r in rows]


@app.post("/habits")
def create_habit(data: HabitCreateIn, telegram_id: int = Depends(get_telegram_id_from_request)) -> dict:
    habit_id = execute(
        """
        INSERT INTO habits (user_id, name, start_date, end_date, reminder_time)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            telegram_id,
            data.name,
            data.start_date.isoformat(),
            data.end_date.isoformat() if data.end_date else None,
            data.reminder_time,
        ),
    )
    return {"id": habit_id}


@app.post("/habits/{habit_id}/checkin")
def add_checkin(habit_id: int, data: CheckinIn, telegram_id: int = Depends(get_telegram_id_from_request)) -> dict:
    day = data.day or date.today()
    habit = fetch_one(
        "SELECT id FROM habits WHERE id=? AND user_id=? AND is_active=1",
        (habit_id, telegram_id),
    )
    if not habit:
        raise HTTPException(status_code=404, detail="Habit not found")
    execute(
        "DELETE FROM habit_skips WHERE habit_id=? AND skip_date=?",
        (habit_id, day.isoformat()),
    )
    try:
        execute(
            "INSERT INTO checkins (habit_id, checkin_date) VALUES (?, ?)",
            (habit_id, day.isoformat()),
        )
    except Exception:
        pass
    return {"ok": True}


@app.post("/habits/{habit_id}/skip")
def add_skip(habit_id: int, data: CheckinIn, telegram_id: int = Depends(get_telegram_id_from_request)) -> dict:
    day = data.day or date.today()
    habit = fetch_one(
        "SELECT id FROM habits WHERE id=? AND user_id=? AND is_active=1",
        (habit_id, telegram_id),
    )
    if not habit:
        raise HTTPException(status_code=404, detail="Habit not found")
    execute(
        "INSERT OR IGNORE INTO habit_skips (habit_id, skip_date) VALUES (?, ?)",
        (habit_id, day.isoformat()),
    )
    return {"ok": True}


@app.delete("/habits/{habit_id}")
def delete_habit(habit_id: int, telegram_id: int = Depends(get_telegram_id_from_request)) -> dict:
    execute(
        "UPDATE habits SET is_active=0 WHERE id=? AND user_id=?",
        (habit_id, telegram_id),
    )
    return {"ok": True}


@app.get("/habits/{habit_id}/stats")
def habit_stats(habit_id: int, telegram_id: int = Depends(get_telegram_id_from_request)) -> dict:
    habit = fetch_one(
        """
        SELECT h.* FROM habits h
        WHERE h.id=? AND (h.user_id=? OR EXISTS(
            SELECT 1 FROM shares s WHERE s.habit_id=h.id AND s.viewer_id=?
        ))
        """,
        (habit_id, telegram_id, telegram_id),
    )
    if not habit:
        raise HTTPException(status_code=404, detail="Habit not found")
    rows = fetch_all(
        "SELECT checkin_date FROM checkins WHERE habit_id=? ORDER BY checkin_date",
        (habit_id,),
    )
    skip_rows = fetch_all(
        "SELECT skip_date FROM habit_skips WHERE habit_id=? ORDER BY skip_date",
        (habit_id,),
    )
    dates = [datetime.strptime(r["checkin_date"], "%Y-%m-%d").date() for r in rows]
    skipped_dates = [datetime.strptime(r["skip_date"], "%Y-%m-%d").date() for r in skip_rows]
    total = len(dates)
    today = date.today()
    start = datetime.strptime(habit["start_date"], "%Y-%m-%d").date()
    end = datetime.strptime(habit["end_date"], "%Y-%m-%d").date() if habit["end_date"] else None
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

    return {
        "id": habit_id,
        "name": habit["name"],
        "total": total,
        "completion": round(completion),
        "month_completion": round(month_completion),
    }


@app.get("/habits/{habit_id}/calendar")
def habit_calendar(habit_id: int, month: str, telegram_id: int = Depends(get_telegram_id_from_request)) -> dict:
    habit = fetch_one(
        """
        SELECT h.* FROM habits h
        WHERE h.id=? AND (h.user_id=? OR EXISTS(
            SELECT 1 FROM shares s WHERE s.habit_id=h.id AND s.viewer_id=?
        ))
        """,
        (habit_id, telegram_id, telegram_id),
    )
    if not habit:
        raise HTTPException(status_code=404, detail="Habit not found")
    try:
        month_date = datetime.strptime(month, "%Y-%m").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Month format YYYY-MM")
    start = month_date.replace(day=1)
    next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    rows = fetch_all(
        """
        SELECT checkin_date FROM checkins
        WHERE habit_id=? AND checkin_date>=? AND checkin_date<?
        """,
        (habit_id, start.isoformat(), next_month.isoformat()),
    )
    skip_rows = fetch_all(
        """
        SELECT skip_date FROM habit_skips
        WHERE habit_id=? AND skip_date>=? AND skip_date<?
        """,
        (habit_id, start.isoformat(), next_month.isoformat()),
    )
    marked = {r["checkin_date"] for r in rows}
    skipped = {r["skip_date"] for r in skip_rows}
    return {"marked": sorted(marked), "skipped": sorted(skipped)}
