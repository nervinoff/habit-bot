import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import jwt
from passlib.context import CryptContext


JWT_SECRET = os.getenv("JWT_SECRET", "change_me")
JWT_ALG = "HS256"
ACCESS_TTL_MIN = int(os.getenv("ACCESS_TTL_MIN", "30"))
REFRESH_TTL_DAYS = int(os.getenv("REFRESH_TTL_DAYS", "30"))

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)


def create_access_token(user_id: int) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=ACCESS_TTL_MIN)).timestamp()),
        "type": "access",
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)


def create_refresh_token(user_id: int) -> tuple[str, datetime]:
    now = datetime.now(timezone.utc)
    exp = now + timedelta(days=REFRESH_TTL_DAYS)
    payload = {
        "sub": str(user_id),
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
        "type": "refresh",
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)
    return token, exp


def decode_token(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
    except jwt.PyJWTError:
        return None
