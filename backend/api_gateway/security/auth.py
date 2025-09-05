from datetime import datetime, timedelta, timezone
from typing import Optional
import os

from jose import jwt, JWTError          # pip install python-jose
from passlib.context import CryptContext  # pip install passlib[bcrypt]

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from database.models import User

# ───── settings (put real values in .env) ────────────────────────────
SECRET_KEY  = os.getenv("AUTH_SECRET_KEY", "CHANGE_ME")
ALGORITHM   = os.getenv("AUTH_ALGORITHM", "HS256")
ACCESS_TTL  = int(os.getenv("AUTH_ACCESS_TTL_MINUTES", str(60 * 24)))  # minutes (default 1 day)

pwd_ctx  = CryptContext(schemes=["bcrypt"], deprecated="auto")
bearer   = HTTPBearer(auto_error=False)  # we’ll raise ourselves

# ───── helpers ───────────────────────────────────────────────────────
def hash_password(pw: str) -> str:
    return pwd_ctx.hash(pw)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_ctx.verify(plain, hashed)

def create_access_token(sub: str, ttl_minutes: int = ACCESS_TTL) -> str:
    exp = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
    to_encode = {"sub": sub, "exp": exp}
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str) -> Optional[str]:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except JWTError:
        return None

# ───── FastAPI dependency: current user (rejects 401) ───────────────
def get_current_user(
    cred: HTTPAuthorizationCredentials = Depends(bearer),
) -> User:
    if cred is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not authenticated")

    uid = decode_token(cred.credentials)
    if not uid:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
    
    from database.db_manager import DBManager
    
    try:
        with DBManager() as db:
            user = db.get_user_by_username(uid)
            if user is None:
                raise HTTPException(status.HTTP_401_UNAUTHORIZED, "User not found")
            return user
    except HTTPException:
        raise
    except Exception:
        # Database connection issues should return 503 Service Unavailable
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, 
            "Authentication service temporarily unavailable"
        )
