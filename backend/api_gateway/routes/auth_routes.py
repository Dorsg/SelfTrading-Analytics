from fastapi import APIRouter, HTTPException, status, Depends

from api_gateway.routes.schemas.auth import UserCreate, UserLogin, Token, UserPublic
from api_gateway.security.auth import create_access_token, get_current_user
from database.db_manager import DBManager
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])

# ───────── signup ─────────
@router.post("/signup", response_model=UserPublic, status_code=201)
def signup(payload: UserCreate):
    with DBManager() as db:
        try:
            user = db.create_user(**payload.model_dump())
            return user      # FastAPI converts via response_model
        except ValueError as e:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(e))
        except Exception:
            logger.exception("Signup failed")
            raise HTTPException(500, "Internal error")

# ───────── login ─────────
@router.post("/login", response_model=Token)
def login(payload: UserLogin):
    try:
        with DBManager() as db:
            if (user := db.authenticate(**payload.model_dump())) is None:
                raise HTTPException(status.HTTP_401_UNAUTHORIZED,
                                    "Incorrect username or password")
            token = create_access_token(user.username)
            return {"access_token": token}
    except HTTPException:
        # Re-raise HTTP exceptions (401) as-is
        raise
    except Exception as e:
        logger.exception("Login failed due to database error")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                           "Login service temporarily unavailable")

# ───────── handy “who-am-I” endpoint ─────────
@router.get("/me", response_model=UserPublic)
def me(current=Depends(get_current_user)):
    return current