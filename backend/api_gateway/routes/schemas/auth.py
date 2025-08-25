from typing import Annotated, Optional
from pydantic import BaseModel, EmailStr, StringConstraints

Username = Annotated[str, StringConstraints(strip_whitespace=True,
                                            min_length=3, max_length=30)]
Password = Annotated[str, StringConstraints(min_length=6, max_length=50)]

class UserCreate(BaseModel):
    username: Username
    email:    EmailStr
    password: Password
    ib_username: Optional[str] = None   # ← added
    ib_password: Optional[str] = None   # ← added

class UserLogin(BaseModel):
    username: Username
    password: Password

class Token(BaseModel):
    access_token: str
    token_type:   str = "bearer"

class UserPublic(BaseModel):
    id:       int
    username: str
    email:    EmailStr