from datetime import datetime, timezone
from typing import Optional
from bson import ObjectId
from app.db.mongodb import get_db
from app.core.security import hash_password, verify_password, create_access_token
from app.schemas.user import UserCreate, UserLogin


async def register_user(data: UserCreate) -> dict:
    db = get_db()
    existing = await db.users.find_one({"email": data.email})
    if existing:
        raise ValueError("Email already registered")

    user_doc = {
        "email": data.email,
        "username": data.username,
        "hashed_password": hash_password(data.password),
        "created_at": datetime.now(timezone.utc),
    }
    result = await db.users.insert_one(user_doc)
    user_doc["_id"] = result.inserted_id
    return _format_user(user_doc)


async def login_user(data: UserLogin) -> dict:
    db = get_db()
    user = await db.users.find_one({"email": data.email})
    if not user or not verify_password(data.password, user["hashed_password"]):
        raise ValueError("Invalid credentials")

    token = create_access_token({"sub": str(user["_id"])})
    return {"access_token": token, "token_type": "bearer"}


async def get_user_by_id(user_id: str) -> Optional[dict]:
    db = get_db()
    user = await db.users.find_one({"_id": ObjectId(user_id)})
    if user:
        return _format_user(user)
    return None


def _format_user(user: dict) -> dict:
    return {
        "id": str(user["_id"]),
        "email": user["email"],
        "username": user["username"],
    }
