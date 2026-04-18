from fastapi import APIRouter, HTTPException, status
from app.schemas.user import UserCreate, UserLogin, UserOut, TokenOut
from app.services.user_service import register_user, login_user

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=UserOut, status_code=201)
async def register(data: UserCreate):
    try:
        return await register_user(data)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/login", response_model=TokenOut)
async def login(data: UserLogin):
    try:
        return await login_user(data)
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))
