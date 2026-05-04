from fastapi import APIRouter, HTTPException
from ..schemas.auth import LoginRequest, TokenResponse
from ..utils.auth import create_access_token

router = APIRouter(prefix='/auth', tags=['auth'])

@router.post('/login', response_model=TokenResponse)
async def login(payload: LoginRequest):
    if not payload.username or not payload.password:
        raise HTTPException(status_code=400, detail='Invalid credentials')
    return TokenResponse(access_token=create_access_token(payload.username))

@router.get('/me')
async def me():
    return {'username': 'demo', 'role': 'admin'}
