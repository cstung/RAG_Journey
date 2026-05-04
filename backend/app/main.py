from fastapi import FastAPI
from .config import settings
from .routers import auth, chat

app = FastAPI(title=settings.app_name)
app.include_router(auth.router)
app.include_router(chat.router)

@app.get('/healthz')
async def healthz():
    return {'ok': True}
