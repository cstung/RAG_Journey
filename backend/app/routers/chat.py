import json
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from ..services.rag import stream_answer

router = APIRouter(prefix='/chat', tags=['chat'])

class ChatRequest(BaseModel):
    message: str
    language: str = 'vi'

@router.post('/stream')
async def chat_stream(payload: ChatRequest):
    async def event_gen():
        async for token in stream_answer(payload.message):
            yield f"data: {json.dumps({'type': 'token', 'content': token})}\n\n"
        yield f"data: {json.dumps({'type': 'done'})}\n\n"
    return StreamingResponse(event_gen(), media_type='text/event-stream')
