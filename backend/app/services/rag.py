from collections.abc import AsyncGenerator

async def stream_answer(message: str) -> AsyncGenerator[str, None]:
    text = f"[Draft legal answer] {message}"
    for token in text.split():
        yield token + ' '
