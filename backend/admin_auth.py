import os
import time

import jwt


JWT_SECRET = os.getenv("ADMIN_JWT_SECRET", "change-me")
JWT_TTL_SECONDS = int(os.getenv("ADMIN_JWT_TTL_SECONDS", str(24 * 60 * 60)))
JWT_ALG = "HS256"


def create_admin_token(username: str) -> tuple[str, int]:
    now = int(time.time())
    exp = now + JWT_TTL_SECONDS
    token = jwt.encode(
        {"sub": username, "iat": now, "exp": exp},
        JWT_SECRET,
        algorithm=JWT_ALG,
    )
    return token, exp


def verify_admin_token(token: str) -> dict:
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
