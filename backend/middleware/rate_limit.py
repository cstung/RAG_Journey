import threading
from collections import defaultdict
from time import time


class RateLimiter:
    """
    Thread-safe sliding window rate limiter.

    Args:
        max_requests: Maximum allowed requests per window.
        window_seconds: Length of the rolling window in seconds.
    """

    def __init__(self, max_requests: int = 20, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window = window_seconds
        self._store: dict[str, list[float]] = defaultdict(list)
        self._lock = threading.Lock()

    def is_allowed(self, key: str) -> bool:
        """
        Returns True if the key is within its rate limit.
        Evicts expired timestamps on every call (no background thread needed).
        """
        now = time()
        with self._lock:
            # Evict timestamps outside the window
            self._store[key] = [t for t in self._store[key] if now - t < self.window]

            if len(self._store[key]) >= self.max_requests:
                return False

            self._store[key].append(now)
            return True


# --- Module-level instances ---

# Tight limit per session: 20 req / 60s
session_limiter = RateLimiter(max_requests=20, window_seconds=60)

# Looser limit per IP: 60 req / 60s
# Catches attackers who cycle session IDs from the same IP
ip_limiter = RateLimiter(max_requests=60, window_seconds=60)
