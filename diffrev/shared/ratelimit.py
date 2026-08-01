import asyncio
import time
from collections import deque


class RateLimiter:
    """Sliding-window rate limiter. `check()` records a hit and returns
    (allowed, retry_after_seconds)."""

    def __init__(self, limit: int, window_seconds: float = 60.0):
        self.limit = limit
        self.window = window_seconds
        self._hits = deque()
        self._lock = asyncio.Lock()

    async def check(self) -> tuple[bool, float]:
        now = time.monotonic()
        async with self._lock:
            while self._hits and now - self._hits[0] >= self.window:
                self._hits.popleft()
            if len(self._hits) >= self.limit:
                retry_after = self.window - (now - self._hits[0])
                return False, max(0.0, retry_after)
            self._hits.append(now)
            return True, 0.0
