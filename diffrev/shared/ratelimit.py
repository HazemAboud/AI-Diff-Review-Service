import asyncio
import time
from collections import deque


class RateLimiter:
    """Sliding-window rate limiter. `check()` records a hit and returns
    (allowed, retry_after_seconds)."""

    def __init__(self, limit, window_seconds=60.0):
        self.limit = limit
        self.window = window_seconds
        self.hits = deque()
        self.lock = asyncio.Lock()

    async def check(self):
        now = time.monotonic()
        async with self.lock:
            while self.hits and now - self.hits[0] >= self.window:
                self.hits.popleft()
            if len(self.hits) >= self.limit:
                retry_after = self.window - (now - self.hits[0])
                return False, max(0.0, retry_after)
            self.hits.append(now)
            return True, 0.0
