import asyncio
import time
from typing import Optional


class AsyncRateLimiter:
    """Simple async rate limiter using a minimum interval between acquires.

    Example: AsyncRateLimiter(min_interval=1.0) -> max ~60 req/min.
    """

    def __init__(self, min_interval: float) -> None:
        self.min_interval = max(0.0, float(min_interval))
        self._lock = asyncio.Lock()
        self._next_time = 0.0

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if now < self._next_time:
                await asyncio.sleep(self._next_time - now)
                now = time.monotonic()
            self._next_time = now + self.min_interval
