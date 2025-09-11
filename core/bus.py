import asyncio
import json
import os
from typing import Any, AsyncIterator, Dict


class MessageBus:
    async def publish(self, topic: str, message: Dict[str, Any]) -> None:
        raise NotImplementedError

    async def subscribe(self, topic: str) -> AsyncIterator[Dict[str, Any]]:
        raise NotImplementedError


class InMemoryBus(MessageBus):
    def __init__(self) -> None:
        self._topics: Dict[str, asyncio.Queue] = {}

    def _get_queue(self, topic: str) -> asyncio.Queue:
        if topic not in self._topics:
            self._topics[topic] = asyncio.Queue()
        return self._topics[topic]

    async def publish(self, topic: str, message: Dict[str, Any]) -> None:
        await self._get_queue(topic).put(message)

    async def subscribe(self, topic: str) -> AsyncIterator[Dict[str, Any]]:
        q = self._get_queue(topic)
        while True:
            msg = await q.get()
            yield msg


_redis_available = False
try:
    import redis.asyncio as aioredis  # type: ignore

    _redis_available = True
except Exception:
    aioredis = None  # type: ignore


class RedisBus(MessageBus):
    def __init__(self, url: str | None = None) -> None:
        if not _redis_available:
            raise RuntimeError("redis-py not installed")
        self._url = url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self._client = aioredis.from_url(self._url)

    async def publish(self, topic: str, message: Dict[str, Any]) -> None:
        payload = json.dumps(message).encode()
        await self._client.xadd(topic, {"data": payload})

    async def subscribe(self, topic: str) -> AsyncIterator[Dict[str, Any]]:
        last_id = "$"
        while True:
            streams = await self._client.xread({topic: last_id}, block=1000, count=10)
            for _, entries in streams or []:
                for entry_id, data in entries:
                    last_id = entry_id
                    payload = data.get(b"data") or data.get("data")
                    if payload:
                        yield json.loads(payload)


def get_bus(prefer_redis: bool = False) -> MessageBus:
    if prefer_redis and _redis_available:
        try:
            return RedisBus()
        except Exception:
            pass
    return InMemoryBus()
