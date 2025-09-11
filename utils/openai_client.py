from __future__ import annotations

import asyncio
import json
import random
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..core.config import load_config
from .rate_limit import AsyncRateLimiter
from ..core.settings import Settings


try:
    from openai import OpenAI
    from openai import RateLimitError
except Exception:  # pragma: no cover - optional dep
    OpenAI = None  # type: ignore
    RateLimitError = Exception  # type: ignore


@dataclass
class SentimentResult:
    score: float  # -1..1
    confidence: float  # 0..1
    reason: Optional[str] = None


class OpenAISentiment:
    def __init__(self, model: str = "gpt-4o-mini") -> None:
        self.cfg = load_config()
        self._api_key = self.cfg.openai_project_key
        self._model = model
        self._client = None
        settings = Settings.load()
        rps = settings.openai_rps or 2.0
        self._rl = AsyncRateLimiter(min_interval=1.0 / float(max(0.1, rps)))
        if OpenAI and self._api_key:
            try:
                self._client = OpenAI(api_key=self._api_key)
            except Exception:
                self._client = None

    @property
    def available(self) -> bool:
        return self._client is not None

    async def analyze(self, text: str) -> Optional[SentimentResult]:
        if not self.available:
            return None
        await self._rl.wait()
        # run the sync SDK call in a thread and retry on rate limits/5xx
        max_tries = 4
        backoff = 0.6
        last_err: Optional[Exception] = None

        def _call() -> Dict[str, Any]:
            assert self._client is not None
            resp = self._client.chat.completions.create(
                model=self._model,
                response_format={"type": "json_object"},
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a precise financial sentiment rater. "
                            "Return JSON with fields: score (-1..1), confidence (0..1), reason (string)."
                        ),
                    },
                    {
                        "role": "user",
                        "content": f"Text: {text}\nReturn only JSON.",
                    },
                ],
                temperature=0.2,
            )
            content = resp.choices[0].message.content or "{}"
            return json.loads(content)

        for attempt in range(1, max_tries + 1):
            try:
                data = await asyncio.to_thread(_call)
                score = float(data.get("score", 0.0))
                confidence = float(data.get("confidence", 0.6))
                reason = data.get("reason")
                score = max(-1.0, min(1.0, score))
                confidence = max(0.0, min(1.0, confidence))
                return SentimentResult(score=score, confidence=confidence, reason=reason)
            except Exception as e:  # includes RateLimitError
                last_err = e
                if attempt < max_tries:
                    await asyncio.sleep(backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.25))
                    continue
                break
        return None
