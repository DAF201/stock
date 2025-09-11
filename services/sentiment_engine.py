import asyncio
import math
from typing import Any, Dict

from ..core.bus import MessageBus
from ..core.config import load_config
from ..models import Sentiment, now_ts
from ..core.storage import insert
from ..utils.openai_client import OpenAISentiment


def time_decay_weight(age_hours: float, half_life_hours: float = 24.0) -> float:
    # weight = e^(-lambda * t); lambda = ln(2)/half_life
    lam = math.log(2.0) / half_life_hours
    return math.exp(-lam * age_hours)


def simple_vader_score(text: str) -> float:
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        analyzer = SentimentIntensityAnalyzer()
        s = analyzer.polarity_scores(text)
        return float(s.get("compound", 0.0))
    except Exception:
        # fallback naive heuristic
        text_l = text.lower()
        pos = sum(text_l.count(w) for w in ["good", "rally", "beat", "up", "gain"]) \
            + text_l.count("+")
        neg = sum(text_l.count(w) for w in ["bad", "fall", "miss", "down", "loss"]) \
            + text_l.count("-")
        score = (pos - neg) / max(1, (pos + neg))
        return max(-1.0, min(1.0, score))


class SentimentEngine:
    def __init__(self, bus: MessageBus) -> None:
        self.bus = bus
        self.cfg = load_config()
        self._oa = OpenAISentiment()

    async def run(self) -> None:
        async for item in self.bus.subscribe("news.raw"):
            text = item.get("text", "")
            symbols = item.get("symbols") or [None]
            score = simple_vader_score(text)
            # Assume age ~ 0 when ingested
            impact = score * time_decay_weight(0.0)
            # Optionally refine with OpenAI if available
            if self._oa.available:
                try:
                    oa_res = await self._oa.analyze(text)
                    if oa_res is not None:
                        score = float(oa_res.score)
                        impact = score * time_decay_weight(0.0)
                except Exception as e:
                    # Keep VADER result on failure
                    pass
            for sym in symbols:
                msg = Sentiment(
                    symbol=sym,
                    ts=now_ts(),
                    score=score,
                    confidence=0.6,
                    relevance=1.0 if sym else 0.5,
                    impact=impact,
                    text=text,
                    source=item.get("source", "news"),
                )
                await self.bus.publish("sentiment.processed", msg.to_dict())
                insert("sentiment", msg.to_dict())
