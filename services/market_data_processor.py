import asyncio
from collections import deque
from typing import Deque, Dict

from ..core.bus import MessageBus
from ..models import Indicators
from ..core.storage import insert


def calc_sma(values: Deque[float]) -> float:
    return sum(values) / max(1, len(values))


def calc_rsi(closes: Deque[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains = 0.0
    losses = 0.0
    for i in range(-period, 0):
        chg = closes[i] - closes[i - 1]
        if chg >= 0:
            gains += chg
        else:
            losses -= chg
    if losses == 0:
        return 100.0
    rs = (gains / period) / (losses / period)
    return 100.0 - (100.0 / (1.0 + rs))


class MarketDataProcessor:
    def __init__(self, bus: MessageBus) -> None:
        self.bus = bus
        self._price_buffers: Dict[str, Deque[float]] = {}

    async def run(self) -> None:
        async for p in self.bus.subscribe("prices.raw"):
            sym = p["symbol"]
            price = float(p["price"])
            ts = p["ts"]
            buf = self._price_buffers.setdefault(sym, deque(maxlen=100))
            buf.append(price)
            sma_fast = calc_sma(deque(list(buf)[-5:])) if len(buf) >= 5 else None
            sma_slow = calc_sma(deque(list(buf)[-20:])) if len(buf) >= 20 else None
            rsi = calc_rsi(buf) if len(buf) >= 15 else None
            ind = Indicators(
                symbol=sym,
                ts=ts,
                rsi=rsi,
                sma_fast=sma_fast,
                sma_slow=sma_slow,
            )
            await self.bus.publish("metrics.indicators", ind.to_dict())
            insert("indicators", ind.to_dict())
