import asyncio
from collections import deque
from typing import Deque, Dict, Tuple

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
        # EMA state for MACD per symbol: (ema_fast, ema_slow, signal)
        self._ema_state: Dict[str, Tuple[float, float, float] | None] = {}

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

            # Bollinger Bands (20, 2)
            bb_upper = bb_middle = bb_lower = None
            if len(buf) >= 20:
                last20 = list(buf)[-20:]
                m = sum(last20) / 20.0
                var = sum((x - m) ** 2 for x in last20) / 20.0
                std = var ** 0.5
                bb_middle = m
                bb_upper = m + 2 * std
                bb_lower = m - 2 * std

            # MACD (12,26,9)
            macd = macd_signal = None
            state = self._ema_state.get(sym)
            alpha_fast = 2.0 / (12 + 1)
            alpha_slow = 2.0 / (26 + 1)
            alpha_sig = 2.0 / (9 + 1)
            if state is None:
                if len(buf) >= 26:
                    # seed EMAs with SMA
                    ema_fast = sum(list(buf)[-12:]) / 12.0
                    ema_slow = sum(list(buf)[-26:]) / 26.0
                    macd_line = ema_fast - ema_slow
                    sig = macd_line  # initialize
                    self._ema_state[sym] = (ema_fast, ema_slow, sig)
                    macd = macd_line
                    macd_signal = sig
            else:
                ema_fast, ema_slow, sig = state
                ema_fast = alpha_fast * price + (1 - alpha_fast) * ema_fast
                ema_slow = alpha_slow * price + (1 - alpha_slow) * ema_slow
                macd_line = ema_fast - ema_slow
                sig = alpha_sig * macd_line + (1 - alpha_sig) * sig
                self._ema_state[sym] = (ema_fast, ema_slow, sig)
                macd = macd_line
                macd_signal = sig

            ind = Indicators(
                symbol=sym,
                ts=ts,
                rsi=rsi,
                macd=macd,
                macd_signal=macd_signal,
                bb_upper=bb_upper,
                bb_middle=bb_middle,
                bb_lower=bb_lower,
                sma_fast=sma_fast,
                sma_slow=sma_slow,
            )
            # Publish with current price for downstream scoring convenience
            payload = ind.to_dict()
            payload["price"] = price
            await self.bus.publish("metrics.indicators", payload)
            # Persist without transient price field
            insert("indicators", ind.to_dict())
