import asyncio
import math
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

import requests

from ..core.bus import MessageBus
from ..core.config import load_config
from ..models import PriceTick, now_ts
from ..core.storage import insert_many
from ..utils.rate_limit import AsyncRateLimiter
from ..utils.http import get_json
from ..core.settings import Settings


class DataCollectionService:
    def __init__(self, bus: MessageBus, symbols: List[str]) -> None:
        self.bus = bus
        self.symbols = symbols
        self.cfg = load_config()
        # Conservative limits: Finnhub ~60/min on free plans; Yahoo unoffical -> keep light
        settings = Settings.load()
        finnhub_rpm = settings.finnhub_rpm or 50
        yahoo_rps = settings.yahoo_rps or 5.0
        self._finnhub_rl = AsyncRateLimiter(min_interval=60.0 / float(max(1, finnhub_rpm)))
        self._yahoo_rl = AsyncRateLimiter(min_interval=1.0 / float(max(0.1, yahoo_rps)))

    async def run(self) -> None:
        tasks = [self._poll_prices(), self._poll_news_sentiment_sources()]
        await asyncio.gather(*tasks)

    async def _poll_prices(self) -> None:
        while True:
            try:
                ticks: List[PriceTick] = []
                # If Finnhub key is present, fetch per symbol (simple, respects Finnhub rate limits externally)
                if self.cfg.finnhub_key:
                    for sym in self.symbols:
                        await self._finnhub_rl.wait()
                        price = await self._fetch_price(sym)
                        tick = PriceTick(symbol=sym, ts=now_ts(), price=price, source="finnhub")
                        ticks.append(tick)
                        await self.bus.publish("prices.raw", tick.to_dict())
                else:
                    # Batch Yahoo requests to reduce request count
                    for i in range(0, len(self.symbols), 50):
                        batch = self.symbols[i : i + 50]
                        await self._yahoo_rl.wait()
                        for sym, price in await self._fetch_prices_yahoo_batch(batch):
                            tick = PriceTick(symbol=sym, ts=now_ts(), price=price, source="yahoo")
                            ticks.append(tick)
                            await self.bus.publish("prices.raw", tick.to_dict())
                insert_many("prices", [t.to_dict() for t in ticks])
            except Exception as e:
                print(f"[DataCollection] price poll error: {e}")
            await asyncio.sleep(60)

    async def _fetch_price(self, symbol: str) -> float:
        # Prefer Finnhub if key present, fall back to Yahoo.
        if self.cfg.finnhub_key:
            try:
                # Try as provided, then fallback to '.' variant for class shares
                candidates = [symbol]
                if "-" in symbol:
                    candidates.append(symbol.replace("-", "."))
                for cand in candidates:
                    data = await get_json(
                        "https://finnhub.io/api/v1/quote",
                        params={"symbol": cand, "token": self.cfg.finnhub_key},
                        timeout=10,
                    )
                    if "c" in data and data["c"]:
                        return float(data["c"])
            except Exception:
                pass
        # Yahoo Finance unofficial quote endpoint (single symbol)
        for sym, price in await self._fetch_prices_yahoo_batch([symbol]):
            return float(price)
        raise RuntimeError(f"No price for {symbol}")

    async def _fetch_prices_yahoo_batch(self, symbols: List[str]) -> List[tuple[str, float]]:
        if not symbols:
            return []
        data = await get_json(
            "https://query1.finance.yahoo.com/v7/finance/quote",
            params={"symbols": ",".join(symbols)},
            timeout=15,
        )
        results = data.get("quoteResponse", {}).get("result", [])
        out: List[tuple[str, float]] = []
        for row in results:
            sym = row.get("symbol")
            price = row.get("regularMarketPrice") or row.get("postMarketPrice")
            if sym and price is not None:
                out.append((sym, float(price)))
        return out

    async def _poll_news_sentiment_sources(self) -> None:
        # Placeholder: publish minimal news placeholders for downstream processing.
        # Real implementation would ingest GDELT, SEC, etc.
        headlines = [
            {"text": "Market opens higher on tech rally", "symbols": self.symbols[:1]},
            {"text": "Regulatory uncertainty clouds outlook", "symbols": self.symbols[1:2]},
        ]
        idx = 0
        while True:
            payload = headlines[idx % len(headlines)]
            payload.update({"ts": now_ts(), "source": "placeholder"})
            await self.bus.publish("news.raw", payload)
            idx += 1
            await asyncio.sleep(300)
