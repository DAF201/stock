import asyncio
from typing import Dict, Optional

import requests

from ..core.bus import MessageBus
from ..core.config import load_config
from ..core.storage import insert
from ..utils.market_hours import should_trade_now
from ..utils.rate_limit import AsyncRateLimiter
from ..utils.http import get_json, post_json
from ..core.settings import Settings


class TradeExecutionManager:
    def __init__(self, bus: MessageBus) -> None:
        self.bus = bus
        self.cfg = load_config()
        self.mode = "paper"  # or "live"
        self.max_positions = 12
        settings = Settings.load()
        self.enable_orders = settings.enable_orders  # default False
        self.mode = settings.alpaca_mode or self.mode
        alpaca_rps = settings.alpaca_rps or 1.0
        yahoo_rps = settings.yahoo_rps or 3.3
        self._alpaca_rl = AsyncRateLimiter(min_interval=1.0 / float(max(0.1, alpaca_rps)))
        self._yahoo_rl = AsyncRateLimiter(min_interval=1.0 / float(max(0.1, yahoo_rps)))
        self._blackout_after_open = settings.blackout_after_open_min
        self._blackout_before_close = settings.blackout_before_close_min

    async def run(self) -> None:
        async for sig in self.bus.subscribe("signals.analysis"):
            await self._handle_signal(sig)

    async def _handle_signal(self, sig: Dict) -> None:
        # Respect market hours and blackout windows (holidays, after open, before close)
        ok, reason = should_trade_now(
            None,
            blackout_after_open_min=self._blackout_after_open,
            blackout_before_close_min=self._blackout_before_close,
        )
        if not ok:
            print(f"[Trade] Skipping signal due to market hours: {reason}")
            return
        # Simple rule: if BUY with pos>0 and enabled, send order; SELL becomes no-op placeholder
        symbol = sig["symbol"]
        rec = sig["recommendation"]
        size_pct = float(sig.get("position_size_pct", 0.0))
        if rec == "BUY" and size_pct > 0 and self.enable_orders:
            qty = await self._estimate_qty(symbol, size_pct)
            if qty > 0:
                self._place_order(symbol, "buy", qty)
        # record decision
        insert(
            "trades",
            {
                "symbol": symbol,
                "ts": sig["ts"],
                "side": rec.lower(),
                "qty": float(sig.get("position_size_pct", 0.0)),
                "order_type": "analysis",
                "limit_price": None,
                "stop_loss": None,
            },
        )

    async def _estimate_qty(self, symbol: str, pos_pct: float) -> int:
        # placeholder: fixed portfolio of 100k
        portfolio_value = 100_000.0
        alloc = portfolio_value * (pos_pct / 100.0)
        price = await self._fetch_price(symbol)
        if price <= 0:
            return 0
        return int(alloc // price)

    async def _fetch_price(self, symbol: str) -> float:
        await self._yahoo_rl.wait()
        data = await get_json(
            "https://query1.finance.yahoo.com/v7/finance/quote",
            params={"symbols": symbol},
            timeout=10,
        )
        result = data.get("quoteResponse", {}).get("result", [{}])[0]
        price = result.get("regularMarketPrice") or result.get("postMarketPrice")
        return float(price or 0.0)

    async def _place_order(self, symbol: str, side: str, qty: int) -> None:
        alp = self.cfg.alpaca.get(self.mode, {})
        key = alp.get("key")
        secret = alp.get("secret")
        base = alp.get("base_url") or alp.get("API")
        if not (key and secret and base):
            print("[Trade] Missing Alpaca creds; skipping order")
            return
        try:
            headers = {
                "APCA-API-KEY-ID": key,
                "APCA-API-SECRET-KEY": secret,
                "Content-Type": "application/json",
            }
            payload = {
                "symbol": symbol,
                "qty": qty,
                "side": side,
                "type": "market",
                "time_in_force": "day",
            }
            # Safety: do not actually hit API unless explicitly enabled
            if not self.enable_orders:
                print(f"[Trade] Would place order: {payload}")
                return
            await self._alpaca_rl.wait()
            status, text = await post_json(f"{base}/orders", json_body=payload, headers=headers, timeout=10)
            print(f"[Trade] Order response {status}: {text[:200]}")
        except Exception as e:
            print(f"[Trade] Order error: {e}")
