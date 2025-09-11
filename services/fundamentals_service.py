import asyncio
from typing import Dict, List, Optional

from ..core.bus import MessageBus
from ..core.config import load_config
from ..core.settings import Settings
from ..core.storage import insert
from ..utils.http import get_json
from ..utils.rate_limit import AsyncRateLimiter
from ..models import now_ts


class FundamentalsService:
    def __init__(self, bus: MessageBus, symbols: List[str]) -> None:
        self.bus = bus
        self.cfg = load_config()
        self.settings = Settings.load()
        self.symbols = symbols[: (self.settings.fundamentals_limit or len(symbols))]
        rpm = self.settings.finnhub_rpm or 50
        self._rl = AsyncRateLimiter(min_interval=60.0 / float(max(1, rpm)))

    async def run(self) -> None:
        # Poll fundamentals periodically; Finnhub fundamentals change slowly, so run every few hours
        while True:
            try:
                await self._refresh_all()
            except Exception as e:
                print(f"[Fundamentals] refresh error: {e}")
            await asyncio.sleep(3 * 60 * 60)

    async def _refresh_all(self) -> None:
        for sym in self.symbols:
            try:
                await self._rl.wait()
                data = await self._fetch_metrics(sym)
                if not data:
                    continue
                score, details = self._score(data)
                payload = {
                    "symbol": sym,
                    "ts": now_ts(),
                    "score": score,  # -100..100
                    "pe": data.get("pe"),
                    "pb": data.get("pb"),
                    "peg": data.get("peg"),
                    "revenue_growth": data.get("revenue_growth"),
                    "eps_growth": data.get("eps_growth"),
                    "debt_to_equity": data.get("debt_to_equity"),
                    "profit_margin": data.get("profit_margin"),
                    "details": details,
                }
                await self.bus.publish("fundamentals.processed", payload)
                rec = dict(payload)
                insert("fundamentals", rec)
            except Exception as e:
                print(f"[Fundamentals] error for {sym}: {e}")

    async def _fetch_metrics(self, symbol: str) -> Optional[Dict]:
        if not self.cfg.finnhub_key:
            return None
        js = await get_json(
            "https://finnhub.io/api/v1/stock/metric",
            params={"symbol": symbol, "metric": "all", "token": self.cfg.finnhub_key},
            timeout=15,
            max_tries=4,
        )
        metric = js.get("metric") or {}
        # Map a few common metrics, with safe fallbacks
        return {
            "pe": metric.get("peBasicExclExtraTTM") or metric.get("peInclExtraTTM"),
            "pb": metric.get("ptbRatioTTM") or metric.get("pbAnnual"),
            "peg": metric.get("pegRatioTTM"),
            "revenue_growth": metric.get("revenueGrowthTTM") or metric.get("revenueGrowthQuarterlyYoy"),
            "eps_growth": metric.get("epsGrowthTTMYoy") or metric.get("epsGrowthQuarterlyYoy"),
            "debt_to_equity": metric.get("totalDebt/totalEquityAnnual") or metric.get("debtToEquityAnnual"),
            "profit_margin": metric.get("netProfitMarginTTM") or metric.get("netProfitMarginAnnual"),
        }

    def _score(self, m: Dict) -> (float, str):
        score = 0.0
        reasons = []
        pe = m.get("pe")
        if pe is not None:
            pe = float(pe)
            if 10 <= pe <= 25:
                score += 15; reasons.append("PE in 10-25 range")
            elif pe < 5:
                score -= 10; reasons.append("PE very low (<5)")
            elif pe > 35:
                score -= 15; reasons.append("PE high (>35)")
        pb = m.get("pb")
        if pb is not None:
            pb = float(pb)
            if pb < 3:
                score += 5; reasons.append("PB < 3")
            elif pb > 8:
                score -= 5; reasons.append("PB > 8")
        peg = m.get("peg")
        if peg is not None:
            peg = float(peg)
            if 0.5 <= peg <= 2.0:
                score += 10; reasons.append("PEG reasonable")
            elif peg > 3.0:
                score -= 10; reasons.append("PEG high")
        rev = m.get("revenue_growth")
        if rev is not None:
            rev = float(rev)
            if rev > 0.05:
                score += 15; reasons.append("Revenue growth >5%")
            elif rev < -0.05:
                score -= 10; reasons.append("Revenue contraction")
        eps = m.get("eps_growth")
        if eps is not None:
            eps = float(eps)
            if eps > 0.05:
                score += 10; reasons.append("EPS growth >5%")
            elif eps < -0.05:
                score -= 10; reasons.append("EPS decline")
        dte = m.get("debt_to_equity")
        if dte is not None:
            dte = float(dte)
            if dte < 1.0:
                score += 10; reasons.append("D/E < 1")
            elif dte > 3.0:
                score -= 10; reasons.append("D/E > 3")
        pm = m.get("profit_margin")
        if pm is not None:
            pm = float(pm)
            if pm > 0.10:
                score += 15; reasons.append("Profit margin >10%")
            elif pm < 0:
                score -= 10; reasons.append("Negative margin")
        # Clamp and return
        score = max(-100.0, min(100.0, score))
        return score, "; ".join(reasons)

