from typing import Dict, List, Optional

from ..core.bus import MessageBus
from ..models import Analysis
from ..core.storage import insert
from ..core.weights import load_latest_weights, Weights


class UnifiedAnalysisEngine:
    def __init__(self, bus: MessageBus) -> None:
        self.bus = bus
        # caches by symbol
        self._last_ind: Dict[str, dict] = {}
        self._last_sent: Dict[str, dict] = {}
        self._weights: Weights = load_latest_weights()

    async def run(self) -> None:
        # multiplex subscriptions by reading from both topics in a simple loop
        async def handle_ind():
            async for ind in self.bus.subscribe("metrics.indicators"):
                self._last_ind[ind["symbol"]] = ind
                await self._maybe_emit(ind["symbol"], ind["ts"])

        async def handle_sent():
            async for sent in self.bus.subscribe("sentiment.processed"):
                sym = sent.get("symbol") or "MARKET"
                self._last_sent[sym] = sent
                await self._maybe_emit(sym if sym != "MARKET" else None, sent["ts"])

        import asyncio

        async def handle_weights():
            async for w in self.bus.subscribe("weights.updated"):
                try:
                    self._weights = Weights(float(w.get("tech", 0.4)), float(w.get("fundamental", 0.35)), float(w.get("context", 0.25)))
                except Exception:
                    pass

        await asyncio.gather(handle_ind(), handle_sent(), handle_weights())

    async def _maybe_emit(self, symbol: Optional[str], ts: str) -> None:
        if not symbol:
            return
        ind = self._last_ind.get(symbol)
        sent = self._last_sent.get(symbol) or self._last_sent.get("MARKET")
        if not ind:
            return
        # scoring components (-100..100)
        tech_score = 0.0
        reasons: List[str] = []
        price = ind.get("price")
        if ind.get("rsi") is not None:
            rsi = float(ind["rsi"])
            # RSI contrarian: oversold <30 -> +, overbought >70 -> -
            if rsi < 30:
                tech_score += 20
                reasons.append(f"RSI oversold {rsi:.1f}")
            elif rsi > 70:
                tech_score -= 20
                reasons.append(f"RSI overbought {rsi:.1f}")
        if ind.get("sma_fast") and ind.get("sma_slow"):
            if ind["sma_fast"] > ind["sma_slow"]:
                tech_score += 20
                reasons.append("Golden cross (fast>slow)")
            else:
                tech_score -= 10
                reasons.append("Bearish MA alignment")
        # MACD
        if ind.get("macd") is not None and ind.get("macd_signal") is not None:
            if float(ind["macd"]) > float(ind["macd_signal"]):
                tech_score += 10
                reasons.append("MACD above signal")
            else:
                tech_score -= 10
                reasons.append("MACD below signal")
        # Bollinger Bands
        if price is not None and ind.get("bb_upper") is not None and ind.get("bb_lower") is not None:
            p = float(price)
            if p < float(ind["bb_lower"]):
                tech_score += 15
                reasons.append("Price below lower Bollinger (oversold)")
            elif p > float(ind["bb_upper"]):
                tech_score -= 15
                reasons.append("Price above upper Bollinger (overbought)")

        sent_score = 0.0
        if sent:
            sent_score = float(sent.get("impact", 0.0)) * 100.0  # map -1..1 to -100..100
            reasons.append(f"Sentiment impact {sent.get('impact', 0.0):.2f}")

        w = self._weights.normalized()
        composite = w.tech * tech_score + w.fundamental * 0.0 + w.context * sent_score
        composite = max(-100.0, min(100.0, composite))

        rec = "HOLD"
        pos = 0.0
        conf = min(100.0, abs(composite))
        if composite >= 40:
            rec = "BUY"
            pos = min(20.0, 5.0 + composite / 10.0)
        elif composite <= -40:
            rec = "SELL"
            pos = 0.0

        analysis = Analysis(
            symbol=symbol,
            ts=ts,
            score=composite,
            recommendation=rec,
            position_size_pct=pos,
            confidence_pct=conf,
            reasons=reasons,
        )
        out = analysis.to_dict()
        # Emit factors for adaptive learning but do not persist them in DB
        out.update({"factors": {"tech": tech_score, "fundamental": 0.0, "context": sent_score}, "weights": w.to_dict()})
        await self.bus.publish("signals.analysis", out)
        rec = analysis.to_dict()
        # Persist reasons as text for SQLite
        rec["reasons"] = "; ".join(reasons)
        insert("analysis", rec)
