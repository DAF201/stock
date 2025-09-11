import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from ..core.bus import MessageBus
from ..core.storage import query_one
from ..core.weights import Weights, load_latest_weights, save_weights


class AdaptiveLearning:
    def __init__(self, bus: MessageBus) -> None:
        self.bus = bus
        self._pending: List[Dict] = []
        self._weights: Weights = load_latest_weights()
        self._horizon_min = 60  # evaluation horizon
        self._alpha = 0.02  # learning rate
        self._last_update: Optional[datetime] = None

    async def run(self) -> None:
        async def collect_signals():
            async for a in self.bus.subscribe("signals.analysis"):
                sym = a.get("symbol")
                if not sym:
                    continue
                # fetch latest price at signal time
                row = query_one("SELECT price FROM prices WHERE symbol=? ORDER BY ts DESC LIMIT 1", (sym,))
                if not row:
                    continue
                entry_price = float(row[0])
                comps = (a.get("factors") or {})
                tech = float(comps.get("tech", 0.0))
                ctx = float(comps.get("context", 0.0))
                score = float(a.get("score", 0.0))
                due = datetime.utcnow() + timedelta(minutes=self._horizon_min)
                self._pending.append({
                    "symbol": sym,
                    "entry_price": entry_price,
                    "due": due,
                    "score": score,
                    "tech": tech,
                    "context": ctx,
                })

        async def evaluate_loop():
            while True:
                now = datetime.utcnow()
                remain: List[Dict] = []
                updates = 0
                for item in self._pending:
                    if item["due"] <= now:
                        cur_row = query_one("SELECT price FROM prices WHERE symbol=? ORDER BY ts DESC LIMIT 1", (item["symbol"],))
                        if not cur_row:
                            continue
                        cur_price = float(cur_row[0])
                        ret = (cur_price - item["entry_price"]) / max(1e-6, item["entry_price"])  # simple return
                        sign = 1.0 if ret >= 0 else -1.0
                        # attribute to components
                        tech_comp = item["tech"] / 100.0
                        ctx_comp = item["context"] / 100.0
                        # update rule: move weights towards components that matched the outcome
                        if sign * item["score"] > 0:  # signal direction was correct
                            d_tech = self._alpha * abs(tech_comp)
                            d_ctx = self._alpha * abs(ctx_comp)
                        else:
                            d_tech = -self._alpha * abs(tech_comp)
                            d_ctx = -self._alpha * abs(ctx_comp)
                        self._weights.tech = max(0.05, self._weights.tech + d_tech)
                        self._weights.context = max(0.05, self._weights.context + d_ctx)
                        # fundamental remains unchanged for now
                        updates += 1
                    else:
                        remain.append(item)
                self._pending = remain
                if updates:
                    # renormalize and persist
                    self._weights = self._weights.normalized()
                    save_weights(self._weights)
                    await self.bus.publish("weights.updated", self._weights.to_dict())
                    print(f"[Learning] Updated weights: {self._weights.to_dict()}")
                await asyncio.sleep(60)

        await asyncio.gather(collect_signals(), evaluate_loop())
