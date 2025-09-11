import asyncio
from typing import Dict

from ..core.bus import MessageBus
from ..models import PortfolioState, now_ts
from ..core.storage import insert


class PortfolioMonitor:
    def __init__(self, bus: MessageBus) -> None:
        self.bus = bus
        self._positions: Dict[str, float] = {}
        self._cash = 100_000.0

    async def run(self) -> None:
        # In a real system, listen to fills/positions; here, periodically publish a stub state.
        while True:
            state = PortfolioState(ts=now_ts(), total_value=self._cash, cash=self._cash, positions=self._positions)
            insert("portfolio", {**state.to_dict(), "positions": str(state.positions)})
            await self.bus.publish("portfolio.state", state.to_dict())
            await asyncio.sleep(300)
