import asyncio
import os
from typing import List

from .core.bus import get_bus
from .core.storage import init_db
from .services.data_collection import DataCollectionService
from .services.sentiment_engine import SentimentEngine
from .services.market_data_processor import MarketDataProcessor
from .services.analysis_engine import UnifiedAnalysisEngine
from .services.trade_execution import TradeExecutionManager
from .services.portfolio_monitor import PortfolioMonitor
from .services.adaptive_learning import AdaptiveLearning
from .utils.symbols import get_sp500_symbols
from .core.settings import Settings


async def main(symbols: List[str] | None = None) -> None:
    settings = Settings.load()
    if symbols is None:
        if settings.symbols:
            symbols = settings.symbols
        else:
            symbols = get_sp500_symbols(limit=settings.sp500_limit, force_refresh=settings.sp500_refresh)
    init_db()
    bus = get_bus(prefer_redis=settings.prefer_redis)

    data = DataCollectionService(bus, symbols)
    sent = SentimentEngine(bus)
    mkt = MarketDataProcessor(bus)
    an = UnifiedAnalysisEngine(bus)
    tr = TradeExecutionManager(bus)
    pf = PortfolioMonitor(bus)
    rl = AdaptiveLearning(bus)

    await asyncio.gather(
        data.run(),
        sent.run(),
        mkt.run(),
        an.run(),
        tr.run(),
        pf.run(),
        rl.run(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
