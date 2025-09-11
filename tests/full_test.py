import asyncio
import os
import sqlite3
from pathlib import Path

from wave.core.bus import get_bus
from wave.core.storage import init_db
from wave.services.data_collection import DataCollectionService
from wave.services.sentiment_engine import SentimentEngine
from wave.services.market_data_processor import MarketDataProcessor
from wave.services.analysis_engine import UnifiedAnalysisEngine
from wave.services.trade_execution import TradeExecutionManager
from wave.services.fundamentals_service import FundamentalsService


async def run_full(duration_sec: int = 20):
    # Safety: ensure orders are disabled and keep the symbol set small
    os.environ["ENABLE_ORDERS"] = "0"
    symbols = ["AAPL", "MSFT", "SPY"]
    init_db()
    bus = get_bus(prefer_redis=False)

    data = DataCollectionService(bus, symbols)
    sent = SentimentEngine(bus)
    # Disable OpenAI use during this test
    try:
        class _OAStub:
            available = False

        sent._oa = _OAStub()
    except Exception:
        pass
    mkt = MarketDataProcessor(bus)
    an = UnifiedAnalysisEngine(bus)
    tr = TradeExecutionManager(bus)
    fn = FundamentalsService(bus, symbols)

    # Start core services
    tasks = [
        asyncio.create_task(data.run()),
        asyncio.create_task(sent.run()),
        asyncio.create_task(mkt.run()),
        asyncio.create_task(an.run()),
        asyncio.create_task(tr.run()),
    ]
    # Trigger a one-off fundamentals fetch so we don't wait for the periodic loop
    await fn._refresh_all()  # type: ignore[attr-defined]

    try:
        await asyncio.sleep(duration_sec)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def db_counts():
    db = Path(__file__).resolve().parents[1] / "data.db"
    conn = sqlite3.connect(db)
    c = conn.cursor()
    info = {}
    for t in [
        "prices",
        "sentiment",
        "indicators",
        "analysis",
        "trades",
        "portfolio",
        "fundamentals",
        "weights",
    ]:
        try:
            c.execute(f"SELECT COUNT(*) FROM {t}")
            info[t] = c.fetchone()[0]
        except Exception as e:
            info[t] = str(e)
    conn.close()
    return info


if __name__ == "__main__":
    asyncio.run(run_full(20))
    print("FULL TEST COUNTS:", db_counts())
