import asyncio
import sqlite3
from pathlib import Path

from .core.bus import get_bus
from .core.storage import init_db
from .services.data_collection import DataCollectionService
from .services.sentiment_engine import SentimentEngine
from .services.market_data_processor import MarketDataProcessor
from .services.analysis_engine import UnifiedAnalysisEngine
from .services.trade_execution import TradeExecutionManager


async def run_for(duration_sec: int = 10) -> None:
    symbols = ["AAPL", "MSFT", "SPY"]
    init_db()
    bus = get_bus(prefer_redis=False)

    data = DataCollectionService(bus, symbols)
    sent = SentimentEngine(bus)
    # Disable OpenAI during smoke test
    try:
        class _OAStub:
            available = False

        sent._oa = _OAStub()
    except Exception:
        pass
    mkt = MarketDataProcessor(bus)
    an = UnifiedAnalysisEngine(bus)
    tr = TradeExecutionManager(bus)

    tasks = [
        asyncio.create_task(data.run()),
        asyncio.create_task(sent.run()),
        asyncio.create_task(mkt.run()),
        asyncio.create_task(an.run()),
        asyncio.create_task(tr.run()),
    ]
    try:
        await asyncio.sleep(duration_sec)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def verify_db(db_path: Path) -> dict:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    out = {}
    for table in ["prices", "sentiment", "indicators", "analysis", "trades"]:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            out[table] = cur.fetchone()[0]
        except Exception:
            out[table] = -1
    conn.close()
    return out


if __name__ == "__main__":
    asyncio.run(run_for(12))
    db = Path(__file__).with_name("data.db")
    counts = verify_db(db)
    print("SMOKE COUNTS:", counts)
