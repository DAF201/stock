import asyncio

from wave.core.bus import get_bus
from wave.services.fundamentals_service import FundamentalsService


async def main():
    bus = get_bus(prefer_redis=False)
    svc = FundamentalsService(bus, symbols=["AAPL", "MSFT"])
    await svc._refresh_all()  # type: ignore[attr-defined]


if __name__ == "__main__":
    asyncio.run(main())
