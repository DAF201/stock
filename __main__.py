import argparse
import asyncio
import os
from typing import List, Optional

from .main import main as run_main
from .symbols import normalize_symbol


def set_env(name: str, value: Optional[str]) -> None:
    if value is None:
        return
    os.environ[name] = str(value)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="wave", description="Wave trading system")
    # Infrastructure
    p.add_argument("--prefer-redis", action="store_true", help="Prefer Redis message bus")
    p.add_argument("--redis-url", help="Redis URL", default=None)

    # Symbols
    p.add_argument("--limit", type=int, default=None, help="Limit number of S&P500 symbols")
    p.add_argument("--refresh", action="store_true", help="Refresh S&P500 cache")
    p.add_argument("--symbols-file", default=None, help="Path to file with symbols (newline-separated)")
    p.add_argument("--symbols", default=None, help="Comma-separated symbols (overrides S&P500)")

    # Rate limits
    p.add_argument("--finnhub-rpm", type=int, default=None, help="Finnhub requests per minute")
    p.add_argument("--yahoo-rps", type=float, default=None, help="Yahoo requests per second (batched)")
    p.add_argument("--alpaca-rps", type=float, default=None, help="Alpaca requests per second")
    p.add_argument("--openai-rps", type=float, default=None, help="OpenAI requests per second")

    # Trading
    p.add_argument("--enable-orders", action="store_true", help="Enable Alpaca order placement")
    p.add_argument("--alpaca-mode", choices=["paper", "live"], default=None, help="Trading mode")
    p.add_argument("--mode", choices=["paper", "live"], default=None, help="Alias for --alpaca-mode")
    p.add_argument("--blackout-after", type=int, default=None, help="Minutes after open to block trades")
    p.add_argument("--blackout-before", type=int, default=None, help="Minutes before close to block trades")

    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Map CLI to environment so Settings.load() picks them up across modules
    if args.prefer_redis:
        set_env("PREFER_REDIS", "1")
    set_env("REDIS_URL", args.redis_url)

    if args.limit is not None:
        set_env("SP500_LIMIT", str(args.limit))
    if args.refresh:
        set_env("SP500_REFRESH", "1")
    set_env("SYMBOLS_FILE", args.symbols_file)

    if args.finnhub_rpm is not None:
        set_env("FINNHUB_RPM", str(args.finnhub_rpm))
    if args.yahoo_rps is not None:
        set_env("YAHOO_RPS", str(args.yahoo_rps))
    if args.alpaca_rps is not None:
        set_env("ALPACA_RPS", str(args.alpaca_rps))
    if args.openai_rps is not None:
        set_env("OPENAI_RPS", str(args.openai_rps))

    if args.enable_orders:
        set_env("ENABLE_ORDERS", "1")
    mode = args.alpaca_mode or args.mode
    if mode is not None:
        set_env("ALPACA_MODE", mode)
    if args.blackout_after is not None:
        set_env("BLACKOUT_AFTER_OPEN_MIN", str(args.blackout_after))
    if args.blackout_before is not None:
        set_env("BLACKOUT_BEFORE_CLOSE_MIN", str(args.blackout_before))

    # Symbols handling for explicit override
    symbols_list: Optional[List[str]] = None
    if args.symbols:
        symbols_list = [normalize_symbol(s) for s in args.symbols.split(",") if s.strip()]

    asyncio.run(run_main(symbols=symbols_list))


if __name__ == "__main__":
    main()

