from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


# Expect app_config.json at package root
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "app_config.json"


def _get_bool(env: str, default: bool = False) -> bool:
    val = os.getenv(env)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_int(env: str, default: Optional[int] = None) -> Optional[int]:
    val = os.getenv(env)
    if not val:
        return default
    try:
        return int(val)
    except Exception:
        return default


def _get_float(env: str, default: Optional[float] = None) -> Optional[float]:
    val = os.getenv(env)
    if not val:
        return default
    try:
        return float(val)
    except Exception:
        return default


@dataclass
class Settings:
    # Infrastructure
    prefer_redis: bool = False
    redis_url: Optional[str] = None

    # Symbols
    sp500_limit: Optional[int] = None
    sp500_refresh: bool = False
    symbols_file: Optional[str] = None
    symbols: Optional[List[str]] = None

    # Rate limits (requests per second/minute as noted)
    finnhub_rpm: Optional[int] = 50  # requests/min
    yahoo_rps: Optional[float] = 5.0  # requests/sec (batched)
    alpaca_rps: Optional[float] = 1.0  # orders/sec
    openai_rps: Optional[float] = 2.0

    # Trading
    enable_orders: bool = False
    alpaca_mode: str = "paper"  # paper | live
    blackout_after_open_min: int = 30
    blackout_before_close_min: int = 30

    @staticmethod
    def load(config_path: Optional[str] = None) -> "Settings":
        cfg = Settings()
        # Optional file overrides
        path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                data = {}
        else:
            data = {}

        def dget(key: str, default):
            return data.get(key, default)

        # Infrastructure
        cfg.prefer_redis = _get_bool("PREFER_REDIS", dget("prefer_redis", cfg.prefer_redis))
        cfg.redis_url = os.getenv("REDIS_URL", dget("redis_url", cfg.redis_url))

        # Symbols
        cfg.sp500_limit = _get_int("SP500_LIMIT", dget("sp500_limit", cfg.sp500_limit))
        cfg.sp500_refresh = _get_bool("SP500_REFRESH", dget("sp500_refresh", cfg.sp500_refresh))
        cfg.symbols_file = os.getenv("SYMBOLS_FILE", dget("symbols_file", cfg.symbols_file))
        if cfg.symbols_file and Path(cfg.symbols_file).exists():
            try:
                lines = [
                    l.strip()
                    for l in Path(cfg.symbols_file).read_text(encoding="utf-8").splitlines()
                    if l.strip()
                ]
                cfg.symbols = lines
            except Exception:
                pass

        # Rate limits
        cfg.finnhub_rpm = _get_int("FINNHUB_RPM", dget("finnhub_rpm", cfg.finnhub_rpm))
        cfg.yahoo_rps = _get_float("YAHOO_RPS", dget("yahoo_rps", cfg.yahoo_rps))
        cfg.alpaca_rps = _get_float("ALPACA_RPS", dget("alpaca_rps", cfg.alpaca_rps))
        cfg.openai_rps = _get_float("OPENAI_RPS", dget("openai_rps", cfg.openai_rps))

        # Trading
        cfg.enable_orders = _get_bool("ENABLE_ORDERS", dget("enable_orders", cfg.enable_orders))
        cfg.alpaca_mode = os.getenv("ALPACA_MODE", dget("alpaca_mode", cfg.alpaca_mode))
        cfg.blackout_after_open_min = _get_int("BLACKOUT_AFTER_OPEN_MIN", dget("blackout_after_open_min", cfg.blackout_after_open_min)) or cfg.blackout_after_open_min
        cfg.blackout_before_close_min = _get_int("BLACKOUT_BEFORE_CLOSE_MIN", dget("blackout_before_close_min", cfg.blackout_before_close_min)) or cfg.blackout_before_close_min

        return cfg
