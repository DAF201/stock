from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass
from typing import Optional, Tuple

import requests
from zoneinfo import ZoneInfo

from ..core.config import load_config


NY = ZoneInfo("America/New_York")


@dataclass
class Session:
    date: dt.date
    open: dt.datetime
    close: dt.datetime


def _alpaca_headers() -> Optional[dict]:
    cfg = load_config()
    mode = os.getenv("ALPACA_MODE", "paper")
    alp = cfg.alpaca.get(mode, {})
    key = alp.get("key")
    secret = alp.get("secret")
    if not (key and secret):
        return None
    return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}


def _alpaca_base() -> Optional[str]:
    cfg = load_config()
    mode = os.getenv("ALPACA_MODE", "paper")
    alp = cfg.alpaca.get(mode, {})
    return alp.get("base_url") or alp.get("API")


def _get_today_session_from_alpaca(today: dt.date) -> Optional[Session]:
    base = _alpaca_base()
    headers = _alpaca_headers()
    if not (base and headers):
        return None
    try:
        r = requests.get(
            f"{base}/calendar",
            params={"start": today.isoformat(), "end": today.isoformat()},
            headers=headers,
            timeout=10,
        )
        r.raise_for_status()
        items = r.json()
        if not items:
            return None
        cal = items[0]
        # Alpaca returns open/close like '09:30' local time for the exchange
        open_str = cal.get("open")
        close_str = cal.get("close")
        if not (open_str and close_str):
            return None
        open_dt = dt.datetime.combine(today, dt.time.fromisoformat(open_str)).replace(tzinfo=NY)
        close_dt = dt.datetime.combine(today, dt.time.fromisoformat(close_str)).replace(tzinfo=NY)
        return Session(date=today, open=open_dt, close=close_dt)
    except Exception:
        return None


def _fallback_session(today: dt.date) -> Optional[Session]:
    # Mon-Fri regular session 09:30–16:00 ET, skip US holidays via holidays lib if available
    if today.weekday() >= 5:
        return None
    try:
        import holidays  # type: ignore

        us_holidays = holidays.US(years=today.year)
        if today in us_holidays:
            return None
    except Exception:
        pass
    open_dt = dt.datetime.combine(today, dt.time(9, 30), NY)
    close_dt = dt.datetime.combine(today, dt.time(16, 0), NY)
    return Session(date=today, open=open_dt, close=close_dt)


def get_today_session(now: Optional[dt.datetime] = None) -> Optional[Session]:
    now = now or dt.datetime.now(tz=NY)
    today = now.date()
    return _get_today_session_from_alpaca(today) or _fallback_session(today)


def is_market_open(now: Optional[dt.datetime] = None) -> Tuple[bool, Optional[Session]]:
    now = now or dt.datetime.now(tz=NY)
    sess = get_today_session(now)
    if not sess:
        return False, None
    return sess.open <= now <= sess.close, sess


def in_blackout_window(now: Optional[dt.datetime] = None, before_close_min: int = 30, after_open_min: int = 30) -> Tuple[bool, Optional[str]]:
    now = now or dt.datetime.now(tz=NY)
    open_, sess = is_market_open(now)
    if not sess:
        return True, "market_closed_or_holiday"
    # windows relative to session
    after_open = sess.open <= now <= (sess.open + dt.timedelta(minutes=after_open_min))
    before_close = (sess.close - dt.timedelta(minutes=before_close_min)) <= now <= sess.close
    if after_open:
        return True, "blackout_after_open"
    if before_close:
        return True, "blackout_before_close"
    return False, None


def should_trade_now(
    now: Optional[dt.datetime] = None,
    blackout_after_open_min: int = 30,
    blackout_before_close_min: int = 30,
) -> Tuple[bool, str]:
    open_, sess = is_market_open(now)
    if not open_:
        return False, "market_closed"
    blk, reason = in_blackout_window(
        now,
        before_close_min=blackout_before_close_min,
        after_open_min=blackout_after_open_min,
    )
    if blk:
        return False, reason or "blackout"
    return True, "ok"
