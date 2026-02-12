from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

import requests
import time
from requests.exceptions import ConnectionError as ReqConnectionError, Timeout as ReqTimeout, RequestException
try:
    # For distinguishing protocol-level errors
    from urllib3.exceptions import ProtocolError  # type: ignore
except Exception:  # pragma: no cover
    ProtocolError = Exception  # type: ignore
from requests.adapters import HTTPAdapter
try:
    # urllib3 Retry is available via requests dependency
    from urllib3.util.retry import Retry  # type: ignore
except Exception:  # pragma: no cover
    Retry = None  # type: ignore


# Reusable HTTP session with retries for Finnhub
_SESSION = None


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is not None:
        return _SESSION  # type: ignore[return-value]
    s = requests.Session()
    if Retry is not None:
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.8,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_maxsize=20, pool_connections=20)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
    _SESSION = s
    return s


def load_json(path: Path) -> Dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing required config: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def finnhub_company_news(token: str, symbol: str, fr, to, limit: int) -> List[Dict]:
    url = "https://finnhub.io/api/v1/company-news"
    params = {
        "symbol": symbol,
        "from": fr.strftime("%Y-%m-%d"),
        "to": to.strftime("%Y-%m-%d"),
        "token": token,
    }
    r = _get_session().get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        return []
    data.sort(key=lambda x: x.get("datetime", 0), reverse=True)
    return data[:limit]


def finnhub_quote(token: str, symbol: str) -> Optional[float]:
    url = "https://finnhub.io/api/v1/quote"
    params = {"symbol": symbol, "token": token}
    r = _get_session().get(url, params=params, timeout=25)
    r.raise_for_status()
    q = r.json()
    price = q.get("c")
    try:
        return float(price) if price is not None else None
    except Exception:
        return None


def safe_error_text(r: requests.Response) -> str:
    try:
        return r.json().get("message") or r.text
    except Exception:
        return r.text


def alpaca_place_order(base_url: str, key: str, secret: str, symbol: str, qty: Optional[int], side: str,
                       type_: str = "market", tif: str = "day", client_order_id: Optional[str] = None,
                       order_class: Optional[str] = None, take_profit: Optional[Dict] = None,
                       stop_loss: Optional[Dict] = None, notional: Optional[float] = None) -> Dict:
    url = f"{base_url.rstrip('/')}/orders"
    headers = {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
        "Content-Type": "application/json",
    }
    payload: Dict = {
        "symbol": symbol,
        "side": side,
        "type": type_,
        "time_in_force": tif,
    }
    if qty is not None:
        payload["qty"] = qty
    if notional is not None:
        payload["notional"] = float(notional)
    if client_order_id:
        payload["client_order_id"] = client_order_id
    if order_class:
        payload["order_class"] = order_class
    if take_profit:
        payload["take_profit"] = take_profit
    if stop_loss:
        payload["stop_loss"] = stop_loss
    # Network-transient safe retry: only on connection/timeout/protocol errors.
    # Alpaca supports idempotency via client_order_id. We assume caller passes it.
    attempts = 3
    backoff = 1.0
    last_exc: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=20)
            if r.status_code >= 400:
                return {"status": "error", "code": r.status_code, "message": safe_error_text(r)}
            return r.json()
        except (ReqConnectionError, ReqTimeout, ProtocolError) as e:
            last_exc = e
            if attempt < attempts:
                time.sleep(backoff)
                backoff *= 1.5
                continue
            return {"status": "error", "code": "network", "message": str(e)}
        except RequestException as e:
            # Other request-layer errors
            return {"status": "error", "code": "request", "message": str(e)}

    # Fallback (should not reach)
    return {"status": "error", "code": "unknown", "message": str(last_exc) if last_exc else "unknown error"}


# ---- GDELT News (macro events) ----
def fetch_gdelt_docs(themes: List[str], max_records: int = 30, timespan_min: int = 180) -> List[Dict]:
    """Fetch recent GDELT Doc 2.1 items for selected themes (macro/political events).

    Returns a normalized list of dicts: id, datetime (epoch seconds), source, headline, summary, url, is_macro.
    """
    if not themes:
        return []
    query = " OR ".join([f"theme:{t.strip()}" for t in themes if t and isinstance(t, str)])
    if not query:
        return []
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": query,
        "mode": "ArtList",
        "maxrecords": str(int(max_records)),
        "timespan": f"{int(timespan_min)}min",
        "format": "json",
        "sort": "DateDesc",
    }
    try:
        r = requests.get(url, params=params, timeout=25)
        r.raise_for_status()
        data = r.json() or {}
        arts = data.get("articles") or []
    except Exception:
        return []

    out: List[Dict] = []
    for a in arts:
        try:
            title = a.get("title") or a.get("seendate") or ""
            url_ = a.get("url")
            domain = a.get("domain") or a.get("sourceCommonName") or "GDELT"
            seendate = a.get("seendate")  # e.g., 2025-09-01T07:05:00Z
            # Convert seendate to epoch seconds if possible
            dt_epoch = 0
            if isinstance(seendate, str):
                try:
                    from dateutil.parser import isoparse
                    dt_epoch = int(isoparse(seendate).timestamp())
                except Exception:
                    dt_epoch = 0
            ident = url_ or (title + seendate)
            out.append({
                "id": ident,
                "datetime": dt_epoch,
                "source": f"GDELT:{domain}",
                "headline": title,
                "summary": url_ or domain,
                "url": url_,
                "is_macro": True,
            })
        except Exception:
            continue
    return out


# ---- Alpaca market clock ----
def alpaca_clock(base_url: str, key: str, secret: str) -> Optional[Dict]:
    """Fetch Alpaca market clock. Returns dict with fields like {"is_open": bool} or None on error."""
    try:
        url = f"{base_url.rstrip('/')}/clock"
        headers = {
            "APCA-API-KEY-ID": key,
            "APCA-API-SECRET-KEY": secret,
        }
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code >= 400:
            return None
        return r.json()
    except Exception:
        return None


def alpaca_snapshots(symbols: List[str], key: str, secret: str, data_base_url: Optional[str] = None, timeout: int = 20) -> Dict[str, Optional[float]]:
    """Fetch multiple stock snapshots from Alpaca Data API.

    Returns a mapping symbol -> last trade price (float) when available, else None.
    """
    if not symbols:
        return {}
    # Default data endpoint
    base = (data_base_url or "https://data.alpaca.markets/v2").rstrip("/")
    url = f"{base}/stocks/snapshots"
    # Alpaca expects comma-separated symbols; limit length to avoid URL size issues
    params = {"symbols": ",".join(symbols)}
    headers = {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
    }
    out: Dict[str, Optional[float]] = {s: None for s in symbols}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json() or {}
        # Alpaca used to nest results under a 'snapshots' key; newer responses are already a symbol map.
        snaps_candidate = data.get("snapshots") if isinstance(data, dict) else None
        snaps = snaps_candidate if isinstance(snaps_candidate, dict) else data
        if isinstance(snaps, dict):
            for sym, snap in snaps.items():
                # Try latest trade price, then daily bar close as fallback.
                price = None
                if isinstance(snap, dict):
                    lt = snap.get("latestTrade") or {}
                    price = lt.get("p") or lt.get("price")
                    if price is None:
                        db = snap.get("dailyBar") or {}
                        price = db.get("c") or db.get("close")
                sym_key = str(sym or "").upper()
                try:
                    out[sym_key] = float(price) if price is not None else None
                except Exception:
                    out[sym_key] = None
        return out
    except Exception:
        return out


def alpaca_account(base_url: str, key: str, secret: str) -> Optional[Dict]:
    """Fetch Alpaca account details (paper). Returns dict or None on error."""
    try:
        url = f"{base_url.rstrip('/')}/account"
        headers = {
            "APCA-API-KEY-ID": key,
            "APCA-API-SECRET-KEY": secret,
        }
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code >= 400:
            return None
        return r.json()
    except Exception:
        return None


def alpaca_positions(base_url: str, key: str, secret: str) -> List[Dict]:
    """Fetch all open positions. Returns a list of position dicts."""
    try:
        url = f"{base_url.rstrip('/')}/positions"
        headers = {
            "APCA-API-KEY-ID": key,
            "APCA-API-SECRET-KEY": secret,
        }
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code >= 400:
            return []
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []


def alpaca_position_for_symbol(base_url: str, key: str, secret: str, symbol: str) -> Optional[Dict]:
    """Fetch open position for a specific symbol, or None if none."""
    try:
        url = f"{base_url.rstrip('/')}/positions/{symbol}"
        headers = {
            "APCA-API-KEY-ID": key,
            "APCA-API-SECRET-KEY": secret,
        }
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 404:
            return None
        if r.status_code >= 400:
            return None
        return r.json()
    except Exception:
        return None


def alpaca_close_position(base_url: str, key: str, secret: str, symbol: str, qty: Optional[str] = None) -> Dict:
    """Close open position. qty can be a string like 'all' or a numeric string."""
    try:
        url = f"{base_url.rstrip('/')}/positions/{symbol}"
        headers = {
            "APCA-API-KEY-ID": key,
            "APCA-API-SECRET-KEY": secret,
        }
        params = {}
        if qty:
            params["qty"] = str(qty)
        r = requests.delete(url, headers=headers, params=params, timeout=20)
        if r.status_code >= 400:
            return {"status": "error", "code": r.status_code, "message": safe_error_text(r)}
        return r.json() if r.text else {"status": "ok"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
