import json
import os
from pathlib import Path
from typing import List, Optional

import requests
from bs4 import BeautifulSoup


_CACHE = Path(__file__).resolve().parent.parent / "sp500.json"
_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def normalize_symbol(sym: str) -> str:
    # Yahoo-style normalization for class shares (e.g., BRK.B -> BRK-B)
    return sym.replace(".", "-").strip().upper()


def fetch_sp500_symbols() -> List[str]:
    r = requests.get(_WIKI_URL, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", id="constituents")
    if not table:
        # fallback: first table on page
        table = soup.find("table")
    syms: List[str] = []
    if table:
        for row in table.select("tbody tr"):
            cols = row.find_all("td")
            if not cols:
                continue
            raw = cols[0].get_text(" ", strip=True)
            if not raw or raw.lower() == "symbol":
                continue
            syms.append(normalize_symbol(raw))
    if not syms:
        raise RuntimeError("Failed to parse S&P 500 symbols from Wikipedia")
    return syms


def get_sp500_symbols(limit: Optional[int] = None, force_refresh: bool = False) -> List[str]:
    if _CACHE.exists() and not force_refresh:
        try:
            data = json.loads(_CACHE.read_text(encoding="utf-8"))
            syms = [normalize_symbol(s) for s in data]
        except Exception:
            syms = []
    else:
        syms = []

    if not syms:
        syms = fetch_sp500_symbols()
        try:
            _CACHE.write_text(json.dumps(syms, indent=2), encoding="utf-8")
        except Exception:
            pass

    if limit:
        return syms[: int(limit)]
    return syms
