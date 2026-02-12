from __future__ import annotations

from typing import List
import requests


def fetch_sp500_tickers() -> List[str]:
    sources = [
        "https://datahub.io/core/s-and-p-500-companies/r/constituents.json",
        "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv",
    ]
    try:
        r = requests.get(sources[0], timeout=20)
        r.raise_for_status()
        data = r.json()
        tickers = [row.get("Symbol") or row.get("symbol") for row in data]
        return sorted({t.strip().upper() for t in tickers if t})
    except Exception:
        pass
    try:
        r = requests.get(sources[1], timeout=20)
        r.raise_for_status()
        lines = r.text.splitlines()
        tickers = []
        for i, line in enumerate(lines):
            if i == 0:
                continue
            parts = line.split(",")
            if parts and parts[0]:
                tickers.append(parts[0].strip().upper())
        return sorted({t for t in tickers if t})
    except Exception:
        pass
    return sorted({"AAPL", "MSFT", "AMZN", "GOOGL", "META", "BRK.B", "NVDA", "JPM", "TSLA", "UNH"})

