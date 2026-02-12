from __future__ import annotations

import csv
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional


@dataclass
class TradeEvent:
    ts: str
    mode: str  # dry-run | trade
    symbol: str
    action: str  # long|short|close|hold
    side: str  # buy|sell|none
    qty: int
    price: float | None
    decision_source: str  # rule|gpt
    sentiment: float
    vader: float
    gpt: float | None
    gpt_decision: str | None
    gpt_exp_move_pct: float | None
    gpt_emotions: str | None
    tp_price: float | None = None
    sl_price: float | None = None
    order_id: str | None = None
    status: str | None = None
    error: str | None = None


class TradeLogger:
    def __init__(self, log_path: str | Path) -> None:
        self.path = Path(log_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_header()

    def _ensure_header(self) -> None:
        if self.path.exists() and self.path.stat().st_size > 0:
            return
        with self.path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "ts","mode","symbol","action","side","qty","price","decision_source",
                "sentiment","vader","gpt","gpt_decision","gpt_exp_move_pct","gpt_emotions",
                "tp_price","sl_price",
                "order_id","status","error"
            ])
            writer.writeheader()

    def log(self, event: TradeEvent) -> None:
        with self.path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "ts","mode","symbol","action","side","qty","price","decision_source",
                "sentiment","vader","gpt","gpt_decision","gpt_exp_move_pct","gpt_emotions",
                "tp_price","sl_price",
                "order_id","status","error"
            ])
            writer.writerow(asdict(event))

    @staticmethod
    def now_iso() -> str:
        return datetime.now().isoformat(timespec="seconds")


@dataclass
class DecisionEvent:
    ts: str
    symbol: str
    price: float | None
    vader: float
    gpt: float | None
    sentiment: float
    action: str
    strategy: str
    gpt_conf: float | None
    emotions: str | None
    exp_move_pct: float | None
    price_change_pct: float | None
    vol_pct: float | None
    trend: str | None


class DecisionsLogger:
    def __init__(self, log_path: str | Path) -> None:
        self.path = Path(log_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_header()

    def _ensure_header(self) -> None:
        if self.path.exists() and self.path.stat().st_size > 0:
            return
        with self.path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "ts","symbol","price","vader","gpt","sentiment","action","strategy",
                "gpt_conf","emotions","exp_move_pct","price_change_pct","vol_pct","trend"
            ])
            writer.writeheader()

    def log(self, event: DecisionEvent) -> None:
        with self.path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "ts","symbol","price","vader","gpt","sentiment","action","strategy",
                "gpt_conf","emotions","exp_move_pct","price_change_pct","vol_pct","trend"
            ])
            writer.writerow(asdict(event))
