from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional
from datetime import datetime


def now_ts() -> str:
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class PriceTick:
    symbol: str
    ts: str
    price: float
    volume: Optional[float] = None
    source: str = "unknown"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Sentiment:
    symbol: Optional[str]
    ts: str
    score: float  # -1..1
    confidence: float  # 0..1
    relevance: float  # 0..1
    impact: float  # time-weighted
    text: Optional[str] = None
    source: str = "unknown"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Indicators:
    symbol: str
    ts: str
    rsi: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_lower: Optional[float] = None
    sma_fast: Optional[float] = None
    sma_slow: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Analysis:
    symbol: str
    ts: str
    score: float  # -100..100
    recommendation: str  # BUY/SELL/HOLD/STANDBY
    position_size_pct: float
    confidence_pct: float
    reasons: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TradeOrder:
    symbol: str
    ts: str
    side: str  # buy/sell
    qty: float
    order_type: str = "market"
    limit_price: Optional[float] = None
    stop_loss: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PortfolioState:
    ts: str
    total_value: float
    cash: float
    positions: Dict[str, float]  # symbol -> qty

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

