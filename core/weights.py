from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

from .settings import Settings
from .storage import insert, query_one
from ..models import now_ts


@dataclass
class Weights:
    tech: float
    fundamental: float
    context: float

    def normalized(self) -> "Weights":
        s = max(1e-9, self.tech + self.fundamental + self.context)
        return Weights(self.tech / s, self.fundamental / s, self.context / s)

    def to_dict(self) -> Dict[str, float]:
        w = self.normalized()
        return {"tech": w.tech, "fundamental": w.fundamental, "context": w.context}


def default_weights() -> Weights:
    # Map settings or fall back to requirement defaults 40/35/25
    s = Settings.load()
    tech = 0.40
    fund = 0.35
    ctx = 0.25
    return Weights(tech, fund, ctx)


def load_latest_weights() -> Weights:
    row = query_one("SELECT tech, fundamental, context FROM weights ORDER BY ts DESC LIMIT 1")
    if row and all(r is not None for r in row):
        return Weights(float(row[0]), float(row[1]), float(row[2]))
    return default_weights()


def save_weights(w: Weights) -> None:
    insert("weights", {"ts": now_ts(), "tech": w.tech, "fundamental": w.fundamental, "context": w.context})

