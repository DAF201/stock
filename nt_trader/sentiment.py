from __future__ import annotations

import json
import os
from typing import Dict, List, Optional, Tuple

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


def get_vader() -> SentimentIntensityAnalyzer:
    return SentimentIntensityAnalyzer()


def score_news_vader(analyzer: SentimentIntensityAnalyzer, item: Dict) -> float:
    parts = []
    for key in ("headline", "title", "summary"):
        v = item.get(key)
        if isinstance(v, str):
            parts.append(v)
    text = ". ".join(parts)
    if not text:
        return 0.0
    s = analyzer.polarity_scores(text)
    return float(s.get("compound", 0.0))


def load_openai_config() -> Dict:
    from pathlib import Path
    cfg_path = Path("openai.json")
    if cfg_path.exists():
        try:
            with cfg_path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def init_openai_client() -> Optional[object]:
    try:
        from openai import OpenAI  # type: ignore
    except Exception:
        return None
    cfg = load_openai_config()
    api_key = os.getenv("OPENAI_API_KEY") or cfg.get("api_key")
    if not api_key:
        return None
    base_url = os.getenv("OPENAI_BASE_URL") or cfg.get("base_url")
    if base_url:
        return OpenAI(api_key=api_key, base_url=base_url)
    return OpenAI(api_key=api_key)


def gpt_analyze_text(
    client: object,
    model: str,
    symbol: str,
    text: str,
    price: Optional[float],
    price_context: Optional[str] = None,
    timeout: float = 20.0,
) -> Optional[Dict]:
    if not text or not client:
        return None
    try:
        from openai import OpenAI  # noqa: F401
    except Exception:
        return None

    system = (
        "You are a disciplined markets analyst. Evaluate short-term (1-5 trading days) price impact of the provided news for the given stock. "
        "Assess sentiment, key emotions, expected move (percent), and provide a clear trading decision anchored on the news and current price. "
        "Respond ONLY as compact JSON."
    )
    px = "unknown" if price is None else f"{price:.2f}"
    user = (
        f"Ticker: {symbol}\n"
        f"Current price: {px}\n"
        f"News: {text}\n"
        + (f"Price context: {price_context}\n" if price_context else "")
        + "\n"
        "Output JSON with keys: score[-1..1], emotions[list<=3], expected_move_pct[number], horizon_days[int 1-5], "
        "decision[long|short|hold|close], confidence[0..1], reason[str<=200]. No extra text."
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            response_format={"type": "json_object"},
            timeout=timeout,
        )
        content = resp.choices[0].message.content  # type: ignore[index]
        data = json.loads(content) if content else {}
        try:
            data["score"] = max(-1.0, min(1.0, float(data.get("score", 0.0))))
        except Exception:
            data["score"] = 0.0
        if not isinstance(data.get("emotions"), list):
            data["emotions"] = []
        try:
            data["expected_move_pct"] = float(data.get("expected_move_pct", 0.0))
        except Exception:
            data["expected_move_pct"] = 0.0
        try:
            data["horizon_days"] = int(data.get("horizon_days", 2))
        except Exception:
            data["horizon_days"] = 2
        dec = str(data.get("decision", "")).lower()
        if dec not in {"long", "short", "hold", "close"}:
            dec = "hold"
        data["decision"] = dec
        try:
            data["confidence"] = max(0.0, min(1.0, float(data.get("confidence", 0.0))))
        except Exception:
            data["confidence"] = 0.0
        data["reason"] = str(data.get("reason") or data.get("explanation") or "")[:300]
        return data
    except Exception:
        return None
