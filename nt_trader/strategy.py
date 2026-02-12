from __future__ import annotations

from typing import Dict, List, Tuple


def aggregate_sentiment(scores: List[float]) -> float:
    if not scores:
        return 0.0
    # Use median for robustness; fallback to mean if needed
    try:
        ss = sorted(scores)
        n = len(ss)
        mid = n // 2
        if n % 2 == 1:
            return float(ss[mid])
        return float((ss[mid - 1] + ss[mid]) / 2.0)
    except Exception:
        return sum(scores) / max(1, len(scores))


def decide_action(sentiment: float, pos_threshold: float, close_threshold: float) -> str:
    if sentiment >= pos_threshold:
        return "long"
    if sentiment <= -pos_threshold:
        return "short"
    if abs(sentiment) < close_threshold:
        return "close"
    return "hold"


def summarize_gpt_details(details: List[Dict]) -> Tuple[List[Tuple[str, int]], float, str, str | None]:
    # Emotions
    emo_counts: Dict[str, int] = {}
    for d in details:
        for e in (d.get("emotions") or []):
            if not isinstance(e, str):
                continue
            key = e.strip().lower()
            if key:
                emo_counts[key] = emo_counts.get(key, 0) + 1
    top_emotions = sorted(emo_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:3]

    # Expected move
    exp_moves = []
    for d in details:
        v = d.get("expected_move_pct")
        if isinstance(v, (int, float)):
            exp_moves.append(float(v))
    avg_move = sum(exp_moves) / len(exp_moves) if exp_moves else 0.0
    move_dir = "up" if avg_move > 0 else ("down" if avg_move < 0 else "flat")

    # Decision majority
    dec_counts: Dict[str, int] = {}
    for d in details:
        dec = str(d.get("decision", "")).lower()
        if dec in {"long", "short", "hold", "close"}:
            dec_counts[dec] = dec_counts.get(dec, 0) + 1
    gpt_decision = max(dec_counts.items(), key=lambda kv: kv[1])[0] if dec_counts else None

    return top_emotions, avg_move, move_dir, gpt_decision
