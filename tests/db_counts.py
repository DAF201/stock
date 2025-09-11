import sqlite3
import json
from pathlib import Path

db = Path(__file__).resolve().parents[1] / "data.db"
conn = sqlite3.connect(db)
c = conn.cursor()
info = {}
for t in [
    "prices",
    "sentiment",
    "indicators",
    "analysis",
    "trades",
    "portfolio",
    "fundamentals",
    "weights",
]:
    try:
        c.execute(f"SELECT COUNT(*) FROM {t}")
        info[t] = c.fetchone()[0]
    except Exception as e:
        info[t] = str(e)
print(json.dumps(info))
