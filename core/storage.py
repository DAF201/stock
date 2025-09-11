import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable


# Store DB at package root (wave/data.db)
DB_PATH = Path(__file__).resolve().parent.parent / "data.db"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db() -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS prices (
            symbol TEXT,
            ts TEXT,
            price REAL,
            volume REAL,
            source TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sentiment (
            symbol TEXT,
            ts TEXT,
            score REAL,
            confidence REAL,
            relevance REAL,
            impact REAL,
            text TEXT,
            source TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS indicators (
            symbol TEXT,
            ts TEXT,
            rsi REAL,
            macd REAL,
            macd_signal REAL,
            bb_upper REAL,
            bb_middle REAL,
            bb_lower REAL,
            sma_fast REAL,
            sma_slow REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamentals (
            symbol TEXT,
            ts TEXT,
            pe REAL,
            pb REAL,
            peg REAL,
            revenue_growth REAL,
            eps_growth REAL,
            debt_to_equity REAL,
            profit_margin REAL,
            score REAL,
            details TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS analysis (
            symbol TEXT,
            ts TEXT,
            score REAL,
            recommendation TEXT,
            position_size_pct REAL,
            confidence_pct REAL,
            reasons TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS weights (
            ts TEXT,
            tech REAL,
            fundamental REAL,
            context REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            symbol TEXT,
            ts TEXT,
            side TEXT,
            qty REAL,
            order_type TEXT,
            limit_price REAL,
            stop_loss REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio (
            ts TEXT,
            total_value REAL,
            cash REAL,
            positions TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def query_one(sql: str, params: tuple = ()) -> Any:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(sql, params)
    row = cur.fetchone()
    conn.close()
    return row


def insert(table: str, record: Dict[str, Any]) -> None:
    keys = ",".join(record.keys())
    placeholders = ",".join([":" + k for k in record.keys()])
    conn = _conn()
    conn.execute(f"INSERT INTO {table} ({keys}) VALUES ({placeholders})", record)
    conn.commit()
    conn.close()


def insert_many(table: str, records: Iterable[Dict[str, Any]]) -> None:
    records = list(records)
    if not records:
        return
    keys = ",".join(records[0].keys())
    placeholders = ",".join([":" + k for k in records[0].keys()])
    conn = _conn()
    conn.executemany(f"INSERT INTO {table} ({keys}) VALUES ({placeholders})", records)
    conn.commit()
    conn.close()
