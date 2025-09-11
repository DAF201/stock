Stock Analysis and Prediction System (Wave)

Quick start
- Python 3.10+
- Optional: Redis for message bus (in-memory default)

Install deps
```bash
pip install -r wave/requirements.txt
```

Run
```bash
python -m wave.main
```

CLI usage
```bash
python -m wave --limit 50 --prefer-redis --enable-orders --alpaca-mode paper \
  --finnhub-rpm 40 --yahoo-rps 3 --openai-rps 1.5 \
  --blackout-after 20 --blackout-before 20

# Override symbols directly (comma-separated) or via file
python -m wave --symbols AAPL,MSFT,GOOG
python -m wave --symbols-file ./my_symbols.txt
```

What it does
- Collects prices periodically (Finnhub if key, fallback to Yahoo).
- Publishes placeholder news items.
- Computes simple indicators (SMA5/20, RSI14).
- Integrates sentiment + technicals into a composite score and recommendation.
  - Sentiment uses VADER, optionally refined by OpenAI (`gpt-4o-mini`) if an API key is present.
- Logs trades as analysis records (order placement disabled by default).
- Emits portfolio snapshots and a toy adaptive learning metric.
- Respects market hours/holidays for trade execution (blackout 30m after open, 30m before close).
 - Handles API rate limits with batching, client-side rate limiting, and retry/backoff on 429/5xx.

Configuration
- Keys: `wave/key.json` (already present). Env vars override sensitive values.
- SP500 symbols: By default the system fetches and caches S&P 500 constituents from Wikipedia.
  - Limit with env var `SP500_LIMIT` (e.g., `SP500_LIMIT=50`) to reduce API load.
- To use Redis bus: set `REDIS_URL` and modify `get_bus(prefer_redis=True)` in `wave/main.py`.
- To enable Alpaca orders, pass `--enable-orders` or set `ENABLE_ORDERS=1`.
- Market hours: Uses Alpaca calendar if keys exist for exact sessions (incl. early closes); otherwise defaults to NYSE hours and US holidays.
- Rate limits: Finnhub limited to ~50/min; Yahoo quotes batched (50/tick) with ~5 req/sec; Alpaca orders ~1 req/sec. Retries use exponential backoff and Retry-After when provided.
- OpenAI: Optional; throttled to ~2 req/sec with retries/backoff. Provide API key via `OPENAI_API_KEY` or `wave/key.json`.

App settings
- Optional `wave/app_config.json` to persist non-secret configuration.
- Env vars override file settings.

Supported keys/env
- `PREFER_REDIS` (bool): prefer Redis bus.
- `SP500_LIMIT` (int): limit number of symbols.
- `SP500_REFRESH` (bool): refresh S&P 500 cache.
- `SYMBOLS_FILE` (path): newline-separated list of symbols to use instead of S&P 500.
- `FINNHUB_RPM` (int): Finnhub requests/minute.
- `YAHOO_RPS` (float): Yahoo requests/second for batch quotes.
- `ALPACA_RPS` (float): Alpaca requests/second.
- `OPENAI_RPS` (float): OpenAI requests/second.
- `ENABLE_ORDERS` (bool): actually send Alpaca orders.
- `ALPACA_MODE` (paper|live): trading mode.
- `BLACKOUT_AFTER_OPEN_MIN` (int): minutes after open to block trades.
- `BLACKOUT_BEFORE_CLOSE_MIN` (int): minutes before close to block trades.

Data persistence
- SQLite database at `wave/data.db` stores prices, sentiment, indicators, analysis, trades, portfolio.

Topics
- `prices.raw`, `news.raw`, `sentiment.processed`, `metrics.indicators`, `signals.analysis`, `portfolio.state`.

Notes
- This is a minimal, modular scaffold aligned with the requirements and safe defaults.
- Extend collectors (GDELT, SEC), add fundamentals, and harden risk/portfolio logic per your needs.

Tests
- Smoke test (short run): `python -m wave.tests.test_smoke`
- Full test (with one-off fundamentals fetch): `python -m wave.tests.full_test`
- DB counts helper: `python -m wave.tests.db_counts`
