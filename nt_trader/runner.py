from __future__ import annotations

import json
import time
import hashlib
import re
from datetime import datetime, timedelta, timezone
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import random

from .clients import (
    load_json,
    finnhub_company_news,
    finnhub_quote,
    alpaca_place_order,
    fetch_gdelt_docs,
    alpaca_clock,
    alpaca_snapshots,
    alpaca_positions,
    alpaca_position_for_symbol,
    alpaca_close_position,
    alpaca_account,
)
from .sentiment import get_vader, score_news_vader, init_openai_client, gpt_analyze_text
from .strategy import aggregate_sentiment, decide_action, summarize_gpt_details
from .universe import fetch_sp500_tickers
from .logger import TradeLogger, TradeEvent, DecisionsLogger, DecisionEvent


class NewsTrader:
    def __init__(self, args) -> None:
        self.args = args
        self.state_path = Path(getattr(args, "state_file", "nt_state.json"))
        self.state: Dict[str, Dict[str, float]] = self._load_state()
        self.finnhub_cfg = load_json(Path("finnhub.json"))
        self.finnhub_token = self.finnhub_cfg.get("api_key") or self.finnhub_cfg.get("token")
        if not self.finnhub_token:
            raise RuntimeError("finnhub.json missing 'api_key' or 'token'")

        self.alpaca_cfg = load_json(Path("alpaca.json")) if args.trade else {}
        self.alpaca_env: str = str(getattr(args, "alpaca_env", "paper")).lower()
        # Resolve credentials from env-specific section when present
        env_cfg = {}
        if isinstance(self.alpaca_cfg, dict):
            env_cfg = self.alpaca_cfg.get(self.alpaca_env) or {}
            if not isinstance(env_cfg, dict):
                env_cfg = {}
        self.alpaca_key = (env_cfg.get("key") if args.trade else None) or (self.alpaca_cfg.get("key") if args.trade else None)
        self.alpaca_secret = (env_cfg.get("secret") if args.trade else None) or (self.alpaca_cfg.get("secret") if args.trade else None)
        # Determine trading API base URL, prefer env-specific override
        base_url = env_cfg.get("base_url") if args.trade else None
        if args.trade and not base_url:
            base_url = "https://api.alpaca.markets/v2" if self.alpaca_env == "live" else "https://paper-api.alpaca.markets/v2"
        self.alpaca_url = base_url if args.trade else None
        if args.trade and self.alpaca_env == "live" and not getattr(args, "confirm_live_trade", False):
            raise RuntimeError("Live trading requested (--alpaca-env live) but --confirm-live-trade not provided. Aborting to avoid accidental live orders.")
        if args.trade and (not self.alpaca_key or not self.alpaca_secret):
            raise RuntimeError("alpaca.json missing 'key' or 'secret'")

        self.analyzer = get_vader()
        self.gpt_client = init_openai_client() if getattr(args, "use_gpt", False) else None
        self.logger = TradeLogger(getattr(args, "log_file", "logs/trades.csv"))
        self.decisions_logger = DecisionsLogger(getattr(args, "decisions_log_file", "logs/decisions.csv"))

        # Locks and thread controls
        self._fh_lock = threading.Lock()
        self._state_io_lock = threading.Lock()
        self._holdings_thread: Optional[threading.Thread] = None
        self._stop_holdings = threading.Event()

        # Dynamic bracket configuration (optional)
        self.dynamic_bracket: bool = bool(getattr(args, "dynamic_bracket", False))
        # Bounds and ratio for dynamic TP/SL
        _tpmin = getattr(args, "tp_pct_min", None)
        _tpmax = getattr(args, "tp_pct_max", None)
        _slmin = getattr(args, "sl_pct_min", None)
        _slmax = getattr(args, "sl_pct_max", None)
        base_tp = float(getattr(args, "tp_pct", 0.02))
        base_sl = float(getattr(args, "sl_pct", 0.01))
        self.tp_pct_min: float = float(base_tp if _tpmin is None else _tpmin)
        self.tp_pct_max: float = float((max(self.tp_pct_min, base_tp) if _tpmax is None else _tpmax))
        self.sl_pct_min: float = float(base_sl if _slmin is None else _slmin)
        self.sl_pct_max: float = float((max(self.sl_pct_min, base_sl) if _slmax is None else _slmax))
        self.rr_multiple: float = float(getattr(args, "rr_multiple", 2.0))  # target reward:risk multiple
        self.scale_by_gpt_conf: bool = bool(getattr(args, "scale_by_gpt_conf", True))

        # Universe
        if args.sp500:
            universe = fetch_sp500_tickers()
            if args.randomize:
                random.shuffle(universe)
            if args.limit_tickers and args.limit_tickers > 0:
                self.tickers = universe[: args.limit_tickers]
            else:
                self.tickers = universe
        else:
            self.tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

        # Dates
        today = datetime.now().date()
        default_from = datetime.combine(today - timedelta(days=3), datetime.min.time())
        # Use current time as default upper bound to include today's news
        default_to = datetime.now()
        self.fr = _as_date(getattr(args, "from_date", None), default_from)
        self.to = _as_date(getattr(args, "to_date", None), default_to)

        # Cooldowns and loop settings
        self.news_poll_seconds: float = float(getattr(args, "news_poll_seconds", 0.0) or 0.0)
        self.price_poll_seconds: float = float(getattr(args, "price_poll_seconds", 0.0) or 0.0)
        self.trade_cooldown_minutes: float = float(getattr(args, "trade_cooldown_minutes", 0.0) or 0.0)
        self.loop: bool = bool(getattr(args, "loop", False))
        self.poll_seconds: float = float(getattr(args, "poll_seconds", 60.0))
        self.strategy: str = str(getattr(args, "strategy", "rule")).lower()
        self.gpt_conf_min: float = float(getattr(args, "gpt_decision_min_confidence", 0.0) or 0.0)
        # Exit threshold when already in a long position (more conservative than general close-threshold)
        self.in_pos_exit_sentiment: float = float(getattr(args, "in_pos_exit_sentiment", -0.05))
        # Price factor blending
        self.factor_weight: float = max(0.0, min(1.0, float(getattr(args, "factor_weight", 0.0))))
        self.factor_window_chg_scale: float = float(getattr(args, "factor_window_chg_scale", 2.0))
        self.factor_vol_penalty_scale: float = float(getattr(args, "factor_vol_penalty_scale", 10.0))
        self.factor_trend_bonus: float = float(getattr(args, "factor_trend_bonus", 0.1))
        self.factor_range_bias_weight: float = float(getattr(args, "factor_range_bias_weight", 0.1))
        # Drawdown/price-fall stop
        self.enable_drawdown_stop: bool = bool(getattr(args, "enable_drawdown_stop", False))
        self.dd_max_drop_pct: float = float(getattr(args, "dd_max_drop_pct", 5.0))
        self.dd_window_min: float = float(getattr(args, "dd_window_min", 15.0))
        self.dd_window_drop_pct: float = float(getattr(args, "dd_window_drop_pct", 1.0))
        self.dd_min_consecutive_down: int = int(getattr(args, "dd_min_consecutive_down", 3))
        # If true, require BOTH window drop and consecutive-down conditions in addition to entry drop
        self.dd_require_both: bool = bool(getattr(args, "dd_require_both", False))
        # Finnhub backoff controls
        self.fh_backoff_enabled: bool = bool(getattr(args, "finnhub_backoff", True))
        self.fh_backoff: float = 0.0
        self.fh_backoff_start: float = float(getattr(args, "finnhub_backoff_start_seconds", 2.0))
        self.fh_backoff_max: float = float(getattr(args, "finnhub_backoff_max_seconds", 120.0))
        self.fh_backoff_mult: float = float(getattr(args, "finnhub_backoff_multiplier", 2.0))
        self.fh_backoff_decay: float = float(getattr(args, "finnhub_backoff_decay", 0.5))
        self.market_only_trade: bool = bool(getattr(args, "market_only_trade", True))
        self.market_only_price: bool = bool(getattr(args, "market_only_price", True))
        self.market_tz: str = str(getattr(args, "market_timezone", "America/New_York"))
        # GDELT settings and cache
        self.use_gdelt: bool = bool(getattr(args, "gdelt", False))
        self.gdelt_themes: List[str] = [t.strip() for t in str(getattr(args, "gdelt_themes", "")).split(",") if t.strip()]
        self.gdelt_max_records: int = int(getattr(args, "gdelt_max_records", 30))
        self.gdelt_timespan_min: int = int(getattr(args, "gdelt_timespan_min", 180))
        self.gdelt_poll_seconds: float = float(getattr(args, "gdelt_poll_seconds", 60.0))
        self._gdelt_cache: List[Dict] = []
        self._gdelt_last_ts: float = 0.0

        # Price history and caching
        self.price_hist_window_min: float = float(getattr(args, "price_history_window_min", 30.0))
        self.price_hist_points: int = int(getattr(args, "price_history_points", 10))

        # Seen-news / GPT cache controls
        self.max_seen_news_per_symbol: int = int(getattr(args, "max_seen_news_per_symbol", 500))
        self.clamp_news_rate: bool = bool(getattr(args, "clamp_news_rate", True))

        # Finnhub global rate limiting and batching
        self.fh_max_rpm: int = int(getattr(args, "finnhub_max_rpm", 50))
        self.fh_min_interval: float = float(getattr(args, "finnhub_min_interval_seconds", 0.25))
        self._fh_last_call_ts: float = 0.0
        self._fh_call_times: List[float] = []
        self.symbols_per_batch: int = int(getattr(args, "symbols_per_batch", 50))
        self.batch_sleep_seconds: float = float(getattr(args, "batch_sleep_seconds", 5.0))
        self.use_alpaca_data: bool = bool(getattr(args, "use_alpaca_data", False))
        self.alpaca_data_base_url: str = str(getattr(args, "alpaca_data_base_url", "https://data.alpaca.markets/v2"))
        self.snapshot_offhours_prices: bool = bool(getattr(args, "snapshot_offhours_prices", False))
        # Risk and allocation
        self.max_open_positions: int = int(getattr(args, "max_open_positions", 20))
        self.max_usd_per_symbol: float = float(getattr(args, "max_usd_per_symbol", 2000.0))
        # Optional percent-of-equity cap per symbol (0..1). When >0, used as the primary cap.
        self.max_pct_per_symbol: float = max(0.0, min(1.0, float(getattr(args, "max_pct_per_symbol", 0.0))))
        self.use_bracket: bool = bool(getattr(args, "use_bracket", False))
        self.tp_pct: float = float(getattr(args, "tp_pct", 0.02))
        self.sl_pct: float = float(getattr(args, "sl_pct", 0.01))
        self.dual_horizon: bool = bool(getattr(args, "dual_horizon", False))
        self.core_allocation_pct: float = max(0.0, min(1.0, float(getattr(args, "core_allocation_pct", 0.6))))
        self.core_use_bracket: bool = bool(getattr(args, "core_use_bracket", False))
        self.core_tp_pct: float = float(getattr(args, "core_tp_pct", 0.06))
        self.core_sl_pct: float = float(getattr(args, "core_sl_pct", 0.03))
        self.allocation_pct: float = max(0.0, min(1.0, float(getattr(args, "allocation_pct", 0.2))))
        self._acct_cache: Dict[str, float] = {}
        self.long_only: bool = bool(getattr(args, "long_only", True))
        # Order sizing
        self.order_size_mode: str = str(getattr(args, "order_size_mode", "auto")).lower()
        self.shares_per_trade: int = int(getattr(args, "shares_per_trade", 0))
        # Local news cache
        self.news_cache_path = Path(getattr(args, "news_cache_file", "logs/news_cache.json"))
        self.news_cache_ttl = int(getattr(args, "news_cache_ttl_seconds", 300))
        self.news_cache_max_items = int(getattr(args, "news_cache_max_items", 50))
        self._news_cache: Dict[str, Dict] = self._load_news_cache()

        # Enforce news polling 1..5 times/minute in loop mode
        if self.loop and self.clamp_news_rate:
            original_poll = self.poll_seconds
            original_news = self.news_poll_seconds
            if self.poll_seconds < 12.0:
                self.poll_seconds = 12.0
            if self.poll_seconds > 60.0:
                self.poll_seconds = 60.0
            if self.news_poll_seconds and self.news_poll_seconds < 12.0:
                self.news_poll_seconds = 12.0
            if self.news_poll_seconds and self.news_poll_seconds > 60.0:
                self.news_poll_seconds = 60.0
            if original_poll != self.poll_seconds or original_news != self.news_poll_seconds:
                print(f"[rate] Clamped poll to {self.poll_seconds}s; news cooldown={self.news_poll_seconds or 0}s (target 1..5x/min)")

        # Holdings watcher options
        self.holdings_watcher: bool = bool(getattr(args, "holdings_watcher", False))
        self.holdings_poll_seconds: float = float(getattr(args, "holdings_poll_seconds", 20.0))
        # PDT / Tax awareness
        self.enforce_pdt: bool = bool(getattr(args, "enforce_pdt", False))
        self.pdt_min_equity: float = float(getattr(args, "pdt_min_equity", 25000.0))
        self.max_daytrades_5d: int = int(getattr(args, "max_daytrades_5d", 3))
        self.pdt_allow_risk_exit: bool = bool(getattr(args, "pdt_allow_risk_exit", True))
        self.tax_aware: bool = bool(getattr(args, "tax_aware", False))
        self.ltcg_days: int = int(getattr(args, "ltcg_days", 365))
        self.tax_min_hold_days: int = int(getattr(args, "tax_min_hold_days", 0))
        self.tax_min_profit_usd_to_close_short_term: float = float(getattr(args, "tax_min_profit_usd_to_close_short_term", 0.0))

    def banner(self) -> None:
        print("News-trader run configuration:")
        if self.args.sp500:
            print(f"  Universe: S&P 500 ({len(self.tickers)} tickers)")
            preview = ", ".join(self.tickers[:10]) + (" ..." if len(self.tickers) > 10 else "")
            print(f"  Sample: {preview}")
        else:
            print(f"  Tickers: {', '.join(self.tickers)}")
        print(f"  Window: {self.fr.date()} to {self.to.date()}")
        mode = "DRY-RUN"
        if self.args.trade:
            mode = "TRADE (live)" if getattr(self, "alpaca_env", "paper") == "live" else "TRADE (paper)"
        print(f"  Mode: {mode}")
        print(f"  Thresholds: pos={self.args.pos_threshold}, close={self.args.close_threshold}, in-pos-exit={self.in_pos_exit_sentiment}")
        if getattr(self.args, "use_gpt", False):
            enabled = bool(self.gpt_client)
            print(f"  GPT: {'ON' if enabled else 'ON (no key found, falling back)'} | model={self.args.gpt_model} | weight={self.args.gpt_weight}")

    def run(self) -> None:
        self.banner()
        if self.use_gdelt:
            self._maybe_refresh_gdelt(force=True)
        # Start holdings watcher in loop mode (trade only)
        if self.loop and self.args.trade and self.holdings_watcher and self._holdings_thread is None:
            self._start_holdings_watcher()
        if not self.loop:
            for i in range(0, len(self.tickers), max(1, self.symbols_per_batch)):
                batch = self.tickers[i : i + max(1, self.symbols_per_batch)]
                self._maybe_fetch_batch_prices(batch)
                # Fetch current positions map once per batch for risk checks
                pos_map = self._positions_map() if self.args.trade else {}
                for symbol in batch:
                    self._process_symbol(symbol, pos_map)
                if i + self.symbols_per_batch < len(self.tickers):
                    time.sleep(max(0.0, self.batch_sleep_seconds))
            self._save_state()
            return

        # Looping mode
        while True:
            if self.use_gdelt:
                self._maybe_refresh_gdelt()
            for i in range(0, len(self.tickers), max(1, self.symbols_per_batch)):
                batch = self.tickers[i : i + max(1, self.symbols_per_batch)]
                self._maybe_fetch_batch_prices(batch)
                pos_map = self._positions_map() if self.args.trade else {}
                # If a dedicated holdings watcher runs, skip held symbols here to avoid duplication
                if self.holdings_watcher and pos_map:
                    batch = [s for s in batch if s not in pos_map]
                for symbol in batch:
                    self._process_symbol(symbol, pos_map)
                if i + self.symbols_per_batch < len(self.tickers):
                    time.sleep(max(0.0, self.batch_sleep_seconds))
            self._save_state()
            time.sleep(max(1.0, self.poll_seconds))

    def _process_symbol(self, symbol: str, pos_map: Optional[Dict[str, Dict]] = None) -> None:
        # News fetch cooldown
        now_ts = time.time()
        if self.news_poll_seconds > 0:
            last_news = self.state.get(symbol, {}).get("last_news", 0)
            if now_ts - last_news < self.news_poll_seconds:
                print("-" * 72)
                print(f"{symbol}: skip (news cooldown {self.news_poll_seconds}s)")
                return

        # Try local news cache first
        news = self._get_cached_news(symbol)
        if news is None:
            # Respect global rate limiting/backoff before Finnhub call
            self._fh_rate_gate()
            try:
                news = finnhub_company_news(self.finnhub_token, symbol, self.fr, self.to, self.args.max_news)
                self._set_state(symbol, "last_news", now_ts)
                # success reduces backoff
                if self.fh_backoff_enabled and self.fh_backoff > 0:
                    self.fh_backoff = self.fh_backoff * max(0.0, min(1.0, self.fh_backoff_decay))
                    if self.fh_backoff < 0.5:
                        self.fh_backoff = 0.0
                self._fh_mark_call()
                # Update local cache
                self._set_cached_news(symbol, news)
            except Exception as e:
                print(f"[{symbol}] Finnhub error: {e}")
                if self.fh_backoff_enabled:
                    self.fh_backoff = max(self.fh_backoff_start, self.fh_backoff * max(1.0, self.fh_backoff_mult))
                news = []

        time.sleep(self.args.sleep)

        # Merge GDELT macro events if available
        if self.use_gdelt and self._gdelt_cache and getattr(self.args, "macro_affects_sentiment", False):
            news = (news or []) + self._gdelt_cache

        # Price (for GPT context and sizing)
        # Price cooldown and caching
        price = None
        market_open = self._is_market_open()
        if self.price_poll_seconds > 0:
            last_ts = self.state.get(symbol, {}).get("last_price_ts", 0)
            last_px = self.state.get(symbol, {}).get("last_price", None)
            if last_ts and (time.time() - last_ts) < self.price_poll_seconds and last_px:
                price = float(last_px)
        if price is None and (market_open or not self.market_only_price):
            self._fh_rate_gate()
            try:
                price = finnhub_quote(self.finnhub_token, symbol)
                if price is not None:
                    self._set_state(symbol, "last_price", float(price))
                    self._set_state(symbol, "last_price_ts", time.time())
                # success reduces backoff
                if self.fh_backoff_enabled and self.fh_backoff > 0:
                    self.fh_backoff = self.fh_backoff * max(0.0, min(1.0, self.fh_backoff_decay))
                    if self.fh_backoff < 0.5:
                        self.fh_backoff = 0.0
                self._fh_mark_call()
            except Exception:
                if self.fh_backoff_enabled:
                    self.fh_backoff = max(self.fh_backoff_start, self.fh_backoff * max(1.0, self.fh_backoff_mult))
                price = self.state.get(symbol, {}).get("last_price", None)
        elif price is None and self.market_only_price:
            # Try reuse cached price silently
            price = self.state.get(symbol, {}).get("last_price", None)

        # Dedupe news items to unique events
        news = self._dedupe_news(symbol, news)

        vader_scores: List[float] = []
        gpt_scores: List[float] = []
        combined_scores: List[float] = []
        gpt_details: List[Dict] = []

        # Prepare price history summary for GPT context
        price_ctx = self._update_and_summarize_price_history(symbol, price)
        price_ctx_obj = self._parse_price_ctx(price_ctx) if price_ctx else None

        # Per-symbol caches
        seen_ids = self._get_seen_news(symbol)
        gpt_cache = self._get_gpt_cache(symbol)
        gpt_cache_global = self._get_gpt_cache_global()

        for idx, item in enumerate(news):
            vscore = score_news_vader(self.analyzer, item)
            vader_scores.append(vscore)

            gscore = None
            if self.gpt_client and idx < max(0, self.args.gpt_max_news):
                ekey = self._event_key(item, symbol)
                cached = None
                if item.get("is_macro"):
                    cached = gpt_cache_global.get(ekey)
                if not cached:
                    cached = gpt_cache.get(ekey) if gpt_cache else None
                if cached:
                    gpt_details.append(cached)
                    try:
                        gscore = float(cached.get("score"))
                        gpt_scores.append(gscore)
                    except Exception:
                        pass
                else:
                    # Guardrails: only call GPT if VADER strong enough or price moved meaningfully
                    call_gpt = False
                    try:
                        if abs(vscore) >= float(getattr(self.args, "gpt_min_abs_vader", 0.1)):
                            call_gpt = True
                    except Exception:
                        pass
                    if not call_gpt:
                        # Parse price_ctx for window change percent if available
                        try:
                            if price_ctx and "change=" in price_ctx and "%" in price_ctx:
                                # e.g., change=+1.23%
                                frag = price_ctx.split("change=")[1].split("%", 1)[0]
                                window_chg = float(frag)
                                if abs(window_chg) >= float(getattr(self.args, "gpt_min_window_move_pct", 0.5)):
                                    call_gpt = True
                        except Exception:
                            pass
                    if not call_gpt:
                        # Skip GPT call; rely on VADER
                        pass
                    else:
                        parts = []
                        for key in ("headline", "title", "summary"):
                            val = item.get(key)
                            if isinstance(val, str) and val:
                                parts.append(val)
                        text = ". ".join(parts)
                        res = gpt_analyze_text(
                            self.gpt_client,
                            self.args.gpt_model,
                            symbol,
                            text,
                            price,
                            price_context=price_ctx,
                            timeout=self.args.gpt_timeout,
                        )
                        if res is not None:
                            gpt_details.append(res)
                            self._cache_gpt(symbol, ekey, res)
                            if item.get("is_macro"):
                                self._cache_gpt_global(ekey, res)
                            self._mark_seen_event(symbol, ekey)
                            try:
                                gscore = float(res.get("score"))
                                gpt_scores.append(gscore)
                            except Exception:
                                pass

            if gscore is not None:
                w = max(0.0, min(1.0, self.args.gpt_weight))
                c = (1.0 - w) * vscore + w * gscore
            else:
                c = vscore
            combined_scores.append(c)

        avg_sent = aggregate_sentiment(combined_scores)
        avg_vader = aggregate_sentiment(vader_scores)
        avg_gpt = aggregate_sentiment(gpt_scores)

        top_emotions, avg_move, move_dir, gpt_decision = summarize_gpt_details(gpt_details)

        # Blend in price-based factors if configured
        used_sent = avg_sent
        if self.factor_weight > 0.0 and isinstance(price_ctx_obj, dict):
            fscore = self._compute_factor_score(price_ctx_obj)
            used_sent = (1.0 - self.factor_weight) * avg_sent + self.factor_weight * fscore

        # Decide using configured strategy
        decision_source = "rule"
        action = decide_action(used_sent, self.args.pos_threshold, self.args.close_threshold)
        if self.strategy == "gpt" and gpt_decision:
            action = gpt_decision
            decision_source = "gpt"
        elif self.strategy == "hybrid":
            # Use GPT if available and avg confidence >= threshold
            if gpt_details:
                confs = [float(d.get("confidence", 0.0) or 0.0) for d in gpt_details]
                avg_conf = sum(confs) / len(confs)
                if gpt_decision and avg_conf >= self.gpt_conf_min:
                    action = gpt_decision
                    decision_source = "gpt"
        else:
            # rule strategy: optionally override if user explicitly asked to use gpt decision via flag
            if getattr(self.args, "use_gpt_decision", False) and gpt_decision:
                action = gpt_decision
                decision_source = "gpt"

        # Position-aware adjustment: if already holding a long, require stronger negative to exit
        has_long = False
        try:
            if pos_map and symbol in pos_map:
                qv = float(pos_map.get(symbol, {}).get("qty", 0))
                has_long = qv > 0
        except Exception:
            has_long = False

        # Drawdown/keep-dropping forced close check (only for existing longs)
        dd_forced = False
        try:
            if has_long and self.enable_drawdown_stop and price is not None:
                entry_px = float(self.state.get(symbol, {}).get("last_entry_price", 0) or 0)
                if entry_px > 0:
                    drop_pct = ((float(price) - entry_px) / entry_px) * 100.0
                    if drop_pct <= -abs(self.dd_max_drop_pct):
                        # Evaluate recent window conditions from stored price series
                        stats = self._price_stats_for_window(symbol, self.dd_window_min)
                        window_ok = False
                        consec_ok = False
                        if isinstance(stats, dict):
                            try:
                                if float(stats.get("change_pct", 0.0) or 0.0) <= -abs(self.dd_window_drop_pct):
                                    window_ok = True
                            except Exception:
                                window_ok = False
                            try:
                                if int(stats.get("downs", 0) or 0) >= max(1, self.dd_min_consecutive_down):
                                    consec_ok = True
                            except Exception:
                                consec_ok = False
                        cond = (window_ok and consec_ok) if self.dd_require_both else (window_ok or consec_ok)
                        if cond:
                            action = "close"
                            decision_source += "+dd_stop"
                            dd_forced = True
        except Exception:
            pass
        if has_long and action == "close" and ("+dd_stop" not in decision_source):
            try:
                # Tax-aware holds: if configured, avoid closing too soon unless strongly negative
                if self.tax_aware:
                    ent_ts = float(self.state.get(symbol, {}).get("last_entry_ts", 0) or 0)
                    days_held = (time.time() - ent_ts) / 86400.0 if ent_ts else None
                    if days_held is not None:
                        # Minimum hold window
                        if self.tax_min_hold_days > 0 and days_held < self.tax_min_hold_days:
                            action = "hold"
                            decision_source += "+tax_hold_min"
                        # Prefer long-term gains
                        elif self.ltcg_days > 0 and days_held < self.ltcg_days and used_sent > -abs(self.in_pos_exit_sentiment):
                            action = "hold"
                            decision_source += "+tax_hold_ltcg"
                        # Require minimum profit to close short-term if configured
                        elif self.tax_min_profit_usd_to_close_short_term > 0:
                            # Approx unrealized PnL = (price - entry_price) * qty (qty unknown here). Use per-share check if available.
                            try:
                                entry_px = float(self.state.get(symbol, {}).get("last_entry_price", 0) or 0)
                                if entry_px > 0 and price is not None:
                                    pnl_ps = float(price) - entry_px
                                    if pnl_ps > 0 and pnl_ps < self.tax_min_profit_usd_to_close_short_term:
                                        action = "hold"
                                        decision_source += "+tax_hold_minpnl"
                            except Exception:
                                pass
                if action == "close" and used_sent > float(getattr(self, "in_pos_exit_sentiment", -0.05)):
                    action = "hold"
                    decision_source += "+hold_pos"
            except Exception:
                pass

        print("-" * 72)
        extra = ""
        if getattr(self.args, "use_gpt", False):
            extra = f" (vader={avg_vader:+.3f}, gpt={(avg_gpt if gpt_scores else float('nan')):+.3f})"
        # Annotate if factors applied
        if self.factor_weight > 0.0 and isinstance(price_ctx_obj, dict):
            decision_source += "+factors"
        print(f"{symbol}: sentiment={used_sent:+.3f}{extra} | action={action} | price={price if price else 'n/a'}")
        if getattr(self.args, "use_gpt", False) and (top_emotions or gpt_details):
            emo_str = ", ".join([f"{k}:{v}" for k, v in top_emotions]) if top_emotions else "none"
            print(f"  GPT emotions: {emo_str}")
            print(f"  GPT exp move: {avg_move:+.2f}% ({move_dir})")
            if gpt_decision:
                print(f"  GPT decision: {gpt_decision}")

        # Decision snapshot logging
        try:
            avg_conf = None
            if gpt_details:
                confs = [float(d.get("confidence", 0.0) or 0.0) for d in gpt_details]
                avg_conf = sum(confs) / len(confs) if confs else None
            emotions_str = ",".join([f"{k}:{v}" for k, v in top_emotions]) if top_emotions else None
            change_pct = price_ctx_obj.get("change_pct") if isinstance(price_ctx_obj, dict) else None
            vol_pct = price_ctx_obj.get("vol_pct") if isinstance(price_ctx_obj, dict) else None
            trend = price_ctx_obj.get("trend") if isinstance(price_ctx_obj, dict) else None
            self.decisions_logger.log(DecisionEvent(
                ts=TradeLogger.now_iso(),
                symbol=symbol,
                price=price,
                vader=avg_vader,
                gpt=(avg_gpt if gpt_scores else None),
                sentiment=used_sent,
                action=action,
                strategy=decision_source,
                gpt_conf=avg_conf,
                emotions=emotions_str,
                exp_move_pct=(avg_move if gpt_details else None),
                price_change_pct=(change_pct if change_pct is not None else None),
                vol_pct=(vol_pct if vol_pct is not None else None),
                trend=(str(trend) if trend is not None else None),
            ))
        except Exception:
            pass

        # Mark fetched events as seen for GPT reuse
        for item in news:
            ekey = self._event_key(item, symbol)
            self._mark_seen_event(symbol, ekey)

        # Order placement
        # Build a base event for logging
        def log_event(side: str, qty: int, order_id: str | None, status: str | None, error: str | None):
            emotions_str = ",".join([f"{k}:{v}" for k, v in top_emotions]) if top_emotions else None
            # identify environment in log mode
            _mode = (
                "trade-live" if (self.args.trade and getattr(self, "alpaca_env", "paper") == "live")
                else ("trade-paper" if self.args.trade else "dry-run")
            )
            ev = TradeEvent(
                ts=TradeLogger.now_iso(),
                mode=_mode,
                symbol=symbol,
                action=action,
                side=side or "none",
                qty=qty,
                price=price,
                decision_source=decision_source,
                sentiment=avg_sent,
                vader=avg_vader,
                gpt=(avg_gpt if gpt_scores else None),
                gpt_decision=gpt_decision,
                gpt_exp_move_pct=(avg_move if gpt_details else None),
                gpt_emotions=emotions_str,
                order_id=order_id,
                status=status,
                error=error,
            )
            self.logger.log(ev)

        if not self.args.trade:
            if price and action in ("long", "short"):
                per_symbol_budget = self._compute_per_symbol_budget(price)
                qty = max(int(per_symbol_budget // max(price, 0.01)), 1)
                # Emulate order sizing decision
                side_sim = "buy" if action == "long" else ("sell" if action == "short" else None)
                want_notional = False
                if self.order_size_mode == "dollars":
                    want_notional = True
                elif self.order_size_mode == "shares":
                    want_notional = False
                else:
                    want_notional = (action == "long" and side_sim == "buy" and not getattr(self, 'use_bracket', False))
                if want_notional and side_sim == "buy" and not getattr(self, 'use_bracket', False):
                    print(f"  DRY-RUN would {action} ${per_symbol_budget:.2f} notional (~{max(1,int(per_symbol_budget//max(price,0.01)))} sh) @ ~{price}")
                else:
                    if self.order_size_mode == "shares" and self.shares_per_trade > 0:
                        qty = max(1, int(self.shares_per_trade))
                    print(f"  DRY-RUN would {action} {qty} sh @ ~{price}")
            elif action == "close":
                print("  DRY-RUN would close any open position")
            return

        if not price:
            print("  Skip trading: missing price")
            return

        # Decide order side based on action
        side: Optional[str] = None
        if action == "long":
            side = "buy"
        elif action == "short":
            # Enforce long-only: do not open shorts
            if self.long_only:
                # If we have a long position, interpret as a signal to close; otherwise hold
                if pos_map and symbol in pos_map and float(pos_map.get(symbol, {}).get("qty", 0)) > 0:
                    action = "close"
                    side = None
                    print("  Long-only: converting short signal to close.")
                else:
                    print("  Long-only: skipping short signal (holding cash).")
                    side = None
                    # fall through to hold/return below
            else:
                side = "sell"
        elif action == "close":
            side = None
        else:
            print("  Hold: no order placed.")
            return

        # Determine order sizing (shares vs dollars) based on mode and constraints
        per_symbol_budget = self._compute_per_symbol_budget(price)
        want_notional = False
        if self.order_size_mode == "dollars":
            want_notional = True
        elif self.order_size_mode == "shares":
            want_notional = False
        else:  # auto
            # Prefer notional for simple market long buys without bracket; otherwise use shares
            want_notional = (action == "long" and side == "buy" and not getattr(self, 'use_bracket', False))

        # Compute qty and notional candidates
        notional_amount = per_symbol_budget
        qty = max(int(per_symbol_budget // max(price, 0.01)), 1)
        if self.order_size_mode == "shares" and self.shares_per_trade > 0:
            qty = max(1, int(self.shares_per_trade))

        # PDT pre-checks (entries): enforce automatically only if equity < threshold
        if side in ("buy", "sell"):
            acct = self._account_info()
            try:
                equity = float(acct.get("equity", 0.0)) if isinstance(acct, dict) else 0.0
            except Exception:
                equity = 0.0
            if equity and equity < self.pdt_min_equity:
                # Estimate recent day trades from local state (last 5 days)
                dt_count = self._daytrade_count_5d()
                if side == "buy" and action == "long" and dt_count >= self.max_daytrades_5d:
                    print(f"  PDT (auto): blocking new entry; {dt_count}/{self.max_daytrades_5d} day trades in 5d and equity ${equity:.2f} < ${self.pdt_min_equity:.2f}")
                    return

        # Trade cooldown for entries (allow close always)
        now_ts = time.time()
        if side in ("buy", "sell") and self.trade_cooldown_minutes > 0:
            last_trade = self.state.get(symbol, {}).get("last_trade", 0)
            min_gap = self.trade_cooldown_minutes * 60.0
            if now_ts - last_trade < min_gap:
                wait = int(min_gap - (now_ts - last_trade))
                print(f"  Skip trading: cooldown active ({wait}s remaining)")
                return

        if side:
            # Risk checks if positions map provided
            if pos_map is not None and self.args.trade:
                open_count = len(pos_map)
                if action in ("long", "short") and symbol not in pos_map and open_count >= self.max_open_positions:
                    print(f"  Risk: max open positions reached ({open_count}/{self.max_open_positions})")
                    return
                p = pos_map.get(symbol)
                current_usd = abs(float(p.get("market_value", 0))) if p else 0.0
                new_usd = (float(notional_amount) if (action == "long" and side == "buy" and want_notional and not getattr(self, 'use_bracket', False)) else (qty * float(price)))
                if True:
                    # Enforce per-symbol exposure cap (percent-of-equity when configured; else fixed USD)
                    cap_usd = self.max_usd_per_symbol
                    if self.max_pct_per_symbol > 0.0 and equity > 0:
                        cap_usd = self.max_pct_per_symbol * equity
                    if (current_usd + new_usd) > cap_usd:
                        if self.max_pct_per_symbol > 0.0 and equity > 0:
                            print(
                                f"  Risk: per-symbol cap {self.max_pct_per_symbol*100:.2f}% of equity exceeded (current ${current_usd:.2f} + new ${new_usd:.2f} > ${cap_usd:.2f})"
                            )
                        else:
                            print(
                                f"  Risk: per-symbol USD limit exceeded (current ${current_usd:.2f} + new ${new_usd:.2f} > ${self.max_usd_per_symbol:.2f})"
                            )
                        return

            if self.market_only_trade and not self._is_market_open():
                print("  Market closed: skipping order per config (market-only-trade)")
                return

            # Optional bracket order (tactical leg defaults)
            order_class = None
            tp = None
            sl = None
            if getattr(self, 'use_bracket', False):
                # Determine TP/SL percentages (static or dynamic per symbol)
                tp_pct_use = max(0.0, float(self.tp_pct))
                sl_pct_use = max(0.0, float(self.sl_pct))
                tp_price = None
                sl_price = None
                if self.dynamic_bracket:
                    # Prefer GPT expected move when available; fallback to sentiment magnitude mapping
                    exp_move_pct = None
                    avg_conf = None
                    try:
                        # avg_move defined earlier when summarizing GPT details
                        exp_move_pct = abs(float(avg_move)) if 'avg_move' in locals() else None
                    except Exception:
                        exp_move_pct = None
                    try:
                        if gpt_details:
                            confs = [float(d.get("confidence", 0.0) or 0.0) for d in gpt_details]
                            avg_conf = sum(confs) / len(confs) if confs else None
                    except Exception:
                        avg_conf = None

                    if exp_move_pct and exp_move_pct > 0:
                        tp_pct_use = exp_move_pct / 100.0  # convert percent -> decimal
                        if self.scale_by_gpt_conf and isinstance(avg_conf, (int, float)):
                            # Scale between 0.5x..1.0x based on confidence (0..1)
                            tp_pct_use *= (0.5 + 0.5 * max(0.0, min(1.0, float(avg_conf))))
                    else:
                        # Map sentiment magnitude [0..1] into [tp_pct_min..tp_pct_max]
                        mag = max(0.0, min(1.0, abs(float(avg_sent)))) if 'avg_sent' in locals() else 0.0
                        tp_pct_use = self.tp_pct_min + (self.tp_pct_max - self.tp_pct_min) * mag

                    # Clamp within configured bounds
                    tp_pct_use = max(self.tp_pct_min, min(self.tp_pct_max, tp_pct_use))
                    # Compute SL from target RR multiple, then clamp
                    rr = max(0.1, float(self.rr_multiple))
                    sl_suggest = tp_pct_use / rr
                    sl_pct_use = max(self.sl_pct_min, min(self.sl_pct_max, sl_suggest))

                    try:
                        rr_eff = (tp_pct_use / sl_pct_use) if sl_pct_use > 0 else float('inf')
                        print(f"  Dynamic bracket: tp={tp_pct_use*100:.2f}%, sl={sl_pct_use*100:.2f}% (RR~{rr_eff:.1f}x)")
                    except Exception:
                        pass

                if action == "long":
                    tp_price = round(price * (1 + max(0.0, tp_pct_use)), 2)
                    sl_price = round(price * (1 - max(0.0, sl_pct_use)), 2)
                else:
                    tp_price = round(price * (1 - max(0.0, tp_pct_use)), 2)
                    sl_price = round(price * (1 + max(0.0, sl_pct_use)), 2)
                order_class = "bracket"
                tp = {"limit_price": tp_price}
                sl = {"stop_price": sl_price}
                # Persist local expectation so we can resume after restart (tactical)
                try:
                    sym = self.state.setdefault(symbol, {})
                    sym["last_bracket"] = {
                        "tp_pct": float(tp_pct_use),
                        "sl_pct": float(sl_pct_use),
                        "tp_price": float(tp_price) if tp_price is not None else None,
                        "sl_price": float(sl_price) if sl_price is not None else None,
                        "entry_price": float(price) if price is not None else None,
                        "side": side,
                        "qty": int(qty),
                        "ts": time.time(),
                    }
                    self._save_state()
                except Exception:
                    pass

            # Dual-horizon split (core + tactical) for long entries
            if self.dual_horizon and action == "long" and side == "buy":
                core_budget = per_symbol_budget * self.core_allocation_pct
                tact_budget = per_symbol_budget - core_budget
                core_qty = max(int(core_budget // max(price, 0.01)), 0)
                tact_qty = max(int(tact_budget // max(price, 0.01)), 0)
                if core_qty + tact_qty <= 0:
                    tact_qty = max(1, int(per_symbol_budget // max(price, 0.01)))
                    core_qty = 0
                # Core leg (larger bracket)
                if self.core_use_bracket and core_qty > 0:
                    core_tp = round(price * (1 + max(0.0, self.core_tp_pct)), 2)
                    core_sl = round(price * (1 - max(0.0, self.core_sl_pct)), 2)
                    core_oid = f"nt-{symbol}-{int(time.time())}-buy-core-{core_qty}"
                    resp1 = alpaca_place_order(
                        self.alpaca_url, self.alpaca_key, self.alpaca_secret, symbol,
                        core_qty, "buy", type_="market", tif="day", client_order_id=core_oid,
                        order_class="bracket", take_profit={"limit_price": core_tp}, stop_loss={"stop_price": core_sl}
                    )
                    if resp1.get("status") == "error":
                        msg = f"{resp1.get('code')} {resp1.get('message')}"
                        print(f"  Core order error: {msg}")
                        log_event("buy", core_qty, None, "error", msg)
                    else:
                        oid1 = resp1.get("id") or resp1.get("client_order_id")
                        print(f"  Placed core buy: qty={core_qty}, id={oid1}")
                        log_event("buy", core_qty, str(oid1), "placed", None)
                        try:
                            sym = self.state.setdefault(symbol, {})
                            sym["last_bracket_core"] = {
                                "tp_pct": float(self.core_tp_pct),
                                "sl_pct": float(self.core_sl_pct),
                                "tp_price": float(core_tp),
                                "sl_price": float(core_sl),
                                "entry_price": float(price),
                                "side": "buy",
                                "qty": int(core_qty),
                                "ts": time.time(),
                            }
                            self._save_state()
                        except Exception:
                            pass
                # Tactical leg
                if tact_qty > 0:
                    tact_oid = f"nt-{symbol}-{int(time.time())}-buy-tact-{tact_qty}"
                    if getattr(self, 'use_bracket', False):
                        resp2 = alpaca_place_order(
                            self.alpaca_url, self.alpaca_key, self.alpaca_secret, symbol,
                            tact_qty, "buy", type_="market", tif="day", client_order_id=tact_oid,
                            order_class="bracket", take_profit=tp, stop_loss=sl
                        )
                    else:
                        resp2 = alpaca_place_order(
                            self.alpaca_url, self.alpaca_key, self.alpaca_secret, symbol,
                            tact_qty, "buy", type_="market", tif="day", client_order_id=tact_oid
                        )
                    if resp2.get("status") == "error":
                        msg = f"{resp2.get('code')} {resp2.get('message')}"
                        print(f"  Tactical order error: {msg}")
                        log_event("buy", tact_qty, None, "error", msg)
                    else:
                        oid2 = resp2.get("id") or resp2.get("client_order_id")
                        print(f"  Placed tactical buy: qty={tact_qty}, id={oid2}")
                        log_event("buy", tact_qty, str(oid2), "placed", None)
                        try:
                            sym = self.state.setdefault(symbol, {})
                            sym["last_bracket_tactical"] = {
                                "tp_pct": float(tp_pct_use if 'tp_pct_use' in locals() else self.tp_pct),
                                "sl_pct": float(sl_pct_use if 'sl_pct_use' in locals() else self.sl_pct),
                                "tp_price": float(tp.get('limit_price')) if isinstance(tp, dict) else None,
                                "sl_price": float(sl.get('stop_price')) if isinstance(sl, dict) else None,
                                "entry_price": float(price),
                                "side": "buy",
                                "qty": int(tact_qty),
                                "ts": time.time(),
                            }
                            self._save_state()
                        except Exception:
                            pass
                self._set_state(symbol, "last_trade", time.time())
                # PDT/Tax bookkeeping for entries
                self._record_entry(symbol, price)
            else:
                client_oid = f"nt-{symbol}-{int(time.time())}-{side}-{qty}"
                use_notional = bool(want_notional and not getattr(self, 'use_bracket', False) and side == "buy")
                resp = alpaca_place_order(
                    self.alpaca_url,
                    self.alpaca_key,
                    self.alpaca_secret,
                    symbol,
                    (None if use_notional else qty),
                    side,
                    type_="market",
                    tif="day",
                    client_order_id=client_oid,
                    order_class=order_class,
                    take_profit=tp,
                    stop_loss=sl,
                    notional=(float(notional_amount) if use_notional else None),
                )
                if resp.get("status") == "error":
                    msg = f"{resp.get('code')} {resp.get('message')}"
                    print(f"  Order error: {msg}")
                    log_event(side, qty, None, "error", msg)
                else:
                    oid = resp.get("id") or resp.get("client_order_id")
                    print(f"  Placed {side} order: qty={qty}, id={oid}")
                    log_event(side, qty, str(oid), "placed", None)
                    self._set_state(symbol, "last_trade", time.time())
                    if side == "buy" and action == "long":
                        self._record_entry(symbol, price)
        elif action == "close":
            # Positions-aware close
            pos = alpaca_position_for_symbol(self.alpaca_url, self.alpaca_key, self.alpaca_secret, symbol)
            if not pos:
                print("  No open position to close")
                return
            qty_pos = abs(int(float(pos.get("qty", 0))))
            if qty_pos <= 0:
                print("  No open position to close")
                return
            # PDT close check: auto-enforce only if equity < threshold
            acct = self._account_info()
            try:
                equity = float(acct.get("equity", 0.0)) if isinstance(acct, dict) else 0.0
            except Exception:
                equity = 0.0
            if equity and equity < self.pdt_min_equity:
                if self._would_be_daytrade(symbol):
                    dt_count = self._daytrade_count_5d()
                    if dt_count >= self.max_daytrades_5d and not self.pdt_allow_risk_exit:
                        print(f"  PDT (auto): blocking close to avoid new day trade; {dt_count}/{self.max_daytrades_5d} in 5d and equity ${equity:.2f} < ${self.pdt_min_equity:.2f}")
                        return
            if self.market_only_trade and not self._is_market_open():
                print("  Market closed: skipping close per config (market-only-trade)")
                return
            resp = alpaca_close_position(self.alpaca_url, self.alpaca_key, self.alpaca_secret, symbol, str(qty_pos))
            if resp.get("status") == "error":
                msg = f"{resp.get('code', '')} {resp.get('message', '')}".strip()
                print(f"  Close error: {msg}")
                log_event("none", qty_pos, None, "error", msg)
            else:
                oid = resp.get("id") or resp.get("order_id") or "close"
                print(f"  Close placed for {qty_pos} sh, id={oid}")
                log_event("none", qty_pos, str(oid), "placed", None)
                # Clear stored bracket expectation since position is closing
                try:
                    sym = self.state.setdefault(symbol, {})
                    if "last_bracket" in sym:
                        del sym["last_bracket"]
                    if "last_bracket_core" in sym:
                        del sym["last_bracket_core"]
                    if "last_bracket_tactical" in sym:
                        del sym["last_bracket_tactical"]
                    self._save_state()
                except Exception:
                    pass
                # PDT/Tax bookkeeping for exits
                self._record_exit(symbol, price)

    # --- state helpers ---
    def _load_state(self) -> Dict[str, Dict[str, float]]:
        try:
            with self._state_io_lock:
                if self.state_path.exists():
                    with self.state_path.open("r", encoding="utf-8") as f:
                        data = json.load(f)
                    if isinstance(data, dict):
                        return data
        except Exception:
            pass
        return {}

    def _save_state(self) -> None:
        try:
            with self._state_io_lock:
                # Trim per-symbol caches to avoid unbounded growth
                max_seen = getattr(self, "max_seen_news_per_symbol", 500)
                for sym, sdict in list(self.state.items()):
                    if not isinstance(sdict, dict):
                        continue
                    if "seen_news" in sdict and isinstance(sdict["seen_news"], list):
                        if len(sdict["seen_news"]) > max_seen:
                            sdict["seen_news"] = sdict["seen_news"][-max_seen:]
                    if "seen_events" in sdict and isinstance(sdict.get("seen_events"), list):
                        if len(sdict["seen_events"]) > max_seen:
                            sdict["seen_events"] = sdict["seen_events"][-max_seen:]
                    if "price_series" in sdict and isinstance(sdict.get("price_series"), list):
                        pts = getattr(self, "price_history_points", 10)
                        if len(sdict["price_series"]) > pts:
                            sdict["price_series"] = sdict["price_series"][-pts:]
                with self.state_path.open("w", encoding="utf-8") as f:
                    json.dump(self.state, f)
        except Exception:
            pass

    def _set_state(self, symbol: str, key: str, value: float) -> None:
        if symbol not in self.state:
            self.state[symbol] = {}
        self.state[symbol][key] = float(value)

    # ---- caching helpers ----
    def _news_id(self, item: Dict) -> str:
        base = str(item.get("id") or "")
        if base:
            return base
        raw = f"{item.get('source','')}|{item.get('datetime','')}|{item.get('headline','')}".encode("utf-8", errors="ignore")
        return hashlib.sha1(raw).hexdigest()

    def _get_seen_news(self, symbol: str) -> set:
        arr = self.state.get(symbol, {}).get("seen_news", [])
        return set(arr) if isinstance(arr, list) else set()

    def _mark_seen_news(self, symbol: str, news_id: str) -> None:
        sym = self.state.setdefault(symbol, {})
        arr = sym.setdefault("seen_news", [])
        if news_id not in arr:
            arr.append(news_id)
        # Trim size
        if len(arr) > self.max_seen_news_per_symbol:
            del arr[: len(arr) - self.max_seen_news_per_symbol]

    def _get_seen_events(self, symbol: str) -> set:
        arr = self.state.get(symbol, {}).get("seen_events", [])
        return set(arr) if isinstance(arr, list) else set()

    def _mark_seen_event(self, symbol: str, ekey: str) -> None:
        sym = self.state.setdefault(symbol, {})
        arr = sym.setdefault("seen_events", [])
        if ekey not in arr:
            arr.append(ekey)
        if len(arr) > self.max_seen_news_per_symbol:
            del arr[: len(arr) - self.max_seen_news_per_symbol]

    def _get_gpt_cache(self, symbol: str) -> Dict[str, Dict]:
        sym = self.state.setdefault(symbol, {})
        cache = sym.setdefault("gpt_cache", {})
        return cache if isinstance(cache, dict) else {}

    def _cache_gpt(self, symbol: str, news_id: str, data: Dict) -> None:
        cache = self._get_gpt_cache(symbol)
        cache[news_id] = data

    def _get_gpt_cache_global(self) -> Dict[str, Dict]:
        g = self.state.setdefault("__global__", {})
        cache = g.setdefault("gpt_cache", {})
        return cache if isinstance(cache, dict) else {}

    def _cache_gpt_global(self, key: str, data: Dict) -> None:
        cache = self._get_gpt_cache_global()
        cache[key] = data

    # ---- price history ----
    def _update_and_summarize_price_history(self, symbol: str, price: Optional[float]) -> Optional[str]:
        if price is None:
            return None
        sym = self.state.setdefault(symbol, {})
        series: List[Dict[str, float]] = sym.setdefault("price_series", [])
        now = time.time()
        series.append({"ts": now, "price": float(price)})
        cutoff = now - self.price_hist_window_min * 60.0
        series[:] = [p for p in series if p.get("ts", 0) >= cutoff]
        # Downsample
        if len(series) > self.price_hist_points:
            step = max(1, len(series) // self.price_hist_points)
            series[:] = series[::step][: self.price_hist_points]
        sym["price_series"] = series
        if len(series) < 2:
            return None
        first = series[0]["price"]
        last = series[-1]["price"]
        window_chg = ((last - first) / first) * 100.0 if first else 0.0
        # simple volatility: stddev of successive returns
        rets: List[float] = []
        for i in range(1, len(series)):
            p0 = series[i - 1]["price"]
            p1 = series[i]["price"]
            if p0:
                rets.append((p1 - p0) / p0)
        if not rets:
            vol_pct = 0.0
        else:
            mean = sum(rets) / len(rets)
            var = sum((r - mean) ** 2 for r in rets) / len(rets)
            vol_pct = (var ** 0.5) * 100.0
        slope = (last - first) / max(1, len(series) - 1)
        trend = "up" if slope > 0 else ("down" if slope < 0 else "flat")
        prices = [round(p["price"], 2) for p in series]
        return f"window={self.price_hist_window_min}m, points={len(series)}, change={window_chg:+.2f}%, vol~{vol_pct:.2f}%, trend={trend}, series={prices}"

    def _price_stats_for_window(self, symbol: str, window_min: float) -> Optional[Dict[str, float]]:
        """Compute simple stats over the last window_min minutes from stored price series.

        Returns dict with keys: change_pct, downs, points. None if insufficient data.
        """
        try:
            sym = self.state.get(symbol, {})
            series = sym.get("price_series")
            if not isinstance(series, list) or len(series) < 2:
                return None
            now = time.time()
            cutoff = now - max(1.0, float(window_min)) * 60.0
            seg = [p for p in series if isinstance(p, dict) and (p.get("ts", 0) >= cutoff)]
            if len(seg) < 2:
                return None
            first = float(seg[0].get("price", 0) or 0)
            last = float(seg[-1].get("price", 0) or 0)
            change_pct = ((last - first) / first) * 100.0 if first else 0.0
            downs = 0
            cur = 0
            for i in range(1, len(seg)):
                try:
                    if float(seg[i].get("price", 0) or 0) < float(seg[i-1].get("price", 0) or 0):
                        cur += 1
                    else:
                        cur = 0
                    downs = max(downs, cur)
                except Exception:
                    pass
            return {"change_pct": change_pct, "downs": float(downs), "points": float(len(seg))}
        except Exception:
            return None

    # ---- GDELT refresh ----
    def _maybe_refresh_gdelt(self, force: bool = False) -> None:
        now = time.time()
        if not self.use_gdelt:
            return
        if not force and (now - self._gdelt_last_ts) < max(1.0, self.gdelt_poll_seconds):
            return
        try:
            evts = fetch_gdelt_docs(self.gdelt_themes, self.gdelt_max_records, self.gdelt_timespan_min)
            self._gdelt_cache = evts or []
            self._gdelt_last_ts = now
        except Exception:
            self._gdelt_last_ts = now

    # ---- market hours ----
    def _is_market_open(self) -> bool:
        # Prefer Alpaca clock when trade creds available
        if self.alpaca_url and self.alpaca_key and self.alpaca_secret:
            clk = alpaca_clock(self.alpaca_url, self.alpaca_key, self.alpaca_secret)
            if isinstance(clk, dict) and isinstance(clk.get("is_open"), bool):
                return bool(clk.get("is_open"))
        # Fallback: local clock in specified timezone, Mon-Fri 09:30-16:00
        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo(self.market_tz)
        except Exception:
            tz = None
        now_local = datetime.now(tz) if tz else datetime.now()
        # Monday=0..Sunday=6
        if now_local.weekday() >= 5:
            return False
        t = now_local.time()
        start = (9, 30)
        end = (16, 0)
        return (t.hour, t.minute) >= start and (t.hour, t.minute) <= end

    # ---- Finnhub global rate limiting ----
    def _fh_rate_gate(self) -> None:
        with self._fh_lock:
            now = time.time()
            # Enforce min interval + small jitter to avoid burst alignment
            if self.fh_min_interval > 0 and self._fh_last_call_ts > 0:
                dt = now - self._fh_last_call_ts
                if dt < self.fh_min_interval:
                    sleep_for = self.fh_min_interval - dt
                    try:
                        import random as _r
                        sleep_for += _r.uniform(0.05, 0.25)
                    except Exception:
                        pass
                    time.sleep(max(0.0, sleep_for))
            # Enforce RPM cap with sliding window
            if self.fh_max_rpm and self.fh_max_rpm > 0:
                window = 60.0
                # Drop old
                self._fh_call_times = [t for t in self._fh_call_times if now - t < window]
                if len(self._fh_call_times) >= self.fh_max_rpm:
                    earliest = min(self._fh_call_times) if self._fh_call_times else now
                    sleep_for = window - (now - earliest) + 0.05
                    if sleep_for > 0:
                        time.sleep(sleep_for)
            # Also respect backoff if active
            if hasattr(self, 'fh_backoff_enabled') and self.fh_backoff_enabled and self.fh_backoff > 0:
                time.sleep(min(self.fh_backoff, getattr(self, 'fh_backoff_max', self.fh_backoff)))

    def _fh_mark_call(self) -> None:
        with self._fh_lock:
            now = time.time()
            self._fh_last_call_ts = now
            self._fh_call_times.append(now)

    def _positions_map(self) -> Dict[str, Dict]:
        try:
            pos = alpaca_positions(self.alpaca_url, self.alpaca_key, self.alpaca_secret)
            out: Dict[str, Dict] = {}
            for p in pos:
                sym = str(p.get("symbol", "")).upper()
                out[sym] = p
            return out
        except Exception:
            return {}

    def _account_info(self) -> Dict:
        """Cached account info with equity and PDT flags when available."""
        now = time.time()
        ts = self._acct_cache.get("ts", 0.0)
        if now - ts < 30.0 and "acct" in self._acct_cache:
            return self._acct_cache.get("acct", {})  # type: ignore[return-value]
        acct = alpaca_account(self.alpaca_url, self.alpaca_key, self.alpaca_secret) if (self.alpaca_url and self.alpaca_key and self.alpaca_secret) else None
        if isinstance(acct, dict):
            self._acct_cache["acct"] = acct
            self._acct_cache["ts"] = now
            try:
                self._acct_cache["bp"] = float(acct.get("buying_power", 0.0))
            except Exception:
                pass
            return acct
        return {}

    # ---- holdings watcher thread ----
    def _start_holdings_watcher(self) -> None:
        if self._holdings_thread is not None:
            return
        def _loop():
            while not self._stop_holdings.is_set():
                try:
                    pos_map = self._positions_map()
                    symbols = list(pos_map.keys())
                    if symbols:
                        # prioritize updating prices for holdings
                        try:
                            self._maybe_fetch_batch_prices(symbols)
                        except Exception:
                            pass
                        for s in symbols:
                            self._process_symbol(s, pos_map)
                    self._save_state()
                except Exception:
                    pass
                self._stop_holdings.wait(max(1.0, self.holdings_poll_seconds))
        t = threading.Thread(target=_loop, name="holdings-watcher", daemon=True)
        t.start()
        self._holdings_thread = t

    def _account_buying_power(self) -> float:
        now = time.time()
        ts = self._acct_cache.get("ts", 0.0)
        if now - ts < 30.0 and "bp" in self._acct_cache:
            return float(self._acct_cache.get("bp", 0.0))
        acct = alpaca_account(self.alpaca_url, self.alpaca_key, self.alpaca_secret) if (self.alpaca_url and self.alpaca_key and self.alpaca_secret) else None
        bp = 0.0
        try:
            if isinstance(acct, dict):
                bp = float(acct.get("buying_power", 0.0))
        except Exception:
            bp = 0.0
        self._acct_cache["ts"] = now
        self._acct_cache["bp"] = bp
        return bp

    def _compute_per_symbol_budget(self, price: float) -> float:
        bp = self._account_buying_power()
        if bp <= 0:
            try:
                return float(getattr(self.args, "dollars_per_trade", 1000.0))
            except Exception:
                return 1000.0
        base = (self.allocation_pct * bp) / max(1, self.max_open_positions)
        # Determine per-symbol cap in USD: prefer percent-of-equity when configured
        limit_usd = self.max_usd_per_symbol
        if self.max_pct_per_symbol > 0.0:
            acct = self._account_info()
            try:
                eq = float(acct.get("equity", 0.0)) if isinstance(acct, dict) else 0.0
            except Exception:
                eq = 0.0
            if eq > 0:
                limit_usd = self.max_pct_per_symbol * eq
        return min(base, limit_usd)

    # ---- local news cache helpers ----
    def _load_news_cache(self) -> Dict[str, Dict]:
        try:
            if self.news_cache_path.exists():
                data = json.loads(self.news_cache_path.read_text(encoding='utf-8'))
                if isinstance(data, dict):
                    return data
        except Exception:
            pass
        self.news_cache_path.parent.mkdir(parents=True, exist_ok=True)
        return {}

    def _save_news_cache(self) -> None:
        try:
            self.news_cache_path.write_text(json.dumps(self._news_cache), encoding='utf-8')
        except Exception:
            pass

    def _trim_news_items(self, items: List[Dict]) -> List[Dict]:
        out = []
        for it in items[: self.news_cache_max_items]:
            out.append({
                "id": it.get("id"),
                "datetime": it.get("datetime"),
                "source": it.get("source"),
                "headline": it.get("headline"),
                "summary": it.get("summary"),
                "url": it.get("url"),
                "is_macro": bool(it.get("is_macro", False)),
            })
        return out

    def _get_cached_news(self, symbol: str) -> Optional[List[Dict]]:
        try:
            entry = self._news_cache.get(symbol)
            if not entry:
                return None
            ts = float(entry.get("ts", 0))
            if self.news_cache_ttl > 0 and (time.time() - ts) <= self.news_cache_ttl:
                items = entry.get("items") or []
                if isinstance(items, list):
                    return items
        except Exception:
            return None
        return None

    def _set_cached_news(self, symbol: str, items: List[Dict]) -> None:
        try:
            self._news_cache[symbol] = {"ts": time.time(), "items": self._trim_news_items(items or [])}
            self._save_news_cache()
        except Exception:
            pass

    # ---- Batch price fetch via Alpaca Data ----
    def _maybe_fetch_batch_prices(self, symbols: List[str]) -> None:
        if not self.use_alpaca_data:
            return
        if not symbols:
            return
        # Only attempt if we have Alpaca keys
        if not (self.alpaca_key and self.alpaca_secret):
            return
        # Respect market-only-price; if closed, skip to avoid stale snapshots
        if self.market_only_price and not self._is_market_open() and not self.snapshot_offhours_prices:
            return
        try:
            snaps = alpaca_snapshots(symbols, self.alpaca_key, self.alpaca_secret, self.alpaca_data_base_url)
            now = time.time()
            for sym, px in snaps.items():
                if px is not None:
                    self._set_state(sym, "last_price", float(px))
                    self._set_state(sym, "last_price_ts", now)
        except Exception:
            pass

    # ---- news dedupe ----
    def _normalize_text(self, text: str, symbol: str) -> List[str]:
        if not isinstance(text, str):
            return []
        t = text.lower()
        t = re.sub(r"[^a-z0-9\s]+", " ", t)
        t = t.replace(symbol.lower(), " ")
        tokens = [w for w in t.split() if len(w) > 2 and w not in {"the","and","for","with","from","into","over","this","that","are","was","were","will","has","have","had","a","an","of","to","in","on","by","as","at","is","it"}]
        return tokens

    def _event_key(self, item: Dict, symbol: str) -> str:
        parts = []
        for key in ("headline", "title", "summary"):
            val = item.get(key)
            if isinstance(val, str) and val:
                parts.append(val)
        text = ". ".join(parts)
        toks = self._normalize_text(text, symbol)
        if not toks:
            # Fallback to hashed headline
            h = (item.get("headline") or "").encode("utf-8", errors="ignore")
            return hashlib.sha1(h).hexdigest()
        try:
            dt = int(item.get("datetime") or 0)
            day_bucket = datetime.fromtimestamp(dt).strftime("%Y-%m-%d") if dt else ""
        except Exception:
            day_bucket = ""
        sig = " ".join(sorted(set(toks))) + (f"|{day_bucket}" if day_bucket else "")
        return hashlib.sha1(sig.encode("utf-8", errors="ignore")).hexdigest()

    def _dedupe_news(self, symbol: str, items: List[Dict]) -> List[Dict]:
        unique: List[Dict] = []
        seen_keys: set = set()
        for it in items:
            ekey = self._event_key(it, symbol)
            if ekey in seen_keys:
                continue
            seen_keys.add(ekey)
            unique.append(it)
        return unique

    # ---- helpers: price context parsing and factor score ----
    def _market_day(self, ts: float) -> str:
        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo(self.market_tz)
        except Exception:
            tz = None
        dt = datetime.fromtimestamp(ts, tz) if tz else datetime.fromtimestamp(ts)
        return dt.strftime("%Y-%m-%d")

    def _record_entry(self, symbol: str, price: Optional[float]) -> None:
        try:
            sym = self.state.setdefault(symbol, {})
            now = time.time()
            sym["last_entry_ts"] = now
            sym["last_entry_day"] = self._market_day(now)
            if price is not None:
                sym["last_entry_price"] = float(price)
            self._save_state()
        except Exception:
            pass

    def _record_exit(self, symbol: str, price: Optional[float]) -> None:
        try:
            sym = self.state.setdefault(symbol, {})
            now = time.time()
            sym["last_exit_ts"] = now
            sym["last_exit_day"] = self._market_day(now)
            if price is not None:
                sym["last_exit_price"] = float(price)
            # PDT rolling window record
            entry_day = str(sym.get("last_entry_day", ""))
            exit_day = str(sym.get("last_exit_day", ""))
            if entry_day and exit_day and entry_day == exit_day:
                pdt = self.state.setdefault("__pdt__", {})
                days = pdt.setdefault("days", [])
                if exit_day not in days:
                    days.append(exit_day)
                # Trim to last ~7 days to approximate 5 trading days
                if len(days) > 7:
                    days[:] = days[-7:]
            self._save_state()
        except Exception:
            pass

    def _daytrade_count_5d(self) -> int:
        try:
            pdt = self.state.get("__pdt__", {})
            days = pdt.get("days", [])
            if not isinstance(days, list):
                return 0
            # Count distinct days in last 5 business days (approx: last 7 calendar days)
            try:
                from datetime import timedelta
                cutoff = datetime.now().date() - timedelta(days=7)
                days_recent = [d for d in days if d >= cutoff.strftime('%Y-%m-%d')]
                return len(set(days_recent))
            except Exception:
                return len(set(days))
        except Exception:
            return 0

    def _would_be_daytrade(self, symbol: str) -> bool:
        try:
            sym = self.state.get(symbol, {})
            eday = str(sym.get("last_entry_day", ""))
            if not eday:
                return False
            return eday == self._market_day(time.time())
        except Exception:
            return False

    def _parse_price_ctx(self, s: Optional[str]) -> Optional[Dict]:
        if not s or not isinstance(s, str):
            return None
        out: Dict[str, object] = {}
        try:
            # change=+1.23%
            if "change=" in s and "%" in s:
                frag = s.split("change=")[1].split("%", 1)[0]
                out["change_pct"] = float(frag)
        except Exception:
            pass
        try:
            # vol~0.45%
            if "vol~" in s and "%" in s:
                frag = s.split("vol~")[1].split("%", 1)[0]
                out["vol_pct"] = float(frag)
        except Exception:
            pass
        try:
            if "trend=" in s:
                frag = s.split("trend=")[1].split(",", 1)[0].strip()
                out["trend"] = frag
        except Exception:
            pass
        # Extract series
        try:
            if "series=[" in s:
                seg = s.split("series=[", 1)[1].split("]", 1)[0]
                nums = []
                for tok in seg.split(","):
                    tok = tok.strip()
                    if tok:
                        try:
                            nums.append(float(tok))
                        except Exception:
                            pass
                if nums:
                    out["series"] = nums
                    out["last_px"] = nums[-1]
                    out["min_px"] = min(nums)
                    out["max_px"] = max(nums)
        except Exception:
            pass
        return out

    def _compute_factor_score(self, ctx: Dict) -> float:
        # Map change %, vol %, and trend to a [-1,1] adjustment
        def _clamp(x: float, lo: float, hi: float) -> float:
            return hi if x > hi else (lo if x < lo else x)
        ch = 0.0
        vol = 0.0
        trend = str(ctx.get("trend", "") or "")
        try:
            ch = float(ctx.get("change_pct", 0.0) or 0.0)
        except Exception:
            ch = 0.0
        try:
            vol = float(ctx.get("vol_pct", 0.0) or 0.0)
        except Exception:
            vol = 0.0
        # Change contribution
        sc_ch = 0.0
        if self.factor_window_chg_scale > 0:
            sc_ch = _clamp(ch / self.factor_window_chg_scale, -1.0, 1.0)
        # Volatility penalty (higher vol reduces score)
        sc_vol = 0.0
        if self.factor_vol_penalty_scale > 0:
            sc_vol = -_clamp(vol / self.factor_vol_penalty_scale, 0.0, 1.0) * 0.3  # cap penalty at -0.3
        # Trend bonus
        sc_tr = 0.0
        if trend == "up":
            sc_tr = abs(self.factor_trend_bonus)
        elif trend == "down":
            sc_tr = -abs(self.factor_trend_bonus)
        # Range position bias within window
        sc_rng = 0.0
        try:
            series = ctx.get("series")
            if isinstance(series, list) and len(series) >= 2:
                mn = float(ctx.get("min_px", min(series)))
                mx = float(ctx.get("max_px", max(series)))
                last = float(ctx.get("last_px", series[-1]))
                if mx > mn:
                    pos = (last - mn) / (mx - mn)  # 0..1
                    sc_rng = (pos - 0.5) * 2.0  # -1..1 centered
                    sc_rng *= max(0.0, self.factor_range_bias_weight)
        except Exception:
            sc_rng = 0.0
        # Combine
        score = sc_ch + sc_vol + sc_tr + sc_rng
        return _clamp(score, -1.0, 1.0)


def _as_date(s, default):
    from dateutil.parser import isoparse
    from datetime import datetime
    if not s:
        return default
    try:
        if isinstance(s, str) and len(s) == 10:
            return datetime.strptime(s, "%Y-%m-%d")
        return isoparse(s).replace(tzinfo=None)
    except Exception as e:
        raise ValueError(f"Invalid date '{s}': {e}")

def _now_ts() -> float:
    return time.time()

def _empty_symbol_state() -> Dict[str, float]:
    return {"last_news": 0.0, "last_trade": 0.0}

