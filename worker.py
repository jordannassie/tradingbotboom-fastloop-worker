# =============================================================================
# WORKER ARCHITECTURE OVERVIEW
# =============================================================================
#
# This is the BTC 5-min Polymarket trading worker (FastLoop).
# Entry point: main() → asyncio.gather() of five long-running tasks.
#
# TASK MAP
# ────────
#   heartbeat_loop      Core trading tick (~5s). Reads settings, computes edge,
#                       fires strategies. COPY-TRADE HOOK: replace strategy body.
#   rotate_loop         BTC-SPECIFIC. Builds timestamp slugs, resolves up/down
#                       outcomes → YES/NO tokens, restarts WS.
#   market_listener     REUSABLE. Polymarket CLOB WebSocket; populates best_quotes.
#   paper_settlement_loop  REUSABLE. Settles expired paper positions.
#   live_balance_loop   REUSABLE. Syncs live USDC balance to Supabase.
#   scan_loop           REUSABLE. Periodic SCAN heartbeat row.
#
# CONFIG
# ──────
#   All constants and env vars → worker_config.py
#   BTC-SPECIFIC sections are clearly marked there.
#   COPY-TRADE HOOK comments in worker_config.py show where copy-trading plugs in.
#
# SAFE-TO-REUSE CORE (do not change carelessly)
# ──────────────────────────────────────────────
#   _run_forever, build_trading_client, submit_order, close_live_position_ladder
#   record_heartbeat, record_trade, read_strategy_settings, read_live_master_enabled
#   market_listener, PAPER / ARM LIVE / LIVE ON / KILL_SWITCH gate logic
#
# BTC-SPECIFIC (replace for copy-trading)
# ────────────────────────────────────────
#   rotate_loop, interval_from_prefix, slug_start_timestamp, asset_key_from_slug
#   slug_from_start, _fetch_btc_spot_price_sync, evaluate_candle_strategies
#   detect_* candle pattern functions, CANDLE_DETECTORS, heartbeat_loop strategy body
#   up/down outcome mapping in rotate_loop
#
# =============================================================================

def log_paper_decision(
    strategy: str,
    slug: str | None,
    time_to_end: float | None,
    edge: float | None,
    threshold: float,
    enabled: bool,
    reason: str,
) -> None:
    logging.info(
        "PAPER_DECISION strategy=%s slug=%s time_to_end=%s edge=%s threshold=%s enabled=%s blocked=%s",
        strategy,
        slug or "none",
        time_to_end,
        edge,
        threshold,
        enabled,
        reason,
    )

import asyncio
import base64
import json
import logging
import os
from collections import deque, defaultdict
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from math import floor
from time import time
from urllib import parse, request
from urllib.error import HTTPError

from cryptography.hazmat.primitives.asymmetric import ed25519

import websockets
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    BalanceAllowanceParams,
    OrderArgs,
    OrderType,
    AssetType,
)
from supabase import create_client

# ── Configuration ─────────────────────────────────────────────────────────────
# All constants and environment variables are defined in worker_config.py.
# BTC-SPECIFIC sections are clearly marked there.
# COPY-TRADE HOOK comments in worker_config.py show where copy-trading plugs in.
from worker_config import *  # noqa: F401, F403

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.info("WORKER_BOOT build=OPUS_FIX_V1")
logging.info(
    "TP_SL_CONFIG fast_tp=%s fast_sl=%s fast_max_hold=%s snipe_tp=%s snipe_sl=%s snipe_max_hold=%s entry_cutoff=%s force_exit=%s low_funds_skip=%s",
    FAST_TP_CENTS,
    FAST_SL_CENTS,
    FAST_MAX_HOLD_SECONDS,
    SNIPE_TP_CENTS,
    SNIPE_SL_CENTS,
    SNIPE_MAX_HOLD_SECONDS,
    ENTRY_CUTOFF_SECONDS,
    FORCE_EXIT_SECONDS,
    LOW_FUNDS_SKIP_USD,
)
logging.info(
    "BANKROLL_GUARD_CONFIG live_min=%s paper_min=%s",
    LIVE_MIN_AVAILABLE_USD,
    PAPER_MIN_AVAILABLE_USD,
)
logging.info(
    "PM_ENV_CHECK access_key_present=%s privkey_present=%s",
    bool(PM_ACCESS_KEY),
    bool(PM_ED25519_PRIVATE_KEY_B64),
)
logging.info("WORKER_BOOT_FINGERPRINT step7_meta_fix=ON ts=%s", int(time()))
logging.info(
    "CANDLE_BIAS_BOOT_CHECK code_present=True strategy_id=%s bot_id=%s",
    "CANDLE_BIAS",
    "paper_candle_bias",
)


# ── Global mutable state ───────────────────────────────────────────────────────
# These are runtime state variables, NOT config. They are mutated by the asyncio
# tasks (rotate_loop, heartbeat_loop, market_listener) during operation.
#
# WARNING: All globals below are shared across asyncio tasks. asyncio is
# single-threaded so there are no data races, but ordering of mutations matters.
# Do not add threading here without replacing these with asyncio primitives.
#
# COPY-TRADE HOOK: current_yes_token / current_no_token will be populated by
#                  the copy market config loader instead of rotate_loop.

current_slug = None
current_yes_token = YES_TOKEN_ID
current_no_token = NO_TOKEN_ID
rotating = False
ws_task = None
live_balance_task = None
scan_task = None
HAS_PAPER_START_BALANCE_COLUMN: bool | None = None

# =============================================================================
# STARTUP VALIDATION & SUPABASE CLIENT
# =============================================================================

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

if not YES_TOKEN_ID or not NO_TOKEN_ID:
    raise SystemExit("Missing YES_TOKEN_ID or NO_TOKEN_ID")

if not HAVE_PRIVATE_KEY:
    logging.warning("Missing PRIVATE_KEY; running in observe-only mode")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

best_quotes = {
    "yes": {"bid": None, "ask": None},
    "no": {"bid": None, "ask": None},
}

ASSET_TO_SIDE = {}
shared_paper_balance_cache: float | None = None
shared_paper_balance_ts: float = 0
shared_paper_balance_error_logged = False

def refresh_asset_map():
    ASSET_TO_SIDE.clear()
    if current_yes_token:
        ASSET_TO_SIDE[current_yes_token] = "yes"
    if current_no_token:
        ASSET_TO_SIDE[current_no_token] = "no"

def reset_best_quotes():
    best_quotes["yes"]["bid"] = None
    best_quotes["yes"]["ask"] = None
    best_quotes["no"]["bid"] = None
    best_quotes["no"]["ask"] = None


def log_rate_limited(
    key: str, interval: int, message: str, *args, value: object | None = None
) -> None:
    now_ts = int(time())
    last_ts, last_value = log_throttle_state.get(key, (0, None))
    changed = value is not None and value != last_value
    if changed or now_ts - last_ts >= interval:
        logging.info(message, *args)
        log_throttle_state[key] = (now_ts, value if value is not None else last_value)


async def _run_forever(name: str, coro_fn, *args) -> None:
    while True:
        try:
            await coro_fn(*args)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logging.exception("TASK_CRASH name=%s err=%s", name, exc)
            await asyncio.sleep(5)



def get_shared_paper_balance():
    if not PAPER_BANKROLL_SHARED_ENABLED:
        return None
    global shared_paper_balance_cache, shared_paper_balance_ts, shared_paper_balance_error_logged
    now_ts = time()
    if shared_paper_balance_cache is not None and (now_ts - shared_paper_balance_ts) < 10:
        return shared_paper_balance_cache
    try:
        resp = (
            supabase.table("bot_settings")
            .select("paper_balance_usd")
            .eq("bot_id", "default")
            .limit(1)
            .execute()
        )
        row = (resp.data or [None])[0]
        balance = float_or_none(row.get("paper_balance_usd")) if row else None
    except Exception as exc:
        if not shared_paper_balance_error_logged:
            logging.warning("PAPER_BANKROLL_SHARED_ERROR err=%s", exc)
            shared_paper_balance_error_logged = True
        return None
    if balance is None:
        balance = DEFAULT_PAPER_START_BALANCE
    shared_paper_balance_cache = balance
    shared_paper_balance_ts = now_ts
    return balance

refresh_asset_map()
strategy_trade_timestamps = {
    STRATEGY_SNIPER: deque(),
    STRATEGY_FASTLOOP: deque(),
    STRATEGY_CANDLE_BIAS: deque(),
    STRATEGY_SWEEP_RECLAIM: deque(),
    STRATEGY_BREAKOUT_CLOSE: deque(),
    STRATEGY_ENGULFING_LEVEL: deque(),
    STRATEGY_REJECTION_WICK: deque(),
    STRATEGY_FOLLOW_THROUGH: deque(),
}
strategy_missing_rows: set[str] = set()
live_master_warned = False
global_trade_mode_cache: str | None = None
last_proof_tick_ts = 0
consecutive_trade_errors = 0
last_trade_error = None
paused_due_to_errors = False
paused_due_to_max_trades = False
trade_triggers = 0  # counts 2-leg attempts (YES+NO)
last_paper_skip_ts = 0
live_balance_cache: float | None = None
live_allowance_cache: float | None = None
last_live_bankroll_log_ts = 0
last_live_order_400_body: str | None = None
last_live_bankroll_refresh_ts = 0
live_signer_address: str | None = None
live_funder_address: str | None = None
last_live_positions_snapshot_ts = 0
trades_auth_mode_logged = False
trades_sample_logged = False
live_order_tracker: dict[str, dict[str, object]] = {}
logging.info(
    "LIVE_TRACKER_INIT empty=%s tokens=%s",
    len(live_order_tracker) == 0,
    len(live_order_tracker),
)
last_tracker_snapshot_log_ts = 0
log_throttle_state: dict[str, tuple[int, object | None]] = {}
last_live_order_ts = 0
last_any_order_ts = 0
live_positions: dict[str, float] = {}
last_asset_key: str | None = None


# =============================================================================
# BTC-SPECIFIC: CANDLE ENGINE
# =============================================================================
# The CandleEngine builds OHLC history from mid-price ticks sampled on each
# heartbeat tick. CandleManager owns one CandleEngine per asset_key.
#
# BTC-SPECIFIC: The candle interval is derived from the BTC slug prefix
# (e.g. "btc-updown-5m" → 300s). Candle patterns are tuned for BTC 5-min.
#
# COPY-TRADE HOOK: The entire CandleEngine / CandleManager stack can be
#                  removed when the copy-trading engine replaces the BTC
#                  candle strategy engine. The heartbeat_loop's candle_manager
#                  calls are the removal points.
# =============================================================================

@dataclass
class Candle:
    start_ts: int
    open: float
    high: float
    low: float
    close: float

    def range(self) -> float:
        return max(self.high - self.low, 0.0)

    def body(self) -> float:
        return abs(self.close - self.open)

    def is_bullish(self) -> bool:
        return self.close >= self.open

    def is_bearish(self) -> bool:
        return self.close < self.open


@dataclass
class CandleSignal:
    signal: str
    metadata: dict[str, object] = field(default_factory=dict)


class CandleEngine:
    def __init__(self, history_size: int = MAX_CANDLE_HISTORY) -> None:
        self.history: deque[Candle] = deque(maxlen=history_size)
        self.current: Candle | None = None

    def observe(
        self,
        price: float,
        now_ts: int,
        interval_seconds: int,
        asset_key: str | None = None,
    ) -> None:
        if price is None or interval_seconds <= 0:
            return
        start_ts = floor(now_ts / interval_seconds) * interval_seconds
        logging.info(
            "CANDLE_OBSERVE asset_key=%s ts=%s bucket=%s price=%s",
            asset_key or "none",
            now_ts,
            start_ts,
            price,
        )
        if not self.current or self.current.start_ts != start_ts:
            if self.current:
                closed = self.current
                self.history.append(closed)
                logging.info(
                    "CANDLE_CLOSE asset_key=%s closed_candles=%s closed_ts=%s o=%s h=%s l=%s c=%s",
                    asset_key or "none",
                    len(self.history),
                    closed.start_ts,
                    closed.open,
                    closed.high,
                    closed.low,
                    closed.close,
                )
            self.current = Candle(start_ts, price, price, price, price)
            logging.info(
                "CANDLE_NEW_CURRENT asset_key=%s bucket=%s",
                asset_key or "none",
                start_ts,
            )
        else:
            assert self.current
            self.current.high = max(self.current.high, price)
            self.current.low = min(self.current.low, price)
            self.current.close = price

    def force_close(self, asset_key: str | None = None) -> None:
        if not self.current:
            return
        self.history.append(self.current)
        logging.info(
            "CANDLE_CLOSE asset_key=%s closed_candles=%s bucket=%s",
            asset_key or "none",
            len(self.history),
            self.current.start_ts,
        )
        self.current = None

    def closed_history(self) -> list[Candle]:
        return list(self.history)

    def has_history(self, minimum: int = CANDLE_HISTORY_MINIMUM) -> bool:
        return len(self.history) >= minimum

    def closed_count(self) -> int:
        return len(self.history)


def asset_key_from_slug(slug: str | None) -> str | None:
    if not slug:
        return None
    parts = slug.rsplit("-", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0]
    return slug


class CandleManager:
    def __init__(self) -> None:
        self.engines: dict[str, CandleEngine] = {}

    def get_engine(self, asset_key: str | None) -> CandleEngine | None:
        if not asset_key:
            return None
        if asset_key not in self.engines:
            self.engines[asset_key] = CandleEngine()
        return self.engines[asset_key]

    def observe(
        self,
        asset_key: str | None,
        slug: str | None,
        price: float | None,
        now_ts: int,
        interval_seconds: int,
    ) -> None:
        engine = self.get_engine(asset_key)
        if engine and price is not None:
            logging.info("CANDLE_KEY asset_key=%s slug=%s", asset_key, slug)
            engine.observe(price, now_ts, interval_seconds, asset_key)

    def closed_history(self, asset_key: str | None) -> list[Candle]:
        engine = self.get_engine(asset_key)
        return engine.closed_history() if engine else []

    def has_history(
        self, asset_key: str | None, minimum: int = CANDLE_HISTORY_MINIMUM
    ) -> bool:
        engine = self.get_engine(asset_key)
        return bool(engine and engine.has_history(minimum))

    def closed_count(self, asset_key: str | None) -> int:
        engine = self.get_engine(asset_key)
        return engine.closed_count() if engine else 0

    def log_status(self) -> None:
        for asset_key, engine in self.engines.items():
            logging.info(
                "CANDLE_HISTORY_STATUS asset_key=%s closed_candles=%s",
                asset_key,
                engine.closed_count(),
            )

    def force_close(self, asset_key: str | None) -> None:
        engine = self.get_engine(asset_key)
        if engine:
            engine.force_close(asset_key)


candle_manager = CandleManager()

logging.info("MARKET_SLUG_PREFIXES parsed: %s", MARKET_SLUG_PREFIXES)
# BTC-SPECIFIC: current_interval_seconds and current_prefix are mutated by rotate_loop.
# COPY-TRADE HOOK: These become irrelevant once rotate_loop is replaced.
current_interval_seconds = INTERVAL_SECONDS
current_prefix = MARKET_SLUG_PREFIXES[0] if MARKET_SLUG_PREFIXES else MARKET_SLUG_PREFIX

logging.info(
    "Worker start BOT_ID=%s EDGE=%s SIZE=%s SIG=%s FUNDER=%s (Supabase connected)",
    BOT_ID,
    EDGE_THRESHOLD,
    TRADE_SIZE,
    SIGNATURE_TYPE,
    (FUNDER[:6] + "...") if FUNDER else "None",
)
logging.info(
    "Rotation config AUTO_ROTATE_ENABLED=%s AUTO_ROTATE_RAW=%s MARKET_SLUG_PREFIXES=%s INTERVAL_SECONDS=%s ROTATE_POLL_SECONDS=%s ROTATE_LOOKAHEAD_SECONDS=%s",
    AUTO_ROTATE_ENABLED,
    AUTO_ROTATE_ENV,
    ",".join(MARKET_SLUG_PREFIXES),
    INTERVAL_SECONDS,
    ROTATE_POLL_SECONDS,
    ROTATE_LOOKAHEAD_SECONDS,
)
shared_mode = "ON" if PAPER_BANKROLL_SHARED_ENABLED else "OFF"
logging.info("PAPER_BANKROLL_SHARED mode=%s", shared_mode)


# =============================================================================
# UTILITY / HELPER FUNCTIONS (REUSABLE)
# =============================================================================

def float_or_none(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def should_skip_new_entries(
    mode: str, available_usd: float | None, min_usd: float, strategy_id: str
) -> bool:
    if available_usd is None:
        return False
    if available_usd < min_usd:
        logging.warning(
            "BANKROLL_GUARD_SKIP mode=%s strategy=%s available_usd=%s min_usd=%s",
            mode,
            strategy_id,
            available_usd,
            min_usd,
        )
        return True
    return False


def should_skip_low_funds(available_usd: float | None) -> bool:
    if available_usd is None:
        return False
    if available_usd < LOW_FUNDS_SKIP_USD:
        logging.warning(
            "LOW_FUNDS_SKIP available=%s required=%s",
            available_usd,
            LOW_FUNDS_SKIP_USD,
        )
        return True
    return False


def _record_live_position(token_id: str, delta_shares: float) -> None:
    if not token_id or delta_shares == 0:
        return
    existing = live_positions.get(token_id, 0.0)
    updated = existing + delta_shares
    if updated <= 0:
        live_positions.pop(token_id, None)
    else:
        live_positions[token_id] = updated


def _get_min_shares_from_client(client: ClobClient, token_id: str) -> float:
    if not client:
        return MIN_ORDER_SHARES
    for method_name in MIN_SIZE_METHODS:
        method = getattr(client, method_name, None)
        if not callable(method):
            continue
        try:
            result = method(token_id)
        except Exception:
            continue
        if result is None:
            continue
        if isinstance(result, (int, float, Decimal)):
            return max(MIN_ORDER_SHARES, float(result))
        if isinstance(result, str):
            try:
                return max(MIN_ORDER_SHARES, float(result))
            except ValueError:
                continue
        if isinstance(result, dict):
            for key in ("min_shares", "minShares", "min_size", "minSize"):
                candidate = result.get(key)
                if candidate is None:
                    continue
                try:
                    return max(MIN_ORDER_SHARES, float(candidate))
                except (ValueError, TypeError):
                    continue
    return MIN_ORDER_SHARES


def _apply_min_shares_guard(
    client: ClobClient,
    token_id: str,
    price: Decimal,
    shares: Decimal,
    budget_usd: float,
    side: str,
) -> Decimal | None:
    min_shares = _get_min_shares_from_client(client, token_id)
    if float(shares) >= min_shares:
        return shares
    adjusted_size_usd = float(price) * min_shares
    if adjusted_size_usd <= budget_usd * 1.1:
        adjusted_shares = Decimal(str(min_shares)).quantize(SHARE_QUANT, rounding=ROUND_UP)
        return adjusted_shares
    logging.info(
        "MIN_SIZE_SKIP mode=LIVE token_id=%s side=%s shares=%.6f min_shares=%.6f price=%.6f size_usd=%.6f",
        token_id,
        side,
        float(shares),
        min_shares,
        float(price),
        budget_usd,
    )
    return None


def _should_skip_min_shares(
    client: ClobClient,
    token_id: str,
    shares: float,
    price: float,
    size_usd: float,
    side: str,
) -> bool:
    min_shares = _get_min_shares_from_client(client, token_id)
    if shares < min_shares:
        logging.warning(
            "MIN_SIZE_SKIP mode=LIVE token_id=%s side=%s shares=%.6f min_shares=%.6f price=%.6f size_usd=%.6f",
            token_id,
            side,
            shares,
            min_shares,
            price,
            size_usd,
        )
        return True
    return False


def tracker_apply_fill(
    token_id: str | int | None,
    order_side: str,
    shares: float,
    price: float | None,
    now_ts: int,
    order_id: str,
    strategy: str | None = None,
) -> None:
    if not token_id:
        return
    normalized_token = str(token_id)
    side = (order_side or "").upper()
    if side not in ("BUY", "SELL"):
        return
    shares_clamped = max(0.0, shares)
    delta = shares_clamped if side == "BUY" else -shares_clamped
    entry = live_order_tracker.setdefault(
        normalized_token,
        {
            "shares": 0.0,
            "last_price": None,
            "last_update_ts": now_ts,
            "last_order_id": order_id,
            "last_side": side,
            "strategy": strategy,
        },
    )
    previous_shares = float(entry.get("shares") or 0.0)
    new_shares = max(0.0, previous_shares + delta)
    entry["shares"] = new_shares
    entry["last_price"] = price if price is not None else entry.get("last_price")
    entry["last_update_ts"] = now_ts
    entry["last_order_id"] = order_id
    entry["last_side"] = side
    entry["strategy"] = strategy
    logging.info(
        "LIVE_TRACKER_APPLY token_id=%s delta=%.6f new_shares=%.6f side=%s order_id=%s price=%s strategy=%s",
        normalized_token,
        delta,
        new_shares,
        side,
        order_id,
        f"{price:.6f}" if price is not None else "none",
        strategy or "none",
    )


def get_live_positions_from_tracker(min_shares: float = 0.01) -> dict[str, float]:
    global last_tracker_snapshot_log_ts
    snapshot: dict[str, float] = {}
    for token, data in live_order_tracker.items():
        shares_value = float(data.get("shares") or 0.0)
        if shares_value > min_shares:
            snapshot[token] = shares_value
    now_ts = int(time())
    if now_ts != last_tracker_snapshot_log_ts:
        tokens = list(snapshot.items())[:3]
        logging.info(
            "LIVE_TRACKER_SNAPSHOT tokens=%s example=%s",
            len(snapshot),
            [(token, round(shares, 4)) for token, shares in tokens],
        )
        last_tracker_snapshot_log_ts = now_ts
    return snapshot


def _safe_parse_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def tpsl_reason(strategy: str, entry_price: float, mark_price: float, held_seconds: int) -> str | None:
    if not entry_price or not mark_price:
        return None
    delta = mark_price - entry_price
    if strategy == STRATEGY_FASTLOOP:
        if delta >= FAST_TP_CENTS:
            return "TP"
        if -delta >= FAST_SL_CENTS:
            return "SL"
        if held_seconds >= FAST_MAX_HOLD_SECONDS:
            return "MAX_HOLD"
    if strategy == STRATEGY_SNIPER:
        if delta >= SNIPE_TP_CENTS:
            return "TP"
        if -delta >= SNIPE_SL_CENTS:
            return "SL"
        if held_seconds >= SNIPE_MAX_HOLD_SECONDS:
            return "MAX_HOLD"
    if strategy == STRATEGY_CANDLE_BIAS:
        if delta >= CANDLE_BIAS_TP_CENTS:
            return "TP"
        if -delta >= CANDLE_BIAS_SL_CENTS:
            return "SL"
        if held_seconds >= CANDLE_BIAS_MAX_HOLD_SECONDS:
            return "MAX_HOLD"
    return None


def live_tpsl_reason(
    strategy: str, entry_price: float, mark_price: float, held_seconds: int
) -> str | None:
    return tpsl_reason(strategy, entry_price, mark_price, held_seconds)


def _extract_token_id(payload: dict[str, object]) -> str | None:
    for key in (
        "token_id",
        "tokenId",
        "asset_id",
        "assetId",
        "asset",
        "clobTokenId",
        "clobTokenID",
    ):
        candidate = payload.get(key)
        if candidate:
            return str(candidate)
    return None


def _extract_identifier(payload: dict[str, object], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value:
            return str(value)
    return None


def _extract_order_id(resp: object) -> str:
    def _from_dict(source: dict[str, object] | None) -> str | None:
        if not source:
            return None
        for key in ("order_id", "orderId", "orderID", "id"):
            value = source.get(key)
            if value:
                return str(value)
        return None

    order_id = _from_dict(resp if isinstance(resp, dict) else None)
    if order_id:
        return order_id
    if isinstance(resp, dict):
        order = resp.get("order")
        order_id = _from_dict(order if isinstance(order, dict) else None)
        if order_id:
            return order_id
    json_method = getattr(resp, "json", None)
    if callable(json_method):
        try:
            payload = json_method()
        except Exception:
            payload = None
        else:
            order_id = _from_dict(payload if isinstance(payload, dict) else None)
            if order_id:
                return order_id
            if isinstance(payload, dict):
                nested = payload.get("order")
                order_id = _from_dict(nested if isinstance(nested, dict) else None)
                if order_id:
                    return order_id
    if hasattr(resp, "order"):
        order = getattr(resp, "order")
        if isinstance(order, dict):
            order_id = _from_dict(order)
            if order_id:
                return order_id
    return "unknown"


def _unwrap_list(result: object) -> list[dict[str, object]]:
    if isinstance(result, list):
        return [item for item in result if isinstance(item, dict)]
    if isinstance(result, dict):
        for key in ("orders", "data", "items", "trades"):
            candidate = result.get(key)
            if isinstance(candidate, list):
                return [item for item in candidate if isinstance(item, dict)]
    return []


def infer_positions_from_trades(client: ClobClient) -> dict[str, float]:
    positions: defaultdict[str, float] = defaultdict(float)
    trades = _unwrap_list(client.get_trades())
    for trade in trades:
        token_id = _extract_token_id(trade)
        if not token_id:
            continue
        shares = (
            _safe_parse_float(trade.get("size"))
            or _safe_parse_float(trade.get("amount"))
            or 0.0
        )
        if shares:
            positions[token_id] += shares
    return dict(positions)


EXIT_TRUTH_PURPOSES = {"tpsl", "force_exit", "exit_ladder"}


def get_live_positions_truth(
    client: ClobClient | None,
    signer_address: str | None,
    purpose: str | None = None,
) -> dict[str, float]:
    tracker = get_live_positions_from_tracker()
    if tracker:
        logging.info(
            "LIVE_POSITIONS_SOURCE source=TRACKER tokens=%s", len(tracker)
        )
        return tracker
    logging.info("LIVE_POSITIONS_SOURCE source=TRACKER_EMPTY")
    if purpose in EXIT_TRUTH_PURPOSES:
        return {}
    positions = get_live_token_holdings_truth(client, signer_address)
    if positions:
        logging.info(
            "LIVE_POSITIONS_SOURCE source=FALLBACK tokens=%s", len(positions)
        )
        return positions
    logging.info("LIVE_POSITIONS_SOURCE source=FILLS_EMPTY")
    return {}


def get_live_positions_snapshot(
    client: ClobClient | None = None, log_snapshot: bool = True
) -> dict[str, object]:
    inferred_initial = {
        token: shares for token, shares in live_positions.items() if shares > 0
    }
    snapshot: dict[str, object] = {
        "open_orders": [],
        "recent_trades": [],
        "inferred_positions": dict(inferred_initial),
    }

    signer = live_signer_address or live_funder_address
    positions_truth = get_live_positions_truth(client, signer, purpose="tpsl")
    if positions_truth:
        snapshot["inferred_positions"] = positions_truth
        if log_snapshot:
            logging.info("LIVE_POSITIONS_SOURCE source=DATA_API")
        return snapshot
    if not client:
        if log_snapshot:
            logging.info(
                "LIVE_POSITIONS_SNAPSHOT open_orders=0 recent_trades=0 inferred_tokens=%s tokens=%s",
                len(snapshot["inferred_positions"]),
                list(snapshot["inferred_positions"].keys())[:3],
            )
            for token_id, shares in list(snapshot["inferred_positions"].items())[:5]:
                logging.info("LIVE_INFERRED_POS token_id=%s shares=%s", token_id, shares)
        return snapshot

    signer = live_signer_address or live_funder_address
    if signer:
        positions = fetch_live_positions_data_api(signer)
        if positions:
            inferred_positions = {}
            for pos in positions:
                inferred_positions[pos["token_id"]] = inferred_positions.get(pos["token_id"], 0.0) + float(
                    pos["shares"]
                )
            snapshot["inferred_positions"] = inferred_positions
            if log_snapshot:
                logging.info("LIVE_POSITIONS_SOURCE source=DATA_API")
            return snapshot
        else:
            if log_snapshot:
                logging.info("LIVE_POSITIONS_SOURCE source=DATA_API error=empty")

    def _call_method(method_names: list[str]) -> list[dict[str, object]]:
        for method_name in method_names:
            method = getattr(client, method_name, None)
            if callable(method):
                try:
                    result = method()
                except Exception as exc:
                    logging.warning(
                        "LIVE_POSITIONS_SNAPSHOT_ERROR where=%s err=%s",
                        method_name,
                        exc,
                    )
                    continue
                return _unwrap_list(result)
        return []

    snapshot["open_orders"] = _call_method(
        ["get_open_orders", "get_openOrders", "get_orders"]
    )[:5]
    snapshot["recent_trades"] = _call_method(
        ["get_trades", "get_trades_paginated", "getTrades", "getTradesPaginated"]
    )[:10]

    inferred_remote: defaultdict[str, float] = defaultdict(float)
    for trade in snapshot["recent_trades"]:
        token = _extract_token_id(trade) or "unknown"
        side = str(trade.get("side") or trade.get("type") or "").upper()
        size = (
            _safe_parse_float(trade.get("size"))
            or _safe_parse_float(trade.get("amount"))
            or _safe_parse_float(trade.get("shares"))
        )
        if size is None:
            continue
        if "BUY" in side:
            inferred_remote[token] += size
        elif "SELL" in side:
            inferred_remote[token] -= size

    for token, shares in inferred_remote.items():
        snapshot["inferred_positions"][token] = (
            snapshot["inferred_positions"].get(token, 0.0) + shares
        )

    tokens = []
    for order in snapshot["open_orders"][:3]:
        token_id = _extract_token_id(order)
        if token_id:
            tokens.append(token_id[:6] + "..." if len(token_id) > 6 else token_id)

    if log_snapshot:
        logging.info(
            "LIVE_POSITIONS_SNAPSHOT open_orders=%s recent_trades=%s inferred_tokens=%s tokens=%s",
            len(snapshot["open_orders"]),
            len(snapshot["recent_trades"]),
            len(snapshot["inferred_positions"]),
            tokens,
        )

    if log_snapshot:
        logging.info("LIVE_POSITIONS_SOURCE source=INFERRED_FALLBACK")

    if log_snapshot:
        inferred_sorted = sorted(
            snapshot["inferred_positions"].items(),
            key=lambda kv: abs(kv[1]),
            reverse=True,
        )
        for token_id, shares in inferred_sorted[:5]:
            logging.info("LIVE_INFERRED_POS token_id=%s shares=%s", token_id, shares)

    return snapshot


def parse_strategy_settings_field(payload) -> dict[str, object]:
    parsed = {}
    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            parsed = {}
    elif isinstance(payload, dict):
        parsed = payload
    return parsed


def clamp_min_order_usd(size_usd: float, min_usd: float) -> float:
    return max(size_usd, min_usd)


def should_force_exit(time_to_end_s: float, force_exit_s: float) -> bool:
    return time_to_end_s <= force_exit_s


def build_exit_order_params(
    token_id: str, shares: float, close_side: str, price_hint: float | None
) -> dict[str, object]:
    shares_abs = abs(shares)
    size_usd = MIN_ORDER_USD
    if price_hint and price_hint > 0:
        size_usd = shares_abs * price_hint
    size_usd = clamp_min_order_usd(size_usd, MIN_ORDER_USD)
    return {
        "token_id": token_id,
        "shares_abs": shares_abs,
        "close_side": close_side,
        "price_hint": price_hint,
        "size_usd": size_usd,
    }


def get_token_midprice(client: ClobClient, token_id: str) -> float | None:
    methods = [
        "get_trades",
        "get_trades_paginated",
        "getTrades",
        "getTradesPaginated",
    ]
    for method_name in methods:
        method = getattr(client, method_name, None)
        if not callable(method):
            continue
        try:
            result = method()
        except Exception:
            continue
        trades = _unwrap_list(result)
        for trade in trades:
            tid = _extract_token_id(trade)
            if tid != token_id:
                continue
            price = (
                _safe_parse_float(trade.get("price"))
                or _safe_parse_float(trade.get("executionPrice"))
                or _safe_parse_float(trade.get("avgPrice"))
            )
            if price:
                return price
    return None


def get_token_mark_price(token_id: str) -> float | None:
    if not token_id:
        return None
    side = ASSET_TO_SIDE.get(token_id)
    if side in ("yes", "no"):
        bid = best_quotes[side]["bid"]
        ask = best_quotes[side]["ask"]
        if bid is not None and ask is not None:
            return (bid + ask) / 2
        if bid is not None:
            return bid
        if ask is not None:
            return ask
    return None


def fetch_data_api_positions(user_address: str | None) -> dict[str, float]:
    if not user_address:
        logging.info("LIVE_POSITIONS_SOURCE source=DATA_API_FAILED error=no_user status=None")
        return {}
    base = "https://data-api.polymarket.com/positions"
    url = f"{base}?user={parse.quote(user_address)}"
    logging.info("LIVE_POSITIONS_DATA_API_REQUEST url=%s", url)
    try:
        req = request.Request(url, headers={"User-Agent": "FastLoopWorker/1.0"})
        with request.urlopen(req, timeout=10) as resp:
            status = getattr(resp, "status", None)
            raw = resp.read()
            data = json.loads(raw)
            if status and status >= 400:
                logging.info(
                    "LIVE_POSITIONS_SOURCE source=DATA_API_FAILED error=status%d status=%s",
                    status,
                    status,
                )
                return {}
    except Exception as exc:
        logging.info(
            "LIVE_POSITIONS_SOURCE source=DATA_API_FAILED error=%s status=None",
            exc,
        )
        return {}

    positions = {}
    items = data if isinstance(data, list) else data.get("positions") or data.get("assets") or []
    for item in items:
        if not isinstance(item, dict):
            continue
        token_id = next(
            (
                str(item.get(k))
                for k in ("token_id", "tokenId", "token", "asset_id", "assetId")
                if item.get(k)
            ),
            None,
        )
        shares = None
        for key in ("shares", "size", "quantity", "balance", "position"):
            value = item.get(key)
            parsed = _safe_parse_float(value)
            if parsed:
                shares = parsed
                break
        if not token_id or not shares or shares <= 0.01:
            continue
        positions[token_id] = positions.get(token_id, 0.0) + shares
    if positions:
        tokens = list(positions.items())[:3]
        logging.info(
            "LIVE_POSITIONS_SOURCE source=DATA_API count=%s example=%s",
            len(positions),
            [(token, round(shares, 4)) for token, shares in tokens],
        )
    else:
        snippet = ""
        try:
            snippet = raw.decode(errors="ignore")[:800]
        except Exception:
            snippet = str(raw)[:800]
        logging.info(
            "LIVE_POSITIONS_DATA_API_EMPTY status=200 body_preview=%s",
            snippet.replace("\\n", ""),
        )
        if isinstance(data, dict):
            keys = list(data.keys())
            logging.info("LIVE_POSITIONS_DATA_API_KEYS keys=%s", keys)
        logging.info(
            "LIVE_POSITIONS_SOURCE source=DATA_API_FAILED error=no_positive_shares status=200"
        )
    return positions


def fetch_authenticated_trades(
    client: ClobClient, cursor: str | None = None
) -> tuple[list[dict[str, object]], str | None]:
    global trades_auth_mode_logged
    method = (
        getattr(client, "get_trades_paginated", None)
        or getattr(client, "getTradesPaginated", None)
        or getattr(client, "get_trades", None)
    )
    if method and not trades_auth_mode_logged:
        logging.info("LIVE_TRADES_AUTH_MODE mode=%s", method.__name__)
        trades_auth_mode_logged = True
    if method is None:
        return [], None
    try:
        data = None
        try:
            data = method(next_cursor=cursor)
        except TypeError:
            data = method(cursor) if cursor else method()
        trades = data if isinstance(data, list) else data.get("trades") or data.get("data") or data.get("items") or []
        next_cursor = None
        if isinstance(data, dict):
            next_cursor = data.get("nextCursor") or data.get("next_cursor")
        return _unwrap_list(trades), next_cursor
    except Exception:
        return [], None


def extract_any_address(trade: dict[str, object]) -> set[str]:
    addresses: set[str] = set()
    keys = [
        "maker",
        "taker",
        "owner",
        "user",
        "address",
        "maker_address",
        "taker_address",
        "user_address",
        "owner_address",
        "makerWallet",
        "takerWallet",
        "makerAddress",
        "takerAddress",
    ]
    for key in keys:
        value = trade.get(key)
        if isinstance(value, str) and value:
            addresses.add(value.lower())
        elif isinstance(value, dict):
            candidate = value.get("address") or value.get("wallet")
            if isinstance(candidate, str) and candidate:
                addresses.add(candidate.lower())
    return addresses


def extract_trade_side(trade: dict[str, object]) -> str | None:
    for key in ("side", "taker_side", "maker_side", "regulator_side"):
        value = trade.get(key)
        if isinstance(value, str):
            sval = value.upper()
            if "BUY" in sval:
                return "BUY"
            if "SELL" in sval:
                return "SELL"
    return None


def extract_token_and_size(trade: dict[str, object]) -> tuple[str | None, float | None]:
    token = next(
        (
            str(trade.get(k))
            for k in ("token_id", "tokenId", "token", "asset_id", "assetId")
            if trade.get(k)
        ),
        None,
    )
    shares = None
    for key in ("size", "shares", "amount", "filled_size", "filledSize", "fillSize"):
        value = trade.get(key)
        parsed = _safe_parse_float(value)
        if parsed:
            shares = parsed
            break
    return token, shares


def _norm_addr(value: object) -> str:
    """Normalise an Ethereum address: lowercase, strip whitespace and optional 0x prefix."""
    if not value:
        return ""
    try:
        s = str(value).strip().lower()
        if s.startswith("0x"):
            s = s[2:]
        return s
    except Exception:
        return ""


def is_our_trade(trade: dict[str, object], signer: str) -> bool:
    s = _norm_addr(signer)
    if not s:
        return False
    keys = (
        "owner",
        "maker_address",
        "taker_address",
        "trader",
        "trader_address",
    )
    for key in keys:
        if _norm_addr(trade.get(key)) == s:
            return True
    maker_orders = trade.get("maker_orders") or []
    for entry in maker_orders:
        if not isinstance(entry, dict):
            continue
        if _norm_addr(entry.get("maker")) == s or _norm_addr(entry.get("maker_address")) == s:
            return True
    return False


def extract_trade_token_id(trade: dict[str, object]) -> str | None:
    for key in ("asset_id", "token_id", "assetId", "tokenId"):
        value = trade.get(key)
        if value:
            return str(value)
    return None


def extract_trade_side(trade: dict[str, object]) -> str | None:
    for key in ("side", "trader_side"):
        value = trade.get(key)
        if isinstance(value, str):
            sval = value.upper()
            if "BUY" in sval:
                return "BUY"
            if "SELL" in sval:
                return "SELL"
    return None


def extract_trade_size(trade: dict[str, object]) -> float:
    for key in ("size", "amount", "shares"):
        value = trade.get(key)
        parsed = _safe_parse_float(value)
        if parsed:
            return parsed
    return 0.0


def get_live_token_holdings_truth(client: ClobClient | None, signer_address: str | None) -> dict[str, float]:
    if not client or not signer_address:
        logging.info("LIVE_HOLDINGS_ENDPOINT_FAILED error=no_client_or_signer")
        return {}
    holdings: defaultdict[str, float] = defaultdict(float)
    cursor: str | None = None
    now_ts = int(time())
    cutoff = now_ts - 86400
    global trades_sample_logged
    pages = 0
    total_trades = 0
    our_trades = 0
    while pages < 5:
        trades, next_cursor = fetch_authenticated_trades(client, cursor)
        if trades and not trades_sample_logged:
            sample = trades[0]
            logging.info(
                "LIVE_TRADES_SAMPLE keys=%s preview=%s",
                list(sample.keys()),
                str(sample)[:200],
            )
            logging.info(
                "LIVE_TRADES_SCHEMA_HINT signer=%s sample_owner=%s sample_maker=%s sample_trader_side=%s sample_side=%s sample_asset_id=%s sample_token_id=%s",
                (_norm_addr(signer_address) if signer_address else "none"),
                _norm_addr(sample.get("owner") or sample.get("maker")),
                _norm_addr(sample.get("maker")),
                extract_trade_side(sample),
                sample.get("side"),
                extract_trade_token_id(sample),
                sample.get("token_id") or sample.get("asset_id"),
            )
            trades_sample_logged = True
        if not trades:
            break
        for trade in trades:
            if not isinstance(trade, dict):
                continue
            total_trades += 1
            if not is_our_trade(trade, signer_address):
                continue
            our_trades += 1
            token_id = extract_trade_token_id(trade)
            if not token_id:
                continue
            direction = extract_trade_side(trade)
            if direction not in ("BUY", "SELL"):
                continue
            sz = extract_trade_size(trade)
            if sz <= 0:
                continue
            if direction == "SELL":
                holdings[token_id] -= sz
            elif direction == "BUY":
                holdings[token_id] += sz
        if not next_cursor:
            break
        cursor = next_cursor
        pages += 1
    positions = {token: shares for token, shares in holdings.items() if shares > 0.01}
    logging.info(
        "LIVE_HOLDINGS_FROM_FILLS_COUNTS total=%s ours=%s tokens=%s",
        total_trades,
        our_trades,
        len(positions),
    )
    if our_trades == 0:
        sample = trades[0] if trades else {}
        norm_signer = _norm_addr(signer_address)
        all_addrs = extract_any_address(sample) if sample else set()
        match_field = "none"
        for _fld in ("owner", "maker_address", "maker", "taker_address", "trader_address"):
            if _norm_addr(sample.get(_fld)) == norm_signer:
                match_field = _fld
                break
        logging.info(
            "LIVE_HOLDINGS_FROM_FILLS_NO_MATCH signer=%s hint=check LIVE_TRADES_SCHEMA_HINT fields sample_owner=%s sample_maker=%s",
            signer_address,
            sample.get("owner"),
            sample.get("maker"),
        )
        logging.info(
            "LIVE_MATCH_DEBUG signer=%s sample_owner=%s sample_maker_address=%s match_field=%s ours_count=%s all_addrs=%s",
            signer_address,
            sample.get("owner"),
            sample.get("maker_address"),
            match_field,
            our_trades,
            list(all_addrs)[:5],
        )
    if positions:
        tokens = list(positions.items())[:3]
        logging.info(
            "LIVE_HOLDINGS_FROM_FILLS tokens=%s example=%s",
            len(positions),
            [(token, round(shares, 4)) for token, shares in tokens],
        )
    else:
        logging.info("LIVE_HOLDINGS_FROM_FILLS_EMPTY")
    return positions



def fetch_live_positions_data_api(user_address: str | None) -> list[dict[str, object]]:
    if not user_address:
        return []
    base = "https://data-api.polymarket.com/positions"
    url = f"{base}?user={parse.quote(user_address)}"
    try:
        req = request.Request(url, headers={"User-Agent": "FastLoopWorker/1.0"})
        with request.urlopen(req, timeout=10) as resp:
            if getattr(resp, "status", 200) != 200:
                logging.warning(
                    "LIVE_POSITIONS_DATA_API err=status=%s", getattr(resp, "status", "n/a")
                )
                return []
            data = json.loads(resp.read())
    except Exception as exc:
        logging.warning("LIVE_POSITIONS_DATA_API_FAILED err=%s", exc)
        return []
    positions: list[dict[str, object]] = []
    items = data if isinstance(data, list) else data.get("positions") or data.get("assets") or []
    for item in items:
        if not isinstance(item, dict):
            continue
        token_id = _extract_token_id(item)
        shares = (
            _safe_parse_float(item.get("size"))
            or _safe_parse_float(item.get("shares"))
            or _safe_parse_float(item.get("amount"))
        )
        if not token_id or not shares or shares <= 0:
            continue
        positions.append(
            {
                "token_id": token_id,
                "shares": shares,
                "side": item.get("side") or item.get("outcome"),
                "market": item.get("market") or item.get("conditionId") or item.get("marketSlug"),
            }
        )
    if positions:
        logging.info(
            "LIVE_POSITIONS_DATA_API count=%s tokens=%s",
            len(positions),
            [p["token_id"][:6] + "..." for p in positions[:3]],
        )
    return positions


def _cancel_live_orders_for_token(client: ClobClient, token_id: str) -> None:
    snapshot = get_live_positions_snapshot(client, log_snapshot=False)
    for order in snapshot["open_orders"]:
        if _extract_token_id(order) != token_id:
            continue
        order_id = _extract_identifier(order, ("orderId", "order_id", "id"))
        if not order_id:
            continue
        for cancel_method_name in ("cancel_order", "cancelOrder", "cancel"):
            cancel_method = getattr(client, cancel_method_name, None)
            if callable(cancel_method):
                try:
                    cancel_method(order_id)
                    logging.info(
                        "LIVE_EXIT_CANCEL order_id=%s token_id=%s method=%s",
                        order_id,
                        token_id,
                        cancel_method_name,
                    )
                    break
                except Exception as exc:
                    logging.warning(
                        "LIVE_EXIT_CANCEL_ERROR order_id=%s err=%s",
                        order_id,
                        exc,
                    )
                    continue


def get_open_paper_positions(strategy_id: str) -> list[dict[str, object]]:
    if not current_slug:
        return []
    bot_id = STRATEGY_TO_BOT_ID.get(strategy_id, BOT_ID)
    try:
        resp = (
            supabase.table("paper_positions")
            .select("id, bot_id, side, entry_price, shares, size_usd, start_ts")
            .eq("bot_id", bot_id)
            .eq("strategy_id", strategy_id)
            .eq("status", "OPEN")
            .eq("market_slug", current_slug)
            .execute()
        )
        return resp.data or []
    except Exception:
        logging.exception(
            "Failed fetching open paper positions for strategy=%s slug=%s",
            strategy_id,
            current_slug,
        )
        return []


def close_paper_position_now(
    position: dict[str, object],
    token_id: str | None,
    mark_price: float,
    strategy_id: str,
    reason: str,
    held_seconds: int,
) -> None:
    row_id = position.get("id")
    if not row_id:
        return
    bot_id = position.get("bot_id") or BOT_ID
    shares = float_or_none(position.get("shares")) or 0.0
    size_usd = float_or_none(position.get("size_usd")) or 0.0
    entry_price = float_or_none(position.get("entry_price")) or 0.0
    payout = shares * mark_price
    pnl_usd = payout - size_usd
    updates = {
        "status": "CLOSED",
        "end_price": mark_price,
        "pnl_usd": pnl_usd,
        "resolved_side": position.get("side"),
        "closed_at": utc_now_iso(),
    }
    try:
        supabase.table("paper_positions").update(updates).eq("id", row_id).execute()
        update_bot_settings_with_realized_pnl(bot_id, pnl_usd)
        logging.info(
            "PAPER_TPSL_EXIT id=%s strategy=%s token_id=%s reason=%s entry=%s mark=%s held=%s",
            row_id,
            strategy_id,
            token_id or position.get("side"),
            reason,
            entry_price,
            mark_price,
            held_seconds,
        )
    except Exception:
        logging.exception("Failed closing PAPER_TPSL_EXIT id=%s reason=%s", row_id, reason)


def process_paper_tpsl_positions(now_ts: int) -> None:
    if not current_slug:
        return
    for strategy in (STRATEGY_SNIPER, STRATEGY_FASTLOOP):
        positions = get_open_paper_positions(strategy)
        for position in positions:
            token_side = (position.get("side") or "").lower()
            token_id = current_yes_token if token_side == "yes" else current_no_token
            mark_price = get_token_mark_price(token_id) if token_id else None
            if mark_price is None:
                logging.info(
                    "PAPER_TPSL_SKIP_NO_MARK token_id=%s strategy=%s id=%s",
                    token_id,
                    strategy,
                    position.get("id"),
                )
                continue
            entry_price = float_or_none(position.get("entry_price"))
            start_ts = int(position.get("start_ts") or 0)
            held_seconds = max(0, now_ts - start_ts)
            reason = tpsl_reason(strategy, entry_price or 0.0, mark_price, held_seconds)
            if reason:
                close_paper_position_now(
                    position, token_id, mark_price, strategy, reason, held_seconds
                )


async def evaluate_live_tpsl_positions(
    client: ClobClient | None, now_ts: int
) -> None:
    if not client:
        return
    signer = live_signer_address or live_funder_address
    positions_truth = get_live_positions_truth(
        client, signer, purpose="tpsl"
    )
    if not positions_truth:
        logging.info("LIVE_TPSL_SKIP_NO_POSITIONS reason=truth_empty")
        return
    for token_id, info in list(live_entry_info.items()):
        strategy = info.get("strategy")
        if strategy not in (STRATEGY_FASTLOOP, STRATEGY_SNIPER):
            continue
        entry_price = info.get("entry_price")
        if entry_price is None:
            logging.info("LIVE_TPSL_SKIP_NO_ENTRY_PRICE token_id=%s", token_id)
            continue
        mark_price = get_token_mark_price(token_id)
        if mark_price is None:
            logging.info("LIVE_TPSL_SKIP_NO_MARK_PRICE token_id=%s", token_id)
            continue
        shares = positions_truth.get(token_id, 0.0)
        if shares <= 0:
            logging.info(
                "LIVE_TPSL_SKIP_NO_POSITIONS token_id=%s shares=%s",
                token_id,
                shares,
            )
            continue
        held_seconds = max(0, now_ts - int(info.get("start_ts") or now_ts))
        reason = live_tpsl_reason(strategy, entry_price, mark_price, held_seconds)
        if not reason:
            continue
        logging.info(
            "LIVE_TPSL_TRIGGER token_id=%s strategy=%s reason=%s entry=%s mark=%s held=%s",
            token_id,
            strategy,
            reason,
            entry_price,
            mark_price,
            held_seconds,
        )
        await close_live_position_ladder(
            client,
            token_id,
            shares,
            base_price=mark_price,
            reason=f"TPSL_{reason}",
        )
        positions_after = get_live_positions_truth(
            client, signer, purpose="tpsl"
        )
        remaining = positions_after.get(token_id, 0.0)
        if abs(remaining) <= 0.01:
            live_entry_info.pop(token_id, None)
            logging.info("LIVE_TPSL_FLAT token_id=%s reason=%s", token_id, reason)
        else:
            logging.info("LIVE_TPSL_NOT_FLAT token_id=%s remaining=%s", token_id, remaining)


async def close_live_position_ladder(
    client: ClobClient,
    token_id: str,
    shares: float,
    base_price: float | None = None,
    reason: str = "FORCE_EXIT",
) -> bool:
    if not client:
        logging.warning(
            "LIVE_EXIT_ERROR token_id=%s err=%s",
            token_id,
            "missing_client",
        )
        return False
    shares_abs = abs(shares)
    if shares_abs < 0.01:
        logging.info(
            "LIVE_EXIT_SKIP_TINY token_id=%s shares=%s reason=%s",
            token_id,
            shares,
            reason,
        )
        return True

    signer = live_signer_address or live_funder_address
    positions_truth = get_live_positions_truth(
        client, signer, purpose="exit_ladder"
    )
    token_key = str(token_id)
    true_shares = positions_truth.get(token_key, 0.0)
    if true_shares <= 0.01:
        logging.info(
            "LIVE_FORCE_EXIT_SKIP_NO_POSITIONS token_id=%s reason=%s",
            token_id,
            reason,
        )
        logging.info(
            "LIVE_EXIT_DONE token_id=%s reason=%s steps=%s",
            token_id,
            reason,
            0,
        )
        return True
    shares_to_close = min(shares_abs, true_shares)
    if shares_to_close <= 0.01:
        logging.info(
            "LIVE_FORCE_EXIT_SKIP_NO_POSITIONS token_id=%s shares_to_close=%s reason=%s",
            token_id,
            shares_to_close,
            reason,
        )
        logging.info(
            "LIVE_EXIT_DONE token_id=%s reason=%s steps=%s",
            token_id,
            reason,
            0,
        )
        return True
    close_side = "SELL"
    token_key = str(token_id)
    try:
        price_base = base_price or get_token_midprice(client, token_id)
        if not price_base:
            logging.warning("LIVE_EXIT_NO_PRICE token_id=%s", token_id)
            return False

        improve = EXIT_LADDER_PRICE_IMPROVE_CENTS / 100.0
        for step in range(1, EXIT_LADDER_MAX_STEPS + 1):
            if close_side == "SELL":
                price = max(
                    0.01, price_base - 0.01 - (step - 1) * improve
                )
            else:
                price = min(
                    0.99, price_base + 0.01 + (step - 1) * improve
                )
            price_decimal = Decimal(str(price))
            trade_budget_usd = float(price_decimal) * shares_to_close
            adjusted_shares = _apply_min_shares_guard(
                client,
                token_id,
                price_decimal,
                Decimal(str(shares_to_close)),
                trade_budget_usd,
                close_side,
            )
            if adjusted_shares is None:
                return False
            shares_to_close = float(adjusted_shares)
            params = build_exit_order_params(token_id, shares_to_close, close_side, price)
            if params["size_usd"] <= 0:
                logging.warning(
                    "LIVE_EXIT_SKIP_ZERO_SIZE token_id=%s price=%s",
                    token_id,
                    price,
                )
                return False
            if _should_skip_min_shares(
                client,
                token_id,
                shares_to_close,
                price,
                params["size_usd"],
                close_side,
            ):
                return False

            success = submit_order(
                client,
                token_id,
                ASSET_TO_SIDE.get(token_id, "yes"),
                price,
                0.0,
                best_quotes["yes"]["ask"] or 0.0,
                best_quotes["no"]["ask"] or 0.0,
                params["size_usd"],
                strategy_id="force_exit",
                order_side=close_side,
                suppress_error_count=True,
            )

            logging.info(
                "LIVE_EXIT_STEP token_id=%s close_side=%s shares=%s price=%s step=%s/%s reason=%s success=%s",
                token_id,
                close_side,
                shares_to_close,
                price,
                step,
                EXIT_LADDER_MAX_STEPS,
                reason,
                success,
            )
            if success:
                positions_truth = get_live_positions_truth(
                    client, signer, purpose="exit_ladder"
                )
                true_shares = positions_truth.get(token_key, 0.0)
                if true_shares <= 0.01:
                    logging.info(
                        "LIVE_EXIT_DONE token_id=%s reason=%s steps=%s",
                        token_id,
                        reason,
                        step,
                    )
                    return True
                shares_to_close = min(shares_abs, true_shares)

        await asyncio.sleep(EXIT_LADDER_STEP_SECONDS)
        signer = live_signer_address or live_funder_address
        positions_truth = get_live_positions_truth(
            client, signer, purpose="exit_ladder"
        )
        shares_now = positions_truth.get(token_id, 0.0)
        if abs(shares_now) <= 0.01:
            logging.info(
                "LIVE_EXIT_DONE token_id=%s reason=%s steps=%s",
                token_id,
                reason,
                step,
            )
            return True

            _cancel_live_orders_for_token(client, token_id)

        logging.warning(
            "LIVE_EXIT_FAILED token_id=%s shares_remaining=%s reason=%s",
            token_id,
            shares_now,
            reason,
        )
        return False
    except Exception as exc:
        logging.warning(
            "LIVE_EXIT_EXCEPTION token_id=%s err=%s",
            token_id,
            exc,
        )
        return False
    except Exception as exc:
        logging.warning(
            "LIVE_EXIT_EXCEPTION token_id=%s err=%s",
            token_id,
            exc,
        )
        return False


def pm_headers(method: str, path: str) -> dict | None:
    if not PM_ACCESS_KEY or not PM_ED25519_PRIVATE_KEY_B64:
        return None
    try:
        timestamp_ms = str(int(time() * 1000))
        message = f"{timestamp_ms}{method.upper()}{path}"
        key_bytes = base64.b64decode(PM_ED25519_PRIVATE_KEY_B64)
        private_key = ed25519.Ed25519PrivateKey.from_private_bytes(key_bytes[:32])
        signature = private_key.sign(message.encode())
        signature_b64 = base64.b64encode(signature).decode()
        return {
            "X-PM-Access-Key": PM_ACCESS_KEY,
            "X-PM-Timestamp": timestamp_ms,
            "X-PM-Signature": signature_b64,
            "Content-Type": "application/json",
        }
    except Exception:
        logging.exception("Failed building PM headers")
        return None


def fetch_account_buying_power_usd() -> float | None:
    path = "/v1/account/balances"
    url = f"{PM_ACCOUNT_HOST.rstrip('/')}{path}"
    headers = pm_headers("GET", path)
    if not headers:
        logging.warning("ACCOUNT_BUYING_POWER_UNAVAILABLE reason=missing_headers")
        return None
    req = request.Request(url, headers=headers, method="GET")
    try:
        with request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode()
            truncated = body[:300]
            status = getattr(resp, "status", None)
            logging.info("PM_BALANCES_HTTP status=%s body=%s", status, truncated)
            try:
                payload = json.loads(body)
                balances = payload.get("balances") or []
                buying_power = (
                    float_or_none(balances[0].get("buyingPower"))
                    if balances and balances[0].get("buyingPower") is not None
                    else None
                )
            except Exception as exc:
                logging.warning("ACCOUNT_BUYING_POWER_UNAVAILABLE reason=parse_error %s", exc)
                buying_power = None
            logging.info("ACCOUNT_BUYING_POWER buying_power_usd=%s", buying_power)
            return buying_power
    except HTTPError as err:
        body = err.read().decode(errors="ignore")[:300]
        logging.warning("PM_BALANCES_HTTP status=%s body=%s", err.code, body)
        logging.warning(
            "ACCOUNT_BUYING_POWER_UNAVAILABLE reason=HTTP %s body=%s", err.code, body
        )
        return None
    except Exception as exc:
        logging.warning("ACCOUNT_BUYING_POWER_UNAVAILABLE reason=%s", exc)
        return None


def log_clob_balance_allowance_response(resp, client: ClobClient | None):
    status = getattr(resp, "status_code", None)
    try:
        body = json.dumps(resp, ensure_ascii=False)
    except Exception:
        body = str(resp)
    truncated = body[:1200]
    logging.info(
        "CLOB_BALANCE_ALLOWANCE_HTTP status=%s body=%s", status, truncated
    )
    parsed_type = type(resp).__name__ if resp is not None else "NoneType"
    keys = list(resp.keys()) if isinstance(resp, dict) else None
    logging.info(
        "CLOB_BALANCE_ALLOWANCE_PARSED type=%s keys=%s", parsed_type, keys
    )
    signer = client.get_address() if client else "none"
    logging.info("CLOB_AUTH signer=%s signature_type=%s", signer, SIGNATURE_TYPE)


def derive_wallet_addresses(client: ClobClient | None) -> bool:
    global live_signer_address, live_funder_address
    if not client:
        return False
    signer = client.get_address()
    funder_addr = FUNDER if FUNDER else signer
    live_signer_address = signer
    live_funder_address = funder_addr
    logging.info(
        "LIVE_WALLET_CHECK expected=%s signer=%s funder=%s",
        LIVE_WALLET_ADDRESS_EXPECTED,
        live_signer_address,
        live_funder_address,
    )
    return True


def refresh_live_bankroll_usd_if_needed(
    client: ClobClient | None, force: bool = False
) -> tuple[float | None, float | None]:
    global last_live_bankroll_refresh_ts, live_balance_cache, live_allowance_cache
    now_ts = int(time())
    if not force and now_ts - last_live_bankroll_refresh_ts < LIVE_BANKROLL_REFRESH_SECONDS:
        return live_balance_cache, live_allowance_cache
    last_live_bankroll_refresh_ts = now_ts
    if not client:
        return live_balance_cache, live_allowance_cache
    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=-1)
    try:
        client.update_balance_allowance(params=params)
        resp = client.get_balance_allowance(params=params)
        log_clob_balance_allowance_response(resp, client)
    except AttributeError as attr:
        methods = [
            m for m in dir(client) if "balance" in m.lower() or "allowance" in m.lower()
        ]
        logging.warning(
            "LIVE_BANKROLL_REFRESH_FAIL err=%s available_methods=%s", attr, methods
        )
        return live_balance_cache, live_allowance_cache
    except Exception as exc:
        logging.warning("LIVE_BANKROLL_REFRESH_FAIL err=%s", exc)
        return live_balance_cache, live_allowance_cache
    raw_balance = (
        resp.get("balance")
        or resp.get("amount")
        or resp.get("collateral_balance")
        or resp.get("collateralBalance")
    )
    raw_allowance = (
        resp.get("allowance")
        or resp.get("allowanceUsd")
        or resp.get("allowance_usd")
        or resp.get("collateral_allowance")
        or resp.get("collateralAllowance")
    )
    decimals = 10 ** LIVE_USDC_DECIMALS
    balance_usd = None
    allowance_usd = None
    buying_power = fetch_account_buying_power_usd()
    try:
        patch_payload = {}
        source_balance = None
        if buying_power is not None:
            source_balance = buying_power
        elif raw_balance is not None:
            source_balance = float(raw_balance) / decimals
            logging.info("LIVE_BANKROLL_FALLBACK_TO_CLOB balance_usd=%s", source_balance)
        if source_balance is not None:
            live_balance_cache = source_balance
            patch_payload["live_balance_usd"] = source_balance
        if patch_payload:
            logging.info("LIVE_BANKROLL_PATCH_KEYS keys=%s", list(patch_payload.keys()))
            resp_update = (
                supabase.table("bot_settings")
                .update(patch_payload)
                .eq("bot_id", LIVE_MASTER_BOT_ID)
                .execute()
            )
            status = getattr(resp_update, "status_code", None)
            ok = status in (200, 201)
            logging.info("LIVE_BANKROLL_WRITE ok=%s status_code=%s", ok, status)
    except Exception as exc:
        logging.exception("Failed updating live_balance_usd")
    logging.info(
        "LIVE_BANKROLL_REFRESH balance_usd=%s allowance_usd=%s raw_balance=%s raw_allowance=%s decimals=%s",
        live_balance_cache,
        live_allowance_cache,
        raw_balance,
        raw_allowance,
        LIVE_USDC_DECIMALS,
    )
    return live_balance_cache, live_allowance_cache


def persist_live_strategy_settings(wallet_address: str | None, allowance_usd: float | None = None):
    if wallet_address is None and allowance_usd is None:
        return
    try:
        resp = (
            supabase.table("bot_settings")
            .select("strategy_settings")
            .eq("bot_id", LIVE_MASTER_BOT_ID)
            .limit(1)
            .execute()
        )
        row = (resp.data or [None])[0]
        current = parse_strategy_settings_field(row.get("strategy_settings") if row else {})
        changed = False
        if wallet_address and current.get("live_wallet_address") != wallet_address:
            current["live_wallet_address"] = wallet_address
            changed = True
        if allowance_usd is not None and current.get("live_allowance_usd") != allowance_usd:
            current["live_allowance_usd"] = allowance_usd
            changed = True
        if changed:
            supabase.table("bot_settings").update(
                {"strategy_settings": current}
            ).eq("bot_id", LIVE_MASTER_BOT_ID).execute()
    except Exception:
        logging.exception("Failed updating live strategy settings")


# =============================================================================
# SUPABASE SETTINGS READERS (REUSABLE CORE)
# =============================================================================
# read_strategy_settings, read_live_master_enabled, get_global_trade_mode
# are generic Supabase readers that work for any strategy bot_id.
# The PAPER / ARM LIVE / LIVE ON / KILL_SWITCH gate logic is also here.
# These are safe to reuse unchanged for copy-trading.
# =============================================================================

def get_global_trade_mode() -> str:
    try:
        resp = (
            supabase.table("bot_settings")
            .select("strategy_settings")
            .eq("bot_id", "default")
            .limit(1)
            .execute()
        )
        data = resp.data or []
        if not data:
            return "ONE"
        raw_settings = data[0].get("strategy_settings")
        parsed = parse_strategy_settings_field(raw_settings)
        mode = (parsed.get("trade_mode") or "ONE").upper()
        return mode if mode in ("ONE", "ALL") else "ONE"
    except Exception:
        logging.exception("Failed reading global trade_mode")
        return "ONE"


def current_global_trade_mode() -> str:
    global global_trade_mode_cache
    mode = get_global_trade_mode()
    if global_trade_mode_cache is None:
        logging.info("GLOBAL_TRADE_MODE trade_mode=%s", mode)
    global_trade_mode_cache = mode
    return mode


def read_strategy_settings(bot_id: str) -> dict[str, object]:
    defaults = {
        "bot_id": bot_id,
        "is_enabled": False,
        "mode": "PAPER",
        "edge_threshold": EDGE_THRESHOLD,
        "trade_size_usd": TRADE_SIZE,
        "max_trades_per_hour": MAX_TRADES_PER_HOUR,
        "paper_balance_usd": DEFAULT_PAPER_START_BALANCE,
        "arm_live": False,
    }
    columns = "is_enabled, mode, edge_threshold, trade_size_usd, max_trades_per_hour, paper_balance_usd, arm_live, strategy_settings"
    try:
        resp = (
            supabase.table("bot_settings")
            .select(columns)
            .eq("bot_id", bot_id)
            .limit(1)
            .execute()
        )
        data = resp.data or []
        if not data:
            if bot_id not in strategy_missing_rows:
                logging.warning("Missing bot_settings row for %s; treating as disabled", bot_id)
                strategy_missing_rows.add(bot_id)
            return defaults
        row = data[0]
        raw_strategy_settings = row.get("strategy_settings")
        parsed_strategy = {}
        if isinstance(raw_strategy_settings, str):
            try:
                parsed_strategy = json.loads(raw_strategy_settings)
            except json.JSONDecodeError:
                parsed_strategy = {}
        elif isinstance(raw_strategy_settings, dict):
            parsed_strategy = raw_strategy_settings

        direction_mode = (parsed_strategy.get("direction_mode") or "normal").lower()
        if direction_mode not in ("normal", "reverse"):
            direction_mode = "normal"
        bias_mode = (parsed_strategy.get("bias_mode") or "off").lower()
        if bias_mode not in ("off", "yes_only", "no_only"):
            bias_mode = "off"
        bias_side = (parsed_strategy.get("bias_side") or "yes").lower()
        if bias_side not in ("yes", "no"):
            bias_side = "yes"
        settings = {
            "bot_id": bot_id,
            "is_enabled": bool(row.get("is_enabled")),
            "mode": (row.get("mode") or "PAPER").upper(),
            "edge_threshold": float_or_none(row.get("edge_threshold")) or EDGE_THRESHOLD,
            "trade_size_usd": float_or_none(row.get("trade_size_usd")) or TRADE_SIZE,
            "max_trades_per_hour": int(row.get("max_trades_per_hour") or MAX_TRADES_PER_HOUR),
            "paper_balance_usd": float_or_none(row.get("paper_balance_usd")) or DEFAULT_PAPER_START_BALANCE,
            "arm_live": bool(row.get("arm_live")),
            "strategy_settings": parsed_strategy,
            "direction_mode": direction_mode,
            "bias_mode": bias_mode,
            "bias_side": bias_side,
        }
        log_rate_limited(
            f"strategy_settings_{bot_id}",
            LOG_THROTTLE_SECONDS,
            "Loaded strategy settings bot_id=%s is_enabled=%s edge_threshold=%s arm_live=%s",
            bot_id,
            settings["is_enabled"],
            settings["edge_threshold"],
            settings["arm_live"],
            value=(
                settings["is_enabled"],
                settings["edge_threshold"],
                settings["arm_live"],
            ),
        )
        return settings
    except Exception:
        logging.exception("Failed reading bot_settings for %s", bot_id)
        return defaults


def read_live_master_enabled() -> bool:
    global live_master_warned
    try:
        resp = (
            supabase.table("bot_settings")
            .select("is_enabled")
            .eq("bot_id", LIVE_MASTER_BOT_ID)
            .limit(1)
            .execute()
        )
        data = resp.data or []
        if not data:
            if not live_master_warned:
                logging.warning("Missing live master bot_settings row; treating LIVE_ON as disabled")
                live_master_warned = True
            return False
        row = data[0]
        live_enabled = row.get("live_enabled")
        enabled = bool(live_enabled) if live_enabled is not None else bool(row.get("is_enabled"))
        log_rate_limited(
            "live_master_enabled",
            LOG_THROTTLE_SECONDS,
            "Live master fetched: live_master_enabled=%s",
            enabled,
            value=enabled,
        )
        return enabled
    except Exception:
        logging.exception("Failed reading live master settings")
        return False


def prune_strategy_trade_history(strategy_id: str):
    cutoff = datetime.now(timezone.utc).timestamp() - 3600
    dq = strategy_trade_timestamps[strategy_id]
    while dq and dq[0] < cutoff:
        dq.popleft()


def has_strategy_trade_capacity(strategy_id: str, required=2, max_per_hour=MAX_TRADES_PER_HOUR) -> bool:
    prune_strategy_trade_history(strategy_id)
    dq = strategy_trade_timestamps[strategy_id]
    return (len(dq) + required) <= max_per_hour


def mark_strategy_trade_attempts(strategy_id: str, n=1):
    ts = datetime.now(timezone.utc).timestamp()
    dq = strategy_trade_timestamps[strategy_id]
    for _ in range(n):
        dq.append(ts)


def get_live_balance_value() -> float:
    try:
        resp = (
            supabase.table("bot_settings")
            .select("live_balance_usd")
            .eq("bot_id", LIVE_MASTER_BOT_ID)
            .limit(1)
            .execute()
        )
        row = (resp.data or [None])[0]
        value = float_or_none(row.get("live_balance_usd")) if row else None
        if value is not None:
            return value
        return live_balance_cache
    except Exception:
        logging.exception("Failed reading live balance")
        return live_balance_cache


def _send_live_bankroll_patch(payload: dict[str, object]) -> tuple[bool, int, str]:
    endpoint = f"{SUPABASE_URL.rstrip('/')}/rest/v1/bot_settings?bot_id=eq.live"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    body_bytes = json.dumps(payload).encode()
    req = request.Request(endpoint, data=body_bytes, headers=headers, method="PATCH")
    try:
        with request.urlopen(req, timeout=10) as resp:
            status = getattr(resp, "status", getattr(resp, "status_code", 0))
            body = resp.read().decode(errors="ignore")
            return status in (200, 201, 204), status, body[:120]
    except HTTPError as exc:
        status = exc.code
        try:
            body = exc.read().decode(errors="ignore")
        except Exception:
            body = str(exc)
        return False, status, body[:120]
    except Exception as exc:
        logging.warning("LIVE_BANKROLL_WRITE_FAILED err=%s", exc)
        return False, 0, ""


def sync_live_bankroll(client: ClobClient | None) -> tuple[float | None, float | None]:
    global live_balance_cache, live_allowance_cache, last_live_bankroll_log_ts
    if not client:
        return None, None
    buying_power = fetch_account_buying_power_usd()
    allowance = None
    raw_balance = None
    raw_allowance = None
    decimals = 10 ** LIVE_USDC_DECIMALS
    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=-1)
    try:
        resp = client.get_balance_allowance(params=params)
        log_clob_balance_allowance_response(resp, client)
        allowance = float_or_none(
            resp.get("allowance")
            or resp.get("allowanceUsd")
            or resp.get("allowance_usd")
            or resp.get("collateral_allowance")
            or resp.get("collateralAllowance")
        )
        raw_allowance = (
            resp.get("allowance")
            or resp.get("allowanceUsd")
            or resp.get("allowance_usd")
            or resp.get("collateral_allowance")
            or resp.get("collateralAllowance")
        )
    except Exception:
        logging.exception("LIVE_BANKROLL_FETCH_FAIL")
    patch_payload = {}
    source_balance = None
    if buying_power is not None:
        source_balance = buying_power
    elif resp is not None:
        raw_balance = (
            resp.get("balance")
            or resp.get("amount")
            or resp.get("collateral_balance")
            or resp.get("collateralBalance")
        )
        if raw_balance is not None:
            source_balance = float(raw_balance) / (10 ** LIVE_USDC_DECIMALS)
            logging.info("LIVE_BANKROLL_FALLBACK_TO_CLOB balance_usd=%s", source_balance)
    if source_balance is not None:
        live_balance_cache = source_balance
        patch_payload["live_balance_usd"] = source_balance
        patch_payload["live_updated_at"] = datetime.now(timezone.utc).isoformat()
    if patch_payload:
        logging.info("LIVE_BANKROLL_PATCH_KEYS_SYNC keys=%s", list(patch_payload.keys()))
        try:
            ok, status, body_preview = _send_live_bankroll_patch(patch_payload)
            logging.info(
                "LIVE_BANKROLL_WRITE_SYNC ok=%s status_code=%s body_preview=%s",
                ok,
                status,
                body_preview,
            )
            if not ok:
                logging.info(
                    "LIVE_BANKROLL_WRITE_FAILED status_code=%s body_preview=%s",
                    status,
                    body_preview,
                )
        except Exception:
            logging.exception("Failed updating live_balance_usd")
    if allowance is not None:
        live_allowance_cache = allowance
        persist_live_strategy_settings(None, allowance)
    now_ts = int(time())
    if now_ts - last_live_bankroll_log_ts >= 60:
        last_live_bankroll_log_ts = now_ts
        balance_usd = live_balance_cache
        allowance_usd = allowance
        logging.info(
            "LIVE_BANKROLL balance_usd=%s allowance_usd=%s raw_balance=%s raw_allowance=%s decimals=%s",
            balance_usd,
            allowance_usd,
            raw_balance,
            raw_allowance,
            decimals,
        )
    return live_balance_cache, allowance


def compute_strategy_size(settings: dict[str, object], strategy_id: str, mode: str) -> float:
    trade_size_input = settings["trade_size_usd"]
    if trade_size_input < 0:
        logging.warning(
            "NEGATIVE_TRADE_SIZE strategy=%s trade_size_usd=%s forcing_zero",
            strategy_id,
            trade_size_input,
        )
        trade_size_input = 0.0
    base_size = max(trade_size_input, 0.0)
    balance_base = "n/a"
    live_balance_val = None
    if base_size <= 1:
        if mode == "LIVE":
            live_balance_val = get_live_balance_value()
            balance_base = live_balance_val if live_balance_val is not None else 0.0
        else:
            balance_base = settings.get("paper_balance_usd") or 0.0
            if PAPER_BANKROLL_SHARED_ENABLED:
                shared_balance = get_shared_paper_balance()
                if shared_balance is not None:
                    balance_base = shared_balance
        size = balance_base * base_size
        is_percent = True
    else:
        size = base_size
        is_percent = False
    cap_applied = False
    cap_value = None
    if strategy_id == STRATEGY_SNIPER:
        if size > SNIPER_SIZE_CAP_USD:
            size = SNIPER_SIZE_CAP_USD
            cap_applied = True
            cap_value = SNIPER_SIZE_CAP_USD
    logging.info(
        "SIZE_COMPUTE strategy=%s mode=%s trade_size_input=%s base_balance=%s size_usd=%s is_percent=%s cap=%s",
        strategy_id,
        mode,
        settings["trade_size_usd"],
        balance_base if base_size <= 1 else "n/a",
        size,
        is_percent,
        cap_value if cap_applied else "none",
    )
    if mode == "PAPER":
        logging.info(
            "PAPER_SIZE_BASE strategy=%s base_balance=%s trade_size_usd=%s size_usd=%s",
            strategy_id,
            balance_base if base_size <= 1 else "n/a",
            settings["trade_size_usd"],
            size,
        )
    return size


def compute_live_size_usd(settings: dict[str, object], strategy_id: str, live_balance_plain: float | None) -> float | None:
    if live_balance_plain is None:
        return None
    base_size = max(settings["trade_size_usd"], 0.0)
    balance_base = live_balance_plain
    is_percent = base_size <= 1
    if base_size <= 1:
        size = balance_base * base_size
    else:
        size = base_size
    cap_applied = False
    cap_value = None
    if strategy_id == STRATEGY_SNIPER and size > SNIPER_SIZE_CAP_USD:
        size = SNIPER_SIZE_CAP_USD
        cap_applied = True
        cap_value = SNIPER_SIZE_CAP_USD
    logging.info(
        "LIVE_SIZE_COMPUTE strategy=%s trade_size_input=%s base_balance=%s size_usd=%s is_percent=%s cap=%s",
        strategy_id,
        settings["trade_size_usd"],
        balance_base,
        size,
        is_percent,
        cap_value if cap_applied else "none",
    )
    return size


def compute_shares_from_size(size_usd: float, price: float) -> float:
    if not price or price <= 0:
        return 0.0
    raw = size_usd / price
    if raw <= 0:
        return 0.0
    return floor(raw * 1e8) / 1e8


def approx_mid_price():
    ya = best_quotes["yes"]["ask"]
    na = best_quotes["no"]["ask"]
    if ya is None or na is None:
        return None
    return (ya + na) / 2


# BTC-SPECIFIC: Infers market interval from BTC slug prefix (e.g. "btc-updown-5m" → 300s).
# COPY-TRADE HOOK: Remove when rotate_loop is replaced by a static market config loader.
def interval_from_prefix(prefix: str) -> int:
    if "-15m" in prefix:
        return 900
    if "-5m" in prefix:
        return 300
    return INTERVAL_SECONDS


def should_trade_strategy(settings: dict[str, object], strategy_id: str, edge: float | None) -> bool:
    decision, _reason = get_paper_trade_decision_reason(settings, strategy_id, edge)
    return decision


def get_paper_trade_decision_reason(
    settings: dict[str, object], strategy_id: str, edge: float | None
) -> tuple[bool, str]:
    if not settings["is_enabled"]:
        return False, "other"
    if settings["mode"] != "PAPER":
        return False, "other"
    if edge is None or edge < settings["edge_threshold"]:
        return False, "below_threshold"
    if not has_strategy_trade_capacity(
        strategy_id, 2, settings["max_trades_per_hour"]
    ):
        logging.info(
            "Rate limit reached for %s max_trades_per_hour=%s",
            strategy_id,
            settings["max_trades_per_hour"],
        )
        return False, "rate_limited"
    return True, "ok"


# =============================================================================
# LIVE EXECUTION (REUSABLE CORE)
# =============================================================================
# execute_live_strategy, close_live_position, close_live_position_ladder,
# and submit_order are generic CLOB execution functions.
# They operate on token_id + size_usd and are not BTC-specific.
# REUSABLE: Keep these unchanged for copy-trading.
# =============================================================================

def execute_live_strategy(
    client: ClobClient | None,
    strategy_id: str,
    edge: float,
    ya: float,
    na: float,
    size_usd: float,
) -> bool:
    global trade_triggers
    if not client or not current_yes_token or not current_no_token:
        return False
    logging.info(
        "LIVE_EXEC strategy=%s slug=%s size_usd=%s",
        strategy_id,
        current_slug,
        size_usd,
    )
    trade_triggers += 1
    try:
        if size_usd is not None and size_usd > 0:
            yes_shares = compute_shares_from_size(size_usd, ya) if ya else 0
            no_shares = compute_shares_from_size(size_usd, na) if na else 0
            if ya is not None and yes_shares > 0 and not _should_skip_min_shares(
                client,
                current_yes_token,
                yes_shares,
                ya,
                size_usd,
                "BUY",
            ):
                logging.info(
                    "LIVE_ORDER_SUBMIT strategy=%s side=yes slug=%s price=%s size=%s shares=%s",
                    strategy_id, current_slug, ya, size_usd, yes_shares,
                )
                submit_order(
                    client,
                    current_yes_token,
                    "yes",
                    ya,
                    edge,
                    ya,
                    na,
                    size_usd,
                    strategy_id=strategy_id,
                )
            if na is not None and no_shares > 0 and not _should_skip_min_shares(
                client,
                current_no_token,
                no_shares,
                na,
                size_usd,
                "BUY",
            ):
                logging.info(
                    "LIVE_ORDER_SUBMIT strategy=%s side=no slug=%s price=%s size=%s shares=%s",
                    strategy_id, current_slug, na, size_usd, no_shares,
                )
                submit_order(
                    client,
                    current_no_token,
                    "no",
                    na,
                    edge,
                    ya,
                    na,
                    size_usd,
                    strategy_id=strategy_id,
                )
        return True
    except Exception:
        logging.exception("Live execution failed for strategy %s", strategy_id)
        return False


def close_live_position(
    client: ClobClient | None,
    token_id: str,
    strategy_id: str | None = None,
) -> bool:
    if not client:
        logging.warning(
            "LIVE_CLOSE_ATTEMPT client_missing token_id=%s strategy=%s",
            token_id,
            strategy_id or "unknown",
        )
        return False
    get_live_positions_snapshot(log_snapshot=False)
    shares = live_positions.get(token_id) or 0.0
    if shares <= 0:
        logging.info(
            "LIVE_CLOSE_ATTEMPT no_position token_id=%s shares=%s",
            token_id,
            shares,
        )
        return False
    side_label = ASSET_TO_SIDE.get(token_id) or "yes"
    ya_price = best_quotes["yes"]["ask"]
    na_price = best_quotes["no"]["ask"]
    edge_value = 0.0
    if ya_price is not None and na_price is not None:
        edge_value = 1.0 - (ya_price + na_price)
    price_reference = (
        best_quotes.get(side_label, {}).get("bid")
        or approx_mid_price()
        or 0.0
    )
    logging.info(
        "LIVE_CLOSE_ATTEMPT strategy=%s token_id=%s side=%s shares=%s price=%s",
        strategy_id or "unknown",
        token_id,
        side_label,
        shares,
        price_reference,
    )
    return submit_order(
        client,
        token_id,
        side_label,
        price_reference,
        edge_value,
        ya_price or 0.0,
        na_price or 0.0,
        float(shares),
        strategy_id=strategy_id,
        order_side="SELL",
    )


async def execute_strategy(
    strategy_id: str,
    action_label: str,
    settings: dict[str, object],
    edge: float,
    total_ask: float | None,
    ya: float,
    na: float,
    client: ClobClient | None,
    live_master_enabled: bool,
    skip_live: bool = False,
    side_override: str | None = None,
) -> bool:
    if not current_slug:
        logging.warning("Missing slug; skipping strategy %s", strategy_id)
        return False
    normal_side = side_override if side_override in ("yes", "no") else ("yes" if ya <= na else "no")
    direction_mode = settings.get("direction_mode", "normal").lower() if strategy_id in (STRATEGY_SNIPER, STRATEGY_CANDLE_BIAS) else "normal"
    final_side = normal_side
    if strategy_id == STRATEGY_SNIPER and direction_mode == "reverse":
        final_side = "no" if normal_side == "yes" else "yes"
    slug_field = current_slug or "none"
    if strategy_id == STRATEGY_SNIPER:
        log_sniper_direction(slug_field, direction_mode, normal_side, final_side)
        bias_mode = settings.get("bias_mode", "off").lower()
        if bias_mode not in ("off", "yes_only", "no_only"):
            bias_mode = "off"
        allowed_by_bias = True
        if bias_mode == "yes_only" and final_side != "yes":
            allowed_by_bias = False
        elif bias_mode == "no_only" and final_side != "no":
            allowed_by_bias = False
        log_sniper_bias(
            slug_field,
            direction_mode,
            bias_mode,
            final_side,
            "ALLOW" if allowed_by_bias else "SKIP_BIAS_MODE",
        )
        if not allowed_by_bias:
            return False
    if strategy_id == STRATEGY_CANDLE_BIAS:
        log_candle_bias_direction(slug_field, settings.get("bias_side", "yes"), direction_mode, normal_side, final_side)
    entry_price = ya if final_side == "yes" else na
    if entry_price is None or entry_price <= 0:
        logging.warning("Invalid entry price for %s slug=%s", strategy_id, current_slug)
        return False

    mode = settings.get("mode", "PAPER").upper()
    size_usd = compute_strategy_size(settings, strategy_id, mode)
    shares = compute_shares_from_size(size_usd, entry_price)

    paper_available = settings.get("paper_balance_usd")
    if size_usd <= 0:
        logging.warning(
            "Skipping %s: size_usd=%s price=%s shares=%s reason=size<=0",
            strategy_id,
            size_usd,
            entry_price,
            shares,
        )
        return False
    if shares <= 0:
        logging.warning(
            "Skipping %s: size_usd=%s price=%s shares=%s reason=shares<=0",
            strategy_id,
            size_usd,
            entry_price,
            shares,
        )
        return False

    route_live = live_master_enabled and settings["arm_live"] and not skip_live
    executed_live = False
    live_size_usd = None
    if not route_live:
        if not live_master_enabled:
            live_skip_reason = "live_master_disabled"
        elif not settings["arm_live"]:
            live_skip_reason = "arm_live_off"
        elif skip_live:
            live_skip_reason = "force_exit_active"
        else:
            live_skip_reason = "unknown"
        logging.info(
            "LIVE_STRATEGY_SKIP strategy=%s reason=%s slug=%s",
            strategy_id, live_skip_reason, current_slug,
        )
    if route_live:
        logging.info(
            "LIVE_STRATEGY_EVALUATED strategy=%s slug=%s arm_live=%s live_master=%s",
            strategy_id, current_slug, settings["arm_live"], live_master_enabled,
        )
        derive_wallet_addresses(client)
        live_balance = get_live_balance_value()
        allowance = live_allowance_cache
        new_balance, new_allowance = refresh_live_bankroll_usd_if_needed(
            client, force=(live_balance is None or live_balance <= 0)
        )
        if new_balance is not None:
            live_balance = new_balance
        if new_allowance is not None:
            allowance = new_allowance
        logging.info("LIVE_BANKROLL_AFTER_REFRESH live_balance_usd=%s", live_balance)
        if should_skip_new_entries(
            "LIVE", live_balance, LIVE_MIN_AVAILABLE_USD, strategy_id
        ):
            route_live = False
        elif live_balance is None or live_balance <= 0:
            logging.warning(
                "LIVE_SKIP_NO_LIVE_BANKROLL strategy=%s live_balance_usd=%s",
                strategy_id,
                live_balance,
            )
            route_live = False
        else:
            logging.info(
                "LIVE_ALLOWANCE_CHECK balance_usd=%s allowance_usd=%s",
                live_balance,
                allowance,
            )
            live_size_usd = compute_live_size_usd(settings, strategy_id, live_balance)
            if live_size_usd is not None and live_size_usd < MIN_ORDER_USD:
                logging.warning(
                    "MIN_ORDER_SKIP mode=LIVE strategy=%s size_usd=%s min=%s",
                    strategy_id,
                    live_size_usd,
                    MIN_ORDER_USD,
                )
                route_live = False
                live_size_usd = None
            if LIVE_TEST_ORDER_USD:
                override_size = min(LIVE_TEST_ORDER_USD, live_balance)
                live_size_usd = override_size
                logging.info(
                    "LIVE_SIZE_OVERRIDE live_size_usd=%s reason=LIVE_TEST_ORDER_USD",
                    live_size_usd,
                )
            if not live_size_usd or live_size_usd <= 0:
                logging.warning(
                    "LIVE_SKIP_NO_LIVE_BANKROLL strategy=%s live_size_usd=%s",
                    strategy_id,
                    live_size_usd,
                )
                route_live = False
            elif allowance is not None and allowance < live_size_usd:
                logging.warning(
                    "LIVE_SKIP_ALLOWANCE strategy=%s allowance_usd=%s live_size_usd=%s",
                    strategy_id,
                    allowance,
                    live_size_usd,
                )
                route_live = False
            elif live_balance < live_size_usd:
                logging.warning(
                    "LIVE_SKIP_INSUFFICIENT_BALANCE strategy=%s balance_usd=%s live_size_usd=%s",
                    strategy_id,
                    live_balance,
                    live_size_usd,
                )
                route_live = False
            else:
                executed_live = execute_live_strategy(client, strategy_id, edge, ya, na, live_size_usd)
                live_bot_id = STRATEGY_TO_BOT_ID.get(strategy_id, BOT_ID)
                if executed_live:
                    logging.info(
                        "LIVE_ENTRY_ATTEMPT strategy=%s bot_id=%s slug=%s side=%s size=%s ok=True",
                        strategy_id, live_bot_id, current_slug, final_side, live_size_usd,
                    )
                else:
                    logging.info(
                        "LIVE_ENTRY_ATTEMPT strategy=%s bot_id=%s slug=%s side=%s size=%s ok=False reason=execute_failed",
                        strategy_id, live_bot_id, current_slug, final_side, live_size_usd,
                    )
                    logging.info(
                        "Falling back to PAPER for strategy=%s live_master_enabled=%s client=%s",
                        strategy_id,
                        live_master_enabled,
                        "available" if client else "missing",
                    )
    if not executed_live:
        skip_paper_entry = should_skip_new_entries(
            "PAPER", paper_available, PAPER_MIN_AVAILABLE_USD, strategy_id
        )
        if not skip_paper_entry:
            logging.info(
                "PAPER_EXEC strategy=%s slug=%s size_usd=%s",
                strategy_id,
                current_slug,
                size_usd,
            )
            if size_usd < MIN_ORDER_USD:
                logging.warning(
                    "MIN_ORDER_SKIP mode=PAPER strategy=%s size_usd=%s min=%s",
                    strategy_id,
                    size_usd,
                    MIN_ORDER_USD,
                )
            else:
                logging.info(
                    "PAPER_ENTRY_ATTEMPT strategy=%s bot_id=%s slug=%s side=%s size=%s shares=%s",
                    strategy_id, get_paper_bot_id(strategy_id), current_slug, final_side, size_usd, shares,
                )
                await create_paper_strategy_position(
                    strategy_id,
                    action_label,
                    edge,
                    ya,
                    na,
                    total_ask,
                    size_usd,
                    shares,
                    settings["mode"],
                    side_override=final_side,
                )

    mark_strategy_trade_attempts(strategy_id, 2)
    return True


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# =============================================================================
# SUPABASE WRITES — HEARTBEAT & TRADE LOGGING (REUSABLE CORE)
# =============================================================================
# record_heartbeat → bot_heartbeat table
# record_trade     → bot_trades table
# These are generic write helpers, fully reusable for copy-trading.
# =============================================================================

def record_heartbeat(status_text: str, message: str):
    payload = {
        "bot_id": BOT_ID,
        "last_seen": utc_now_iso(),
        "status": status_text,
        "message": message[:500],
    }
    try:
        supabase.table("bot_heartbeat").insert(payload).execute()
    except Exception:
        logging.exception("Failed inserting bot_heartbeat")


def meta_template(edge, ya, na):
    return {
        "timestamp": utc_now_iso(),
        "edge": edge,
        "ya": ya,
        "na": na,
    }


def record_opportunity(total_ask, edge, ya, yb, na, nb):
    payload = {
        "bot_id": BOT_ID,
        "market": "FASTLOOP",
        "side": "BUY_BOTH",
        "price": total_ask,
        "size": 0,
        "status": "OPPORTUNITY",
        "meta": {**meta_template(edge, ya, na), "yes_bid": yb, "no_bid": nb},
    }
    payload["meta"]["slug"] = current_slug
    try:
        supabase.table("bot_trades").insert(payload).execute()
    except Exception:
        logging.exception("Failed inserting OPPORTUNITY")


def record_trade(
    token_id,
    side_label,
    status,
    price,
    edge,
    ya,
    na,
    trade_size,
    response=None,
    error=None,
    strategy_id: str | None = None,
):
    global last_any_order_ts
    meta = {**meta_template(edge, ya, na), "token_id": token_id, "side_label": side_label}
    if response is not None:
        meta["response"] = response
    if error is not None:
        meta["error"] = error
    meta["slug"] = current_slug
    if strategy_id:
        meta["strategy_id"] = strategy_id

    trade_bot_id = STRATEGY_TO_BOT_ID.get(strategy_id, BOT_ID) if strategy_id else BOT_ID
    payload = {
        "bot_id": trade_bot_id,
        "market": "FASTLOOP",
        "market_slug": current_slug,
        "strategy_id": strategy_id,
        "side": side_label,
        "price": price,
        "size": trade_size,
        "status": status,
        "meta": meta,
    }
    try:
        supabase.table("bot_trades").insert(payload).execute()
        global last_any_order_ts
        last_any_order_ts = int(time())
        logging.info(
            "LIVE_ACTIVITY_WRITE strategy=%s bot_id=%s status=%s slug=%s",
            strategy_id or "unknown",
            trade_bot_id,
            status,
            current_slug or "none",
        )
    except Exception:
        logging.exception("Failed inserting bot_trades row")


def log_market_decision(
    strategy: str,
    slug: str,
    ya: float | None,
    na: float | None,
    total: float | None,
    edge: float | None,
    enabled: bool,
    arm_live: bool,
    live_master_enabled: bool,
    result: str,
):
    logging.info(
        "MARKET_DECISION strategy=%s slug=%s ya=%s na=%s total=%s edge=%s enabled=%s arm_live=%s live_master=%s result=%s",
        strategy,
        slug,
        fmt(ya),
        fmt(na),
        fmt(total),
        fmt(edge),
        enabled,
        arm_live,
        live_master_enabled,
        result,
    )


def log_sniper_bias(
    slug: str,
    direction_mode: str,
    bias_mode: str,
    final_side: str,
    result: str,
):
    logging.info(
        "SNIPER_BIAS slug=%s direction_mode=%s bias_mode=%s final_side=%s result=%s",
        slug,
        direction_mode,
        bias_mode,
        final_side,
        result,
    )


def log_sniper_direction(
    slug: str,
    direction_mode: str,
    normal_side: str,
    final_side: str,
):
    logging.info(
        "SNIPER_DIRECTION slug=%s direction_mode=%s normal_side=%s final_side=%s",
        slug,
        direction_mode,
        normal_side,
        final_side,
    )


def log_candle_bias_direction(
    slug: str,
    bias_side: str,
    direction_mode: str,
    normal_side: str,
    final_side: str,
):
    logging.info(
        "CANDLE_BIAS_DIRECTION slug=%s bias_side=%s direction_mode=%s normal_side=%s final_side=%s",
        slug,
        bias_side,
        direction_mode,
        normal_side,
        final_side,
    )


def detect_sweep_reclaim(history: list[Candle]) -> CandleSignal:
    if len(history) < 3:
        return CandleSignal("NEUTRAL", {"reason": "need_3_candles"})
    baseline = history[-3]
    sweep = history[-2]
    reclaim = history[-1]
    sweep_range = sweep.range()
    if sweep_range <= 0:
        return CandleSignal("NEUTRAL", {"reason": "sweep_range_zero"})
    if (
        sweep.is_bullish()
        and sweep.high > baseline.high
        and sweep.close > baseline.high
        and reclaim.close < sweep.high - sweep_range * 0.35
        and reclaim.close > baseline.high
    ):
        return CandleSignal(
            "NO",
            {
                "reason": "bull_sweep_reclaim",
                "sweep_close": sweep.close,
                "reclaim_close": reclaim.close,
            },
        )
    if (
        sweep.is_bearish()
        and sweep.low < baseline.low
        and sweep.close < baseline.low
        and reclaim.close > sweep.low + sweep_range * 0.35
        and reclaim.close < baseline.low
    ):
        return CandleSignal(
            "YES",
            {
                "reason": "bear_sweep_reclaim",
                "sweep_close": sweep.close,
                "reclaim_close": reclaim.close,
            },
        )
    return CandleSignal("NEUTRAL", {"reason": "sweep_reclaim_no_match"})


def detect_breakout_close(history: list[Candle]) -> CandleSignal:
    if not history:
        return CandleSignal("NEUTRAL", {"reason": "no_history"})
    last = history[-1]
    candle_range = last.range()
    body = last.body()
    if candle_range <= 0:
        if body > 0.0005:
            if last.is_bullish():
                return CandleSignal("YES", {"reason": "bullish_flat", "body": body})
            if last.is_bearish():
                return CandleSignal("NO", {"reason": "bearish_flat", "body": body})
        return CandleSignal("NEUTRAL", {"reason": "range_zero"})
    min_body = candle_range * 0.25
    threshold = candle_range * 0.25
    if last.is_bullish() and (last.high - last.close) <= threshold and body >= min_body:
        return CandleSignal(
            "YES", {"reason": "bullish_breakout_close", "body": body, "range": candle_range}
        )
    if last.is_bearish() and (last.close - last.low) <= threshold and body >= min_body:
        return CandleSignal(
            "NO", {"reason": "bearish_breakdown_close", "body": body, "range": candle_range}
        )
    if candle_range < 0.008 and body > 0.0005:
        if last.is_bullish():
            return CandleSignal("YES", {"reason": "bullish_tight_range", "body": body, "range": candle_range})
        if last.is_bearish():
            return CandleSignal("NO", {"reason": "bearish_tight_range", "body": body, "range": candle_range})
    return CandleSignal("NEUTRAL", {"reason": "breakout_close_no_match", "body": body, "range": candle_range})


def detect_engulfing_level(history: list[Candle]) -> CandleSignal:
    if len(history) < 2:
        return CandleSignal("NEUTRAL", {"reason": "need_2_candles"})
    prev = history[-2]
    last = history[-1]
    last_body = last.body()
    prev_body = prev.body()
    if last.high < prev.high or last.low > prev.low or prev_body <= 0:
        return CandleSignal("NEUTRAL", {"reason": "no_engulf"})
    if last_body < prev_body * 0.6:
        return CandleSignal("NEUTRAL", {"reason": "body_too_small"})
    if last.is_bullish() and prev.is_bearish():
        return CandleSignal(
            "YES",
            {
                "reason": "bullish_engulf",
                "prev_close": prev.close,
                "last_close": last.close,
            },
        )
    if last.is_bearish() and prev.is_bullish():
        return CandleSignal(
            "NO",
            {
                "reason": "bearish_engulf",
                "prev_close": prev.close,
                "last_close": last.close,
            },
        )
    return CandleSignal("NEUTRAL", {"reason": "engulf_no_match"})


def detect_rejection_wick(history: list[Candle]) -> CandleSignal:
    if not history:
        return CandleSignal("NEUTRAL", {"reason": "no_history"})
    last = history[-1]
    candle_range = last.range()
    body = last.body()
    if candle_range <= 0 or body <= 0:
        return CandleSignal("NEUTRAL", {"reason": "range_or_body_zero"})
    lower_wick = min(last.open, last.close) - last.low
    upper_wick = last.high - max(last.open, last.close)
    wick_threshold = max(body * 1.5, candle_range * 0.3)
    if lower_wick >= wick_threshold:
        return CandleSignal(
            "YES",
            {
                "reason": "lower_wick_rejection",
                "lower_wick": lower_wick,
                "upper_wick": upper_wick,
            },
        )
    if upper_wick >= wick_threshold:
        return CandleSignal(
            "NO",
            {
                "reason": "upper_wick_rejection",
                "lower_wick": lower_wick,
                "upper_wick": upper_wick,
            },
        )
    return CandleSignal("NEUTRAL", {"reason": "rejection_wick_no_match", "lower_wick": lower_wick, "upper_wick": upper_wick})


def detect_follow_through(history: list[Candle]) -> CandleSignal:
    if len(history) < 2:
        return CandleSignal("NEUTRAL", {"reason": "need_2_candles"})
    prev = history[-2]
    last = history[-1]
    last_body = last.body()
    prev_range = prev.range()
    if prev_range <= 0 or last_body <= 0:
        return CandleSignal("NEUTRAL", {"reason": "prev_range_or_last_body_zero"})
    prev_body = prev.body()
    if prev_body <= 0 or last_body < prev_body * 0.3:
        return CandleSignal("NEUTRAL", {"reason": "continuation_too_weak"})
    if prev.is_bullish() and last.is_bullish() and last.close > prev.close:
        return CandleSignal(
            "YES",
            {"reason": "bull_follow_through", "prev_body": prev_body, "last_body": last_body},
        )
    if prev.is_bearish() and last.is_bearish() and last.close < prev.close:
        return CandleSignal(
            "NO",
            {"reason": "bear_follow_through", "prev_body": prev_body, "last_body": last_body},
        )
    return CandleSignal("NEUTRAL", {"reason": "follow_through_no_match"})


# =============================================================================
# BTC-SPECIFIC: CANDLE PATTERN DETECTORS & STRATEGY REGISTRY
# =============================================================================
# All detect_* functions below are pure BTC 5-min candle pattern detectors.
# They operate on list[Candle] and return CandleSignal("YES"|"NO"|"NEUTRAL").
#
# COPY-TRADE HOOK: The entire detect_* block + CANDLE_DETECTORS dict +
#                  evaluate_candle_strategies() can be deleted when the copy
#                  engine is built. Replace with a wallet delta signal function:
#
#   def detect_copy_signal(target_positions_prev, target_positions_now) -> list[CopySignal]
#
# CANDLE_STRATEGY_IDS is defined in worker_config.py.

# BTC-SPECIFIC: CANDLE_DETECTORS maps strategy IDs to detector functions.
# These detectors are BTC 5-min candle pattern implementations.
# COPY-TRADE HOOK: Remove CANDLE_DETECTORS when replacing the candle engine
#                  with a wallet watcher signal.
CANDLE_DETECTORS: dict[str, Callable[[list[Candle]], CandleSignal]] = {
    STRATEGY_SWEEP_RECLAIM: detect_sweep_reclaim,
    STRATEGY_BREAKOUT_CLOSE: detect_breakout_close,
    STRATEGY_ENGULFING_LEVEL: detect_engulfing_level,
    STRATEGY_REJECTION_WICK: detect_rejection_wick,
    STRATEGY_FOLLOW_THROUGH: detect_follow_through,
}


# BTC-SPECIFIC: Runs all candle detectors against current OHLC history.
# COPY-TRADE HOOK: Remove this function. Replace with copy_signal_engine().
async def evaluate_candle_strategies(
    candle_strategy_settings: dict[str, dict[str, object]],
    total_ask: float | None,
    edge: float | None,
    ya: float | None,
    na: float | None,
    client: ClobClient | None,
    live_master_enabled: bool,
    entry_cutoff_active: bool,
    force_exit_triggered: bool,
    time_to_end: float | None,
    slug: str | None,
    asset_key: str | None,
) -> None:
    if not candle_manager.has_history(asset_key):
        logging.info(
            "CANDLE_SKIP reason=insufficient_history asset_key=%s closed_candles=%s minimum=%s",
            asset_key or "none",
            candle_manager.closed_count(asset_key),
            CANDLE_HISTORY_MINIMUM,
        )
        return
    slug_field = slug or "none"
    asset_field = asset_key or "none"
    history = candle_manager.closed_history(asset_key)
    last_n = history[-5:] if len(history) >= 5 else history
    candles_repr = [
        {"o": c.open, "h": c.high, "l": c.low, "c": c.close}
        for c in last_n
    ]
    logging.info(
        "CANDLE_INPUT strategy=candle_strategies asset_key=%s candles=%s",
        asset_field,
        candles_repr,
    )
    for strategy_id in CANDLE_STRATEGY_IDS:
        settings = candle_strategy_settings.get(strategy_id)
        if not settings:
            continue
        logging.info(
            "CANDLE_STRATEGY_SETTINGS_LOADED strategy=%s slug=%s enabled=%s mode=%s trade_size=%s max_trades=%s arm_live=%s paper_balance=%s",
            strategy_id,
            slug_field,
            settings["is_enabled"],
            settings["mode"],
            settings["trade_size_usd"],
            settings["max_trades_per_hour"],
            settings["arm_live"],
            settings["paper_balance_usd"],
        )

        detector = CANDLE_DETECTORS.get(strategy_id)
        if not detector:
            continue
        logging.info(
            "CANDLE_DETECTOR_EVALUATED strategy=%s slug=%s asset_key=%s candles=%s",
            strategy_id,
            slug_field,
            asset_field,
            len(history),
        )
        signal_result = detector(history)
        logging.info(
            "CANDLE_RULE_RESULT strategy=%s slug=%s signal=%s metadata=%s",
            strategy_id,
            slug_field,
            signal_result.signal,
            signal_result.metadata,
        )
        if signal_result.signal == "NEUTRAL":
            neutral_reason = (signal_result.metadata or {}).get("reason", "no_pattern")
            logging.info(
                "CANDLE_NEUTRAL_REASON strategy=%s reason=%s",
                strategy_id,
                neutral_reason,
            )
        def log_candle_decision(outcome: str, skip_reason: str | None = None) -> None:
            logging.info(
                "CANDLE_STRATEGY_DECISION strategy=%s slug=%s signal=%s metadata=%s outcome=%s skip_reason=%s",
                strategy_id,
                slug_field,
                signal_result.signal,
                signal_result.metadata,
                outcome,
                skip_reason or "none",
            )
        if signal_result.signal not in ("YES", "NO"):
            log_candle_decision("SKIPPED", "neutral_signal")
            log_market_decision(
                strategy_id,
                slug_field,
                ya,
                na,
                total_ask,
                edge,
                settings["is_enabled"],
                settings["arm_live"],
                live_master_enabled,
                "SKIP_NEUTRAL",
            )
            continue
        if entry_cutoff_active:
            log_candle_decision("SKIPPED", "entry_cutoff")
            log_paper_decision(
                strategy_id,
                current_slug,
                time_to_end,
                edge,
                settings["edge_threshold"],
                settings["is_enabled"],
                "entry_cutoff",
            )
            log_market_decision(
                strategy_id,
                slug_field,
                ya,
                na,
                total_ask,
                edge,
                settings["is_enabled"],
                settings["arm_live"],
                live_master_enabled,
                "SKIP_ENTRY_CUTOFF",
            )
            continue
        if not settings["is_enabled"]:
            log_candle_decision("SKIPPED", "disabled")
            log_market_decision(
                strategy_id,
                slug_field,
                ya,
                na,
                total_ask,
                edge,
                False,
                settings["arm_live"],
                live_master_enabled,
                "SKIP_DISABLED",
            )
            continue
        if settings["mode"] != "PAPER":
            log_candle_decision("SKIPPED", "mode_not_paper")
            log_paper_decision(
                strategy_id,
                current_slug,
                time_to_end,
                edge,
                settings["edge_threshold"],
                settings["is_enabled"],
                "other",
            )
            log_market_decision(
                strategy_id,
                slug_field,
                ya,
                na,
                total_ask,
                edge,
                settings["is_enabled"],
                settings["arm_live"],
                live_master_enabled,
                "SKIP_OTHER",
            )
            continue
        if should_skip_low_funds(settings["paper_balance_usd"]):
            log_candle_decision("SKIPPED", "low_funds")
            log_paper_decision(
                strategy_id,
                current_slug,
                time_to_end,
                edge,
                settings["edge_threshold"],
                settings["is_enabled"],
                "low_funds",
            )
            log_market_decision(
                strategy_id,
                slug_field,
                ya,
                na,
                total_ask,
                edge,
                settings["is_enabled"],
                settings["arm_live"],
                live_master_enabled,
                "SKIP_LOW_FUNDS",
            )
            continue
        if not has_strategy_trade_capacity(
            strategy_id, 2, settings["max_trades_per_hour"]
        ):
            log_candle_decision("SKIPPED", "rate_limited")
            log_paper_decision(
                strategy_id,
                current_slug,
                time_to_end,
                edge,
                settings["edge_threshold"],
                settings["is_enabled"],
                "rate_limited",
            )
            log_market_decision(
                strategy_id,
                slug_field,
                ya,
                na,
                total_ask,
                edge,
                settings["is_enabled"],
                settings["arm_live"],
                live_master_enabled,
                "SKIP_RATE_LIMIT",
            )
            continue
        side_override = signal_result.signal.lower()
        executed = await execute_strategy(
            strategy_id,
            strategy_id,
            settings,
            edge or 0.0,
            total_ask,
            ya,
            na,
            client,
            live_master_enabled,
            skip_live=force_exit_triggered,
            side_override=side_override,
        )
        log_market_decision(
            strategy_id,
            slug_field,
            ya,
            na,
            total_ask,
            edge,
            settings["is_enabled"],
            settings["arm_live"],
            live_master_enabled,
            "ENTER_LIVE"
            if executed and live_master_enabled and settings["arm_live"]
            else "ENTER_PAPER"
            if executed
            else "SKIP_OTHER",
        )
        log_candle_decision("EXECUTED" if executed else "FAILED", None if executed else "execute_failed")


def _reason_to_result(reason: str) -> str:
    if reason == "below_threshold":
        return "SKIP_EDGE"
    if reason == "rate_limited":
        return "SKIP_RATE_LIMIT"
    if reason == "other":
        return "SKIP_OTHER"
    return "SKIP_OTHER"


def normalize_list_field(entry, key):
    value = entry.get(key)
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass
    if value is None:
        return []
    return [value]


def extract_event_payload(event):
    outcomes = normalize_list_field(event, "outcomes")
    clob_ids = normalize_list_field(event, "clobTokenIds")
    if outcomes and clob_ids:
        return outcomes, clob_ids

    for market in event.get("markets") or []:
        market_outcomes = normalize_list_field(market, "outcomes")
        market_clob = normalize_list_field(market, "clobTokenIds")
        if market_outcomes and market_clob:
            return market_outcomes, market_clob

    return None, None


# =============================================================================
# BTC-SPECIFIC: GAMMA API — MARKET DISCOVERY
# =============================================================================
# fetch_event_by_slug_sync / fetch_event_by_slug_async call the Polymarket
# Gamma API to resolve a BTC slug → outcomes + clobTokenIds.
#
# The Gamma API call itself is generic and reusable.
# The BTC coupling is in HOW the slug is constructed (timestamp suffix) and
# HOW the outcomes are mapped ("up"→YES, "down"→NO) in rotate_loop.
#
# COPY-TRADE HOOK: For copy-trading, fetch_event_by_slug_sync is still useful
#                  for looking up target market token IDs from a slug stored in
#                  Supabase. The caller changes; the function can stay.
# =============================================================================

def fetch_event_by_slug_sync(slug):
    if not slug:
        return None
    base = "https://gamma-api.polymarket.com"
    endpoints = [
        f"{base}/events?slug={parse.quote(slug)}",
        f"{base}/events/slug/{parse.quote(slug)}",
    ]

    for url in endpoints:
        try:
            req = request.Request(url, headers={"User-Agent": "FastLoopWorker/1.0"})
            with request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
        except Exception:
            continue

        event = None
        if isinstance(data, list) and data:
            event = data[0]
        elif isinstance(data, dict):
            event = data

        if not event:
            continue

        outcomes, clob_ids = extract_event_payload(event)
        if outcomes and clob_ids:
            return {"slug": slug, "outcomes": outcomes, "clobTokenIds": clob_ids}

    return None


async def fetch_event_by_slug_async(slug):
    return await asyncio.to_thread(fetch_event_by_slug_sync, slug)


def update_shared_paper_balance(pnl_usd: float, strategy_id: str) -> float:
    if not PAPER_BANKROLL_SHARED_ENABLED:
        return 0.0
    global shared_paper_balance_cache, shared_paper_balance_ts
    base_balance = get_shared_paper_balance()
    if base_balance is None:
        return 0.0
    new_balance = base_balance + pnl_usd
    try:
        supabase.table("bot_settings").update(
            {"paper_balance_usd": new_balance}
        ).eq("bot_id", "default").execute()
        shared_paper_balance_cache = new_balance
        shared_paper_balance_ts = time()
        logging.info(
            "PAPER_BANKROLL_UPDATE strategy=%s pnl_usd=%s new_shared_balance=%s",
            strategy_id,
            pnl_usd,
            new_balance,
        )
    except Exception:
        logging.exception("Failed updating shared paper_balance_usd")
    return new_balance

# BTC-SPECIFIC: Constructs a BTC 5-min slug from a Unix timestamp.
# e.g. slug_from_start(1713000000) → "btc-updown-5m-1713000000"
def slug_from_start(target_start):
    return f"{MARKET_SLUG_PREFIX}-{target_start}"


# BTC-SPECIFIC: Fetches BTC spot price from Coinbase.
# COPY-TRADE HOOK: Remove this function entirely for copy-trading.
def _fetch_btc_spot_price_sync() -> float | None:
    try:
        req = request.Request(COINBASE_SPOT_URL, headers={"User-Agent": "FastLoopWorker/1.0"})
        with request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        amount = data.get("data", {}).get("amount")
        return float(amount) if amount is not None else None
    except Exception:
        logging.exception("Failed fetching BTC spot price")
        return None


async def fetch_btc_spot_price() -> float | None:
    return await asyncio.to_thread(_fetch_btc_spot_price_sync)


def get_live_balance_usd(client: ClobClient | None) -> float | None:
    try:
        resp = (
            supabase.table("bot_settings")
            .select("live_balance_usd")
            .eq("bot_id", LIVE_MASTER_BOT_ID)
            .limit(1)
            .execute()
        )
        row = (resp.data or [None])[0]
        value = float_or_none(row.get("live_balance_usd")) if row else None
        return value or 0.0
    except Exception as exc:
        logging.warning("LIVE_BALANCE_FETCH_FAIL err=%s", exc)
        return 0.0


async def live_balance_loop(client: ClobClient | None):
    while True:
        try:
            sync_live_bankroll(client)
        except Exception:
            logging.exception("LIVE_BANKROLL_LOOP_FAIL")
        await asyncio.sleep(60)


async def scan_loop():
    while True:
        prefix = current_prefix or (MARKET_SLUG_PREFIXES[0] if MARKET_SLUG_PREFIXES else MARKET_SLUG_PREFIX)
        slug = current_slug or "none"
        ya = best_quotes["yes"]["ask"]
        na = best_quotes["no"]["ask"]
        total = (ya + na) if (ya is not None and na is not None) else None
        edge = (1.0 - total) if (total is not None) else None
        reason = ""
        if ya is None or na is None:
            reason = "prices_n/a"
        payload = {
            "bot_id": BOT_ID,
            "market": "FASTLOOP",
            "market_slug": slug,
            "side": "SYSTEM",
            "price": total,
            "size": 0,
            "type": "SCAN",
            "status": "SCAN",
            "strategy_id": "SYSTEM",
            "meta": {
                "active_prefix": prefix,
                "slug": slug,
                "ya": ya,
                "na": na,
                "total": total,
                "edge": edge,
                "reason": reason,
                "timestamp": utc_now_iso(),
            },
        }
        try:
            supabase.table("bot_trades").insert(payload).execute()
        except Exception:
            logging.exception("Failed inserting SCAN bot_trades row")
        await asyncio.sleep(60)


def get_paper_bot_id(strategy_id: str) -> str:
    return STRATEGY_TO_BOT_ID.get(strategy_id, BOT_ID)


async def has_open_paper_position_for_strategy(market_slug: str | None, strategy_id: str, bot_id: str) -> bool:
    if not market_slug:
        return False
    try:
        resp = (
            supabase.table("paper_positions")
            .select("id")
            .eq("bot_id", bot_id)
            .eq("market_slug", market_slug)
            .eq("strategy_id", strategy_id)
            .eq("status", "OPEN")
            .limit(1)
            .execute()
        )
        return bool(resp.data)
    except Exception:
        logging.exception(
            "Failed checking open paper_positions for strategy=%s slug=%s",
            strategy_id,
            market_slug,
        )
        return True


async def insert_paper_position_row(
    bot_id: str,
    strategy_id: str,
    market_slug: str,
    side: str,
    entry_price: float,
    size_usd: float,
    shares: float,
    start_ts: int,
) -> tuple[bool, str | None, str | None]:
    end_ts = start_ts + (current_interval_seconds or INTERVAL_SECONDS)
    payload = {
        "bot_id": bot_id,
        "strategy_id": strategy_id,
        "market_slug": market_slug,
        "side": side,
        "entry_price": entry_price,
        "size_usd": size_usd,
        "shares": shares,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "status": "OPEN",
    }
    if "meta" in payload:
        payload.pop("meta", None)
    start_price_at_open = await fetch_btc_spot_price()
    if start_price_at_open is not None:
        payload["start_price"] = start_price_at_open
    logging.info(
        "PAPER_INSERT keys=%s bot_id=%s strategy_id=%s slug=%s side=%s entry_price=%s size_usd=%s shares=%s",
        sorted(payload.keys()),
        bot_id,
        strategy_id,
        market_slug,
        side,
        entry_price,
        size_usd,
        shares,
    )
    try:
        resp = supabase.table("paper_positions").insert(payload).execute()
        row_id = None
        if resp and getattr(resp, "data", None):
            first_row = resp.data[0]
            if isinstance(first_row, dict):
                row_id = first_row.get("id")
        logging.info(
            "PAPER_OPEN ok=True id=%s bot_id=%s strategy_id=%s slug=%s end_ts=%s",
            row_id or "unknown",
            bot_id,
            strategy_id,
            market_slug,
            end_ts,
        )
        return True, row_id, None
    except Exception as exc:
        error_text = str(exc)
        logging.info(
            "PAPER_OPEN ok=False error=%s keys=%s bot_id=%s strategy_id=%s slug=%s",
            error_text,
            sorted(payload.keys()),
            bot_id,
            strategy_id,
            market_slug,
        )
        logging.exception(
            "Failed inserting paper_positions row for strategy_id=%s slug=%s",
            strategy_id,
            market_slug,
        )
        return False, None, error_text


async def create_paper_strategy_position(
    strategy_id: str,
    action_label: str,
    edge: float | None,
    ya: float | None,
    na: float | None,
    total_ask: float | None,
    size_usd: float,
    shares: float,
    mode: str,
    side_override: str | None = None,
) -> None:
    if not current_slug:
        logging.warning("Missing slug for strategy %s paper decision", strategy_id)
        return

    strategy_bot_id = get_paper_bot_id(strategy_id)
    if await has_open_paper_position_for_strategy(current_slug, strategy_id, strategy_bot_id):
        logging.info(
            "Skipping new paper_position since one is already open slug=%s strategy_id=%s",
            current_slug,
            strategy_id,
        )
        return

    if ya is None or na is None:
        return

    paper_side = side_override if side_override in ("yes", "no") else ("yes" if ya <= na else "no")
    entry_price = ya if paper_side == "yes" else na
    start_ts = slug_start_timestamp(current_slug)
    if entry_price is None or entry_price <= 0 or start_ts is None:
        logging.warning(
            "Skipping paper_positions insert slug=%s entry_price=%s start_ts=%s",
            current_slug,
            entry_price,
            start_ts,
        )
        return

    meta = {
        **meta_template(edge, ya, na),
        "slug": current_slug,
        "action": action_label,
        "mode": mode,
        "strategy_id": strategy_id,
    }

    bot_id_override = get_paper_bot_id(strategy_id)
    paper_payload = {
        "bot_id": bot_id_override,
        "market": "FASTLOOP",
        "market_slug": current_slug,
        "side": "BUY_BOTH",
        "price": total_ask,
        "size": size_usd,
        "status": "PAPER_DECISION",
        "strategy_id": strategy_id,
        "meta": meta,
    }

    try:
        supabase.table("bot_trades").insert(paper_payload).execute()
        logging.info(
            "ACTIVITY_WRITE strategy=%s bot_id=%s status=PAPER_DECISION slug=%s side=%s size=%s",
            strategy_id, bot_id_override, current_slug, paper_side, size_usd,
        )
    except Exception:
        logging.exception("Failed inserting PAPER_DECISION for strategy %s", strategy_id)
        logging.info(
            "ACTIVITY_WRITE strategy=%s bot_id=%s status=PAPER_DECISION_FAILED slug=%s",
            strategy_id, bot_id_override, current_slug,
        )
        return

    await insert_paper_position_row(
        bot_id_override,
        strategy_id,
        current_slug,
        paper_side,
        entry_price,
        size_usd,
        shares,
        start_ts,
    )
    if strategy_id in CANDLE_STRATEGY_IDS:
        logging.info(
            "CANDLE_PAPER_ENTRY strategy=%s slug=%s side=%s size=%s shares=%s",
            strategy_id,
            current_slug,
            paper_side,
            size_usd,
            shares,
        )


def fetch_bot_settings_row() -> dict[str, object] | None:
    global HAS_PAPER_START_BALANCE_COLUMN
    columns = ["paper_balance_usd", "paper_pnl_usd"]
    include_start = HAS_PAPER_START_BALANCE_COLUMN is not False
    if include_start:
        columns.append("paper_start_balance_usd")

    col_str = ",".join(columns)
    try:
        resp = (
            supabase.table("bot_settings")
            .select(col_str)
            .eq("bot_id", BOT_ID)
            .limit(1)
            .execute()
        )
        if include_start:
            HAS_PAPER_START_BALANCE_COLUMN = True
        return (resp.data or [None])[0]
    except Exception:
        if include_start:
            HAS_PAPER_START_BALANCE_COLUMN = False
            try:
                resp = (
                    supabase.table("bot_settings")
                    .select("paper_balance_usd, paper_pnl_usd")
                    .eq("bot_id", BOT_ID)
                    .limit(1)
                    .execute()
                )
                return (resp.data or [None])[0]
            except Exception:
                logging.exception("Failed reading bot_settings without start balance")
                return None
        logging.exception("Failed reading bot_settings")
        return None


def summation_columns_for_closed_paper_pnl() -> float | None:
    try:
        resp = (
            supabase.table("paper_positions")
            .select("pnl_usd")
            .eq("bot_id", BOT_ID)
            .eq("status", "CLOSED")
            .execute()
        )
        data = resp.data or []
        return sum(float_or_none(row.get("pnl_usd")) or 0.0 for row in data)
    except Exception:
        logging.exception("Failed summing closed paper_positions pnl")
        return None


def update_paper_settings_from_positions() -> None:
    pnl_total = summation_columns_for_closed_paper_pnl()
    if pnl_total is None:
        return

    settings_row = fetch_bot_settings_row()
    start_balance = None
    if settings_row:
        if HAS_PAPER_START_BALANCE_COLUMN and "paper_start_balance_usd" in settings_row:
            start_balance = float_or_none(settings_row.get("paper_start_balance_usd"))
        if start_balance is None:
            start_balance = float_or_none(settings_row.get("paper_balance_usd"))

    if start_balance is None:
        start_balance = DEFAULT_PAPER_START_BALANCE

    new_balance = start_balance + pnl_total
    payload = {
        "paper_balance_usd": new_balance,
        "paper_pnl_usd": pnl_total,
    }
    if HAS_PAPER_START_BALANCE_COLUMN:
        payload["paper_start_balance_usd"] = start_balance

    try:
        if settings_row:
            supabase.table("bot_settings").update(payload).eq("bot_id", BOT_ID).execute()
        else:
            supabase.table("bot_settings").insert({"bot_id": BOT_ID, **payload}).execute()
    except Exception:
        logging.exception("Failed updating bot_settings after paper settlement")


def update_bot_settings_with_realized_pnl(bot_id: str, realized_pnl: float) -> None:
    try:
        resp = (
            supabase.table("bot_settings")
            .select("paper_balance_usd, paper_pnl_usd")
            .eq("bot_id", bot_id)
            .limit(1)
            .execute()
        )
        row = (resp.data or [None])[0]
        current_balance = float_or_none(row.get("paper_balance_usd")) if row else 0.0
        current_pnl = float_or_none(row.get("paper_pnl_usd")) if row else 0.0
        balance = (current_balance or 0.0) + realized_pnl
        pnl = (current_pnl or 0.0) + realized_pnl
        payload = {
            "paper_balance_usd": balance,
            "paper_pnl_usd": pnl,
            "updated_at": utc_now_iso(),
        }
        if row:
            supabase.table("bot_settings").update(payload).eq("bot_id", bot_id).execute()
        else:
            supabase.table("bot_settings").insert({"bot_id": bot_id, **payload}).execute()
        logging.info(
            "PAPER_BALANCE_UPDATE bot_id=%s pnl_delta=%s new_balance=%s new_pnl=%s",
            bot_id,
            realized_pnl,
            balance,
            pnl,
        )
    except Exception:
        logging.exception("Failed updating bot_settings after paper settlement for bot_id=%s", bot_id)


def slug_start_timestamp(slug: str | None) -> int | None:
    if not slug:
        return None
    try:
        return int(slug.rsplit("-", 1)[-1])
    except ValueError:
        return None


def restart_ws_task():
    global ws_task
    if ws_task:
        ws_task.cancel()
    ws_task = asyncio.create_task(market_listener())
    return ws_task


# =============================================================================
# BTC-SPECIFIC: MARKET ROTATION LOOP
# =============================================================================
# rotate_loop cycles through MARKET_SLUG_PREFIXES, constructs timestamp-based
# slugs (e.g. "btc-updown-5m-1713000000"), fetches the Gamma API to resolve
# outcomes, and maps "up"→YES / "down"→NO token IDs.
#
# This entire loop is BTC 5-min specific:
#   • Slug format: "{prefix}-{unix_timestamp}" is a BTC-only convention.
#   • Outcome names "up" and "down" are BTC prediction market specific.
#   • The interval is inferred from the slug prefix string.
#
# COPY-TRADE HOOK: Replace rotate_loop with a copy_market_config_loop() that:
#   1. Reads target market slugs from a Supabase "copy_markets" table.
#   2. Resolves each slug → (yes_token_id, no_token_id) via fetch_event_by_slug_sync.
#   3. Uses "yes"/"no" outcome names (standard for non-BTC markets).
#   4. Does NOT use timestamp suffixes or interval logic.
#   current_yes_token / current_no_token / restart_ws_task() pattern is REUSABLE.
# =============================================================================

async def rotate_loop():
    if not AUTO_ROTATE_ENABLED:
        return
    global current_slug, current_yes_token, current_no_token, rotating, current_interval_seconds, current_prefix
    current_slug = None
    prefix_index = 0
    prefixes = MARKET_SLUG_PREFIXES
    while True:
        prefix = prefixes[prefix_index]
        interval = interval_from_prefix(prefix)
        now = int(time())
        target_start = floor(now / interval) * interval
        slug = f"{prefix}-{target_start}"
        logging.info(
            "ROTATE_PREFIX i=%d/%d prefix=%s interval=%s target_start=%s slug=%s",
            prefix_index + 1,
            len(prefixes),
            prefix,
            interval,
            target_start,
            slug,
        )
        current_interval_seconds = interval
        current_prefix = prefix
        await asyncio.sleep(0)
        try:
            market = await fetch_event_by_slug_async(slug)
        except Exception:
            logging.exception("ROTATE_ERROR slug=%s", slug)
            prefix_index = (prefix_index + 1) % len(prefixes)
            await asyncio.sleep(ROTATE_POLL_SECONDS)
            continue
        found = bool(market)
        if found and slug != current_slug:
            current_slug = slug
            outcomes = [o.lower() for o in market["outcomes"]]
            clobs = market["clobTokenIds"]
            # BTC-SPECIFIC: "up" maps to YES, "down" maps to NO.
            # COPY-TRADE HOOK: Change to "yes" / "no" for non-BTC markets.
            for name, token in zip(outcomes, clobs):
                if name == "up":
                    current_yes_token = token
                elif name == "down":
                    current_no_token = token
            refresh_asset_map()
            reset_best_quotes()
            logging.info(
                "ASSET_MAP refreshed yes=%s no=%s",
                current_yes_token or "none",
                current_no_token or "none",
            )
            rotating = True
            restart_ws_task()
            logging.info(
                "ROTATED slug=%s yes=%s no=%s",
                current_slug,
                (current_yes_token[:6] + "...") if current_yes_token else "none",
                (current_no_token[:6] + "...") if current_no_token else "none",
            )
        prefix_index = (prefix_index + 1) % len(prefixes)
        await asyncio.sleep(ROTATE_POLL_SECONDS)


async def force_rotate_to_slug(prefix: str, target_start: int) -> str | None:
    global current_slug, current_yes_token, current_no_token, current_interval_seconds, current_prefix, rotating
    interval = interval_from_prefix(prefix)
    slug = f"{prefix}-{target_start}"
    try:
        market = await fetch_event_by_slug_async(slug)
    except Exception as exc:
        logging.warning("ROTATE_FORCE_ERROR slug=%s err=%s", slug, exc)
        return None
    if not market:
        logging.warning("ROTATE_FORCE_NO_MARKET slug=%s", slug)
        return None
    current_interval_seconds = interval
    current_prefix = prefix
    current_slug = slug
    outcomes = [o.lower() for o in market["outcomes"]]
    clobs = market["clobTokenIds"]
    for name, token in zip(outcomes, clobs):
        if name == "up":
            current_yes_token = token
        elif name == "down":
            current_no_token = token
    refresh_asset_map()
    reset_best_quotes()
    rotating = True
    restart_ws_task()
    logging.info("ROTATE_FORCED slug=%s start_ts=%s", slug, target_start)
    return slug



def update_best_quotes(asset_id, bid, ask):
    side = ASSET_TO_SIDE.get(asset_id)
    if not side:
        return
    if bid is not None:
        best_quotes[side]["bid"] = bid
    if ask is not None:
        best_quotes[side]["ask"] = ask


# =============================================================================
# POLYMARKET CLOB WEBSOCKET — QUOTE FEED (REUSABLE CORE)
# =============================================================================
# market_listener subscribes to any assets_ids list and populates best_quotes.
# The WS protocol is generic; the BTC coupling is only in which token IDs are
# subscribed (populated by rotate_loop via current_yes_token / current_no_token).
#
# REUSABLE: Keep market_listener and update_best_quotes unchanged.
# COPY-TRADE HOOK: Feed it the copy market token IDs instead of BTC tokens.
# =============================================================================

async def market_listener():
    base_payload = {
        "type": "market",
        "custom_feature_enabled": True,
    }

    def process_event(evt):
        try:
            if not isinstance(evt, dict):
                return
            event_type = evt.get("event_type") or evt.get("eventType") or evt.get("type")
            if event_type == "best_bid_ask":
                data = evt.get("data") if isinstance(evt.get("data"), dict) else evt
                asset_id = (
                    data.get("asset_id")
                    or data.get("assetId")
                    or data.get("token_id")
                )
                bid = float_or_none(data.get("best_bid") or data.get("bestBid"))
                ask = float_or_none(data.get("best_ask") or data.get("bestAsk"))
                update_best_quotes(asset_id, bid, ask)
            elif event_type == "book":
                asset_id = evt.get("asset_id") or evt.get("assetId")
                bids = evt.get("bids") or []
                asks = evt.get("asks") or []
                best_bid = None
                best_ask = None
                for bid_entry in bids:
                    price = float_or_none(bid_entry.get("price"))
                    if price is not None and (best_bid is None or price > best_bid):
                        best_bid = price
                for ask_entry in asks:
                    price = float_or_none(ask_entry.get("price"))
                    if price is not None and (best_ask is None or price < best_ask):
                        best_ask = price
                update_best_quotes(asset_id, best_bid, best_ask)
            elif event_type == "price_change":
                for change in evt.get("price_changes") or []:
                    asset_id = change.get("asset_id") or change.get("assetId")
                    if not asset_id:
                        continue
                    bid = float_or_none(change.get("best_bid") or change.get("bestBid"))
                    ask = float_or_none(change.get("best_ask") or change.get("bestAsk"))
                    update_best_quotes(asset_id, bid, ask)
            else:
                return
        except Exception:
            logging.exception("Error in process_event")

    while True:
        try:
            async with websockets.connect(WS_MARKET, ping_interval=20, ping_timeout=20) as ws:
                assets = [token for token in (current_yes_token, current_no_token) if token]
                payload = {**base_payload, "assets_ids": assets}
                await ws.send(json.dumps(payload))
                logging.info("WS connected")
                async for raw in ws:
                    msg = raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
                    if not msg:
                        continue
                    try:
                        payload = json.loads(msg)
                    except json.JSONDecodeError:
                        continue

                    try:
                        if isinstance(payload, list):
                            for evt in payload:
                                process_event(evt)
                        elif isinstance(payload, dict):
                            process_event(payload)
                    except Exception:
                        logging.exception("Error processing WS payload")
        except asyncio.CancelledError:
            break
        except Exception:
            logging.exception("WS error, reconnecting")
            await asyncio.sleep(2)


# =============================================================================
# CLOB CLIENT CONSTRUCTION (REUSABLE CORE)
# =============================================================================
# build_trading_client() constructs a ClobClient from env-provided credentials.
# REUSABLE: Unchanged for copy-trading.
# =============================================================================

def build_trading_client() -> ClobClient | None:
    if not HAVE_PRIVATE_KEY:
        return None
    sig = int(SIGNATURE_TYPE)
    funder = FUNDER if FUNDER else None
    client = ClobClient(HOST, key=PRIVATE_KEY, chain_id=CHAIN_ID, signature_type=sig, funder=funder)
    client.set_api_creds(client.create_or_derive_api_creds())
    address = client.get_address()
    if address:
        logging.info("BOT_WALLET address=%s", address)
        logging.info("LIVE_WALLET address=%s", address)
        persist_live_strategy_settings(address)
        derive_wallet_addresses(client)
    return client


def fmt(v):
    return f"{v:.6f}" if v is not None else "n/a"


# =============================================================================
# ORDER SUBMISSION (REUSABLE CORE)
# =============================================================================
# submit_order() is a generic CLOB order builder. It:
#   1. Reads the best quote from best_quotes for the given side_label
#   2. Fetches tick size from the client
#   3. Quantizes price and shares with Decimal arithmetic
#   4. Enforces MIN_ORDER_SHARES guard
#   5. Creates and posts a GTC limit order via ClobClient
#   6. Calls record_trade() on success
#
# REUSABLE: This function is market-agnostic. Use it unchanged for copy-trading
#            by passing the copy market token_id and desired size_usd.
# =============================================================================

def submit_order(
    client: ClobClient,
    token_id: str,
    side_label: str,
    price: float,
    edge: float,
    ya: float,
    na: float,
    trade_size: float,
    strategy_id: str | None = None,
    order_side: str = "BUY",
    suppress_error_count: bool = False,
    reason: str = "ENTRY",
):
    global consecutive_trade_errors, last_trade_error, paused_due_to_errors, last_live_order_400_body, last_live_order_ts, last_any_order_ts

    order_side_normalized = order_side.upper()
    price_decimal = None
    quote_key = "ask" if order_side_normalized == "BUY" else "bid"
    try:
        quote_value = best_quotes[side_label][quote_key]
    except KeyError:
        quote_value = None
    if quote_value is not None:
        price_decimal = Decimal(str(quote_value))
    else:
        fallback_quotes: list[Decimal] = []
        for label in ("yes", "no"):
            fallback_val = best_quotes[label][quote_key]
            if fallback_val is not None:
                fallback_quotes.append(Decimal(str(fallback_val)))
        if fallback_quotes:
            price_decimal = sum(fallback_quotes, Decimal("0")) / Decimal(len(fallback_quotes))
    if price_decimal is None or price_decimal <= 0:
        logging.warning(
            "LIVE_SKIP_NO_PRICE token_id=%s side=%s order_side=%s",
            token_id,
            side_label,
            order_side_normalized,
        )
        return False

    try:
        tick_str = client.get_tick_size(token_id)
        tick_size = Decimal(str(tick_str)) if tick_str else DEFAULT_TICK
    except Exception:
        tick_size = DEFAULT_TICK
    if tick_size <= 0:
        tick_size = DEFAULT_TICK

    if order_side_normalized == "BUY":
        price_candidate = price_decimal + tick_size
    else:
        price_candidate = price_decimal - tick_size
        if price_candidate <= 0:
            price_candidate = price_decimal
    try:
        price_q = price_candidate.quantize(tick_size, rounding=ROUND_DOWN)
    except InvalidOperation:
        price_q = price_decimal.quantize(tick_size, rounding=ROUND_DOWN)
    if price_q <= 0:
        logging.warning(
            "LIVE_SKIP_INVALID_PRICE token_id=%s price=%s tick=%s",
            token_id,
            price_q,
            tick_size,
        )
        return False

    size_decimal = Decimal(str(trade_size))
    if size_decimal <= 0:
        logging.warning("LIVE_SKIP_INVALID_SIZE token_id=%s size_usd=%s", token_id, trade_size)
        return False

    shares_raw = size_decimal / price_q if price_q != 0 else Decimal("0")
    shares_q = shares_raw.quantize(SHARE_QUANT, rounding=ROUND_DOWN)
    if shares_q <= 0:
        logging.warning(
            "LIVE_SKIP_ZERO_SHARES token_id=%s shares=%s price=%s size_usd=%s",
            token_id,
            shares_q,
            price_q,
            trade_size,
        )
        return False
    shares_q = _apply_min_shares_guard(
        client,
        token_id,
        price_q,
        shares_q,
        trade_size,
        order_side_normalized,
    )
    if shares_q is None:
        return False

    order_args = OrderArgs(
        token_id=token_id,
        price=float(price_q),
        size=float(shares_q),
        side=order_side_normalized,
    )
    logging.info(
        "LIVE_LIMIT_ARGS token_id=%s side=%s order_side=%s price=%s shares=%s size_usd=%s",
        token_id,
        side_label,
        order_side_normalized,
        price_q,
        shares_q,
        trade_size,
    )

    signed = None
    try:
        signed = client.create_order(order_args)
        resp = client.post_order(signed, OrderType.GTC)
        record_trade(
            token_id,
            side_label,
            "SUBMITTED",
            float(price_q),
            edge,
            ya,
            na,
            trade_size,
            strategy_id=strategy_id,
            response=resp,
        )
        delta = float(shares_q) if order_side_normalized == "BUY" else -float(shares_q)
        _record_live_position(token_id, delta)
        now_ts = int(time())
        order_id = _extract_order_id(resp)
        price_value = float(price_q)
        shares_value = float(shares_q)
        logging.info(
            "LIVE_ORDER_OK token_id=%s side=%s price=%.6f shares=%.6f order_id=%s",
            token_id,
            order_side_normalized,
            price_value,
            shares_value,
            order_id,
        )
        last_live_order_ts = now_ts
        last_any_order_ts = now_ts
        missing_field = None
        if not token_id:
            missing_field = "token_id"
        elif not order_side_normalized:
            missing_field = "order_side"
        elif shares_value <= 0:
            missing_field = "shares"
        elif price_value <= 0:
            missing_field = "price"
        if missing_field:
            logging.warning(
                "LIVE_TRACKER_APPLY_SKIP reason=missing_%s token_id=%s order_id=%s",
                missing_field,
                token_id or "unknown",
                order_id,
            )
        else:
            tracker_apply_fill(
                token_id,
                order_side_normalized,
                shares_value,
                price_value,
                now_ts,
                order_id,
                strategy_id,
            )
        if strategy_id:
            live_entry_info[token_id] = {
                "entry_price": float(price_q),
                "start_ts": now_ts,
                "strategy": strategy_id,
                "side": side_label,
            }
        consecutive_trade_errors = 0
        last_trade_error = None
        return True
    except Exception as exc:
        resp = getattr(exc, "response", None)
        status = getattr(resp, "status_code", None)
        err_lower = str(exc).lower()
        if "not enough balance / allowance" in err_lower:
            resp_payload = None
            try:
                resp_payload = resp.json() if resp else None
            except Exception:
                resp_payload = None
            logging.warning(
                "LIVE_CLOSE_REJECTED token_id=%s shares=%s price=%s side=%s json=%s",
                token_id,
                float(shares_q),
                float(price_q),
                order_side_normalized,
                json.dumps(resp_payload) if resp_payload else None,
            )
            logging.warning("LIVE_SKIP_ALLOWANCE error=%s", exc)
            last_trade_error = str(exc)[:512]
            record_trade(
                token_id,
                side_label,
                "ERROR",
                float(price_q),
                edge,
                ya,
                na,
                trade_size,
                error=str(exc),
                strategy_id=strategy_id,
            )
            return False
        resp_text = None
        resp_json = None
        if resp is not None:
            try:
                resp_text = resp.text
            except Exception:
                resp_text = str(resp)
            if resp_text:
                resp_text = resp_text[:500]
            try:
                resp_json = resp.json()
            except Exception:
                resp_json = None

        signed_order = getattr(signed, "order", None)
        payload_info = {
            "token_id": token_id,
            "order_side": order_side_normalized,
            "price": float(price_q),
            "shares": float(shares_q),
        }
        if isinstance(signed_order, dict):
            payload_info.update(
                {
                    "negRisk": signed_order.get("negRisk"),
                    "tickSize": signed_order.get("tickSize"),
                    "orderType": signed_order.get("orderType"),
                    "signature_type": signed_order.get("signature_type"),
                }
            )

        if status is not None and status >= 400:
            resp_json_str = (
                json.dumps(resp_json, ensure_ascii=False) if resp_json else None
            )
            logging.warning(
                "LIVE_ORDER_400 status=%s text=%s json=%s %s",
                status,
                resp_text,
                resp_json_str,
                " ".join(
                    f"{k}={v}"
                    for k, v in payload_info.items()
                    if v is not None and v != ""
                ),
            )
            if resp_text and not last_live_order_400_body:
                last_live_order_400_body = resp_text
            if suppress_error_count:
                logging.info(
                    "LIVE_EXIT_ORDER_FAILED_NO_PAUSE token_id=%s status=%s msg=%s",
                    token_id,
                    status,
                    resp_json_str or resp_text,
                )

        if not suppress_error_count:
            consecutive_trade_errors += 1
            last_trade_error = str(exc)[:512]
            record_trade(
                token_id,
                side_label,
                "ERROR",
                float(price_q),
                edge,
                ya,
                na,
                trade_size,
                error=str(exc),
                strategy_id=strategy_id,
            )
            if consecutive_trade_errors >= MAX_CONSECUTIVE_ERRORS:
                paused_due_to_errors = True
                logging.warning("Paused due to consecutive trade errors=%s", consecutive_trade_errors)
        return False


# =============================================================================
# HEARTBEAT LOOP — CORE TRADING TICK  ★ PRIMARY COPY-TRADE HOOK ★
# =============================================================================
# heartbeat_loop runs every ~5s and is the main trading engine.
#
# REUSABLE PARTS (keep as-is for copy-trading):
#   • read_strategy_settings() per bot_id
#   • read_live_master_enabled()
#   • PAPER / ARM LIVE / LIVE ON / KILL_SWITCH gate logic
#   • record_heartbeat()
#   • process_paper_tpsl_positions() / evaluate_live_tpsl_positions()
#   • force-exit ladder
#
# BTC-SPECIFIC PARTS (replace for copy-trading):
#   • Edge calculation: edge = 1 - (ya + na) — BTC arbitrage signal
#   • candle_manager.observe() — BTC OHLC candle building
#   • evaluate_candle_strategies() — BTC candle pattern detection
#   • execute_strategy() / execute_live_strategy() — BTC strategy execution
#   • entry_cutoff_active / time_to_end — BTC market expiry logic
#   • ENTRY_CUTOFF_SECONDS / FORCE_EXIT_SECONDS — BTC timing assumptions
#
# COPY-TRADE HOOK — Strategy body replacement:
#   Replace the block guarded by "if paper_mode_active and edge >= threshold"
#   and "if arm_live_active and edge >= threshold" with:
#
#     target_delta = compute_wallet_delta(target_positions_prev, target_positions_now)
#     for signal in target_delta:
#         if paper mode:  create_paper_copy_position(signal)
#         if live mode:   submit_order(client, signal.token_id, signal.side, ...)
# =============================================================================

async def heartbeat_loop(client: ClobClient | None):
    global paused_due_to_max_trades, trade_triggers, rotating, last_paper_skip_ts
    global last_live_positions_snapshot_ts, last_proof_tick_ts
    global last_asset_key
    logging.info("HEARTBEAT_LOOP_OK")

    while current_slug is None:
        logging.info("CANDLE_SKIP reason=no_active_slug")
        await asyncio.sleep(1)

    while True:
        now_ts = int(time())
        sniper_settings = read_strategy_settings(STRATEGY_SNIPER_BOT_ID)
        fastloop_settings = read_strategy_settings(STRATEGY_FASTLOOP_BOT_ID)
        candle_bias_settings = read_strategy_settings(STRATEGY_CANDLE_BIAS_BOT_ID)
        candle_strategy_settings = {
            strategy_id: read_strategy_settings(STRATEGY_TO_BOT_ID[strategy_id])
            for strategy_id in CANDLE_STRATEGY_IDS
        }
        trade_mode = current_global_trade_mode()
        live_master_enabled = read_live_master_enabled()
        candle_strategy_enabled = any(
            settings["is_enabled"] for settings in candle_strategy_settings.values()
        )
        is_enabled_combined = (
            sniper_settings["is_enabled"]
            or fastloop_settings["is_enabled"]
            or candle_bias_settings["is_enabled"]
            or candle_strategy_enabled
        )
        mode = fastloop_settings["mode"]
        edge_threshold = min(sniper_settings["edge_threshold"], fastloop_settings["edge_threshold"])
        arm_live_active = (
            sniper_settings["arm_live"]
            or fastloop_settings["arm_live"]
            or candle_bias_settings["arm_live"]
            or any(settings["arm_live"] for settings in candle_strategy_settings.values())
        )
        slug_field = current_slug or "none"
        if now_ts - last_proof_tick_ts >= 60:
            last_proof_tick_ts = now_ts
            logging.info(
                "PROOF_TICK slug=%s sniper_enabled=%s fastloop_enabled=%s candle_bias_enabled=%s",
                slug_field,
                sniper_settings["is_enabled"],
                fastloop_settings["is_enabled"],
                candle_bias_settings["is_enabled"],
            )
            logging.info(
                "CANDLE_BIAS_SETTINGS bot_id=%s is_enabled=%s arm_live=%s direction_mode=%s bias_side=%s",
                STRATEGY_CANDLE_BIAS_BOT_ID,
                candle_bias_settings["is_enabled"],
                candle_bias_settings["arm_live"],
                candle_bias_settings.get("direction_mode", "normal"),
                candle_bias_settings.get("bias_side", "yes"),
            )
        if now_ts - last_live_order_ts > STUCK_LIVE_SECONDS:
            logging.warning(
                "STUCK_DETECTOR_LIVE no_live_orders_for_s=%s live_master_enabled=%s armed_sniper=%s armed_fastloop=%s slug=%s",
                now_ts - last_live_order_ts,
                live_master_enabled,
                sniper_settings["arm_live"],
                fastloop_settings["arm_live"],
                slug_field,
            )
        if now_ts - last_any_order_ts > STUCK_ANY_SECONDS:
            logging.warning(
                "STUCK_DETECTOR_ANY no_orders_for_s=%s live_master_enabled=%s armed_sniper=%s armed_fastloop=%s slug=%s",
                now_ts - last_any_order_ts,
                live_master_enabled,
                sniper_settings["arm_live"],
                fastloop_settings["arm_live"],
                slug_field,
            )
        if (
            live_master_enabled
            and arm_live_active
            and client
            and (now_ts - last_live_positions_snapshot_ts >= 60)
        ):
            last_live_positions_snapshot_ts = now_ts
            get_live_positions_snapshot(client)

        ya = best_quotes["yes"]["ask"]
        na = best_quotes["no"]["ask"]
        yb = best_quotes["yes"]["bid"]
        nb = best_quotes["no"]["bid"]

        total_ask = (ya + na) if (ya is not None and na is not None) else None
        edge = (1.0 - total_ask) if (total_ask is not None) else None
        mid_price = approx_mid_price()
        if not current_slug and last_asset_key:
            candle_manager.force_close(last_asset_key)
            last_asset_key = None
        asset_key = asset_key_from_slug(current_slug) if current_slug else None
        if asset_key and last_asset_key and asset_key != last_asset_key:
            candle_manager.force_close(last_asset_key)
        if rotating and asset_key:
            candle_manager.force_close(asset_key)
        last_asset_key = asset_key
        if asset_key and current_slug:
            logging.info(
                "CANDLE_ACTIVE slug=%s asset_key=%s",
                current_slug,
                asset_key,
            )
            candle_manager.observe(
                asset_key, current_slug, mid_price, now_ts, current_interval_seconds
            )
            candle_manager.log_status()
        else:
            logging.info("CANDLE_SKIP reason=no_active_slug")
        start_ts = slug_start_timestamp(current_slug) if current_slug else None
        time_to_end = (
            (start_ts + current_interval_seconds) - now_ts if start_ts is not None else None
        )
        entry_cutoff_active = (
            time_to_end is not None and time_to_end <= ENTRY_CUTOFF_SECONDS
        )
        process_paper_tpsl_positions(now_ts)
        await evaluate_live_tpsl_positions(client, now_ts)
        force_exit_triggered = False
        force_exit_ok = 0
        force_exit_fail = 0
        if (
            live_master_enabled
            and client
            and current_slug
            and (
                sniper_settings["arm_live"]
                or fastloop_settings["arm_live"]
                or candle_bias_settings["arm_live"]
                or any(settings["arm_live"] for settings in candle_strategy_settings.values())
            )
            and time_to_end is not None
        ):
            if should_force_exit(time_to_end, FORCE_EXIT_SECONDS):
                    signer = live_signer_address or live_funder_address
                    positions_truth = get_live_positions_truth(
                        client, signer, purpose="force_exit"
                    )
                    if not positions_truth:
                        logging.info(
                            "LIVE_FORCE_EXIT_SKIP_NO_POSITIONS slug=%s time_to_end=%s",
                            current_slug,
                            time_to_end,
                        )
                        continue
                    force_exit_triggered = True
                    logging.info(
                        "LIVE_FORCE_EXIT_TRIGGER slug=%s time_to_end=%s",
                        current_slug,
                        time_to_end,
                    )
                    for token_id, shares in positions_truth.items():
                        if shares <= 0.01:
                            logging.info(
                                "LIVE_FORCE_EXIT_SKIP_NO_POSITIONS token_id=%s shares=%s",
                                token_id,
                                shares,
                            )
                            continue
                        try:
                            ok = await close_live_position_ladder(
                                client,
                                token_id,
                                shares,
                                base_price=best_quotes.get(
                                    ASSET_TO_SIDE.get(token_id, "yes"), {}
                                ).get("bid"),
                                reason="FORCE_EXIT",
                            )
                        except Exception as exc:
                            logging.warning(
                                "LIVE_EXIT_ERROR token_id=%s err=%s",
                                token_id,
                                exc,
                            )
                            ok = False
                        if ok:
                            force_exit_ok += 1
                        else:
                            force_exit_fail += 1
                    logging.info(
                        "LIVE_FORCE_EXIT_RESULT slug=%s ok_count=%s fail_count=%s",
                        current_slug,
                        force_exit_ok,
                        force_exit_fail,
                    )

        status = "ENABLED" if is_enabled_combined else "DISABLED"
        msg = (
            f"ya={fmt(ya)} na={fmt(na)} total={fmt(total_ask)} edge={fmt(edge)} "
            f"sniper_enabled={sniper_settings['is_enabled']} fastloop_enabled={fastloop_settings['is_enabled']} "
            f"candle_bias_enabled={candle_bias_settings['is_enabled']}"
        )

        if rotating:
            status = "PAUSED_ROTATING"
            msg = msg + " rotating"

        if not rotating and paused_due_to_errors:
            status = "PAUSED_ERRORS"
            msg = msg + f" last_error={last_trade_error or 'unknown'}"

        if trade_triggers >= MAX_RUNTIME_TRADES:
            paused_due_to_max_trades = True

        if not rotating and paused_due_to_max_trades:
            status = "PAUSED_MAX_TRADES"
            msg = msg + f" trade_triggers={trade_triggers}"

        if (
            not rotating
            and not HAVE_PRIVATE_KEY
            and is_enabled_combined
            and edge is not None
            and edge >= edge_threshold
        ):
            status = "PAUSED_NO_PRIVATE_KEY"

        slug_field = current_slug or "none"
        msg_with_slug = f"{msg} slug={slug_field}"
        logging.info("%s %s", status, msg_with_slug)
        record_heartbeat(status, msg_with_slug)

        if (
            is_enabled_combined
            and edge is not None
            and edge >= edge_threshold
        ):
            record_opportunity(total_ask, edge, ya, yb, na, nb)

        paper_mode_active = (
            is_enabled_combined
            and (
                (sniper_settings["mode"] == "PAPER")
                or (fastloop_settings["mode"] == "PAPER")
                or (candle_bias_settings["mode"] == "PAPER")
                or any(
                    settings["mode"] == "PAPER"
                    for settings in candle_strategy_settings.values()
                )
                or KILL_SWITCH
            )
        )
        if (
            paper_mode_active
            and edge is not None
            and edge < edge_threshold
            and now_ts - last_paper_skip_ts >= current_interval_seconds
        ):
            paper_skip_payload = {
                "bot_id": BOT_ID,
                "market": "FASTLOOP",
                "market_slug": current_slug,
                "side": "SKIP",
                "price": total_ask,
                "size": 0,
                "status": "PAPER_DECISION",
                "meta": {
                    **meta_template(edge, ya, na),
                    "threshold": edge_threshold,
                    "mode": mode,
                },
            }
            try:
                supabase.table("bot_trades").insert(paper_skip_payload).execute()
            except Exception:
                logging.exception("Failed inserting PAPER_DECISION skip")
            last_paper_skip_ts = now_ts

        slug_field = current_slug or "none"
        candle_strategy_condition = (
            current_slug
            and asset_key
            and candle_strategy_enabled
            and ya is not None
            and na is not None
            and not paused_due_to_errors
            and not paused_due_to_max_trades
            and current_yes_token
            and current_no_token
            and not entry_cutoff_active
        )
        trading_condition = (
            is_enabled_combined
            and edge is not None
            and ya is not None
            and na is not None
            and edge >= edge_threshold
            and not paused_due_to_errors
            and not paused_due_to_max_trades
            and current_yes_token
            and current_no_token
            and not rotating
        )

        if not trading_condition:
            gate_reasons = []
            if not is_enabled_combined:
                gate_reasons.append("all_disabled")
            if edge is None:
                gate_reasons.append("no_edge")
            elif edge < edge_threshold:
                gate_reasons.append("edge_below_threshold")
            if ya is None or na is None:
                gate_reasons.append("quotes_missing")
            if paused_due_to_errors:
                gate_reasons.append("paused_errors")
            if paused_due_to_max_trades:
                gate_reasons.append("paused_max_trades")
            if not current_yes_token or not current_no_token:
                gate_reasons.append("no_tokens")
            if rotating:
                gate_reasons.append("rotating")
            logging.info(
                "STRATEGY_GATE status=skip strategy=legacy reason=%s",
                ",".join(gate_reasons) if gate_reasons else "unknown",
            )

        if trading_condition:
            logging.info(
                "STRATEGY_GATE status=pass strategy=legacy reason=ok",
            )
            sniper_traded = False
            sniper_time_to_end = time_to_end or 0
            sniper_can_trade, sniper_reason = get_paper_trade_decision_reason(
                sniper_settings, STRATEGY_SNIPER, edge
            )
            if sniper_can_trade:
                if entry_cutoff_active:
                    logging.info(
                        "ENTRY_CUTOFF_SKIP mode=PAPER strategy=%s time_to_end=%s",
                        STRATEGY_SNIPER,
                        sniper_time_to_end,
                    )
                    log_paper_decision(
                        STRATEGY_SNIPER,
                        current_slug,
                        time_to_end,
                        edge,
                        edge_threshold,
                        sniper_settings["is_enabled"],
                        "entry_cutoff",
                    )
                    log_market_decision(
                        STRATEGY_SNIPER,
                        slug_field,
                        ya,
                        na,
                        total_ask,
                        edge,
                        sniper_settings["is_enabled"],
                        sniper_settings["arm_live"],
                        live_master_enabled,
                        "SKIP_EDGE",
                    )
                elif should_skip_low_funds(sniper_settings["paper_balance_usd"]):
                    log_paper_decision(
                        STRATEGY_SNIPER,
                        current_slug,
                        time_to_end,
                        edge,
                        edge_threshold,
                        sniper_settings["is_enabled"],
                        "low_funds",
                    )
                    log_market_decision(
                        STRATEGY_SNIPER,
                        slug_field,
                        ya,
                        na,
                        total_ask,
                        edge,
                        sniper_settings["is_enabled"],
                        sniper_settings["arm_live"],
                        live_master_enabled,
                        "SKIP_LOW_FUNDS",
                    )
                    pass
                else:
                    sniper_traded = await execute_strategy(
                        STRATEGY_SNIPER,
                        "SNIPER",
                        sniper_settings,
                        edge,
                        total_ask,
                        ya,
                        na,
                        client,
                        live_master_enabled,
                        skip_live=force_exit_triggered,
                    )
                    log_market_decision(
                        STRATEGY_SNIPER,
                        slug_field,
                        ya,
                        na,
                        total_ask,
                        edge,
                        sniper_settings["is_enabled"],
                        sniper_settings["arm_live"],
                        live_master_enabled,
                        "ENTER_LIVE"
                        if sniper_traded and live_master_enabled and sniper_settings["arm_live"]
                        else "ENTER_PAPER",
                    )
            else:
                log_paper_decision(
                    STRATEGY_SNIPER,
                    current_slug,
                    time_to_end,
                    edge,
                    edge_threshold,
                    sniper_settings["is_enabled"],
                    sniper_reason,
                )
                log_market_decision(
                    STRATEGY_SNIPER,
                    slug_field,
                    ya,
                    na,
                    total_ask,
                    edge,
                    sniper_settings["is_enabled"],
                    sniper_settings["arm_live"],
                    live_master_enabled,
                    _reason_to_result(sniper_reason),
                )
            if sniper_traded and trade_mode == "ONE":
                logging.info(
                    "TRADE_MODE_BLOCK trade_mode=ONE blocked_strategy=%s by_strategy=%s slug=%s",
                    STRATEGY_FASTLOOP,
                    STRATEGY_SNIPER,
                    current_slug or "none",
                )
                log_paper_decision(
                    STRATEGY_FASTLOOP,
                    current_slug,
                    time_to_end,
                    edge,
                    edge_threshold,
                    fastloop_settings["is_enabled"],
                    "trade_mode_blocked",
                )
            else:
                fast_time_to_end = time_to_end or 0
                fastloop_can_trade, fastloop_reason = get_paper_trade_decision_reason(
                    fastloop_settings, STRATEGY_FASTLOOP, edge
                )
                if fastloop_can_trade:
                    if entry_cutoff_active:
                        logging.info(
                            "ENTRY_CUTOFF_SKIP mode=PAPER strategy=%s time_to_end=%s",
                            STRATEGY_FASTLOOP,
                            fast_time_to_end,
                        )
                        log_paper_decision(
                            STRATEGY_FASTLOOP,
                            current_slug,
                            time_to_end,
                            edge,
                            edge_threshold,
                            fastloop_settings["is_enabled"],
                            "entry_cutoff",
                        )
                        log_market_decision(
                            STRATEGY_FASTLOOP,
                            slug_field,
                            ya,
                            na,
                            total_ask,
                            edge,
                            fastloop_settings["is_enabled"],
                            fastloop_settings["arm_live"],
                            live_master_enabled,
                            "SKIP_EDGE",
                        )
                    elif should_skip_low_funds(fastloop_settings["paper_balance_usd"]):
                        log_paper_decision(
                            STRATEGY_FASTLOOP,
                            current_slug,
                            time_to_end,
                            edge,
                            edge_threshold,
                            fastloop_settings["is_enabled"],
                            "low_funds",
                        )
                        log_market_decision(
                            STRATEGY_FASTLOOP,
                            slug_field,
                            ya,
                            na,
                            total_ask,
                            edge,
                            fastloop_settings["is_enabled"],
                            fastloop_settings["arm_live"],
                            live_master_enabled,
                            "SKIP_LOW_FUNDS",
                        )
                    else:
                        fastloop_executed = await execute_strategy(
                            STRATEGY_FASTLOOP,
                            "BUY_BOTH",
                            fastloop_settings,
                            edge,
                            total_ask,
                            ya,
                            na,
                            client,
                            live_master_enabled,
                            skip_live=force_exit_triggered,
                        )
                        log_market_decision(
                            STRATEGY_FASTLOOP,
                            slug_field,
                            ya,
                            na,
                            total_ask,
                            edge,
                            fastloop_settings["is_enabled"],
                            fastloop_settings["arm_live"],
                            live_master_enabled,
                            "ENTER_LIVE"
                            if fastloop_executed and live_master_enabled and fastloop_settings["arm_live"]
                            else "ENTER_PAPER",
                        )
                else:
                    log_paper_decision(
                        STRATEGY_FASTLOOP,
                        current_slug,
                        time_to_end,
                        edge,
                        edge_threshold,
                        fastloop_settings["is_enabled"],
                        fastloop_reason,
                    )
                    log_market_decision(
                        STRATEGY_FASTLOOP,
                        slug_field,
                        ya,
                        na,
                        total_ask,
                        edge,
                        fastloop_settings["is_enabled"],
                        fastloop_settings["arm_live"],
                        live_master_enabled,
                        _reason_to_result(fastloop_reason),
                    )

        # --- CANDLE_BIAS: legacy forced-side logic (no candle/OHLCV analysis) ---
        candle_bias_condition = (
            candle_bias_settings["is_enabled"]
            and ya is not None
            and na is not None
            and not paused_due_to_errors
            and not paused_due_to_max_trades
            and current_yes_token
            and current_no_token
            and not rotating
            and not entry_cutoff_active
        )
        if candle_bias_condition:
            logging.info(
                "CANDLE_BIAS_EVAL strategy=CANDLE_BIAS slug=%s bias_side=%s direction_mode=%s enabled=%s result=EVALUATING",
                slug_field,
                candle_bias_settings.get("bias_side", "yes"),
                candle_bias_settings.get("direction_mode", "normal"),
                candle_bias_settings["is_enabled"],
            )
            candle_bias_capacity = has_strategy_trade_capacity(
                STRATEGY_CANDLE_BIAS, 2, candle_bias_settings["max_trades_per_hour"]
            )
            if not candle_bias_capacity:
                logging.info(
                    "CANDLE_BIAS_SKIP strategy=%s reason=rate_limited slug=%s",
                    STRATEGY_CANDLE_BIAS,
                    slug_field,
                )
                log_market_decision(
                    STRATEGY_CANDLE_BIAS,
                    slug_field,
                    ya,
                    na,
                    total_ask,
                    edge,
                    candle_bias_settings["is_enabled"],
                    candle_bias_settings["arm_live"],
                    live_master_enabled,
                    "SKIP_RATE_LIMIT",
                )
            elif should_skip_low_funds(candle_bias_settings["paper_balance_usd"]):
                logging.info(
                    "CANDLE_BIAS_SKIP strategy=%s reason=low_funds slug=%s",
                    STRATEGY_CANDLE_BIAS,
                    slug_field,
                )
                log_market_decision(
                    STRATEGY_CANDLE_BIAS,
                    slug_field,
                    ya,
                    na,
                    total_ask,
                    edge,
                    candle_bias_settings["is_enabled"],
                    candle_bias_settings["arm_live"],
                    live_master_enabled,
                    "SKIP_LOW_FUNDS",
                )
            else:
                cb_bias_side = candle_bias_settings.get("bias_side", "yes")
                cb_direction_mode = candle_bias_settings.get("direction_mode", "normal")
                cb_final_side = cb_bias_side
                if cb_direction_mode == "reverse":
                    cb_final_side = "no" if cb_bias_side == "yes" else "yes"
                candle_bias_traded = await execute_strategy(
                    STRATEGY_CANDLE_BIAS,
                    "CANDLE_BIAS",
                    candle_bias_settings,
                    edge or 0.0,
                    total_ask,
                    ya,
                    na,
                    client,
                    live_master_enabled,
                    skip_live=force_exit_triggered,
                    side_override=cb_final_side,
                )
                log_market_decision(
                    STRATEGY_CANDLE_BIAS,
                    slug_field,
                    ya,
                    na,
                    total_ask,
                    edge,
                    candle_bias_settings["is_enabled"],
                    candle_bias_settings["arm_live"],
                    live_master_enabled,
                    "ENTER_LIVE"
                    if candle_bias_traded and live_master_enabled and candle_bias_settings["arm_live"]
                    else "ENTER_PAPER" if candle_bias_traded
                    else "SKIP_OTHER",
                )

        if candle_strategy_condition:
            logging.info(
                "CANDLE_GATE status=pass strategy=candle_strategies slug=%s asset_key=%s closed_candles=%s",
                current_slug or "none",
                asset_key or "none",
                candle_manager.closed_count(asset_key),
            )
            await evaluate_candle_strategies(
                candle_strategy_settings,
                total_ask,
                edge,
                ya,
                na,
                client,
                live_master_enabled,
                entry_cutoff_active,
                force_exit_triggered,
                time_to_end,
                current_slug,
                asset_key,
            )
        else:
            skip_reasons = []
            if not current_slug:
                skip_reasons.append("no_slug")
            if not asset_key:
                skip_reasons.append("no_asset_key")
            if not candle_strategy_enabled:
                skip_reasons.append("all_candle_strategies_disabled")
            if ya is None or na is None:
                skip_reasons.append("quotes_missing")
            if paused_due_to_errors:
                skip_reasons.append("paused_errors")
            if paused_due_to_max_trades:
                skip_reasons.append("paused_max_trades")
            if not current_yes_token or not current_no_token:
                skip_reasons.append("no_tokens")
            if entry_cutoff_active:
                skip_reasons.append("entry_cutoff")
            logging.info(
                "CANDLE_GATE status=skip strategy=candle_strategies reason=%s slug=%s asset_key=%s",
                ",".join(skip_reasons) if skip_reasons else "unknown",
                current_slug or "none",
                asset_key or "none",
            )

        await asyncio.sleep(5)
        if rotating:
            rotating = False


# =============================================================================
# PAPER SETTLEMENT LOOP (REUSABLE CORE)
# =============================================================================
# Polls paper_positions every 15s. Closes expired positions by resolving
# YES/NO outcome, computing PnL, updating paper_positions and bot_trades.
#
# REUSABLE: The settlement mechanics (Supabase read/write, PnL calc, balance
#            update) work for any binary market. The "FASTLOOP" market label
#            in the trade_payload is the only cosmetic BTC-ism; update it.
# =============================================================================

async def paper_settlement_loop():
    while True:
        now_ts = int(time())
        try:
            resp = (
                supabase.table("paper_positions")
                .select(
                    "id, bot_id, market_slug, side, shares, size_usd, start_price, strategy_id",
                )
                .in_(
                    "bot_id",
                    [
                        STRATEGY_FASTLOOP_BOT_ID,
                        STRATEGY_SNIPER_BOT_ID,
                        STRATEGY_CANDLE_BIAS_BOT_ID,
                        STRATEGY_SWEEP_RECLAIM_BOT_ID,
                        STRATEGY_BREAKOUT_CLOSE_BOT_ID,
                        STRATEGY_ENGULFING_LEVEL_BOT_ID,
                        STRATEGY_REJECTION_WICK_BOT_ID,
                        STRATEGY_FOLLOW_THROUGH_BOT_ID,
                        BOT_ID,
                    ],
                )
                .eq("status", "OPEN")
                .lte("end_ts", now_ts)
                .execute()
            )
            rows = resp.data or []
        except Exception:
            logging.exception("Failed querying OPEN paper_positions")
            rows = []

        if rows:
            logging.info(
                "SETTLEMENT_PENDING open_positions=%d",
                len(rows),
            )

        for row in rows:
            row_id = row.get("id")
            if not row_id:
                continue
            bot_id = row.get("bot_id") or BOT_ID
            market_slug = row.get("market_slug")
            row_side = (row.get("side") or "").lower()
            strategy_id = row.get("strategy_id")
            shares = float_or_none(row.get("shares")) or 0.0
            size_usd = float_or_none(row.get("size_usd")) or 0.0
            start_price = float_or_none(row.get("start_price"))
            if start_price is None:
                start_price = await fetch_btc_spot_price()
                if start_price is not None:
                    try:
                        supabase.table("paper_positions").update(
                            {"start_price": start_price}
                        ).eq("id", row_id).execute()
                    except Exception:
                        logging.exception("Failed updating paper_positions start_price")
                else:
                    logging.warning(
                        "Skipping settlement without start_price id=%s slug=%s",
                        row_id,
                        market_slug,
                    )
                    continue

            end_price = await fetch_btc_spot_price()
            if end_price is None:
                logging.warning(
                    "Skipping settlement without end_price id=%s slug=%s",
                    row_id,
                    market_slug,
                )
                continue

            resolved_side = "yes" if end_price >= start_price else "no"
            payout_usd = shares if row_side == resolved_side else 0.0
            pnl_usd = payout_usd - size_usd
            closed_at = utc_now_iso()

            position_updates = {
                "status": "CLOSED",
                "resolved_side": resolved_side,
                "end_price": end_price,
                "pnl_usd": pnl_usd,
                "closed_at": closed_at,
            }
            try:
                supabase.table("paper_positions").update(position_updates).eq("id", row_id).execute()
            except Exception:
                logging.exception("Failed updating paper_positions row id=%s", row_id)
                continue
            else:
                update_bot_settings_with_realized_pnl(bot_id, pnl_usd)

            trade_payload = {
                "bot_id": bot_id,
                "market": "FASTLOOP",
                "market_slug": market_slug,
                "strategy_id": strategy_id,
                "side": row.get("side"),
                "price": end_price,
                "size": size_usd,
                "status": "PAPER_CLOSED",
                "meta": {
                    "timestamp": closed_at,
                    "pnl_usd": pnl_usd,
                    "start_price": start_price,
                    "end_price": end_price,
                    "resolved_side": resolved_side,
                    "shares": shares,
                    "market_slug": market_slug,
                    "strategy_id": strategy_id,
                },
            }
            try:
                supabase.table("bot_trades").insert(trade_payload).execute()
                logging.info(
                    "Closed paper_position id=%s slug=%s pnl_usd=%s",
                    row_id,
                    market_slug,
                    pnl_usd,
                )
                logging.info(
                    "PAPER_CLOSE bot_id=%s strategy_id=%s slug=%s pnl_usd=%s",
                    bot_id,
                    strategy_id,
                    market_slug,
                    pnl_usd,
                )
                logging.info(
                    "ACTIVITY_WRITE strategy=%s bot_id=%s status=PAPER_CLOSED slug=%s pnl=%s",
                    strategy_id, bot_id, market_slug, pnl_usd,
                )
                if strategy_id in CANDLE_STRATEGY_IDS:
                    logging.info(
                        "CANDLE_PAPER_SETTLEMENT strategy=%s slug=%s pnl_usd=%s resolved_side=%s",
                        strategy_id,
                        market_slug,
                        pnl_usd,
                        resolved_side,
                    )
            except Exception:
                logging.exception("Failed inserting PAPER_CLOSED bot_trades row")
                logging.info(
                    "ACTIVITY_WRITE strategy=%s bot_id=%s status=PAPER_CLOSED_FAILED slug=%s pnl=%s",
                    strategy_id, bot_id, market_slug, pnl_usd,
                )

        if rows:
            update_paper_settings_from_positions()

        await asyncio.sleep(15)


# =============================================================================
# COPY-TRADING ENGINE — PAPER MODE ONLY
# =============================================================================
#
# This section adds a fully isolated copy-trading worker path.
# It runs as a separate asyncio task alongside the existing BTC strategy tasks.
#
# ISOLATION GUARANTEE:
#   - Does NOT touch: rotate_loop, heartbeat_loop, BTC strategies, live orders,
#     bot_settings, bot_trades, paper_positions, or any BTC-specific tables.
#   - Only reads/writes: tracked_wallets, wallet_metrics, wallet_trades,
#     copy_bots, copy_attempts, copied_positions, market_cache,
#     copy_global_settings.
#
# LIVE TRADING: Not implemented. All copy bots with mode='LIVE' are skipped
#   with skip_reason='live_mode_not_supported_yet'.
#
# ENTRY POINT: copy_trade_loop() — wired into main() as a _run_forever task.
# =============================================================================


# ── Copy-trading global state ─────────────────────────────────────────────────
# In-memory per-bot trade rate tracking.
# Keyed by copy_bot UUID (str). Each value is a deque of Unix timestamps.
# Pruned to a rolling 1-hour window on each access.
# Mirrors the pattern used by BTC strategy_trade_timestamps.
copy_bot_trade_timestamps: dict[str, deque] = defaultdict(deque)


# ── Supabase loaders ──────────────────────────────────────────────────────────

def load_tracked_wallets() -> list[dict]:
    """Return all active tracked wallets from Supabase."""
    try:
        resp = (
            supabase.table("tracked_wallets")
            .select("id, wallet_address, display_name, is_active, tags")
            .eq("is_active", True)
            .execute()
        )
        return resp.data or []
    except Exception:
        logging.exception("COPY_LOAD_WALLETS_FAIL")
        return []


def load_enabled_copy_bots() -> list[dict]:
    """Return all enabled copy bots from Supabase."""
    try:
        resp = supabase.table("copy_bots").select("*").eq("is_enabled", True).execute()
        return resp.data or []
    except Exception:
        logging.exception("COPY_LOAD_BOTS_FAIL")
        return []


def load_copy_global_settings() -> dict:
    """
    Load the singleton copy_global_settings row.
    Returns conservative safe defaults on any failure so the loop
    never proceeds with an unknown global state.
    """
    defaults: dict = {
        "live_on": False,
        "emergency_stop": True,   # fail-safe: default to stopped
        "max_total_live_exposure": 500,
        "default_slippage_cap": 0.03,
        "default_position_size": 10,
        "default_max_positions": 10,
    }
    try:
        resp = (
            supabase.table("copy_global_settings")
            .select("*")
            .eq("id", 1)
            .limit(1)
            .execute()
        )
        if resp.data:
            return {**defaults, **resp.data[0]}
        logging.warning("COPY_GLOBAL_SETTINGS_MISSING using defaults")
        return defaults
    except Exception:
        logging.exception("COPY_LOAD_GLOBAL_SETTINGS_FAIL using defaults")
        return defaults


# ── Wallet trade fetching ─────────────────────────────────────────────────────

def _fetch_wallet_activity_sync(wallet_address: str, limit: int = 50) -> list[dict]:
    """
    Fetch recent activity for a wallet from the Polymarket data API.
    Returns a list of raw activity dicts; empty list on any error.

    Primary endpoint: https://data-api.polymarket.com/activity?user={address}
    Falls back to CLOB trades endpoint if primary returns nothing.
    """
    results: list[dict] = []

    # Primary: Polymarket data API
    url = (
        f"{COPY_DATA_API_BASE}/activity"
        f"?user={parse.quote(wallet_address)}&limit={limit}"
    )
    try:
        req = request.Request(url, headers={"User-Agent": "FastLoopWorker/1.0"})
        with request.urlopen(req, timeout=10) as resp:
            raw = json.loads(resp.read())
        if isinstance(raw, list) and raw:
            return raw
        if isinstance(raw, dict) and "data" in raw:
            return raw["data"] if isinstance(raw["data"], list) else []
    except Exception as exc:
        logging.warning(
            "COPY_FETCH_ACTIVITY_DATA_API_FAIL wallet=%s err=%s",
            wallet_address[:10],
            exc,
        )

    # Fallback: CLOB trades endpoint
    clob_url = (
        f"{HOST}/trades"
        f"?user_address={parse.quote(wallet_address)}&limit={limit}"
    )
    try:
        req = request.Request(clob_url, headers={"User-Agent": "FastLoopWorker/1.0"})
        with request.urlopen(req, timeout=10) as resp:
            raw = json.loads(resp.read())
        if isinstance(raw, list):
            results = raw
        elif isinstance(raw, dict) and "data" in raw:
            results = raw.get("data") or []
    except Exception as exc:
        logging.warning(
            "COPY_FETCH_ACTIVITY_CLOB_FAIL wallet=%s err=%s",
            wallet_address[:10],
            exc,
        )

    return results


async def fetch_wallet_trades_for_address(wallet_address: str) -> list[dict]:
    """Async wrapper: fetch raw wallet activity without blocking the event loop."""
    return await asyncio.to_thread(
        _fetch_wallet_activity_sync,
        wallet_address,
        COPY_WALLET_TRADE_FETCH_LIMIT,
    )


# ── Normalization helpers ─────────────────────────────────────────────────────

def _normalize_outcome(outcome_raw: str | None) -> str | None:
    """Map raw outcome string → 'YES' | 'NO' | None."""
    if not outcome_raw:
        return None
    o = str(outcome_raw).strip().upper()
    if o in ("YES", "Y", "1", "UP", "TRUE"):
        return "YES"
    if o in ("NO", "N", "0", "DOWN", "FALSE"):
        return "NO"
    return o  # preserve unknown values for inspection


def _normalize_side(side_raw: str | None) -> str | None:
    """Map raw side string → 'BUY' | 'SELL' | None."""
    if not side_raw:
        return None
    s = str(side_raw).strip().upper()
    if s in ("BUY", "B", "LONG"):
        return "BUY"
    if s in ("SELL", "S", "SHORT"):
        return "SELL"
    return s


def normalize_activity_to_wallet_trade(raw: dict, wallet_address: str) -> dict | None:
    """
    Normalize a raw Polymarket activity/trade record to the wallet_trades schema.

    Returns None if the record is not a trade or lacks enough data to be useful.
    Handles both the Polymarket data API format and the CLOB trades format.

    Fields mapped:
      Data API:  id/transactionHash → source_trade_id, market → market_slug,
                 title → market_title, conditionId → condition_id,
                 tokenId → token_id, outcome, side, price, shares → size,
                 amount → notional, timestamp → traded_at
      CLOB API:  id → source_trade_id, market → condition_id,
                 asset_id → token_id, size, price, outcome, side,
                 match_time → traded_at
    """
    # Filter: only process trade/fill events; skip order placements / cancellations
    event_type = str(raw.get("type", "TRADE")).strip().upper()
    if event_type not in ("TRADE", "FILL", "MATCH", "BUY", "SELL", ""):
        return None

    # --- source_trade_id ---
    source_trade_id = (
        raw.get("transactionHash")
        or raw.get("transaction_hash")
        or raw.get("id")
        or raw.get("trade_id")
    )
    if not source_trade_id:
        # Compose a fallback dedup key from available fields
        cid = raw.get("conditionId") or raw.get("condition_id") or raw.get("market") or ""
        ts = str(raw.get("timestamp") or raw.get("match_time") or raw.get("created_at") or "")
        side = str(raw.get("side") or "")
        amt = str(raw.get("amount") or raw.get("notional") or raw.get("usdcAmount") or "")
        composed = f"{cid}_{ts}_{side}_{amt}"
        if composed.count("_") == 3 and all(p == "" for p in composed.split("_")):
            return None  # all parts empty — not usable
        source_trade_id = composed

    # --- price / size / notional ---
    price_raw = (
        raw.get("price") or raw.get("avgPrice") or raw.get("avg_price")
    )
    size_raw = (
        raw.get("shares") or raw.get("size") or raw.get("quantity")
    )
    notional_raw = (
        raw.get("amount") or raw.get("notional") or raw.get("usdcAmount")
        or raw.get("usdc_amount")
    )

    try:
        price = float(price_raw) if price_raw is not None else None
    except (TypeError, ValueError):
        price = None

    try:
        size = float(size_raw) if size_raw is not None else None
    except (TypeError, ValueError):
        size = None

    try:
        notional = float(notional_raw) if notional_raw is not None else None
    except (TypeError, ValueError):
        notional = None

    if notional is None and price is not None and size is not None:
        notional = round(price * size, 6)

    # --- traded_at ---
    ts_raw = (
        raw.get("timestamp") or raw.get("match_time")
        or raw.get("created_at") or raw.get("createdAt")
        or raw.get("last_update")
    )
    if not ts_raw:
        return None  # require a timestamp

    try:
        if isinstance(ts_raw, (int, float)):
            ts_val = ts_raw / 1000 if ts_raw > 1e12 else ts_raw
            traded_at = datetime.fromtimestamp(ts_val, tz=timezone.utc).isoformat()
        else:
            traded_at = str(ts_raw)
    except Exception:
        traded_at = str(ts_raw)

    # --- field extraction (handles both API response shapes) ---
    market_slug = (
        raw.get("market") if not str(raw.get("market", "")).startswith("0x")
        else None
    ) or raw.get("marketSlug") or raw.get("slug")

    condition_id = (
        raw.get("conditionId") or raw.get("condition_id")
        # CLOB API stores condition_id in "market" field (hex string)
        or (raw.get("market") if str(raw.get("market", "")).startswith("0x") else None)
    )

    token_id = raw.get("tokenId") or raw.get("token_id") or raw.get("asset_id")
    outcome = _normalize_outcome(raw.get("outcome"))
    side = _normalize_side(raw.get("side"))

    return {
        "wallet_address": wallet_address,
        "source_trade_id": str(source_trade_id),
        "market_slug": market_slug,
        "market_title": raw.get("title") or raw.get("marketTitle") or raw.get("question"),
        "condition_id": condition_id,
        "token_id": token_id,
        "side": side,
        "outcome": outcome,
        "price": price,
        "size": size,
        "notional": notional,
        "traded_at": traded_at,
        "raw_json": raw,
    }


# ── Supabase write helpers ────────────────────────────────────────────────────

def insert_wallet_trade_if_new(trade_row: dict) -> bool:
    """
    Insert a wallet_trade row if (wallet_address, source_trade_id) is new.
    Returns True if inserted, False if duplicate or error.
    wallet_trades is append-only — we never update existing rows.
    """
    try:
        resp = supabase.table("wallet_trades").insert(trade_row).execute()
        return bool(resp.data)
    except Exception as exc:
        exc_str = str(exc).lower()
        if any(kw in exc_str for kw in ("duplicate", "unique", "23505", "conflict")):
            return False  # expected dedup — not an error
        logging.warning(
            "COPY_INSERT_WALLET_TRADE_FAIL wallet=%s trade_id=%s err=%s",
            str(trade_row.get("wallet_address", ""))[:10],
            str(trade_row.get("source_trade_id", ""))[:20],
            exc,
        )
        return False


def upsert_market_cache(trade_row: dict) -> None:
    """
    Upsert market_cache from a normalized wallet_trade dict.
    Populates what we know from the trade; leaves other fields as DB defaults.

    When we see the same market_slug from a YES trade and later a NO trade,
    the upserts progressively fill in yes_token_id then no_token_id.
    """
    market_slug = trade_row.get("market_slug")
    if not market_slug:
        return

    payload: dict = {"market_slug": market_slug, "raw_json": {}}

    if trade_row.get("market_title"):
        payload["market_title"] = trade_row["market_title"]
    if trade_row.get("condition_id"):
        payload["condition_id"] = trade_row["condition_id"]

    token_id = trade_row.get("token_id")
    outcome = trade_row.get("outcome")
    if token_id and outcome == "YES":
        payload["yes_token_id"] = token_id
    elif token_id and outcome == "NO":
        payload["no_token_id"] = token_id

    try:
        supabase.table("market_cache").upsert(payload, on_conflict="market_slug").execute()
    except Exception as exc:
        logging.warning("COPY_UPSERT_MARKET_CACHE_FAIL slug=%s err=%s", market_slug, exc)


def get_unevaluated_trades_for_bot(
    wallet_address: str,
    bot_id: str,
    lookback_hours: int = 24,
    limit: int = 50,
) -> list[dict]:
    """
    Return wallet_trades for this wallet that have NOT yet been evaluated
    by this specific copy_bot (i.e., no copy_attempts row exists).

    Two-step query:
      1. Fetch recent wallet_trades for the wallet within the lookback window.
      2. Fetch already-attempted source_trade_ids for this bot.
      3. Return the difference.
    """
    try:
        cutoff_ts = time() - (lookback_hours * 3600)
        cutoff = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc).isoformat()

        trades_resp = (
            supabase.table("wallet_trades")
            .select("*")
            .eq("wallet_address", wallet_address)
            .gte("traded_at", cutoff)
            .order("traded_at", desc=True)
            .limit(limit)
            .execute()
        )
        all_trades = trades_resp.data or []
        if not all_trades:
            return []

        trade_ids = [t["source_trade_id"] for t in all_trades]

        attempted_resp = (
            supabase.table("copy_attempts")
            .select("source_trade_id")
            .eq("copy_bot_id", bot_id)
            .in_("source_trade_id", trade_ids)
            .execute()
        )
        attempted_ids = {row["source_trade_id"] for row in (attempted_resp.data or [])}

        return [t for t in all_trades if t["source_trade_id"] not in attempted_ids]

    except Exception:
        logging.exception(
            "COPY_GET_UNEVALUATED_TRADES_FAIL wallet=%s bot=%s",
            wallet_address[:10],
            bot_id[:8],
        )
        return []


def get_open_positions_count(bot_id: str) -> int:
    """Count currently OPEN copied_positions for a copy bot."""
    try:
        resp = (
            supabase.table("copied_positions")
            .select("id", count="exact")
            .eq("copy_bot_id", bot_id)
            .eq("status", "OPEN")
            .execute()
        )
        return resp.count or 0
    except Exception:
        logging.warning("COPY_GET_OPEN_POS_COUNT_FAIL bot=%s", bot_id[:8])
        return 0


# =============================================================================
# COPY-TRADING LIVE EXECUTION — PAPER PILOT EXTENSION
# =============================================================================
#
# This section extends the copy_trade_loop to support live CLOB orders for
# copy bots with mode='LIVE'.
#
# ISOLATION GUARANTEE:
#   - Does NOT call record_trade(), _record_live_position(), tracker_apply_fill()
#     or any other BTC-specific function.
#   - Does NOT touch rotate_loop, scan_loop, BTC strategies, or bot_trades.
#   - Uses the same ClobClient built in main() — no parallel auth stack.
#
# SAFETY GATES (ALL must pass for any live order):
#   ENV:  COPY_LIVE_ENABLED=true
#   DB:   copy_global_settings.live_on = true
#   DB:   copy_global_settings.emergency_stop = false
#   DB:   copy_bots.arm_live = true  (per-bot arm)
#   CAP:  number of enabled LIVE bots ≤ COPY_LIVE_MAX_BOTS
#   CAP:  live open positions ≤ COPY_LIVE_MAX_OPEN_POSITIONS
#   CAP:  live trades this hour ≤ COPY_LIVE_MAX_TRADES_PER_HOUR  (global)
#   CAP:  per-trade USD ≤ COPY_LIVE_MAX_TRADE_USD  (hard clamp)
#   DATA: token_id must be present
#   DATA: source price must be valid (0 < price ≤ 1)
#
# PRICE STRATEGY (first pilot pass):
#   BUY limit price  = min(source_price × (1 + max_slippage), 0.99)
#   SELL limit price = max(source_price × (1 - max_slippage), 0.01)
#   This gives headroom to fill vs a slightly moved market while capping
#   our overpay. If market has moved too far, the limit order won't fill.
# =============================================================================


# ── Live copy global rate tracker ─────────────────────────────────────────────
# Tracks ALL live copy trades across ALL bots. Pruned to a rolling 1-hour window.
# Separate from copy_bot_trade_timestamps (which is per-bot).
copy_live_trade_timestamps: deque = deque()


def _prune_live_copy_history() -> None:
    """Prune in-memory live copy timestamps older than 1 hour."""
    cutoff = time() - 3600
    while copy_live_trade_timestamps and copy_live_trade_timestamps[0] < cutoff:
        copy_live_trade_timestamps.popleft()


def _get_live_copy_trades_this_hour() -> int:
    """Return total live copy trades placed in the last hour (all bots)."""
    _prune_live_copy_history()
    return len(copy_live_trade_timestamps)


def _mark_live_copy_trade() -> None:
    """Record a live copy trade timestamp for global rate limiting."""
    copy_live_trade_timestamps.append(time())


def get_live_open_positions_count(live_bot_ids: list[str]) -> int:
    """
    Count OPEN copied_positions that belong to LIVE-mode copy bots.

    Returns 999 on any DB failure as a fail-safe to block further live orders.
    """
    if not live_bot_ids:
        return 0
    try:
        resp = (
            supabase.table("copied_positions")
            .select("id")
            .in_("copy_bot_id", live_bot_ids)
            .eq("status", "OPEN")
            .execute()
        )
        return len(resp.data or [])
    except Exception:
        logging.exception("COPY_LIVE_OPEN_POS_COUNT_FAIL")
        return 999  # fail-safe: block orders when count is unknown


def submit_copy_live_order(
    client: "ClobClient",
    token_id: str,
    order_side: str,
    source_price: float,
    size_usd: float,
    max_slippage: float = 0.03,
) -> "tuple[bool, float, float, dict]":
    """
    Submit a GTC limit order on the Polymarket CLOB for a live copy trade.

    This is a clean copy-trade-only order path. It deliberately does NOT call:
      record_trade()          — writes to BTC bot_trades table
      _record_live_position() — BTC position tracker
      tracker_apply_fill()    — BTC fill tracker

    Price with slippage buffer:
      BUY:  limit = min(source_price × (1 + max_slippage), 0.99)
      SELL: limit = max(source_price × (1 - max_slippage), 0.01)

    Returns (success, actual_price, actual_shares, raw_response_dict).
    On any failure returns (False, 0.0, 0.0, {"error": ...}).
    """
    order_side = order_side.upper()

    # Fetch tick size — fall back to DEFAULT_TICK on failure
    try:
        tick_str = client.get_tick_size(token_id)
        tick_size = Decimal(str(tick_str)) if tick_str else DEFAULT_TICK
    except Exception:
        tick_size = DEFAULT_TICK
    if tick_size <= 0:
        tick_size = DEFAULT_TICK

    # Compute limit price with slippage buffer
    if order_side == "BUY":
        limit_price = min(source_price * (1.0 + max_slippage), 0.99)
    else:
        limit_price = max(source_price * (1.0 - max_slippage), 0.01)

    price_decimal = Decimal(str(round(limit_price, 6)))
    try:
        price_q = price_decimal.quantize(tick_size, rounding=ROUND_DOWN)
    except InvalidOperation:
        logging.warning(
            "COPY_LIVE_SKIP_PRICE_QUANTIZE token=%s price=%s tick=%s",
            token_id[:16], limit_price, tick_size,
        )
        return False, 0.0, 0.0, {}

    if price_q <= 0:
        logging.warning(
            "COPY_LIVE_SKIP_INVALID_PRICE token=%s price=%s", token_id[:16], price_q
        )
        return False, 0.0, 0.0, {}

    size_decimal = Decimal(str(size_usd))
    shares_raw = size_decimal / price_q
    shares_q = shares_raw.quantize(SHARE_QUANT, rounding=ROUND_DOWN)

    if shares_q <= 0:
        logging.warning(
            "COPY_LIVE_SKIP_ZERO_SHARES token=%s shares=%s price=%s size_usd=%s",
            token_id[:16], shares_q, price_q, size_usd,
        )
        return False, 0.0, 0.0, {}

    actual_price  = float(price_q)
    actual_shares = float(shares_q)

    logging.info(
        "COPY_LIVE_ORDER_SUBMIT token=%s side=%s price=%.4f shares=%.4f "
        "size_usd=%.2f slippage_cap=%.3f",
        token_id[:16], order_side, actual_price, actual_shares, size_usd, max_slippage,
    )

    order_args = OrderArgs(
        token_id=token_id,
        price=actual_price,
        size=actual_shares,
        side=order_side,
    )
    try:
        signed = client.create_order(order_args)
        resp   = client.post_order(signed, OrderType.GTC)
        raw_response: dict = resp if isinstance(resp, dict) else {"response": str(resp)}
        logging.info(
            "COPY_LIVE_ORDER_OK token=%s side=%s price=%.4f shares=%.4f resp_keys=%s",
            token_id[:16], order_side, actual_price, actual_shares,
            list(raw_response.keys())[:5],
        )
        return True, actual_price, actual_shares, raw_response
    except Exception as exc:
        logging.exception(
            "COPY_LIVE_ORDER_FAIL token=%s side=%s err=%s", token_id[:16], order_side, exc
        )
        return False, actual_price, actual_shares, {"error": str(exc)}


def evaluate_and_execute_live_copy_trade(
    copy_bot: dict,
    wallet_trade: dict,
    global_settings: dict,
    trading_client: "ClobClient",
    live_bot_ids: list[str],
) -> "tuple[bool, str | None, float | None, float | None, dict]":
    """
    Run all live safety gates and, if every gate passes, submit a real CLOB order.

    Returns (copied, skip_reason, submitted_size_usd, submitted_price, raw_response).

    Gates are evaluated in strict order; the first failure exits with a named reason.

    Skip reasons:
      live_master_off               — copy_global_settings.live_on = false
      emergency_stop_on             — copy_global_settings.emergency_stop = true
      arm_live_disabled             — copy_bots.arm_live = false
      too_many_live_bots_enabled    — enabled LIVE bots > COPY_LIVE_MAX_BOTS
      closes_not_enabled            — SELL trade, copy_closes = false
      delay_not_elapsed             — trade too recent for delay_seconds
      live_trades_per_hour_reached  — per-bot hourly cap exceeded
      live_global_hourly_cap        — global hourly live cap exceeded
      live_open_positions_limit     — live open positions ≥ cap
      insufficient_market_data      — token_id missing
      unsupported_trade_shape       — price missing or out of range
      live_trade_size_exceeds_cap   — computed size ≤ 0 after capping
      order_submission_failed       — CLOB order rejected or exception
    """
    bot_id = str(copy_bot["id"])

    # ── Gate L1: master live_on ───────────────────────────────────────────────
    if not global_settings.get("live_on"):
        return False, "live_master_off", None, None, {}

    # ── Gate L2: emergency stop ───────────────────────────────────────────────
    if global_settings.get("emergency_stop"):
        return False, "emergency_stop_on", None, None, {}

    # ── Gate L3: per-bot arm_live required ───────────────────────────────────
    if not copy_bot.get("arm_live"):
        return False, "arm_live_disabled", None, None, {}

    # ── Gate L4: live bot count cap ───────────────────────────────────────────
    if len(live_bot_ids) > COPY_LIVE_MAX_BOTS:
        return False, "too_many_live_bots_enabled", None, None, {}

    # ── Gate L5: opens/closes filter ─────────────────────────────────────────
    trade_side = str(wallet_trade.get("side") or "").upper()
    copy_closes = bool(copy_bot.get("copy_closes", False))
    if trade_side == "SELL" and not copy_closes:
        return False, "closes_not_enabled", None, None, {}

    # ── Gate L6: delay filter ────────────────────────────────────────────────
    delay_sec = int(copy_bot.get("delay_seconds") or 0)
    if delay_sec > 0:
        try:
            traded_at_dt = datetime.fromisoformat(
                str(wallet_trade.get("traded_at") or "").replace("Z", "+00:00")
            )
            if (datetime.now(timezone.utc) - traded_at_dt).total_seconds() < delay_sec:
                return False, "delay_not_elapsed", None, None, {}
        except Exception:
            pass  # unparseable timestamp — skip delay gate

    # ── Gate L7: per-bot hourly rate limit ───────────────────────────────────
    max_per_hour_bot = int(copy_bot.get("max_trades_per_hour") or 20)
    if _copy_bot_trades_this_hour(bot_id) >= max_per_hour_bot:
        return False, "live_trades_per_hour_reached", None, None, {}

    # ── Gate L8: global live hourly cap ──────────────────────────────────────
    if _get_live_copy_trades_this_hour() >= COPY_LIVE_MAX_TRADES_PER_HOUR:
        return False, "live_global_hourly_cap", None, None, {}

    # ── Gate L9: live open positions cap ─────────────────────────────────────
    if trade_side in ("BUY", ""):
        live_open = get_live_open_positions_count(live_bot_ids)
        if live_open >= COPY_LIVE_MAX_OPEN_POSITIONS:
            return False, "live_open_positions_limit", None, None, {}

    # ── Gate L10: token_id required ──────────────────────────────────────────
    token_id = wallet_trade.get("token_id")
    if not token_id:
        return False, "insufficient_market_data", None, None, {}

    # ── Gate L11: valid source price ─────────────────────────────────────────
    price_raw = wallet_trade.get("price")
    if price_raw is None:
        return False, "unsupported_trade_shape", None, None, {}
    try:
        source_price = float(price_raw)
        if not (0 < source_price <= 1):
            return False, "unsupported_trade_shape", None, None, {}
    except (TypeError, ValueError):
        return False, "unsupported_trade_shape", None, None, {}

    # ── Compute and hard-cap size ─────────────────────────────────────────────
    computed_size = compute_copy_size(copy_bot, wallet_trade, global_settings)
    if not computed_size or computed_size <= 0:
        return False, "unsupported_trade_shape", None, None, {}

    submitted_size = min(float(computed_size), COPY_LIVE_MAX_TRADE_USD)
    if submitted_size <= 0:
        return False, "live_trade_size_exceeds_cap", None, None, {}

    if float(computed_size) > COPY_LIVE_MAX_TRADE_USD:
        logging.info(
            "COPY_LIVE_SIZE_CAPPED bot=%s computed=%.2f cap=%.2f final=%.2f",
            str(copy_bot.get("name") or bot_id[:8]),
            float(computed_size), COPY_LIVE_MAX_TRADE_USD, submitted_size,
        )

    # ── Submit CLOB order ─────────────────────────────────────────────────────
    max_slippage = float(
        copy_bot.get("max_slippage")
        or global_settings.get("default_slippage_cap")
        or 0.03
    )
    order_side = trade_side if trade_side in ("BUY", "SELL") else "BUY"

    ok, actual_price, actual_shares, raw_response = submit_copy_live_order(
        trading_client,
        token_id,
        order_side,
        source_price,
        submitted_size,
        max_slippage,
    )
    if not ok:
        return False, "order_submission_failed", submitted_size, source_price, raw_response

    return True, None, submitted_size, actual_price, raw_response


# ── Audit + position helpers ──────────────────────────────────────────────────

def log_copy_attempt(
    copy_bot: dict,
    wallet_trade: dict,
    copied: bool,
    skip_reason: str | None,
    submitted_size: float | None,
    submitted_price: float | None,
    order_status: str = "SKIPPED",
    raw_response: dict | None = None,
) -> None:
    """Write a copy_attempts audit row. Always written — even for skipped trades."""
    try:
        row = {
            "copy_bot_id": str(copy_bot["id"]),
            "wallet_address": wallet_trade["wallet_address"],
            "source_trade_id": wallet_trade["source_trade_id"],
            "market_slug": wallet_trade.get("market_slug"),
            "market_title": wallet_trade.get("market_title"),
            "token_id": wallet_trade.get("token_id"),
            "source_side": wallet_trade.get("side"),
            "source_outcome": wallet_trade.get("outcome"),
            "source_price": wallet_trade.get("price"),
            "source_size": wallet_trade.get("size"),
            "submitted_price": submitted_price,
            "submitted_size": submitted_size,
            "copied": copied,
            "skip_reason": skip_reason,
            "order_status": order_status,
            "raw_response": raw_response or {},
        }
        supabase.table("copy_attempts").insert(row).execute()
    except Exception:
        logging.exception(
            "COPY_LOG_ATTEMPT_FAIL bot=%s trade=%s",
            str(copy_bot.get("id", "?"))[:8],
            str(wallet_trade.get("source_trade_id", "?"))[:20],
        )


def open_copied_position(
    copy_bot: dict,
    wallet_trade: dict,
    submitted_size: float,
    submitted_price: float,
    mode: str = "PAPER",
) -> None:
    """
    Create a copied_positions row for a paper or live copy trade.

    mode: "PAPER" (default) or "LIVE"

    'size' stores the USD position size (submitted_size).
    'entry_price' is the per-share price at which we entered.
    PnL at close: pnl = size * (exit_price - entry_price) / entry_price

    The raw_json includes mode metadata so copied_positions rows can be
    distinguished as paper vs live without joining to copy_bots.
    """
    is_live = str(mode).upper() == "LIVE"
    try:
        row = {
            "copy_bot_id": str(copy_bot["id"]),
            "wallet_address": wallet_trade["wallet_address"],
            "source_trade_id": wallet_trade.get("source_trade_id"),
            "market_slug": wallet_trade.get("market_slug"),
            "market_title": wallet_trade.get("market_title"),
            "condition_id": wallet_trade.get("condition_id"),
            "token_id": wallet_trade.get("token_id"),
            "side": wallet_trade.get("side"),
            "outcome": wallet_trade.get("outcome"),
            "entry_price": submitted_price,
            "size": submitted_size,
            "status": "OPEN",
            "pnl": 0,
            "raw_json": {
                "paper": not is_live,
                "live": is_live,
                "mode": mode.upper(),
                "copy_mode": copy_bot.get("copy_mode"),
                "sizing_value": copy_bot.get("sizing_value"),
                "source": {
                    "price": wallet_trade.get("price"),
                    "size": wallet_trade.get("size"),
                    "notional": wallet_trade.get("notional"),
                    "side": wallet_trade.get("side"),
                    "outcome": wallet_trade.get("outcome"),
                },
            },
        }
        supabase.table("copied_positions").insert(row).execute()
        logging.info(
            "COPY_POSITION_OPENED mode=%s bot=%s wallet=%s slug=%s "
            "side=%s outcome=%s size=%s price=%s",
            mode.upper(),
            str(copy_bot.get("id", "?"))[:8],
            wallet_trade["wallet_address"][:10],
            wallet_trade.get("market_slug") or "unknown",
            wallet_trade.get("side"),
            wallet_trade.get("outcome"),
            submitted_size,
            submitted_price,
        )
    except Exception:
        logging.exception(
            "COPY_OPEN_POSITION_FAIL bot=%s trade=%s",
            str(copy_bot.get("id", "?"))[:8],
            str(wallet_trade.get("source_trade_id", "?"))[:20],
        )


def _parse_ts(s) -> "datetime | None":
    """
    Parse an ISO-8601 timestamp string (or datetime) into a timezone-aware datetime.
    Returns None on any failure — callers should treat None as "unknown".
    Handles both 'Z' suffix and '+00:00' offset styles.
    """
    if s is None:
        return None
    if isinstance(s, datetime):
        return s if s.tzinfo else s.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def _compute_copy_score(
    pnl_30d: float,
    win_rate: float,
    trade_count: int,
    max_drawdown: float,
    last_trade_at: "str | None",
) -> float:
    """
    Compute a composite copy_score in the range 0–100.

    Component weights and normalisation:

      pnl_score      (30 pts) — clamped linear: -$50 → 0, $0 → 0.33, +$100 → 1.0
                                Formula: clamp((pnl_30d + 50) / 150, 0, 1)
                                Rationale: rewards recent profitability; neutral at $0.

      win_rate_score (25 pts) — raw win_rate (0.0–1.0)
                                Rationale: direct measure of hit rate.

      activity_score (15 pts) — clamp(trade_count / 100, 0, 1)
                                Rationale: rewards active, higher-conviction wallets.

      drawdown_score (20 pts) — clamp(1 - max_drawdown / 100, 0, 1)
                                max_drawdown is stored as % (0–100).
                                Rationale: penalises high drawdown / volatile equity.

      recency_score  (10 pts) — clamp(1 - days_since_last_trade / 30, 0, 1)
                                Rationale: wallets inactive >30d get 0 on this component.

    Wallets with no closed positions score ~33 on pnl + 0 on win_rate + varies on
    activity/recency. This is intentionally conservative: unproven wallets rank lower.
    """
    def _clamp(v: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, v))

    pnl_score = _clamp((pnl_30d + 50.0) / 150.0, 0.0, 1.0)
    win_rate_score = _clamp(win_rate, 0.0, 1.0)
    activity_score = _clamp(trade_count / 100.0, 0.0, 1.0)
    drawdown_score = _clamp(1.0 - max_drawdown / 100.0, 0.0, 1.0)

    recency_score = 0.0
    if last_trade_at:
        last_dt = _parse_ts(last_trade_at)
        if last_dt:
            days_ago = (datetime.now(timezone.utc) - last_dt).total_seconds() / 86400
            recency_score = _clamp(1.0 - days_ago / 30.0, 0.0, 1.0)

    raw = (
        pnl_score      * 30.0
        + win_rate_score * 25.0
        + activity_score * 15.0
        + drawdown_score * 20.0
        + recency_score  * 10.0
    )
    return round(raw, 2)


def update_wallet_metrics_for_address(wallet_address: str) -> None:
    """
    Upsert wallet_metrics with fully computed performance fields.

    DATA SOURCES
    ─────────────────────────────────────────────────────────────────────────────
    • wallet_trades   → trade_count, volume, last_trade_at, category_focus
    • copied_positions (CLOSED) → pnl_all, pnl_7d, pnl_30d, win_rate,
                                   avg_hold_minutes, max_drawdown

    APPROXIMATIONS (documented)
    ─────────────────────────────────────────────────────────────────────────────
    • pnl_7d / pnl_30d    — PnL of OUR paper copy positions settled in that window,
                             not the source wallet's realised PnL. Best proxy
                             available until we can track the source wallet's own
                             exit prices from raw CLOB data.
    • avg_hold_minutes    — Duration our copy positions were held (opened_at →
                             closed_at), which reflects market resolution speed
                             rather than the source wallet's actual exit timing.
    • max_drawdown        — Peak-to-trough of cumulative paper PnL, expressed as
                             a percentage of the equity peak. 0 if no peak yet.
    • category_focus      — Modal market category from market_cache for markets
                             this wallet has traded. None if market_cache has no
                             category data for those markets.

    FULLY LIVE FIELDS
    ─────────────────────────────────────────────────────────────────────────────
    trade_count, volume, last_trade_at, pnl_all, win_rate, copy_score, updated_at
    """
    try:
        now_utc = datetime.now(timezone.utc)
        cutoff_7d  = now_utc - timedelta(days=7)
        cutoff_30d = now_utc - timedelta(days=30)

        # ── Query 1: wallet_trades ────────────────────────────────────────
        wt_resp = (
            supabase.table("wallet_trades")
            .select("notional, traded_at, market_slug")
            .eq("wallet_address", wallet_address)
            .execute()
        )
        trades = wt_resp.data or []
        trade_count = len(trades)
        volume = round(sum(float(t.get("notional") or 0) for t in trades), 4)

        last_trade_at: str | None = None
        if trades:
            ts_strs = [str(t["traded_at"]) for t in trades if t.get("traded_at")]
            if ts_strs:
                last_trade_at = max(ts_strs)

        # ── Query 2: closed copied_positions ──────────────────────────────
        cp_resp = (
            supabase.table("copied_positions")
            .select("pnl, opened_at, closed_at")
            .eq("wallet_address", wallet_address)
            .eq("status", "CLOSED")
            .order("closed_at", desc=False)
            .execute()
        )
        closed_pos = cp_resp.data or []

        # pnl_all, pnl_7d, pnl_30d
        pnl_all_vals: list[float] = []
        pnl_7d_vals:  list[float] = []
        pnl_30d_vals: list[float] = []
        for p in closed_pos:
            v = float(p.get("pnl") or 0)
            pnl_all_vals.append(v)
            closed_dt = _parse_ts(p.get("closed_at"))
            if closed_dt:
                if closed_dt >= cutoff_7d:
                    pnl_7d_vals.append(v)
                if closed_dt >= cutoff_30d:
                    pnl_30d_vals.append(v)

        pnl_all  = round(sum(pnl_all_vals),  4)
        pnl_7d   = round(sum(pnl_7d_vals),   4)
        pnl_30d  = round(sum(pnl_30d_vals),  4)

        # win_rate (0.0–1.0)
        wins = sum(1 for v in pnl_all_vals if v > 0)
        win_rate = round(wins / len(pnl_all_vals), 4) if pnl_all_vals else 0.0

        # avg_hold_minutes — from opened_at/closed_at diffs
        hold_mins: list[float] = []
        for p in closed_pos:
            opened_dt = _parse_ts(p.get("opened_at"))
            closed_dt = _parse_ts(p.get("closed_at"))
            if opened_dt and closed_dt and closed_dt > opened_dt:
                hold_mins.append((closed_dt - opened_dt).total_seconds() / 60.0)
        avg_hold_minutes = round(sum(hold_mins) / len(hold_mins), 2) if hold_mins else 0.0

        # max_drawdown (%) — peak-to-trough of cumulative PnL curve
        max_drawdown = 0.0
        if pnl_all_vals:
            equity = 0.0
            peak   = 0.0
            max_dd = 0.0
            for v in pnl_all_vals:   # already ordered by closed_at asc
                equity += v
                if equity > peak:
                    peak = equity
                dd = peak - equity
                if dd > max_dd:
                    max_dd = dd
            if peak > 0:
                max_drawdown = round(max_dd / peak * 100.0, 2)

        # ── Query 3: category_focus (optional, fail-safe) ─────────────────
        category_focus: str | None = None
        market_slugs = list({t.get("market_slug") for t in trades if t.get("market_slug")})
        if market_slugs:
            try:
                mc_resp = (
                    supabase.table("market_cache")
                    .select("category")
                    .in_("market_slug", market_slugs[:50])
                    .execute()
                )
                cats = [r["category"] for r in (mc_resp.data or []) if r.get("category")]
                if cats:
                    from collections import Counter
                    category_focus = Counter(cats).most_common(1)[0][0]
            except Exception:
                pass  # category_focus stays None — non-critical

        # ── copy_score (0–100 composite) ──────────────────────────────────
        copy_score = _compute_copy_score(
            pnl_30d=pnl_30d,
            win_rate=win_rate,
            trade_count=trade_count,
            max_drawdown=max_drawdown,
            last_trade_at=last_trade_at,
        )

        # ── Upsert wallet_metrics ─────────────────────────────────────────
        metrics: dict = {
            "wallet_address":   wallet_address,
            "trade_count":      trade_count,
            "volume":           volume,
            "last_trade_at":    last_trade_at,
            "pnl_all":          pnl_all,
            "pnl_7d":           pnl_7d,
            "pnl_30d":          pnl_30d,
            "win_rate":         win_rate,
            "avg_hold_minutes": avg_hold_minutes,
            "max_drawdown":     max_drawdown,
            "copy_score":       copy_score,
            "category_focus":   category_focus,
            "updated_at":       now_utc.isoformat(),
        }
        supabase.table("wallet_metrics").upsert(
            metrics, on_conflict="wallet_address"
        ).execute()

        logging.info(
            "COPY_METRICS_UPDATED wallet=%s trades=%s volume=%.2f "
            "pnl_30d=%.4f win_rate=%.3f avg_hold=%.1fmin "
            "max_dd=%.1f%% copy_score=%.1f",
            wallet_address[:10],
            trade_count,
            volume,
            pnl_30d,
            win_rate,
            avg_hold_minutes,
            max_drawdown,
            copy_score,
        )

    except Exception:
        logging.exception("COPY_UPDATE_METRICS_FAIL wallet=%s", wallet_address[:10])


# ── Copy bot evaluation ───────────────────────────────────────────────────────

def _copy_bot_prune_history(bot_id: str) -> None:
    """Prune in-memory trade timestamps older than 1 hour for a copy bot."""
    cutoff = time() - 3600
    dq = copy_bot_trade_timestamps[bot_id]
    while dq and dq[0] < cutoff:
        dq.popleft()


def _copy_bot_trades_this_hour(bot_id: str) -> int:
    """Return how many trades this bot has logged in the last hour (in-memory)."""
    _copy_bot_prune_history(bot_id)
    return len(copy_bot_trade_timestamps[bot_id])


def _copy_bot_mark_trade(bot_id: str) -> None:
    """Record a trade timestamp for rate-limit tracking."""
    copy_bot_trade_timestamps[bot_id].append(time())


def compute_copy_size(
    copy_bot: dict,
    wallet_trade: dict,
    global_settings: dict,
) -> float:
    """
    Compute the paper USD size for a copy trade based on copy_mode:

      exact   — fixed USD amount equal to sizing_value (e.g. $10 per trade)
      scaled  — source notional × sizing_value  (e.g. 0.5 = half the source size)
      percent — sizing_value% of default_position_size from global settings

    Always capped at copy_bot.max_trade_size.
    """
    copy_mode = str(copy_bot.get("copy_mode") or "exact")
    sizing_value = float(copy_bot.get("sizing_value") or 1)
    max_trade_size = float(copy_bot.get("max_trade_size") or 25)

    if copy_mode == "exact":
        size = sizing_value
    elif copy_mode == "scaled":
        source_notional = float(wallet_trade.get("notional") or 0)
        size = source_notional * sizing_value
    elif copy_mode == "percent":
        base = float(global_settings.get("default_position_size") or 10)
        size = base * (sizing_value / 100.0)
    else:
        size = sizing_value  # fallback to exact

    size = min(size, max_trade_size)
    size = max(size, 0.01)
    return round(size, 4)


def evaluate_copy_bot_for_trade(
    copy_bot: dict,
    wallet_trade: dict,
    global_settings: dict,
) -> tuple[bool, str | None, float | None, float | None]:
    """
    Decide whether to paper-copy a wallet trade for a given copy bot.

    Returns (should_copy, skip_reason, submitted_size_usd, submitted_price).
    Evaluates all filters in order; returns on first failure with a clear reason.

    Skip reasons:
      live_mode_not_supported_yet   — bot is LIVE mode (not yet implemented)
      emergency_stop_active         — global emergency_stop is true
      closes_not_enabled            — SELL trade but copy_closes = false
      delay_not_elapsed             — trade is too recent for delay_seconds
      max_trades_per_hour_reached   — in-memory rate limit exceeded
      max_open_positions_reached    — too many open copied_positions
      insufficient_market_data      — no token_id or market_slug
      unsupported_trade_shape       — no price data
    """
    bot_id = str(copy_bot["id"])

    # Gate 1: LIVE mode not implemented yet
    if str(copy_bot.get("mode", "PAPER")).upper() == "LIVE":
        return False, "live_mode_not_supported_yet", None, None

    # Gate 2: Emergency stop
    if global_settings.get("emergency_stop"):
        return False, "emergency_stop_active", None, None

    # Gate 3: opens_only / copy_closes filter
    trade_side = str(wallet_trade.get("side") or "").upper()
    opens_only = bool(copy_bot.get("opens_only", True))
    copy_closes = bool(copy_bot.get("copy_closes", False))

    if trade_side == "SELL" and not copy_closes:
        return False, "closes_not_enabled", None, None
    # If opens_only and this is a BUY → allow (opens_only only blocks explicit opens mode)
    # If opens_only=False, copy everything including sells (if copy_closes allows it)

    # Gate 4: Delay filter
    delay_sec = int(copy_bot.get("delay_seconds") or 0)
    if delay_sec > 0:
        try:
            traded_at_str = str(wallet_trade.get("traded_at") or "")
            traded_at_dt = datetime.fromisoformat(traded_at_str.replace("Z", "+00:00"))
            age_seconds = (datetime.now(timezone.utc) - traded_at_dt).total_seconds()
            if age_seconds < delay_sec:
                return False, "delay_not_elapsed", None, None
        except Exception:
            pass  # unparseable timestamp — skip delay gate

    # Gate 5: max_trades_per_hour (in-memory)
    max_per_hour = int(copy_bot.get("max_trades_per_hour") or 20)
    if _copy_bot_trades_this_hour(bot_id) >= max_per_hour:
        return False, "max_trades_per_hour_reached", None, None

    # Gate 6: max_open_positions (DB query — only relevant for BUY/entry trades)
    if trade_side in ("BUY", ""):
        max_open = int(copy_bot.get("max_open_positions") or 10)
        open_count = get_open_positions_count(bot_id)
        if open_count >= max_open:
            return False, "max_open_positions_reached", None, None

    # Gate 7: Minimum market data required
    if not wallet_trade.get("token_id") and not wallet_trade.get("market_slug"):
        return False, "insufficient_market_data", None, None

    # Gate 8: Price must be present
    price = wallet_trade.get("price")
    if price is None:
        return False, "unsupported_trade_shape", None, None

    try:
        submitted_price = float(price)
        if submitted_price <= 0 or submitted_price > 1:
            return False, "unsupported_trade_shape", None, None
    except (TypeError, ValueError):
        return False, "unsupported_trade_shape", None, None

    # Category filter (best-effort — market_cache lookup not yet wired)
    # If category_filter is set but we can't verify, allow for now
    # TODO: enrich from market_cache.category when available

    submitted_size = compute_copy_size(copy_bot, wallet_trade, global_settings)
    return True, None, submitted_size, submitted_price


# ── Copy trade loop ───────────────────────────────────────────────────────────

async def copy_trade_loop(trading_client: "ClobClient | None" = None) -> None:
    """
    Copy-trading ingestion and execution loop. Handles both PAPER and LIVE bots.

    Runs every COPY_TRADE_LOOP_INTERVAL seconds alongside existing BTC tasks.
    Does NOT affect BTC strategy logic, bot_trades, or any BTC-specific tables.

    Per-tick flow:
      1. Load active tracked_wallets + enabled copy_bots + global_settings
      2. Identify LIVE bots (for cap checks) and PAPER bots
      3. For each wallet: fetch + normalize + ingest wallet_trades
      4. For each bot watching this wallet:

         PAPER bots (mode='PAPER'):
           a. Find unevaluated trades (no copy_attempts row yet)
           b. evaluate_copy_bot_for_trade() — existing paper gate logic
           c. Write copy_attempts (always, even skips)
           d. On copy: write copied_positions (status=OPEN, mode=PAPER)

         LIVE bots (mode='LIVE'):
           a. Pre-check COPY_LIVE_ENABLED and trading_client availability
           b. evaluate_and_execute_live_copy_trade() — 11 live safety gates
              including live_on, emergency_stop, arm_live, caps, token_id
           c. On gate pass: submit real CLOB GTC order
           d. Write copy_attempts (with order_status, raw_response)
           e. On copy: write copied_positions (status=OPEN, mode=LIVE)

      5. Update wallet_metrics for each wallet
      6. Log per-tick summary (paper + live positions opened, errors)
      7. Sleep COPY_TRADE_LOOP_INTERVAL seconds
    """
    if not COPY_TRADE_ENABLED:
        logging.info("COPY_TRADE_LOOP disabled via COPY_TRADE_ENABLED=false — exiting task")
        return

    logging.info(
        "COPY_TRADE_LOOP_BOOT interval=%ss lookback=%sh fetch_limit=%s live_enabled=%s",
        COPY_TRADE_LOOP_INTERVAL,
        COPY_TRADE_LOOKBACK_HOURS,
        COPY_WALLET_TRADE_FETCH_LIMIT,
        COPY_LIVE_ENABLED,
    )

    while True:
        wallets        = load_tracked_wallets()
        all_bots       = load_enabled_copy_bots()
        global_settings = load_copy_global_settings()

        # Identify LIVE bots once per tick for cap checks
        live_bots    = [b for b in all_bots if str(b.get("mode", "PAPER")).upper() == "LIVE"]
        live_bot_ids = [str(b["id"]) for b in live_bots]

        total_wallets      = len(wallets)
        total_new_trades   = 0
        total_attempts     = 0
        total_paper_opened = 0
        total_live_opened  = 0
        total_errors       = 0

        log_rate_limited(
            "copy_loop_global_settings",
            LOG_THROTTLE_SECONDS,
            "COPY_GLOBAL_SETTINGS emergency_stop=%s live_on=%s "
            "wallets=%s bots=%s live_bots=%s",
            global_settings.get("emergency_stop"),
            global_settings.get("live_on"),
            total_wallets,
            len(all_bots),
            len(live_bots),
        )

        for wallet in wallets:
            wallet_address = wallet["wallet_address"]
            wallet_label   = wallet_address[:10] + "..."

            try:
                # ── Step 1: Fetch raw activity ────────────────────────────
                raw_activities = await fetch_wallet_trades_for_address(wallet_address)

                # ── Step 2: Normalize + ingest wallet_trades ──────────────
                newly_inserted: list[dict] = []
                for raw in raw_activities:
                    trade_row = normalize_activity_to_wallet_trade(raw, wallet_address)
                    if not trade_row:
                        continue
                    upsert_market_cache(trade_row)
                    if insert_wallet_trade_if_new(trade_row):
                        newly_inserted.append(trade_row)
                        total_new_trades += 1

                if newly_inserted:
                    logging.info(
                        "COPY_TRADES_INGESTED wallet=%s new=%s raw_fetched=%s",
                        wallet_label, len(newly_inserted), len(raw_activities),
                    )

                # ── Step 3: Match copy bots + evaluate ───────────────────
                wallet_bots = [b for b in all_bots if b["wallet_address"] == wallet_address]

                for bot in wallet_bots:
                    bot_id    = str(bot["id"])
                    bot_label = bot.get("name") or bot_id[:8]
                    bot_mode  = str(bot.get("mode", "PAPER")).upper()

                    unevaluated = get_unevaluated_trades_for_bot(
                        wallet_address, bot_id, COPY_TRADE_LOOKBACK_HOURS,
                    )
                    if not unevaluated:
                        continue

                    logging.info(
                        "COPY_BOT_EVAL bot=%s mode=%s wallet=%s unevaluated=%s",
                        bot_label, bot_mode, wallet_label, len(unevaluated),
                    )

                    for wallet_trade in unevaluated:
                        trade_label = str(wallet_trade.get("source_trade_id", "?"))[:20]

                        # ── PAPER path ────────────────────────────────────
                        if bot_mode == "PAPER":
                            try:
                                copied, skip_reason, submitted_size, submitted_price = (
                                    evaluate_copy_bot_for_trade(bot, wallet_trade, global_settings)
                                )
                                order_status = "PAPER_MATCHED" if copied else "SKIPPED"
                                log_copy_attempt(
                                    bot, wallet_trade, copied, skip_reason,
                                    submitted_size, submitted_price, order_status,
                                )
                                total_attempts += 1
                                if copied:
                                    open_copied_position(
                                        bot, wallet_trade, submitted_size, submitted_price,
                                        mode="PAPER",
                                    )
                                    _copy_bot_mark_trade(bot_id)
                                    total_paper_opened += 1
                                    logging.info(
                                        "COPY_PAPER_COPIED bot=%s wallet=%s trade=%s "
                                        "slug=%s side=%s outcome=%s size=%s price=%s",
                                        bot_label, wallet_label, trade_label,
                                        wallet_trade.get("market_slug") or "?",
                                        wallet_trade.get("side"),
                                        wallet_trade.get("outcome"),
                                        submitted_size, submitted_price,
                                    )
                                else:
                                    logging.info(
                                        "COPY_PAPER_SKIPPED bot=%s wallet=%s trade=%s reason=%s",
                                        bot_label, wallet_label, trade_label, skip_reason,
                                    )
                            except Exception:
                                logging.exception(
                                    "COPY_PAPER_EVALUATE_FAIL bot=%s trade=%s",
                                    bot_label, trade_label,
                                )
                                total_errors += 1

                        # ── LIVE path ─────────────────────────────────────
                        elif bot_mode == "LIVE":
                            try:
                                # Pre-checks before calling evaluate (fast fails)
                                if not COPY_LIVE_ENABLED:
                                    log_copy_attempt(
                                        bot, wallet_trade, False,
                                        "live_copy_globally_disabled",
                                        None, None, "SKIPPED",
                                    )
                                    total_attempts += 1
                                    continue

                                if not trading_client:
                                    log_copy_attempt(
                                        bot, wallet_trade, False,
                                        "live_client_unavailable",
                                        None, None, "SKIPPED",
                                    )
                                    total_attempts += 1
                                    continue

                                (
                                    copied,
                                    skip_reason,
                                    submitted_size,
                                    submitted_price,
                                    raw_response,
                                ) = evaluate_and_execute_live_copy_trade(
                                    bot, wallet_trade, global_settings,
                                    trading_client, live_bot_ids,
                                )

                                order_status = "LIVE_MATCHED" if copied else "SKIPPED"
                                log_copy_attempt(
                                    bot, wallet_trade, copied, skip_reason,
                                    submitted_size, submitted_price,
                                    order_status, raw_response,
                                )
                                total_attempts += 1

                                if copied:
                                    open_copied_position(
                                        bot, wallet_trade, submitted_size, submitted_price,
                                        mode="LIVE",
                                    )
                                    _copy_bot_mark_trade(bot_id)
                                    _mark_live_copy_trade()
                                    total_live_opened += 1
                                    logging.info(
                                        "COPY_LIVE_COPIED bot=%s wallet=%s trade=%s "
                                        "slug=%s side=%s outcome=%s size=%s price=%s",
                                        bot_label, wallet_label, trade_label,
                                        wallet_trade.get("market_slug") or "?",
                                        wallet_trade.get("side"),
                                        wallet_trade.get("outcome"),
                                        submitted_size, submitted_price,
                                    )
                                else:
                                    logging.info(
                                        "COPY_LIVE_SKIPPED bot=%s wallet=%s trade=%s reason=%s",
                                        bot_label, wallet_label, trade_label, skip_reason,
                                    )

                            except Exception:
                                logging.exception(
                                    "COPY_LIVE_EVALUATE_FAIL bot=%s trade=%s",
                                    bot_label, trade_label,
                                )
                                total_errors += 1

                # ── Step 4: Update wallet metrics ─────────────────────────
                update_wallet_metrics_for_address(wallet_address)

            except Exception:
                logging.exception("COPY_WALLET_ERROR wallet=%s", wallet_label)
                total_errors += 1

        logging.info(
            "COPY_TRADE_LOOP_TICK wallets=%s new_trades=%s attempts=%s "
            "paper_opened=%s live_opened=%s errors=%s",
            total_wallets, total_new_trades, total_attempts,
            total_paper_opened, total_live_opened, total_errors,
        )

        await asyncio.sleep(COPY_TRADE_LOOP_INTERVAL)


# =============================================================================
# COPY-TRADING SETTLEMENT — PAPER POSITION CLOSE LOGIC
# =============================================================================
#
# copy_settlement_loop resolves open copied_positions by fetching market
# resolution status from the Polymarket Gamma API.
#
# ISOLATION: Only reads/writes copied_positions, market_cache, wallet_metrics.
#            No BTC tables, no live orders, no existing BTC loops touched.
#
# ASSUMPTION (this pass): All positions are treated as long BUY entries.
#   A YES position wins if the market resolves "Yes" → exit_price = 1.0
#   A NO  position wins if the market resolves "No"  → exit_price = 1.0
#   The losing side exits at 0.0.
#   SELL-shaped source trades are not settled here — left as future work.
#
# PnL FORMULA:
#   shares = size / entry_price          (size is USD paper stake)
#   pnl    = (exit_price - entry_price) * shares
#           = size * (exit_price - entry_price) / entry_price
#   Win:  pnl = size * (1.0 - entry_price) / entry_price   (profit)
#   Loss: pnl = -size                                        (full loss)
#
# MARKET RESOLUTION LOOKUP ORDER (per position):
#   1. Gamma API: /markets?condition_id={condition_id}
#   2. Gamma API: /events?slug={market_slug}  (fallback)
#   3. Gamma API: /markets?clob_token_ids={token_id}  (last resort)
#
# Per-tick, each unique market key is fetched at most ONCE regardless of how
# many open positions reference it (in-memory dedup cache per tick).
# =============================================================================


# ── Settlement helpers ────────────────────────────────────────────────────────

def load_open_copied_positions(limit: int = 100) -> list[dict]:
    """
    Load open copied_positions from Supabase, oldest first.
    Bounded by limit to avoid overloading the settlement pass.
    """
    try:
        resp = (
            supabase.table("copied_positions")
            .select("*")
            .eq("status", "OPEN")
            .order("opened_at", desc=False)
            .limit(limit)
            .execute()
        )
        return resp.data or []
    except Exception:
        logging.exception("COPY_SETTLE_LOAD_OPEN_POSITIONS_FAIL")
        return []


def _parse_resolution_from_gamma_market(market: dict) -> dict | None:
    """
    Extract settlement information from a single Gamma API market object.

    Returns a resolution dict or None if the market data is unusable.

    Resolution dict fields:
      resolved           — bool: True when the market has settled
      resolution_outcome — "YES" | "NO" | None (None = inconclusive/N_A)
      active             — bool: whether the market is still trading
      end_date           — ISO str or None
      closed_time        — ISO str or None (when market stopped trading)
      yes_token_id       — CLOB token ID for the YES outcome, if known
      no_token_id        — CLOB token ID for the NO outcome, if known
      raw                — the original Gamma market dict (for market_cache)

    Handles two quirks of the Gamma API:
      - outcomes / outcomePrices are sometimes JSON-encoded strings
      - resolution can be "N/A" (treated as inconclusive, not a win/loss)
    """
    if not market or not isinstance(market, dict):
        return None

    resolved = bool(market.get("resolved") or market.get("isResolved"))
    active = bool(market.get("active", True))

    resolution_raw = str(
        market.get("resolution") or market.get("resolutionValue") or ""
    ).strip()
    resolution_outcome: str | None = None
    if resolution_raw:
        r = resolution_raw.upper()
        if r in ("YES", "Y", "1", "TRUE", "UP"):
            resolution_outcome = "YES"
        elif r in ("NO", "N", "0", "FALSE", "DOWN"):
            resolution_outcome = "NO"
        # "N/A", "CANCELLED", "INVALID" → keep None (inconclusive)

    # Parse clobTokenIds / outcomes (may be JSON strings or lists)
    def _parse_list(raw) -> list:
        if isinstance(raw, list):
            return raw
        if isinstance(raw, str):
            try:
                return json.loads(raw.replace("'", '"'))
            except Exception:
                pass
        return []

    clob_ids = _parse_list(market.get("clobTokenIds") or market.get("clob_token_ids"))
    outcomes = _parse_list(market.get("outcomes"))

    yes_token_id: str | None = None
    no_token_id: str | None = None
    for outcome_str, token_id in zip(outcomes, clob_ids):
        norm = _normalize_outcome(str(outcome_str))
        if norm == "YES" and token_id:
            yes_token_id = str(token_id)
        elif norm == "NO" and token_id:
            no_token_id = str(token_id)

    return {
        "resolved": resolved,
        "resolution_outcome": resolution_outcome,
        "active": active,
        "end_date": market.get("endDate") or market.get("end_date_iso"),
        "closed_time": market.get("closedTime") or market.get("closed_time"),
        "yes_token_id": yes_token_id,
        "no_token_id": no_token_id,
        "raw": market,
    }


def _fetch_gamma_market_data_sync(
    condition_id: str | None,
    market_slug: str | None,
    token_id: str | None,
) -> dict | None:
    """
    Fetch market resolution metadata from the Polymarket Gamma API.

    Tries three approaches in order:
      1. /markets?condition_id={condition_id}    — most direct
      2. /events?slug={slug}                     — slug-based fallback
      3. /markets?clob_token_ids={token_id}      — token-based last resort

    Returns a parsed resolution dict (from _parse_resolution_from_gamma_market)
    or None if no usable data could be retrieved.
    """
    headers = {"User-Agent": "FastLoopWorker/1.0"}

    # ── Attempt 1: markets?condition_id ──────────────────────────────────
    if condition_id:
        url = f"{GAMMA_API_BASE}/markets?condition_id={parse.quote(str(condition_id))}"
        try:
            req = request.Request(url, headers=headers)
            with request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read())
            markets = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
            for m in markets:
                result = _parse_resolution_from_gamma_market(m)
                if result is not None:
                    return result
        except Exception as exc:
            logging.debug(
                "COPY_SETTLE_GAMMA_CONDID_FAIL cid=%s err=%s",
                str(condition_id)[:16],
                exc,
            )

    # ── Attempt 2: events?slug / events/slug/{slug} ───────────────────────
    if market_slug:
        slug_urls = [
            f"{GAMMA_API_BASE}/events?slug={parse.quote(market_slug)}",
            f"{GAMMA_API_BASE}/events/slug/{parse.quote(market_slug)}",
        ]
        for url in slug_urls:
            try:
                req = request.Request(url, headers=headers)
                with request.urlopen(req, timeout=8) as resp:
                    data = json.loads(resp.read())
                events = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
                for event in events:
                    if not event:
                        continue
                    # Try each market nested inside the event
                    nested = event.get("markets") or []
                    for m in (nested if isinstance(nested, list) else []):
                        result = _parse_resolution_from_gamma_market(m)
                        if result is not None:
                            return result
                    # Some single-market events have resolution fields at the event level
                    result = _parse_resolution_from_gamma_market(event)
                    if result is not None:
                        return result
            except Exception:
                continue

    # ── Attempt 3: markets?clob_token_ids ────────────────────────────────
    if token_id:
        url = f"{GAMMA_API_BASE}/markets?clob_token_ids={parse.quote(str(token_id))}"
        try:
            req = request.Request(url, headers=headers)
            with request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read())
            markets = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
            for m in markets:
                result = _parse_resolution_from_gamma_market(m)
                if result is not None:
                    return result
        except Exception as exc:
            logging.debug(
                "COPY_SETTLE_GAMMA_TOKEN_FAIL token=%s err=%s",
                str(token_id)[:16],
                exc,
            )

    return None


def _update_market_cache_from_resolution(
    market_slug: str | None,
    resolution_data: dict,
) -> None:
    """
    Update market_cache with resolution metadata fetched from the Gamma API.

    Enriches: active, end_date, last_event_at, yes_token_id, no_token_id, raw_json.
    Uses upsert on market_slug so a missing row is created if needed.
    """
    if not market_slug:
        return
    payload: dict = {
        "market_slug": market_slug,
        "active": resolution_data.get("active", True),
        "raw_json": resolution_data.get("raw") or {},
    }
    if resolution_data.get("end_date"):
        payload["end_date"] = resolution_data["end_date"]
    if resolution_data.get("closed_time"):
        payload["last_event_at"] = resolution_data["closed_time"]
    if resolution_data.get("yes_token_id"):
        payload["yes_token_id"] = resolution_data["yes_token_id"]
    if resolution_data.get("no_token_id"):
        payload["no_token_id"] = resolution_data["no_token_id"]
    try:
        supabase.table("market_cache").upsert(payload, on_conflict="market_slug").execute()
    except Exception as exc:
        logging.warning(
            "COPY_SETTLE_UPDATE_MARKET_CACHE_FAIL slug=%s err=%s", market_slug, exc
        )


def compute_settlement_exit_price(pos: dict, resolution_data: dict) -> float | None:
    """
    Determine the paper exit price (1.0 = win, 0.0 = loss) for a copied position.

    Logic:
      - pos.outcome  = what the copied trade held ("YES" or "NO")
      - resolution_outcome = what the market resolved to ("YES" or "NO")
      - If they match → exit_price = 1.0  (the token pays out $1/share)
      - If they differ → exit_price = 0.0  (the token pays out $0/share)
      - If either is unknown → return None (cannot settle safely)

    Token ID fallback:
      If pos.outcome is missing, we try to infer it by matching pos.token_id
      against yes_token_id / no_token_id from the Gamma resolution data.

    Assumption: Only long BUY positions are handled in this pass.
    SELL-shaped source trades remain OPEN and are flagged in the log.
    """
    pos_outcome = _normalize_outcome(pos.get("outcome"))

    # Infer outcome from token_id if missing
    if not pos_outcome and pos.get("token_id"):
        yes_tkn = resolution_data.get("yes_token_id")
        no_tkn = resolution_data.get("no_token_id")
        if yes_tkn and pos["token_id"] == yes_tkn:
            pos_outcome = "YES"
        elif no_tkn and pos["token_id"] == no_tkn:
            pos_outcome = "NO"

    resolution_outcome = resolution_data.get("resolution_outcome")  # "YES" | "NO" | None

    if not pos_outcome or not resolution_outcome:
        return None  # insufficient data to settle

    # Note: SELL positions would need inverse logic — not handled here.
    if str(pos.get("side") or "BUY").upper() == "SELL":
        logging.info(
            "COPY_SETTLE_SKIP_SELL_POSITION pos=%s slug=%s — SELL settlement not yet implemented",
            str(pos.get("id"))[:8],
            pos.get("market_slug") or "?",
        )
        return None

    return 1.0 if pos_outcome == resolution_outcome else 0.0


def close_copied_position(
    pos: dict,
    exit_price: float,
    resolution_data: dict,
) -> None:
    """
    Update a copied_positions row to CLOSED with computed PnL.

    PnL formula (long paper positions):
      shares = size / entry_price
      pnl    = (exit_price - entry_price) * shares
             = size * (exit_price - entry_price) / entry_price

    Examples (size=$10, entry_price=0.65):
      Win  (exit=1.0): pnl = 10 * (1.0 - 0.65) / 0.65 = +$5.38
      Loss (exit=0.0): pnl = 10 * (0.0 - 0.65) / 0.65 = -$10.00

    The original raw_json is preserved and a 'settlement' sub-object is added.
    closed_at uses the Gamma-reported closedTime when available, else utc_now_iso().
    """
    pos_id = str(pos.get("id") or "")
    entry_price = float_or_none(pos.get("entry_price")) or 0.0
    size = float_or_none(pos.get("size")) or 0.0

    if entry_price > 0:
        pnl = round(size * (exit_price - entry_price) / entry_price, 6)
    else:
        pnl = 0.0

    closed_at = resolution_data.get("closed_time") or utc_now_iso()

    updates = {
        "status": "CLOSED",
        "exit_price": exit_price,
        "pnl": pnl,
        "closed_at": closed_at,
        "raw_json": {
            **(pos.get("raw_json") or {}),
            "settlement": {
                "resolved": resolution_data.get("resolved"),
                "resolution_outcome": resolution_data.get("resolution_outcome"),
                "exit_price": exit_price,
                "pnl": pnl,
                "settled_by": "copy_settlement_loop",
                "settled_at": utc_now_iso(),
            },
        },
    }
    try:
        supabase.table("copied_positions").update(updates).eq("id", pos_id).execute()
        logging.info(
            "COPY_POSITION_CLOSED pos=%s slug=%s outcome=%s resolution=%s "
            "exit_price=%s entry_price=%s size=%s pnl=%s",
            pos_id[:8],
            pos.get("market_slug") or "?",
            pos.get("outcome"),
            resolution_data.get("resolution_outcome"),
            exit_price,
            entry_price,
            size,
            pnl,
        )
    except Exception:
        logging.exception("COPY_SETTLE_CLOSE_POSITION_FAIL pos=%s", pos_id)


# ── Settlement loop ───────────────────────────────────────────────────────────

async def copy_settlement_loop() -> None:
    """
    PAPER copy-trading settlement loop.

    Runs every COPY_SETTLEMENT_LOOP_INTERVAL seconds alongside copy_trade_loop.
    Closes OPEN copied_positions when their source market resolves.

    Per-tick flow:
      1. Load up to COPY_SETTLEMENT_BATCH_SIZE OPEN copied_positions (oldest first)
      2. Deduplicate markets — build a {market_key → resolution_data} cache so
         each unique market is fetched from the Gamma API at most once per tick
      3. For each position:
           a. Look up resolution_data for its market
           b. If not resolved → skip (keep OPEN)
           c. Compute exit_price (1.0 win / 0.0 loss)
           d. Close position: update copied_positions (status, exit_price, pnl, closed_at)
           e. Update market_cache with fresh Gamma metadata
      4. For each wallet that had at least one position settled: refresh wallet_metrics
         (triggers pnl_all + win_rate recompute from closed positions)
      5. Log settlement summary

    Market lookup key priority: condition_id > market_slug > token_id
    All three Gamma API approaches are tried before giving up on a market.
    """
    if not COPY_TRADE_ENABLED:
        logging.info("COPY_SETTLEMENT_LOOP disabled via COPY_TRADE_ENABLED=false — exiting task")
        return

    logging.info(
        "COPY_SETTLEMENT_LOOP_BOOT interval=%ss batch=%s",
        COPY_SETTLEMENT_LOOP_INTERVAL,
        COPY_SETTLEMENT_BATCH_SIZE,
    )

    while True:
        open_positions = load_open_copied_positions(COPY_SETTLEMENT_BATCH_SIZE)

        settled = 0
        skipped_unresolved = 0
        skipped_no_data = 0
        skipped_sell = 0
        errors = 0
        settled_wallets: set[str] = set()

        # Per-tick market resolution cache: market_key → resolution_data | None
        # Avoids duplicate Gamma API calls for positions on the same market.
        market_resolution_cache: dict[str, dict | None] = {}

        logging.info(
            "COPY_SETTLEMENT_TICK_START open_positions=%s",
            len(open_positions),
        )

        for pos in open_positions:
            pos_id = str(pos.get("id") or "")
            wallet_address = pos.get("wallet_address") or ""

            try:
                # ── Step 1: Build market lookup key ───────────────────────
                condition_id = pos.get("condition_id")
                market_slug = pos.get("market_slug")
                token_id = pos.get("token_id")

                market_key = condition_id or market_slug or token_id
                if not market_key:
                    logging.info(
                        "COPY_SETTLE_NO_MARKET_KEY pos=%s — no condition_id/slug/token_id",
                        pos_id[:8],
                    )
                    skipped_no_data += 1
                    continue

                # ── Step 2: Fetch resolution (deduplicated per market) ─────
                if market_key not in market_resolution_cache:
                    resolution_data = await asyncio.to_thread(
                        _fetch_gamma_market_data_sync,
                        condition_id,
                        market_slug,
                        token_id,
                    )
                    market_resolution_cache[market_key] = resolution_data

                    # Enrich market_cache with whatever we learned
                    if resolution_data and market_slug:
                        _update_market_cache_from_resolution(market_slug, resolution_data)

                resolution_data = market_resolution_cache[market_key]

                if not resolution_data:
                    logging.debug(
                        "COPY_SETTLE_NO_RESOLUTION_DATA pos=%s slug=%s",
                        pos_id[:8],
                        market_slug or "?",
                    )
                    skipped_no_data += 1
                    continue

                # ── Step 3: Check whether market has resolved ─────────────
                if not resolution_data.get("resolved"):
                    skipped_unresolved += 1
                    continue

                # ── Step 4: Compute exit price ────────────────────────────
                exit_price = compute_settlement_exit_price(pos, resolution_data)
                if exit_price is None:
                    # Could be SELL position or missing outcome data
                    if str(pos.get("side") or "BUY").upper() == "SELL":
                        skipped_sell += 1
                    else:
                        skipped_no_data += 1
                    continue

                # ── Step 5: Close the position ────────────────────────────
                close_copied_position(pos, exit_price, resolution_data)
                settled += 1
                if wallet_address:
                    settled_wallets.add(wallet_address)

            except Exception:
                logging.exception("COPY_SETTLE_POSITION_FAIL pos=%s", pos_id)
                errors += 1

        # ── Step 6: Refresh wallet_metrics for wallets with new closures ──
        for wallet_address in settled_wallets:
            try:
                update_wallet_metrics_for_address(wallet_address)
            except Exception:
                logging.exception(
                    "COPY_SETTLE_METRICS_FAIL wallet=%s", wallet_address[:10]
                )

        logging.info(
            "COPY_SETTLEMENT_TICK_DONE scanned=%s settled=%s "
            "skipped_unresolved=%s skipped_no_data=%s skipped_sell=%s errors=%s",
            len(open_positions),
            settled,
            skipped_unresolved,
            skipped_no_data,
            skipped_sell,
            errors,
        )

        await asyncio.sleep(COPY_SETTLEMENT_LOOP_INTERVAL)


# =============================================================================
# STARTUP — MAIN ENTRY POINT (REUSABLE SKELETON)
# =============================================================================
# main() builds the ClobClient and launches all long-running asyncio tasks.
# _run_forever() wraps each coroutine in a crash-safe restart loop.
#
# REUSABLE: The asyncio.gather + _run_forever pattern is fully reusable.
# COPY-TRADE HOOK: Add a copy_market_config_loop task.
#                  Remove or keep rotate_loop (BTC-SPECIFIC) as appropriate.
# =============================================================================

async def main():
    trading_client = build_trading_client()
    tasks = []
    # ── BTC strategy tasks (existing — do not reorder or remove) ──────────────
    tasks.append(asyncio.create_task(_run_forever("rotate_loop", rotate_loop)))
    tasks.append(asyncio.create_task(_run_forever("paper_settlement_loop", paper_settlement_loop)))
    tasks.append(asyncio.create_task(_run_forever("live_balance_loop", live_balance_loop, trading_client)))
    tasks.append(asyncio.create_task(_run_forever("scan_loop", scan_loop)))
    tasks.append(asyncio.create_task(_run_forever("heartbeat_loop", heartbeat_loop, trading_client)))
    # ── Copy-trading tasks (additive — isolated from BTC strategy tasks) ──────
    # Both loops are controlled by COPY_TRADE_ENABLED env var.
    # trading_client is passed to copy_trade_loop so LIVE bots can submit CLOB
    # orders. PAPER bots ignore it. Safe to disable without affecting BTC tasks.
    tasks.append(asyncio.create_task(_run_forever("copy_trade_loop", copy_trade_loop, trading_client)))
    tasks.append(asyncio.create_task(_run_forever("copy_settlement_loop", copy_settlement_loop)))
    restart_ws_task()
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        if ws_task:
            ws_task.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.gather(*tasks, return_exceptions=True)
        with suppress(asyncio.CancelledError):
            if ws_task:
                await ws_task


if __name__ == "__main__":
    asyncio.run(main())
