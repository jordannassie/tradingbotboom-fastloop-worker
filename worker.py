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
from contextlib import nullcontext, suppress
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
# ── Module-level boot markers ─────────────────────────────────────────────────
# These fire the instant Python executes worker.py — before main(), before any
# asyncio task, before any conditional branch.  Search Railway logs for these
# to confirm the correct build is deployed.
#
# COPY_WORKER_BUILD: proves the copy-trading arm_live routing code is present.
#   If absent after restart → git push / redeploy has not completed.
#   live_routing=copy_bots.arm_live  confirms new routing (not legacy mode field).
#
# WORKER_BOOT: general build marker.  Update build= when deploying a new version.
logging.warning(
    "COPY_WORKER_BUILD "
    "architecture=shared_copy_brain "
    "live_routing=copy_bots.arm_live "
    "legacy_btc_routing=disabled_for_copy_trading "
    "env_COPY_LIVE_ENABLED=%s "
    "env_COPY_TRADE_ENABLED=%s "
    "COPY_LIVE_MAX_TRADE_USD=%s "
    "multi_live_bots=UNLIMITED",
    COPY_LIVE_ENABLED,
    COPY_TRADE_ENABLED,
    COPY_LIVE_MAX_TRADE_USD,
)
logging.warning("WORKER_BOOT build=SHARED_BRAIN_V1")
logging.warning(
    "COPY_DB_LIMIT_EFFECTIVE COPY_WALLET_TRADE_DB_LIMIT=%s "
    "COPY_WALLET_TRADE_FETCH_LIMIT=%s COPY_TRADE_LOOKBACK_HOURS=%s "
    "— if COPY_WALLET_TRADE_DB_LIMIT is 200 in Railway env vars, "
    "remove or raise it; old value hides SELL events and blocks closes",
    COPY_WALLET_TRADE_DB_LIMIT,
    COPY_WALLET_TRADE_FETCH_LIMIT,
    COPY_TRADE_LOOKBACK_HOURS,
)
# ── Proof-of-deploy marker ────────────────────────────────────────────────────
# This line proves which code version Railway is actually running.
# If you see COPY_LIVE_GATE_L4_FAIL in Railway, the container below is NOT
# the one emitting it — an older deployment is still alive.
# Confirm by searching Railway logs for DEPLOY_PROOF — only the new container
# will emit it.  If you cannot find DEPLOY_PROOF, Railway has NOT deployed
# this commit.
logging.warning(
    "DEPLOY_PROOF commit=THIS_COMMIT "
    "gate_L4=REMOVED "
    "gate_too_many_live_bots=REMOVED "
    "multi_live_bots=UNLIMITED "
    "sell_always_closes_db=TRUE "
    "g6_condition_id_for_sell=FIXED "
    "live_sell_clob_decoupled_from_db_close=FIXED "
    "copy_closes_default=TRUE "
    "ema_5m_btc_strategy=ADDED "
    "— if you see COPY_LIVE_GATE_L4_FAIL alongside this, "
    "two containers are running simultaneously"
)
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
            "scope=btcbot_strategy_only Loaded strategy settings bot_id=%s is_enabled=%s edge_threshold=%s arm_live=%s",
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
                "STUCK_DETECTOR_LIVE scope=btcbot_strategy_only "
                "no_live_orders_for_s=%s live_master_enabled=%s "
                "armed_sniper=%s armed_fastloop=%s slug=%s",
                now_ts - last_live_order_ts,
                live_master_enabled,
                sniper_settings["arm_live"],
                fastloop_settings["arm_live"],
                slug_field,
            )
        if now_ts - last_any_order_ts > STUCK_ANY_SECONDS:
            logging.warning(
                "STUCK_DETECTOR_ANY scope=btcbot_strategy_only "
                "no_orders_for_s=%s live_master_enabled=%s "
                "armed_sniper=%s armed_fastloop=%s slug=%s",
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
                        EMA_5M_BOT_ID,   # btc_5m_ema settled via BTC price move
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
                if bot_id == EMA_5M_BOT_ID:
                    # EMA strategy uses its own isolated accounting path with
                    # EMA_BALANCE_BEFORE / EMA_POSITION_CLOSE_PNL / EMA_BALANCE_AFTER logs.
                    _ema5m_apply_realized_pnl_sync(pnl_usd, str(row_id), market_slug or "")
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
        # ── Exposure caps (read fresh from DB every tick — no redeploy needed) ──
        # paper_max_exposure_usd: hard USD cap on total open PAPER exposure
        #   across all OPEN positions for a given bot. 0 = unlimited.
        "paper_max_exposure_usd": 1000.0,
        # live_max_exposure_usd: hard USD cap on total open LIVE exposure across
        #   all LIVE-mode bots combined. 0 = unlimited.
        #   Update this field in copy_global_settings to change the cap at runtime.
        "live_max_exposure_usd": 500.0,
        # ── Paper reset ───────────────────────────────────────────────────────
        # paper_reset_pending: set to true in the DB to trigger a clean paper
        #   reset on the next copy_trade_loop tick. Worker sets it back to false
        #   after completing the reset.
        "paper_reset_pending": False,
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
            # Merge DB row over defaults, but SKIP null/None DB values so that
            # a column that exists in the schema but is NULL does not override a
            # non-null Python default (e.g. paper_max_exposure_usd: null in DB
            # must not zero-out the default 1000.0 and disable the cap).
            db_row = {k: v for k, v in resp.data[0].items() if v is not None}
            return {**defaults, **db_row}
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


# ── Market classification ─────────────────────────────────────────────────────
#
# classify_market() assigns a class to a Polymarket market based on slug/title
# alone — no DB query required.  The class is stored in market_cache.market_class
# (column must be added via migration; see Phase 2 migration notes below).
#
# Classes:
#   FAST_MARKET    — short-duration crypto up/down prediction markets
#   SLOW_MARKET    — long-dated / monthly resolution markets
#   BLOCKED_MARKET — sports, esports, politics (blocked for copy trading)
#   UNKNOWN        — anything not confidently classified
#
# Rules are intentionally conservative: when in doubt, return UNKNOWN rather
# than FAST so the new gates in evaluate_copy_trade_shared don't over-block.
#
# Phase 2 migration: ALTER TABLE market_cache ADD COLUMN IF NOT EXISTS
#   market_class text DEFAULT 'UNKNOWN';

_FAST_MARKET_SLUG_PREFIXES: tuple[str, ...] = (
    "btc-updown-", "eth-updown-", "sol-updown-", "xrp-updown-",
    "btc-up-", "eth-up-", "sol-up-", "xrp-up-",
    "bitcoin-up", "ethereum-up", "solana-up",
)
_FAST_MARKET_SLUG_CONTAINS: tuple[str, ...] = (
    "updown", "up-down", "higher-or-lower", "above-or-below",
    "will-btc", "will-eth", "will-sol", "will-xrp",
    "btc-price", "eth-price", "sol-price",
)
_SLOW_MARKET_KEYWORDS: tuple[str, ...] = (
    "in january", "in february", "in march", "in april", "in may", "in june",
    "in july", "in august", "in september", "in october", "in november", "in december",
    "by january", "by february", "by march", "by april", "by may", "by june",
    "by july", "by august", "by september", "by october", "by november", "by december",
    "by end of", "will reach", "in 2025", "in 2026", "in 2027", "end of year",
    "q1 ", "q2 ", "q3 ", "q4 ", "quarterly", "monthly", "annual", "year-end",
    "ath by", "all-time high by", "hit $", "reach $", "exceed $",
)
_BLOCKED_MARKET_KEYWORDS: tuple[str, ...] = (
    "nba", "nfl", "nhl", "mlb", "soccer", "football-", "basketball", "baseball",
    "hockey", "tennis", "golf-", "boxing", "mma", "ufc", "csgo", "dota", "-lol-",
    "esport", "fortnite", "election", "president", "senate", "congress", "vote",
    "republican", "democrat", "trump", "harris", "premier-league", "world-cup",
    "super-bowl", "champions-league", "nba-finals", "world-series",
)


def classify_market(
    market_slug: "str | None",
    market_title: "str | None",
) -> str:
    """
    Classify a Polymarket market into FAST_MARKET | SLOW_MARKET | BLOCKED_MARKET | UNKNOWN.

    Conservative: prefers UNKNOWN over incorrect FAST classification.
    Uses slug + title only — no DB or API calls.
    """
    slug  = (market_slug  or "").lower().strip()
    title = (market_title or "").lower().strip()
    combined = slug + " " + title

    # BLOCKED — checked first (highest priority)
    for kw in _BLOCKED_MARKET_KEYWORDS:
        if kw in combined:
            return "BLOCKED_MARKET"

    # SLOW — long-dated resolution keywords
    for kw in _SLOW_MARKET_KEYWORDS:
        if kw in combined:
            return "SLOW_MARKET"

    # FAST — must match slug prefix or explicit updown-style slug pattern
    for prefix in _FAST_MARKET_SLUG_PREFIXES:
        if slug.startswith(prefix):
            return "FAST_MARKET"
    for kw in _FAST_MARKET_SLUG_CONTAINS:
        if kw in slug:
            return "FAST_MARKET"

    return "UNKNOWN"


def upsert_market_cache(trade_row: dict) -> None:
    """
    Upsert market_cache from a normalized wallet_trade dict.
    Populates what we know from the trade; leaves other fields as DB defaults.

    When we see the same market_slug from a YES trade and later a NO trade,
    the upserts progressively fill in yes_token_id then no_token_id.

    Phase 1 addition: also writes market_class computed from classify_market().
    If the market_class column does not yet exist in the DB, falls back to
    an upsert without it — no crash, just an INFO log.
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

    # Compute and include market classification (Phase 1 addition).
    market_class = classify_market(
        trade_row.get("market_slug"),
        trade_row.get("market_title"),
    )

    try:
        supabase.table("market_cache").upsert(
            {**payload, "market_class": market_class},
            on_conflict="market_slug",
        ).execute()
    except Exception as exc:
        exc_str = str(exc).lower()
        # If the column doesn't exist yet, fall back gracefully.
        if any(kw in exc_str for kw in ("market_class", "column", "schema", "42703")):
            logging.info(
                "COPY_MARKET_CLASS_COL_MISSING slug=%s class=%s "
                "— market_class column not yet in DB; upserting without it. "
                "Run Phase 2 migration to add the column.",
                market_slug, market_class,
            )
            try:
                supabase.table("market_cache").upsert(payload, on_conflict="market_slug").execute()
            except Exception as exc2:
                logging.warning("COPY_UPSERT_MARKET_CACHE_FAIL slug=%s err=%s", market_slug, exc2)
        else:
            logging.warning(
                "COPY_UPSERT_MARKET_CACHE_FAIL slug=%s class=%s err=%s",
                market_slug, market_class, exc,
            )


def get_unevaluated_trades_for_bot(
    wallet_address: str,
    bot_id: str,
    lookback_hours: int = 24,
    limit: int = 1000,
    copy_closes: bool = False,
    bot_label: str = "",
) -> list[dict]:
    """
    Return wallet_trades for this wallet that have NOT yet been evaluated
    by this specific copy_bot (i.e., no copy_attempts row exists).

    Two-step query:
      1. Fetch recent wallet_trades for the wallet within the lookback window.
      2. Fetch already-attempted source_trade_ids for this bot.
      3. Return the difference.

    Re-evaluation of locked-out SELL trades (copy_attempts dedup unlock):
      When copy_closes=True, SELL trades that were previously skipped ONLY
      because copy_closes was False at the time (skip_reason='closes_not_enabled')
      are treated as unevaluated and returned for re-evaluation. This allows
      positions to be closed after copy_closes is enabled on a bot that was
      running with copy_closes=False.

    limit controls how many wallet_trades rows are scanned.  The default is
    1000 (raised from 200 → 500 → 1000).  If Railway has a lower value set
    as an env var (COPY_WALLET_TRADE_DB_LIMIT), older SELL events will be
    invisible.  Set to 0 to remove the cap entirely (full lookback window).
    """
    _label = bot_label or bot_id[:8]
    try:
        cutoff_ts = time() - (lookback_hours * 3600)
        cutoff = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc).isoformat()

        _query = (
            supabase.table("wallet_trades")
            .select("*")
            .eq("wallet_address", wallet_address)
            .gte("traded_at", cutoff)
            .order("traded_at", desc=True)
        )
        # limit=0 means no cap — return every row in the lookback window.
        if limit > 0:
            _query = _query.limit(limit)
        trades_resp = _query.execute()
        all_trades = trades_resp.data or []
        if not all_trades:
            return []

        # Warn if we saturated the limit — SELL events beyond the window are
        # invisible.  Also cross-check open positions so the operator knows
        # whether any existing positions are at risk of a missed close.
        if limit > 0 and len(all_trades) >= limit:
            # Count open positions for this bot to judge severity.
            _open_count = get_open_positions_count(bot_id)
            if _open_count > 0:
                logging.warning(
                    "COPY_UNEVALUATED_LIMIT_HIT bot=%s wallet=%s limit=%s "
                    "open_positions=%s — query saturated AND bot has open "
                    "positions.  SELL events for older opens ARE INVISIBLE. "
                    "Fix: remove COPY_WALLET_TRADE_DB_LIMIT env var in Railway "
                    "or raise it above %s.",
                    _label, wallet_address[:10], limit, _open_count, limit,
                )
            else:
                logging.warning(
                    "COPY_UNEVALUATED_LIMIT_HIT bot=%s wallet=%s limit=%s "
                    "open_positions=0 — query saturated but no open positions "
                    "currently; SELL events for future opens may be invisible if "
                    "this wallet stays active.  Fix: remove or raise "
                    "COPY_WALLET_TRADE_DB_LIMIT in Railway.",
                    _label, wallet_address[:10], limit,
                )

        trade_ids = [t["source_trade_id"] for t in all_trades]

        # Fetch already-attempted source_trade_ids for this bot + wallet.
        #
        # ROOT CAUSE (original bug): .in_("source_trade_id", trade_ids) built a
        # GET URL with all UUIDs in the query string — >37 KB with 1 000 IDs —
        # causing httpx.InvalidURL.
        #
        # ROOT CAUSE (Phase 3 partial fix): chunking at 50 IDs still left URL
        # length risk for wallets with long Polymarket hex trade IDs (~66 chars
        # each: 50 × 66 chars + overhead can breach httpx/rfc3986 limits on
        # certain builds).
        #
        # FINAL FIX: scalar-only filters — no .in_() list param, no URL-size
        # risk regardless of wallet activity or trade-ID length.
        # Logical equivalence: all copy_attempts for this bot + wallet within the
        # same lookback window cover exactly the trades we are about to evaluate.
        _attempts_fetch_limit = min(max(len(trade_ids) * 3, 200), 3000)
        try:
            _att_resp = (
                supabase.table("copy_attempts")
                .select("source_trade_id, skip_reason, source_side, copied")
                .eq("copy_bot_id", bot_id)
                .eq("wallet_address", wallet_address)
                .gte("created_at", cutoff)
                .order("created_at", desc=True)
                .limit(_attempts_fetch_limit)
                .execute()
            )
            attempt_rows: list[dict] = _att_resp.data or []
        except Exception as _att_exc:
            logging.warning(
                "COPY_UNEVALUATED_ATTEMPTED_FETCH_CHUNK_FAIL bot=%s wallet=%s err=%s "
                "— attempts fetch failed; all %s trades treated as unevaluated this tick",
                _label, wallet_address[:10], _att_exc, len(trade_ids),
            )
            attempt_rows = []
        logging.info(
            "COPY_UNEVALUATED_ATTEMPTED_FETCH_SUMMARY bot=%s wallet=%s "
            "trades_in_window=%s attempt_rows=%s limit=%s",
            _label, wallet_address[:10],
            len(trade_ids), len(attempt_rows), _attempts_fetch_limit,
        )
        logging.info(
            "COPY_UNEVALUATED_SUMMARY bot=%s wallet=%s "
            "total_trades=%s attempt_rows=%s",
            _label, wallet_address[:10],
            len(trade_ids), len(attempt_rows),
        )

        # Group attempts by source_trade_id to handle trades with multiple rows.
        from collections import defaultdict
        attempts_by_trade: dict[str, list[dict]] = defaultdict(list)
        for row in attempt_rows:
            attempts_by_trade[row["source_trade_id"]].append(row)

        attempted_ids: set[str] = set()
        unlocked_sell_count = 0

        for tid, attempts in attempts_by_trade.items():
            was_ever_copied = any(bool(a.get("copied")) for a in attempts)
            all_closes_not_enabled = all(
                a.get("skip_reason") == "closes_not_enabled" for a in attempts
            )
            any_is_sell = any(
                str(a.get("source_side") or "").upper() == "SELL" for a in attempts
            )

            # Unlock: when copy_closes is now True and this SELL was only ever
            # skipped because copy_closes was False at the time.
            if (
                copy_closes
                and all_closes_not_enabled
                and any_is_sell
                and not was_ever_copied
            ):
                unlocked_sell_count += 1
                logging.info(
                    "COPY_SELL_UNLOCKED bot=%s trade=%s — was closes_not_enabled, "
                    "re-eligible now that copy_closes=True",
                    _label,
                    tid[:24],
                )
                # Do NOT add to attempted_ids — this trade will be re-evaluated.
                continue

            attempted_ids.add(tid)

        if unlocked_sell_count:
            logging.info(
                "COPY_SELL_UNLOCK_SUMMARY bot=%s wallet=%s unlocked=%s",
                _label,
                wallet_address[:10],
                unlocked_sell_count,
            )

        unevaluated = [t for t in all_trades if t["source_trade_id"] not in attempted_ids]

        # Log any SELL events present in the unevaluated set so the close path
        # is fully traceable in logs.  WARNING so it's visible in Railway at any
        # log-level filter — this is the first confirmation that SELLs will be
        # processed by the shared brain.
        sell_unevaluated = [t for t in unevaluated if str(t.get("side") or "").upper() == "SELL"]
        if sell_unevaluated:
            logging.warning(
                "COPY_SELL_EVENTS_QUEUED bot=%s wallet=%s sell_count=%s "
                "slugs=%s tokens=%s "
                "— these SELL events will now enter evaluate_copy_trade_shared",
                _label,
                wallet_address[:10],
                len(sell_unevaluated),
                [t.get("market_slug") or "?" for t in sell_unevaluated[:5]],
                [str(t.get("token_id") or "?")[:16] for t in sell_unevaluated[:5]],
            )

        return unevaluated

    except Exception:
        logging.exception(
            "COPY_GET_UNEVALUATED_TRADES_FAIL wallet=%s bot=%s",
            wallet_address[:10],
            _label,
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


def _get_paper_exposure_simple() -> float:
    """
    Single source of truth for current PAPER open exposure.

    Direct query on copied_positions: status=OPEN AND raw_json->>'mode'='PAPER'.
    No RPC, no bot joins, no copy_bots lookup, no enabled/disabled filtering.
    raw_json.mode is ALWAYS stored as uppercase by open_copied_position.

    Returns 999_999.0 on any failure (fail-closed).
    """
    try:
        resp = (
            supabase.table("copied_positions")
            .select("size")
            .eq("status", "OPEN")
            .filter("raw_json->>mode", "eq", "PAPER")
            .limit(50000)
            .execute()
        )
        rows  = resp.data or []
        total = round(sum(float_or_none(r.get("size")) or 0.0 for r in rows), 4)
        if len(rows) >= 50000:
            logging.warning("PAPER_EXPOSURE_LIMIT_HIT total>=%.4f", total)
        return total
    except Exception:
        logging.exception("PAPER_EXPOSURE_DIRECT_FAIL — returning 999999 (fail-closed)")
        return 999_999.0


def _get_exposure_direct_query(mode: str) -> float:
    """
    Direct table-query fallback for exposure lookup.

    Called when the RPC aggregate returns None, an unexpected type, or raises.
    Queries copied_positions.raw_json->>'mode' directly — the mode field is
    ALWAYS stored as uppercase (e.g. 'PAPER', 'LIVE') by open_copied_position,
    so this is immune to any copy_bots.mode column case-sensitivity issue.

    Returns 999_999.0 on any failure (fail-closed).
    """
    mode_upper = mode.upper()
    try:
        pos_resp = (
            supabase.table("copied_positions")
            .select("size")
            .eq("status", "OPEN")
            .filter("raw_json->>mode", "eq", mode_upper)
            .limit(50000)
            .execute()
        )
        rows  = pos_resp.data or []
        total = round(sum(float_or_none(r.get("size")) or 0.0 for r in rows), 4)

        if len(rows) >= 50000:
            logging.warning(
                "COPY_EXPOSURE_DIRECT_LIMIT_HIT mode=%s total>=%.4f",
                mode_upper, total,
            )

        logging.info(
            "COPY_EXPOSURE_DIRECT_OK mode=%s exposure=%.4f positions=%s",
            mode_upper, total, len(rows),
        )
        return total

    except Exception:
        logging.exception(
            "COPY_EXPOSURE_DIRECT_FAIL mode=%s — returning 999999 (fail-closed)",
            mode,
        )
        return 999_999.0


def get_copy_open_exposure_for_mode(mode: str) -> float:
    """
    Return current total open USD exposure for all positions in the given mode
    ('paper' or 'live').

    Primary path: calls the Postgres aggregate RPC function
    public.copy_open_exposure_for_mode(mode) — zero row-limit risk.

    Fallback path: _get_exposure_direct_query(mode) — used when the RPC
    returns None (function missing, NULL result, wrong SQL) or an unexpected
    type.  This ensures the gate always has a real value and never silently
    treats a broken RPC as "zero exposure".

    Fail-closed contract:
      RPC numeric result   → use it
      RPC None/unexpected  → _get_exposure_direct_query (may still be 0.0
                             if there are genuinely no open positions, but
                             at least it's sourced from real table data)
      RPC exception        → _get_exposure_direct_query
      Direct query fail    → 999_999.0 (blocks BUYs)
    """
    try:
        resp = supabase.rpc(
            "copy_open_exposure_for_mode", {"p_mode": mode.lower()}
        ).execute()
        val = resp.data

        # Always log the raw RPC response so we can see exactly what the
        # DB function is returning on every BUY evaluation.
        logging.info(
            "COPY_EXPOSURE_RPC_RAW mode=%s data_type=%s raw_value=%r",
            mode, type(val).__name__, val,
        )

        # Scalar numeric — the expected happy path.
        if isinstance(val, (int, float)):
            result = round(float(val), 4)
            logging.info("COPY_EXPOSURE_RPC_OK mode=%s exposure=%.4f", mode, result)
            return result

        # String — PostgREST returns NUMERIC as string in some library versions.
        if isinstance(val, str):
            try:
                result = round(float(val), 4)
                logging.info(
                    "COPY_EXPOSURE_RPC_OK mode=%s exposure=%.4f (from string)",
                    mode, result,
                )
                return result
            except ValueError:
                pass  # fall through to direct query below

        # None or unexpected type — RPC function may not exist, may return NULL
        # (missing COALESCE), or may have wrong filter logic.
        # CRITICAL: do NOT return 0.0 here — that would silently disable the cap.
        # Instead, fall through to the direct table query so we always have a
        # real exposure value.
        logging.warning(
            "COPY_EXPOSURE_RPC_UNUSABLE mode=%s data_type=%s raw=%r — "
            "falling back to direct table query.  "
            "If data_type=NoneType: add COALESCE to copy_open_exposure_for_mode "
            "or the function may not exist.",
            mode, type(val).__name__, repr(val)[:120],
        )
        return _get_exposure_direct_query(mode)

    except Exception:
        logging.exception(
            "COPY_EXPOSURE_RPC_FAIL mode=%s — falling back to direct table query",
            mode,
        )
        return _get_exposure_direct_query(mode)


# ── Per-mode BUY execution locks ─────────────────────────────────────────────
# One asyncio.Lock per mode (PAPER / LIVE).  Acquired around the full
# exposure-check → position-open sequence for BUY trades to prevent two
# concurrent coroutines from both passing the cap gate before either one
# has committed its new position row to the DB.
# SELL / CLOSE paths must never acquire these locks.
_copy_buy_locks: dict[str, asyncio.Lock] = {}


def _get_copy_buy_lock(mode: str) -> asyncio.Lock:
    """Return (creating lazily) the asyncio BUY lock for the given mode."""
    key = mode.upper()
    if key not in _copy_buy_locks:
        _copy_buy_locks[key] = asyncio.Lock()
    return _copy_buy_locks[key]


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

    Primary query scopes to raw_json->>'mode' = 'LIVE' rows so that paper
    positions accumulated before arm_live was set do not inflate the count.

    If the JSON-path filter raises (e.g. PostgREST version or JSONB mismatch),
    falls back to an unfiltered query rather than returning 999.  The fallback
    count may include paper rows and is logged with a warning so the operator
    can investigate.  Returns 999 only if both queries fail.
    """
    if not live_bot_ids:
        return 0

    # ── Primary: raw_json->>'mode' = 'LIVE' scoped count ──────────────────────
    try:
        resp = (
            supabase.table("copied_positions")
            .select("id, market_slug")
            .in_("copy_bot_id", live_bot_ids)
            .eq("status", "OPEN")
            .filter("raw_json->>mode", "eq", "LIVE")
            .execute()
        )
        rows  = resp.data or []
        count = len(rows)
        logging.info(
            "COPY_LIVE_OPEN_COUNT_RESULT scope=live_mode_only "
            "live_bots=%s open_live_count=%s",
            len(live_bot_ids), count,
        )
        if count >= COPY_LIVE_MAX_OPEN_POSITIONS:
            _slugs = [r.get("market_slug") or r.get("id") for r in rows]
            logging.warning(
                "COPY_LIVE_OPEN_POSITIONS_BLOCKING live_bots=%s count=%s max=%s "
                "blocking_positions=%s — check DB for stale open rows",
                len(live_bot_ids), count, COPY_LIVE_MAX_OPEN_POSITIONS, _slugs,
            )
        return count
    except Exception as _mode_exc:
        logging.warning(
            "COPY_LIVE_OPEN_COUNT_MODE_FILTER_FAIL live_bots=%s err=%s "
            "— retrying without mode filter; result may include paper positions",
            len(live_bot_ids), _mode_exc,
        )

    # ── Fallback: unfiltered count (paper + live positions) ───────────────────
    try:
        resp_fb = (
            supabase.table("copied_positions")
            .select("id")
            .in_("copy_bot_id", live_bot_ids)
            .eq("status", "OPEN")
            .execute()
        )
        count_fb = len(resp_fb.data or [])
        logging.warning(
            "COPY_LIVE_OPEN_COUNT_FALLBACK scope=all_modes "
            "live_bots=%s open_count=%s — includes paper rows if any",
            len(live_bot_ids), count_fb,
        )
        return count_fb
    except Exception:
        logging.exception("COPY_LIVE_OPEN_POS_COUNT_FAIL")
        return 999  # fail-safe: block orders when count is completely unknown


def get_live_open_exposure(live_bot_ids: list[str]) -> float:
    """
    Deprecated shim — delegates to get_copy_open_exposure_for_mode('live').
    Kept so any remaining call sites continue to work without changes.
    """
    return get_copy_open_exposure_for_mode("live")


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


def _check_live_account_ready() -> "tuple[bool, str]":
    """
    In-memory readiness check for the live Polymarket CLOB account.

    Uses cached values populated by live_balance_loop / derive_wallet_addresses —
    no additional API calls, zero latency.

    Signals checked (in order):
      live_signer_address   — proxy wallet registered and returned by CLOB API.
                              None means the account has never completed setup
                              or the API auth failed at startup.
      live_allowance_cache  — USDC token approval (collateral allowance) > 0.
                              Zero or None means the USDC approval transaction
                              has not been submitted / confirmed yet.
      live_balance_cache    — USDC balance > 0.
                              Zero or None means the account has no funds.

    Only applied to BUY orders — SELL/mirror closes are always allowed through.

    Returns (True, "") when all signals are healthy.
    Returns (False, reason_string) on the first failing signal.
    """
    if not live_signer_address:
        return False, "proxy_wallet_not_initialized"
    # None means the live_balance_loop hasn't refreshed yet — allow through so a
    # cold-start worker can still attempt live orders; the CLOB will reject if the
    # account truly has no funds.  Only block if the cache was fetched and is
    # explicitly zero (approval revoked / account empty).
    if live_allowance_cache is not None and live_allowance_cache <= 0:
        return False, "usdc_allowance_zero"
    if live_balance_cache is not None and live_balance_cache <= 0:
        return False, "usdc_balance_zero"
    return True, ""


def evaluate_and_execute_live_copy_trade(
    copy_bot: dict,
    wallet_trade: dict,
    global_settings: dict,
    trading_client: "ClobClient",
    live_bot_ids: list[str],
    submitted_size: float,
    submitted_price: float,
) -> "tuple[bool, str | None, float | None, float | None, dict]":
    """
    Apply live-ONLY safety gates and submit a real CLOB order.

    Called AFTER evaluate_copy_trade_shared() (the shared brain) passes.
    Common gates (emergency_stop, closes filter, delay, rate limit, market data,
    price, sizing) are intentionally NOT re-checked here — they already ran in
    the shared brain.

    Returns (ok, skip_reason, final_size_usd, actual_price, raw_response).

    Live-only gates (in order):
      L8   live_global_hourly_cap      — global hourly live cap exceeded
      L9   live_open_positions_limit   — open live positions ≥ COPY_LIVE_MAX_OPEN_POSITIONS
      L9b  live_max_exposure_reached   — BUY would exceed live_max_exposure_usd
      L10  insufficient_market_data    — token_id missing (required for CLOB)
    Then: apply COPY_LIVE_MAX_TRADE_USD hard cap → submit CLOB order.
    """
    bot_id     = str(copy_bot["id"])
    _bot_name  = copy_bot.get("name") or bot_id[:8]
    _wallet    = str(wallet_trade.get("wallet_address") or "?")[:16]
    _trade_id  = str(wallet_trade.get("source_trade_id") or "?")[:20]
    trade_side = str(wallet_trade.get("side") or "").upper()

    # Entry diagnostic — after shared brain, before live-only gates.
    logging.info(
        "COPY_LIVE_EVAL_ENTRY bot=%s wallet=%s trade=%s side=%s "
        "live_on=%s arm_live=%s live_bots=%s "
        "max_open_pos=%s max_trades_hr=%s size=%.4f price=%.4f",
        _bot_name, _wallet, _trade_id, trade_side,
        bool(global_settings.get("live_on")),
        bool(copy_bot.get("arm_live")),
        len(live_bot_ids),
        COPY_LIVE_MAX_OPEN_POSITIONS, COPY_LIVE_MAX_TRADES_PER_HOUR,
        submitted_size, submitted_price,
    )

    # ── Token-ID resolution (pre-gate) ───────────────────────────────────────
    # wallet_trade.token_id may be null when the Polymarket Data API omits it
    # for recent trades, or when a market token rotated between the source
    # wallet's trade and our evaluation window.
    #
    # Resolution order (additive — no change if token_id already present):
    #   Source 1: current_slug globals   — zero-latency; current BTC 5m window only.
    #   Source 2: market_cache DB lookup — any slug previously seen by the worker.
    #   Source 3: Gamma API exact-slug   — authoritative fallback; populates
    #             market_cache on success so future ticks skip the API call.
    #
    # All sources use exact slug match. No token is ever borrowed from a different
    # market. If all sources fail, Gate LB / Gate L10 block the trade as before.
    if not wallet_trade.get("token_id"):
        _wt_slug      = str(wallet_trade.get("market_slug") or "")
        _wt_outcome   = str(wallet_trade.get("outcome") or "").upper()
        _wt_cond_id   = wallet_trade.get("condition_id")
        _sources_tried: list[str] = []
        logging.info(
            "COPY_LIVE_TOKEN_RESOLVE_ATTEMPT bot=%s trade=%s "
            "slug=%s outcome=%s current_slug=%s "
            "token_id_present=%s condition_id_present=%s "
            "has_yes_token=%s has_no_token=%s",
            _bot_name, _trade_id,
            _wt_slug or "?", _wt_outcome or "?", current_slug or "none",
            bool(wallet_trade.get("token_id")), bool(_wt_cond_id),
            bool(current_yes_token), bool(current_no_token),
        )
        _resolved_token: str | None = None
        _resolve_source = "none"

        # Source 1: current slug globals (zero-latency, no DB/API call)
        _sources_tried.append("current_slug")
        if _wt_slug and current_slug and _wt_slug == current_slug:
            if _wt_outcome in ("YES", "UP") and current_yes_token:
                _resolved_token = current_yes_token
                _resolve_source = "current_slug"
            elif _wt_outcome in ("NO", "DOWN") and current_no_token:
                _resolved_token = current_no_token
                _resolve_source = "current_slug"

        # Source 2: market_cache exact-slug lookup
        if not _resolved_token and _wt_slug:
            _sources_tried.append("market_cache")
            try:
                _mc_resp = (
                    supabase.table("market_cache")
                    .select("yes_token_id, no_token_id")
                    .eq("market_slug", _wt_slug)
                    .limit(1)
                    .execute()
                )
                _mc_rows = _mc_resp.data or []
                if _mc_rows:
                    _mc = _mc_rows[0]
                    if _wt_outcome in ("YES", "UP") and (_mc.get("yes_token_id") or ""):
                        _resolved_token = str(_mc["yes_token_id"])
                        _resolve_source = "market_cache"
                    elif _wt_outcome in ("NO", "DOWN") and (_mc.get("no_token_id") or ""):
                        _resolved_token = str(_mc["no_token_id"])
                        _resolve_source = "market_cache"
            except Exception as _mc_exc:
                logging.warning(
                    "COPY_LIVE_TOKEN_MARKET_CACHE_FAIL bot=%s trade=%s "
                    "slug=%s err=%s",
                    _bot_name, _trade_id, _wt_slug, _mc_exc,
                )

        # Source 3: Gamma API exact-slug fetch (authoritative; populates cache)
        if not _resolved_token and _wt_slug:
            _sources_tried.append("gamma_api")
            try:
                _gd = _fetch_gamma_market_data_sync(
                    condition_id=_wt_cond_id,
                    market_slug=_wt_slug,
                    token_id=None,
                )
                if _gd:
                    _g_yes = _gd.get("yes_token_id") or ""
                    _g_no  = _gd.get("no_token_id")  or ""
                    if _wt_outcome in ("YES", "UP") and _g_yes:
                        _resolved_token = str(_g_yes)
                        _resolve_source = "gamma_api"
                    elif _wt_outcome in ("NO", "DOWN") and _g_no:
                        _resolved_token = str(_g_no)
                        _resolve_source = "gamma_api"
                    else:
                        # Gamma found the market but has no clobTokenId for this
                        # outcome — market may be resolved, not yet in CLOB, or
                        # outcome label doesn't match YES/NO/UP/DOWN.
                        logging.warning(
                            "COPY_LIVE_TOKEN_GAMMA_NO_CLOB_TOKEN bot=%s trade=%s "
                            "slug=%s outcome=%s gamma_yes_present=%s gamma_no_present=%s "
                            "gamma_resolved=%s gamma_active=%s",
                            _bot_name, _trade_id, _wt_slug, _wt_outcome,
                            bool(_g_yes), bool(_g_no),
                            _gd.get("resolved"), _gd.get("active"),
                        )
                    # Populate market_cache so future ticks skip this API call.
                    if _g_yes or _g_no:
                        try:
                            supabase.table("market_cache").upsert(
                                {
                                    "market_slug":   _wt_slug,
                                    "yes_token_id":  _g_yes or None,
                                    "no_token_id":   _g_no  or None,
                                },
                                on_conflict="market_slug",
                            ).execute()
                            logging.info(
                                "COPY_LIVE_TOKEN_GAMMA_CACHE_POPULATED "
                                "slug=%s yes=%s no=%s",
                                _wt_slug,
                                str(_g_yes)[:16] if _g_yes else "none",
                                str(_g_no)[:16]  if _g_no  else "none",
                            )
                        except Exception as _cp_exc:
                            logging.warning(
                                "COPY_LIVE_TOKEN_GAMMA_CACHE_FAIL slug=%s err=%s",
                                _wt_slug, _cp_exc,
                            )
            except Exception as _gamma_exc:
                logging.warning(
                    "COPY_LIVE_TOKEN_GAMMA_API_FAIL bot=%s trade=%s "
                    "slug=%s err=%s",
                    _bot_name, _trade_id, _wt_slug, _gamma_exc,
                )

        if _resolved_token:
            # Shallow-copy the dict so the caller's reference is unchanged.
            wallet_trade = {**wallet_trade, "token_id": _resolved_token}
            logging.info(
                "COPY_LIVE_TOKEN_RESOLVE_OK bot=%s trade=%s "
                "slug=%s outcome=%s resolved_token=%s source=%s",
                _bot_name, _trade_id,
                _wt_slug, _wt_outcome, str(_resolved_token)[:20], _resolve_source,
            )
        else:
            logging.warning(
                "COPY_LIVE_TOKEN_RESOLVE_FAIL bot=%s trade=%s "
                "slug=%s outcome=%s current_slug=%s "
                "token_id_present=%s condition_id_present=%s "
                "sources_tried=%s "
                "— all resolution sources exhausted; gates will block this trade",
                _bot_name, _trade_id,
                _wt_slug or "?", _wt_outcome or "?", current_slug or "none",
                bool(wallet_trade.get("token_id")), bool(_wt_cond_id),
                _sources_tried,
            )

    # ── Gate L8: global live hourly cap ──────────────────────────────────────
    # 0 = unlimited. SELL/close orders are exempt — exits must not be capped.
    if COPY_LIVE_MAX_TRADES_PER_HOUR > 0 and trade_side != "SELL":
        _global_hr_count = _get_live_copy_trades_this_hour()
        if _global_hr_count >= COPY_LIVE_MAX_TRADES_PER_HOUR:
            logging.warning(
                "COPY_LIVE_GATE_L8_FAIL bot=%s trade=%s "
                "reason=live_global_hourly_cap count=%s max=%s",
                _bot_name, _trade_id, _global_hr_count, COPY_LIVE_MAX_TRADES_PER_HOUR,
            )
            return False, "live_global_hourly_cap", None, None, {}

    # ── Gate L9: live open positions cap (BUY only; 0 = unlimited) ───────────
    if trade_side in ("BUY", "") and COPY_LIVE_MAX_OPEN_POSITIONS > 0:
        live_open = get_live_open_positions_count(live_bot_ids)
        if live_open >= COPY_LIVE_MAX_OPEN_POSITIONS:
            logging.warning(
                "COPY_LIVE_GATE_L9_FAIL bot=%s trade=%s "
                "reason=live_open_positions_limit open=%s max=%s",
                _bot_name, _trade_id, live_open, COPY_LIVE_MAX_OPEN_POSITIONS,
            )
            return False, "live_open_positions_limit", None, None, {}

    # ── Gate L9b: live portfolio exposure cap (BUY only; 0 = unlimited) ──────
    if trade_side in ("BUY", ""):
        live_max_exposure = float(global_settings.get("live_max_exposure_usd") or 0)
        if live_max_exposure > 0:
            current_open_exposure = get_copy_open_exposure_for_mode("live")
            projected_exposure    = current_open_exposure + submitted_size
            if projected_exposure > live_max_exposure:
                logging.warning(
                    "COPY_LIVE_GATE_L9B_FAIL bot=%s trade=%s "
                    "reason=live_max_exposure_reached "
                    "current=%.2f size=%.2f projected=%.2f cap=%.2f",
                    _bot_name, _trade_id,
                    current_open_exposure, submitted_size,
                    projected_exposure, live_max_exposure,
                )
                return False, "live_max_exposure_reached", None, None, {}
            logging.info(
                "COPY_LIVE_GATE_L9B_OK bot=%s trade=%s "
                "current=%.2f size=%.2f projected=%.2f cap=%.2f decision=allowed",
                _bot_name, _trade_id,
                current_open_exposure, submitted_size, projected_exposure, live_max_exposure,
            )

    # ── Gate LB: token_id required for live BUY ──────────────────────────────
    # The shared brain allows market_slug as a fallback for paper matching.
    # Live BUY CLOB orders always need a token_id; missing token_id here means
    # all resolution sources were exhausted.  L10 (below) still guards SELLs.
    if trade_side in ("BUY", "") and not wallet_trade.get("token_id"):
        logging.warning(
            "COPY_LIVE_SKIP_TOKEN_ID_MISSING bot=%s trade=%s "
            "reason=live_token_id_missing slug=%s outcome=%s "
            "token_id_present=%s condition_id_present=%s",
            _bot_name, _trade_id,
            str(wallet_trade.get("market_slug") or "?"),
            str(wallet_trade.get("outcome") or "?"),
            bool(wallet_trade.get("token_id")),
            bool(wallet_trade.get("condition_id")),
        )
        return False, "live_token_id_missing", None, None, {}

    # ── Gate LC: max-hold required for live BUY ──────────────────────────────
    # Without max-hold, a live position that cannot be TP-closed stays open
    # indefinitely.  Block live BUY entries unless the bot has max-hold fully
    # configured.  SELL/mirror orders are exempt.
    if trade_side in ("BUY", ""):
        _live_fast   = _read_bot_fast_settings(copy_bot)
        _exit_mode   = _live_fast["exit_mode"]
        _max_hold_m  = _live_fast["max_hold_minutes"]
        if "max_hold" not in _exit_mode or not (_max_hold_m and _max_hold_m > 0):
            logging.warning(
                "COPY_LIVE_GATE_MAX_HOLD_FAIL bot=%s trade=%s "
                "reason=live_max_hold_required exit_mode=%s max_hold_minutes=%s",
                _bot_name, _trade_id, _exit_mode, _max_hold_m,
            )
            return False, "live_max_hold_required", None, None, {}

    # ── Gate L10: token_id required for CLOB ─────────────────────────────────
    # The shared brain allows market_slug as fallback for paper matching.
    # Live CLOB orders (BUY and SELL) always need a token_id.
    token_id = wallet_trade.get("token_id")
    if not token_id:
        logging.warning(
            "COPY_LIVE_SKIP_INSUFFICIENT_MARKET_DATA bot=%s trade=%s "
            "reason=token_id_missing_for_clob side=%s slug=%s outcome=%s "
            "token_id_present=%s condition_id_present=%s",
            _bot_name, _trade_id, trade_side,
            str(wallet_trade.get("market_slug") or "?"),
            str(wallet_trade.get("outcome") or "?"),
            bool(wallet_trade.get("token_id")),
            bool(wallet_trade.get("condition_id")),
        )
        return False, "insufficient_market_data", None, None, {}

    # ── Apply live trade size hard cap ────────────────────────────────────────
    # submitted_size comes from shared brain (compute_copy_size).
    # Clamp to COPY_LIVE_MAX_TRADE_USD before sending to CLOB.
    final_size = min(float(submitted_size), COPY_LIVE_MAX_TRADE_USD)
    if final_size <= 0:
        logging.warning(
            "COPY_LIVE_SIZE_FAIL bot=%s trade=%s "
            "reason=final_size_zero shared_size=%.4f cap=%.2f",
            _bot_name, _trade_id, submitted_size, COPY_LIVE_MAX_TRADE_USD,
        )
        return False, "live_trade_size_exceeds_cap", None, None, {}
    if float(submitted_size) > COPY_LIVE_MAX_TRADE_USD:
        logging.info(
            "COPY_LIVE_SIZE_CAPPED bot=%s shared=%.2f cap=%.2f final=%.2f",
            _bot_name, float(submitted_size), COPY_LIVE_MAX_TRADE_USD, final_size,
        )

    # ── Live account readiness (BUY only) ────────────────────────────────────
    # SELL/mirror-close orders are exempt — exits must always be attempted.
    # For BUY entries, verify the cached CLOB account state before submitting.
    if trade_side in ("BUY", ""):
        _acct_ok, _acct_fail = _check_live_account_ready()
        if not _acct_ok:
            logging.warning(
                "COPY_LIVE_ACCOUNT_READY_FAIL bot=%s trade=%s reason=%s "
                "proxy_wallet=%s allowance=%s balance=%s",
                _bot_name, _trade_id, _acct_fail,
                bool(live_signer_address),
                live_allowance_cache,
                live_balance_cache,
            )
            return False, f"live_account_not_ready_{_acct_fail}", None, None, {}
        logging.info(
            "COPY_LIVE_ACCOUNT_READY_OK bot=%s trade=%s "
            "signer=%s allowance=%.4f balance=%.4f",
            _bot_name, _trade_id,
            str(live_signer_address or "?")[:16],
            live_allowance_cache or 0.0,
            live_balance_cache or 0.0,
        )

    # ── Submit CLOB order ─────────────────────────────────────────────────────
    max_slippage = float(
        copy_bot.get("max_slippage")
        or global_settings.get("default_slippage_cap")
        or 0.03
    )
    order_side   = trade_side if trade_side in ("BUY", "SELL") else "BUY"
    source_price = submitted_price  # validated by shared brain

    logging.info(
        "COPY_LIVE_ORDER_ATTEMPT bot=%s wallet=%s trade=%s "
        "token_id=%s side=%s price=%.4f size=%.2f slippage=%.4f",
        _bot_name, _wallet, _trade_id,
        str(token_id)[:20], order_side, source_price, final_size, max_slippage,
    )

    ok, actual_price, actual_shares, raw_response = submit_copy_live_order(
        trading_client,
        token_id,
        order_side,
        source_price,
        final_size,
        max_slippage,
    )
    if not ok:
        _err_detail = (
            raw_response.get("errorMsg")
            or raw_response.get("error")
            or raw_response.get("message")
            or raw_response.get("errorCode")
            or str(raw_response)[:300]
        )
        logging.warning(
            "COPY_LIVE_ORDER_SUBMIT_FAIL bot=%s trade=%s "
            "token_id=%s side=%s size=%.2f price=%.4f error=%s",
            _bot_name, _trade_id,
            str(token_id)[:20], order_side, final_size, source_price,
            str(_err_detail)[:200],
        )
        return False, "order_submission_failed", final_size, source_price, raw_response

    logging.info(
        "COPY_LIVE_ORDER_OK bot=%s wallet=%s trade=%s "
        "side=%s actual_price=%.4f actual_shares=%s submitted_size=%.2f",
        _bot_name, _wallet, _trade_id,
        order_side, actual_price or 0, actual_shares, final_size,
    )
    return True, None, final_size, actual_price, raw_response


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


def log_per_bot_position_audit(all_bots: list[dict]) -> None:
    """
    Query copied_positions and emit a per-bot summary of OPEN / CLOSED / CANCELLED counts.

    Flags bots where OPEN is high and CLOSED is zero — indicating positions are getting
    stuck open (e.g. copy_closes disabled, wallet mis-match, or close path not firing).

    Called from copy_settlement_loop with rate-limiting so it does not run every tick.
    """
    try:
        resp = (
            supabase.table("copied_positions")
            .select("copy_bot_id, status")
            .execute()
        )
        rows = resp.data or []
    except Exception:
        logging.exception("COPY_BOT_AUDIT_LOAD_FAIL")
        return

    bot_name_map = {str(b["id"]): (b.get("name") or str(b["id"])[:8]) for b in all_bots}

    counts: dict[str, dict[str, int]] = {}
    for row in rows:
        bid = str(row.get("copy_bot_id") or "unknown")
        status = str(row.get("status") or "OTHER").upper()
        if status not in ("OPEN", "CLOSED", "CANCELLED"):
            status = "OTHER"
        if bid not in counts:
            counts[bid] = {"OPEN": 0, "CLOSED": 0, "CANCELLED": 0, "OTHER": 0}
        counts[bid][status] += 1

    if not counts:
        logging.info("COPY_BOT_AUDIT no copied_positions found")
        return

    for bot_id, stat in sorted(counts.items(), key=lambda x: -x[1]["OPEN"]):
        bot_name = bot_name_map.get(bot_id, bot_id[:8])
        total = stat["OPEN"] + stat["CLOSED"] + stat["CANCELLED"] + stat["OTHER"]
        close_rate = stat["CLOSED"] / max(1, total) * 100
        suspicious = stat["OPEN"] > 5 and stat["CLOSED"] == 0

        logging.info(
            "COPY_BOT_AUDIT bot=%s open=%s closed=%s cancelled=%s "
            "total=%s close_rate=%.1f%% suspicious=%s",
            bot_name,
            stat["OPEN"],
            stat["CLOSED"],
            stat["CANCELLED"],
            total,
            close_rate,
            suspicious,
        )

        if suspicious:
            # Fetch the full bot config so we can surface the relevant settings in the warning.
            bot_cfg = next((b for b in all_bots if str(b["id"]) == bot_id), {})
            logging.warning(
                "COPY_BOT_AUDIT_SUSPICIOUS bot=%s bot_id=%s — %s OPEN positions with "
                "zero CLOSED; copy_closes=%s opens_only=%s mode=%s wallet=%s. "
                "Verify wallet_address mapping and copy_closes flag.",
                bot_name,
                bot_id[:8],
                stat["OPEN"],
                bot_cfg.get("copy_closes"),
                bot_cfg.get("opens_only"),
                bot_cfg.get("mode"),
                str(bot_cfg.get("wallet_address") or "?")[:12],
            )


def _db_close_position_with_retry(
    pos_id: str,
    updates: dict,
    extra_filters: "dict | None" = None,
    max_attempts: int = 2,
) -> "tuple[bool, int]":
    """
    Safe wrapper for a copied_positions DB close/update with lightweight retry.

    Phase 1 scaffold: performs up to max_attempts=2 attempts with a short sleep
    between tries. Returns (success, rows_updated).

    extra_filters: additional .eq() conditions added to the UPDATE query.
      Pass {"status": "OPEN"} for a concurrency guard — ensures another path
      hasn't already closed the position between load and this write.

    This wrapper is the clean insertion point for Phase 3 retry queue logic:
      - Increase max_attempts for transient network errors
      - Add exponential backoff
      - Push failed pos_ids to a dead-letter table for manual review

    Log tags emitted:
      COPY_CLOSE_DB_ATTEMPT   — about to execute DB update (attempt N)
      COPY_CLOSE_DB_OK        — update confirmed (rows_updated > 0)
      COPY_CLOSE_DB_ZERO_ROWS — update matched 0 rows (likely already closed)
      COPY_CLOSE_DB_RETRY     — retrying after transient error
      COPY_CLOSE_DB_FAIL      — all attempts exhausted, update failed
    """
    import time as _time_mod

    rows_updated = 0
    for attempt in range(1, max_attempts + 1):
        try:
            logging.info(
                "COPY_CLOSE_DB_ATTEMPT pos=%s attempt=%s/%s filters=%s",
                pos_id[:8], attempt, max_attempts,
                list(extra_filters.keys()) if extra_filters else [],
            )
            q = supabase.table("copied_positions").update(updates).eq("id", pos_id)
            if extra_filters:
                for k, v in extra_filters.items():
                    q = q.eq(k, v)
            resp = q.execute()
            rows_updated = len(resp.data) if resp.data else 0

            if rows_updated > 0:
                logging.info(
                    "COPY_CLOSE_DB_OK pos=%s attempt=%s rows_updated=%s",
                    pos_id[:8], attempt, rows_updated,
                )
                return True, rows_updated

            # 0 rows — position already closed by another path (settlement, reset, etc.)
            logging.warning(
                "COPY_CLOSE_DB_ZERO_ROWS pos=%s attempt=%s/%s "
                "— update matched 0 rows; position may already be closed",
                pos_id[:8], attempt, max_attempts,
            )
            # Don't retry on zero-rows — another path won the race; not a transient error.
            return False, 0

        except Exception as exc:
            logging.warning(
                "COPY_CLOSE_DB_RETRY pos=%s attempt=%s/%s err=%s",
                pos_id[:8], attempt, max_attempts, exc,
            )
            if attempt < max_attempts:
                _time_mod.sleep(0.4)

    logging.warning(
        "COPY_CLOSE_DB_FAIL pos=%s — all %s attempts exhausted",
        pos_id[:8], max_attempts,
    )
    return False, rows_updated


def close_matching_open_positions_on_exit(
    copy_bot: dict,
    wallet_trade: dict,
) -> int:
    """
    Close open copied_positions that match the source wallet's SELL trade.

    Called when copy_closes=True and a source wallet SELL is observed.
    Finds OPEN positions for this bot on the same market and closes them using
    the SELL trade's price as the exit price.

    Matching priority: token_id > market_slug > condition_id

    PnL formula (long position closed by source exit):
      pnl = size * (exit_price - entry_price) / entry_price

    Returns the number of positions closed.

    Assumption: Only long BUY positions are mirrored in this pass.
    If the copied position was opened as a SELL (short), exit logic would invert
    — not handled here, logged and skipped.

    Every decision in this function emits a tagged log prefixed SELL_MIRROR_*
    so Railway search can confirm the full chain:
      SELL_MIRROR_ENTER  — function called
      SELL_MIRROR_NO_PRICE   — no price, abort
      SELL_MIRROR_NO_ID      — no market identifier, abort
      SELL_MIRROR_QUERY_FAIL — DB query exception, abort
      SELL_MIRROR_NO_MATCH   — query returned 0 rows
      SELL_MIRROR_FOUND      — N rows returned, processing each
      SELL_MIRROR_SKIP_SHORT — position side=SELL, skipped
      SELL_MIRROR_DB_ATTEMPT — about to write close to DB
      SELL_MIRROR_DB_OK      — DB update confirmed
      SELL_MIRROR_DB_FAIL    — DB update exception
      SELL_MIRROR_SUMMARY    — final count at end of call
    """
    bot_id       = str(copy_bot["id"])
    bot_label    = copy_bot.get("name") or bot_id[:8]
    token_id     = wallet_trade.get("token_id")
    market_slug  = wallet_trade.get("market_slug")
    condition_id = wallet_trade.get("condition_id")
    trade_id     = str(wallet_trade.get("source_trade_id") or "?")[:24]
    wallet_short = str(wallet_trade.get("wallet_address") or "?")[:12]

    exit_price_raw = wallet_trade.get("price")
    try:
        exit_price = float(exit_price_raw) if exit_price_raw is not None else None
    except (TypeError, ValueError):
        exit_price = None

    # ── SELL_MIRROR_ENTER ─────────────────────────────────────────────────────
    # Always log entry so we can confirm this function was reached for any SELL.
    logging.warning(
        "SELL_MIRROR_ENTER bot=%s wallet=%s trade=%s "
        "slug=%s token=%s condition=%s exit_price=%s",
        bot_label, wallet_short, trade_id,
        market_slug or "NONE",
        str(token_id or "NONE")[:20],
        str(condition_id or "NONE")[:20],
        exit_price,
    )

    if exit_price is None:
        logging.warning(
            "SELL_MIRROR_NO_PRICE bot=%s trade=%s slug=%s token=%s "
            "— SELL trade has no usable price; cannot compute exit. "
            "raw_price=%r",
            bot_label, trade_id,
            market_slug or "?",
            str(token_id or "?")[:20],
            exit_price_raw,
        )
        # keep legacy tag for backwards compat
        logging.warning(
            "COPY_EXIT_MIRROR_NO_PRICE bot=%s trade=%s slug=%s token=%s condition=%s "
            "— cannot close without a valid exit price from SELL trade",
            bot_label, trade_id,
            market_slug or "?",
            str(token_id or "?")[:16],
            str(condition_id or "?")[:16],
        )
        return 0

    # ── Build query ───────────────────────────────────────────────────────────
    match_field: str
    try:
        base_q = (
            supabase.table("copied_positions")
            .select("*")
            .eq("copy_bot_id", bot_id)
            .eq("status", "OPEN")
        )
        if token_id:
            match_field = "token_id"
            resp = base_q.eq("token_id", token_id).execute()
        elif market_slug:
            match_field = "market_slug"
            resp = base_q.eq("market_slug", market_slug).execute()
        elif condition_id:
            match_field = "condition_id"
            resp = base_q.eq("condition_id", condition_id).execute()
        else:
            logging.warning(
                "SELL_MIRROR_NO_ID bot=%s wallet=%s trade=%s "
                "— SELL trade has no token_id, market_slug, or condition_id; "
                "cannot query open positions. raw_trade=%r",
                bot_label, wallet_short, trade_id,
                {k: wallet_trade.get(k) for k in
                 ("side", "outcome", "market_slug", "token_id", "condition_id")},
            )
            logging.info(
                "COPY_EXIT_MIRROR_SKIP bot=%s wallet=%s — SELL trade has no market identifier "
                "(no token_id, market_slug, or condition_id); trade=%s",
                bot_label, wallet_short, trade_id,
            )
            return 0
        positions_to_close = resp.data or []
    except Exception:
        logging.warning(
            "SELL_MIRROR_QUERY_FAIL bot=%s trade=%s slug=%s token=%s "
            "— DB query for open positions threw an exception",
            bot_label, trade_id,
            market_slug or "?",
            str(token_id or "?")[:20],
        )
        logging.exception(
            "COPY_EXIT_MIRROR_QUERY_FAIL bot=%s slug=%s token=%s",
            bot_label, market_slug or "?", str(token_id or "?")[:16],
        )
        return 0

    # ── No rows returned ──────────────────────────────────────────────────────
    if not positions_to_close:
        logging.warning(
            "SELL_MIRROR_NO_MATCH bot=%s wallet=%s trade=%s "
            "match_field=%s match_value=%s "
            "— 0 OPEN positions found for this bot+market. "
            "Possible reasons: position never opened for this bot, "
            "already closed, or market identifier mismatch between "
            "wallet_trade and copied_positions.",
            bot_label, wallet_short, trade_id,
            match_field,
            (token_id or market_slug or condition_id or "NONE"),
        )
        logging.info(
            "COPY_EXIT_MIRROR_NO_MATCH bot=%s wallet=%s slug=%s token=%s condition=%s "
            "match_field=%s — no open positions found to close for this SELL trade",
            bot_label, wallet_short,
            market_slug or "?",
            str(token_id or "?")[:16],
            str(condition_id or "?")[:16],
            match_field,
        )
        return 0

    # ── Rows found ────────────────────────────────────────────────────────────
    logging.warning(
        "SELL_MIRROR_FOUND bot=%s wallet=%s trade=%s "
        "match_field=%s match_value=%s found=%s "
        "— processing each open position for DB close",
        bot_label, wallet_short, trade_id,
        match_field,
        (token_id or market_slug or condition_id or "NONE"),
        len(positions_to_close),
    )

    closed_count  = 0
    skipped_count = 0
    failed_count  = 0

    for pos in positions_to_close:
        pos_id     = str(pos.get("id") or "")
        pos_slug   = pos.get("market_slug") or "?"
        pos_side   = str(pos.get("side") or "BUY").upper()
        pos_outcome = pos.get("outcome") or "?"

        try:
            entry_price = float_or_none(pos.get("entry_price")) or 0.0
            size        = float_or_none(pos.get("size")) or 0.0

            if pos_side == "SELL":
                logging.warning(
                    "SELL_MIRROR_SKIP_SHORT pos=%s bot=%s slug=%s outcome=%s "
                    "— position side=SELL (short); exit mirror not implemented for shorts",
                    pos_id[:12], bot_label, pos_slug, pos_outcome,
                )
                logging.info(
                    "COPY_EXIT_MIRROR_SKIP_SHORT pos=%s bot=%s slug=%s "
                    "— position is a SELL/short, exit mirroring not yet implemented",
                    pos_id[:8], bot_label, pos_slug,
                )
                skipped_count += 1
                continue

            pnl = round(size * (exit_price - entry_price) / entry_price, 6) if entry_price > 0 else 0.0
            is_live_position = bool((pos.get("raw_json") or {}).get("live"))

            # ── SELL_MIRROR_DB_ATTEMPT ────────────────────────────────────────
            _now_ts = utc_now_iso()
            logging.warning(
                "SELL_MIRROR_DB_ATTEMPT pos=%s bot=%s slug=%s outcome=%s "
                "entry=%.4f exit=%.4f size=%.4f pnl=%+.4f live=%s "
                "match_field=%s — writing CLOSED to DB now",
                pos_id[:12], bot_label, pos_slug, pos_outcome,
                entry_price, exit_price, size, pnl, is_live_position,
                match_field,
            )

            updates = {
                "status":    "CLOSED",
                "exit_price": exit_price,
                "pnl":        pnl,
                "closed_at":  _now_ts,
                "raw_json": {
                    **(pos.get("raw_json") or {}),
                    # Standardized top-level close_reason (Phase 2)
                    "close_reason": CLOSE_REASON_SOURCE_WALLET_EXIT,
                    # Detailed sub-object (preserved for backward compatibility)
                    "close": {
                        "reason":          CLOSE_REASON_SOURCE_WALLET_EXIT,
                        "source_trade_id": wallet_trade.get("source_trade_id"),
                        "exit_price":      exit_price,
                        "pnl":             pnl,
                        "closed_at":       _now_ts,
                        "match_field":     match_field,
                    },
                },
            }

            # Use retry scaffold — Phase 1/2: max 2 attempts.
            # No concurrency guard (.eq status=OPEN) here because the source-wallet
            # exit path is the authoritative close and should always win.
            _close_ok, _rows_updated = _db_close_position_with_retry(
                pos_id, updates, max_attempts=2,
            )

            if _close_ok:
                # ── SELL_MIRROR_DB_OK ──────────────────────────────────────
                logging.warning(
                    "SELL_MIRROR_DB_OK pos=%s bot=%s slug=%s outcome=%s "
                    "entry=%.4f exit=%.4f size=%.4f pnl=%+.4f live=%s "
                    "rows_updated=%s match_field=%s close_reason=%s",
                    pos_id[:12], bot_label, pos_slug, pos_outcome,
                    entry_price, exit_price, size, pnl, is_live_position,
                    _rows_updated, match_field, CLOSE_REASON_SOURCE_WALLET_EXIT,
                )
                # Legacy tag kept for backward compatibility with Railway search filters
                logging.info(
                    "COPY_EXIT_MIRROR_CLOSED pos=%s bot=%s slug=%s outcome=%s "
                    "entry=%.4f exit=%.4f size=%.2f pnl=%+.4f live=%s match=%s",
                    pos_id[:8], bot_label, pos_slug, pos_outcome,
                    entry_price, exit_price, size, pnl, is_live_position, match_field,
                )
                # Best-effort write to dedicated close_reason column (Phase 2 migration)
                _try_write_close_reason_col(pos_id, CLOSE_REASON_SOURCE_WALLET_EXIT)
                if pnl != 0.0 and not is_live_position:
                    _update_copy_paper_bankroll(pnl, pos_id, close_path="exit_mirror")
                closed_count += 1
            else:
                # 0 rows updated — position was already closed by another path.
                # Not a DB error; the retry wrapper already logged COPY_CLOSE_DB_ZERO_ROWS.
                logging.warning(
                    "SELL_MIRROR_DB_NOOP pos=%s bot=%s slug=%s "
                    "— DB update returned 0 rows; position already closed by another path "
                    "(settlement, auto-exit, or paper reset). No retry needed.",
                    pos_id[:12], bot_label, pos_slug,
                )
                skipped_count += 1

        except Exception:
            failed_count += 1
            logging.warning(
                "SELL_MIRROR_DB_FAIL pos=%s bot=%s slug=%s "
                "— exception during DB close attempt",
                pos_id[:12], bot_label, pos_slug,
            )
            logging.exception(
                "COPY_EXIT_MIRROR_CLOSE_FAIL pos=%s bot=%s slug=%s token=%s",
                pos_id[:8], bot_label,
                pos.get("market_slug") or "?",
                str(pos.get("token_id") or "?")[:16],
            )

    # ── SELL_MIRROR_SUMMARY / COPY_SOURCE_EXIT_BATCH_SUMMARY ─────────────────
    logging.warning(
        "SELL_MIRROR_SUMMARY bot=%s wallet=%s trade=%s slug=%s "
        "found=%s closed=%s skipped=%s failed=%s",
        bot_label, wallet_short, trade_id,
        market_slug or "?",
        len(positions_to_close),
        closed_count, skipped_count, failed_count,
    )
    # Searchable alias used by Phase 3 diagnostics — same data as SELL_MIRROR_SUMMARY.
    logging.warning(
        "COPY_SOURCE_EXIT_BATCH_SUMMARY bot=%s wallet=%s trade=%s slug=%s "
        "match_field=%s found=%s closed=%s skipped=%s failed=%s",
        bot_label, wallet_short, trade_id,
        market_slug or "?",
        match_field if positions_to_close else "N/A",
        len(positions_to_close),
        closed_count, skipped_count, failed_count,
    )

    return closed_count


def open_copied_position(
    copy_bot: dict,
    wallet_trade: dict,
    submitted_size: float,
    submitted_price: float,
    mode: str = "PAPER",
) -> "str | None":
    """
    Create a copied_positions row for a paper or live copy trade.

    mode: "PAPER" (default) or "LIVE"

    'size' stores the USD position size (submitted_size).
    'entry_price' is the per-share price at which we entered.
    PnL at close: pnl = size * (exit_price - entry_price) / entry_price

    The raw_json includes mode metadata so copied_positions rows can be
    distinguished as paper vs live without joining to copy_bots.

    Returns the newly created position's DB id (str) on success, None on error.
    """
    is_live = str(mode).upper() == "LIVE"
    mode_upper = mode.upper()
    bot_label = copy_bot.get("name") or str(copy_bot.get("id", "?"))[:8]

    # Log every write attempt with full context so we can see every path
    # that creates an OPEN paper position.
    logging.info(
        "COPY_POSITION_WRITE_ATTEMPT mode=%s bot=%s wallet=%s slug=%s "
        "side=%s size=%.4f price=%.4f source_trade=%s",
        mode_upper, bot_label,
        str(wallet_trade.get("wallet_address") or "")[:16],
        wallet_trade.get("market_slug") or "unknown",
        wallet_trade.get("side"),
        submitted_size, submitted_price,
        str(wallet_trade.get("source_trade_id") or "")[:24],
    )

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
                "mode": mode_upper,
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
        resp    = supabase.table("copied_positions").insert(row).execute()
        pos_id  = (resp.data[0].get("id") if resp.data else None) or "unknown"
        logging.info(
            "COPY_POSITION_OPENED mode=%s bot=%s wallet=%s slug=%s "
            "side=%s outcome=%s size=%.4f price=%.4f position_id=%s",
            mode_upper, bot_label,
            str(wallet_trade.get("wallet_address") or "")[:16],
            wallet_trade.get("market_slug") or "unknown",
            wallet_trade.get("side"),
            wallet_trade.get("outcome"),
            submitted_size, submitted_price,
            pos_id,
        )
        return str(pos_id)
    except Exception:
        logging.exception(
            "COPY_OPEN_POSITION_FAIL mode=%s bot=%s trade=%s",
            mode_upper, bot_label,
            str(wallet_trade.get("source_trade_id", "?"))[:20],
        )
        return None


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


# ── Wallet fast-turnover classification ───────────────────────────────────────
#
# _classify_wallet_class() assigns a copy-trading suitability class based on
# hold-time distribution and recent performance.
#
# Classes:
#   FAST_COPY      — short hold times (< 30 min), positive or neutral PnL
#   CONVICTION_COPY — long hold times, good win rate
#   MIXED          — does not fit cleanly into either; may still be tradeable
#   AVOID          — consistently losing wallet
#   UNSCORABLE     — insufficient closed trade data (< 3 positions)
#
# Phase 2 migration: ALTER TABLE wallet_metrics ADD COLUMN IF NOT EXISTS
#   wallet_class     text    DEFAULT 'UNSCORABLE',
#   median_hold_minutes numeric,
#   pct_under_15min    numeric,
#   pct_under_30min    numeric,
#   recent_closed_count int;


def _compute_median(values: "list[float]") -> "float | None":
    """Return the median of a non-empty list, or None if empty."""
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 == 1 else (s[mid - 1] + s[mid]) / 2.0


def _classify_wallet_class(
    avg_hold_minutes: float,
    median_hold_minutes: "float | None",
    pct_under_15min: float,
    pct_under_30min: float,
    pnl_30d: float,
    win_rate: float,
    closed_count: int,
) -> str:
    """
    Classify a wallet's copy-trading suitability.

    Returns one of: FAST_COPY | CONVICTION_COPY | MIXED | AVOID | UNSCORABLE

    All thresholds are intentionally conservative — uncertain cases fall through
    to MIXED rather than FAST_COPY so gates don't over-restrict.
    """
    # UNSCORABLE: too little data for reliable classification
    if closed_count < 3:
        return "UNSCORABLE"

    # AVOID: consistently negative performance
    if pnl_30d < -20.0 and win_rate < 0.35:
        return "AVOID"
    if win_rate < 0.30 and closed_count >= 10:
        return "AVOID"

    # FAST_COPY: dominant short-hold behaviour with neutral/positive PnL
    _median_fast = median_hold_minutes is not None and median_hold_minutes < 25.0
    _pct_fast    = pct_under_30min >= 0.60
    _avg_fast    = avg_hold_minutes < 25.0
    _profitable  = pnl_30d >= 0.0

    if (_pct_fast or _avg_fast or _median_fast) and _profitable:
        return "FAST_COPY"

    # CONVICTION_COPY: long-hold, solid win rate
    if avg_hold_minutes >= 60.0 and win_rate >= 0.50 and pnl_30d >= 0.0:
        return "CONVICTION_COPY"

    # AVOID: low win rate with negative PnL (less extreme than above)
    if win_rate < 0.40 and pnl_30d < 0.0:
        return "AVOID"

    return "MIXED"


def _get_wallet_class_from_metrics(wallet_address: str) -> "str | None":
    """
    Look up wallet_class from wallet_metrics for the given address.

    Returns None if the row, column, or table doesn't exist yet.
    Fail-safe: never raises; always returns None on error.
    """
    try:
        resp = (
            supabase.table("wallet_metrics")
            .select("wallet_class")
            .eq("wallet_address", wallet_address)
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        if rows:
            return rows[0].get("wallet_class")
        return None
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

        # avg_hold_minutes + fast-turnover distribution metrics
        hold_mins: list[float] = []
        for p in closed_pos:
            opened_dt = _parse_ts(p.get("opened_at"))
            closed_dt = _parse_ts(p.get("closed_at"))
            if opened_dt and closed_dt and closed_dt > opened_dt:
                hold_mins.append((closed_dt - opened_dt).total_seconds() / 60.0)
        avg_hold_minutes = round(sum(hold_mins) / len(hold_mins), 2) if hold_mins else 0.0

        # Phase 1 fast-turnover additions (safe if hold_mins is empty)
        median_hold_minutes: float | None = _compute_median(hold_mins)
        recent_closed_count = len(closed_pos)
        pct_under_15min = (
            round(sum(1 for h in hold_mins if h < 15.0) / len(hold_mins), 4)
            if hold_mins else 0.0
        )
        pct_under_30min = (
            round(sum(1 for h in hold_mins if h < 30.0) / len(hold_mins), 4)
            if hold_mins else 0.0
        )

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

        # ── wallet_class (Phase 1 fast-turnover classification) ────────────
        wallet_class = _classify_wallet_class(
            avg_hold_minutes=avg_hold_minutes,
            median_hold_minutes=median_hold_minutes,
            pct_under_15min=pct_under_15min,
            pct_under_30min=pct_under_30min,
            pnl_30d=pnl_30d,
            win_rate=win_rate,
            closed_count=recent_closed_count,
        )

        # ── Upsert wallet_metrics ─────────────────────────────────────────
        # Base payload — columns that are guaranteed to exist.
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

        # Phase 1 extended fields — only included if columns exist in DB.
        # Attempt upsert with extended payload first; fall back gracefully.
        _extended_fields: dict = {
            "wallet_class":          wallet_class,
            "median_hold_minutes":   round(median_hold_minutes, 2) if median_hold_minutes is not None else None,
            "pct_under_15min":       pct_under_15min,
            "pct_under_30min":       pct_under_30min,
            "recent_closed_count":   recent_closed_count,
        }
        _extended_payload = {**metrics, **_extended_fields}

        _upsert_ok = False
        try:
            supabase.table("wallet_metrics").upsert(
                _extended_payload, on_conflict="wallet_address"
            ).execute()
            _upsert_ok = True
        except Exception as _ext_exc:
            _exc_str = str(_ext_exc).lower()
            # Column missing — fall back to base payload without Phase 1 fields.
            if any(kw in _exc_str for kw in ("column", "schema", "42703", "wallet_class",
                                              "median_hold", "pct_under", "recent_closed")):
                logging.info(
                    "COPY_METRICS_EXTENDED_COL_MISSING wallet=%s "
                    "— Phase 1 extended columns not yet in DB; upserting base fields only. "
                    "Run Phase 2 migration to add wallet_class, median_hold_minutes, "
                    "pct_under_15min, pct_under_30min, recent_closed_count.",
                    wallet_address[:10],
                )
                try:
                    supabase.table("wallet_metrics").upsert(
                        metrics, on_conflict="wallet_address"
                    ).execute()
                    _upsert_ok = True
                except Exception as _base_exc:
                    logging.exception(
                        "COPY_UPDATE_METRICS_BASE_FAIL wallet=%s err=%s",
                        wallet_address[:10], _base_exc,
                    )
            else:
                logging.exception(
                    "COPY_UPDATE_METRICS_FAIL_EXTENDED wallet=%s err=%s",
                    wallet_address[:10], _ext_exc,
                )

        logging.info(
            "COPY_METRICS_UPDATED wallet=%s trades=%s volume=%.2f "
            "pnl_30d=%.4f win_rate=%.3f avg_hold=%.1fmin "
            "median_hold=%s pct_u15=%.0f%% pct_u30=%.0f%% "
            "closed_count=%s max_dd=%.1f%% copy_score=%.1f "
            "wallet_class=%s upsert_ok=%s",
            wallet_address[:10],
            trade_count,
            volume,
            pnl_30d,
            win_rate,
            avg_hold_minutes,
            f"{median_hold_minutes:.1f}" if median_hold_minutes is not None else "N/A",
            pct_under_15min * 100,
            pct_under_30min * 100,
            recent_closed_count,
            max_drawdown,
            copy_score,
            wallet_class,
            _upsert_ok,
        )

    except Exception:
        logging.exception("COPY_UPDATE_METRICS_FAIL wallet=%s", wallet_address[:10])


# ── Copy bot evaluation ───────────────────────────────────────────────────────


def _read_bot_fast_settings(bot: dict) -> dict:
    """
    Read all Phase 1/2 fast-copy feature flags from a copy_bot row with safe defaults.

    All fields are optional in the DB — this function never raises.
    Callers can safely do: settings = _read_bot_fast_settings(bot); settings["exit_mode"]

    Returned keys and defaults:
      block_blocked_markets  bool    True   — always block BLOCKED_MARKET BUYs
      fast_markets_only      bool    False  — only copy FAST_MARKET trades
      require_fast_copy      bool    False  — only copy FAST_COPY wallets
      max_entry_age_minutes  int     0      — 0 = disabled; >0 = max BUY age in minutes
      exit_mode              str     "mirror_only"
      take_profit_pct        float|None  None
      max_hold_minutes       float|None  None
    """
    return {
        "block_blocked_markets": bool(bot.get("block_blocked_markets", True)),
        "fast_markets_only":     bool(bot.get("fast_markets_only",    False)),
        "require_fast_copy":     bool(bot.get("require_fast_copy",    False)),
        "max_entry_age_minutes": int(bot.get("max_entry_age_minutes") or 0),
        "exit_mode":             str(bot.get("exit_mode") or "mirror_only").strip().lower(),
        "take_profit_pct":       float_or_none(bot.get("take_profit_pct")),
        "max_hold_minutes":      float_or_none(bot.get("max_hold_minutes")),
    }


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


def evaluate_copy_trade_shared(
    copy_bot: dict,
    wallet_trade: dict,
    global_settings: dict,
    mode: str = "paper",
) -> tuple[bool, str | None, float | None, float | None]:
    """
    Shared copy-trading decision brain — identical logic for PAPER and LIVE.

    Called for every bot regardless of execution mode.  Only the final
    execution layer (paper write vs live CLOB) differs between modes.

    Returns (should_copy, skip_reason, submitted_size_usd, submitted_price).

    Gates (in order):
      G1  emergency_stop_active        — global emergency_stop is true
      G2  closes_not_enabled           — SELL trade but copy_closes = false
      G3  delay_not_elapsed            — trade is too recent for delay_seconds
      G4  max_trades_per_hour_reached  — in-memory hourly rate limit exceeded
      G5  max_open_positions_reached   — per-bot open positions cap exceeded
      G6  insufficient_market_data     — no token_id or market_slug
      G7  unsupported_trade_shape      — price missing or out of range

    Mode-specific gates (exposure cap) are applied in the execution layer.
    """
    bot_id      = str(copy_bot["id"])
    _bot_label  = copy_bot.get("name") or bot_id[:8]
    _wallet_lbl = str(wallet_trade.get("wallet_address") or "unknown")[:16]

    # G1: Emergency stop — blocks ALL trades (paper + live)
    if global_settings.get("emergency_stop"):
        return False, "emergency_stop_active", None, None

    # G2: opens_only / copy_closes filter
    # Default True so that bots without an explicit copy_closes column still close
    # positions.  Set copy_closes=false on the copy_bots row to disable mirroring.
    trade_side  = str(wallet_trade.get("side") or "").upper()
    copy_closes = bool(copy_bot.get("copy_closes", True))
    if trade_side == "SELL" and not copy_closes:
        logging.warning(
            "SELL_BLOCKED_COPY_CLOSES_DISABLED bot=%s wallet=%s trade=%s slug=%s "
            "— copy_closes=False is explicitly set on this bot; "
            "SELL will NOT close any copied_position. "
            "Set copy_closes=true on the copy_bots row to enable close mirroring.",
            _bot_label, _wallet_lbl,
            str(wallet_trade.get("source_trade_id", "?"))[:20],
            wallet_trade.get("market_slug") or "?",
        )
        return False, "closes_not_enabled", None, None

    # G3: Delay filter — SELL/close events are exempt; the source already sold.
    delay_sec = int(copy_bot.get("delay_seconds") or 0)
    if delay_sec > 0 and trade_side != "SELL":
        try:
            traded_at_str = str(wallet_trade.get("traded_at") or "")
            traded_at_dt  = datetime.fromisoformat(traded_at_str.replace("Z", "+00:00"))
            age_seconds   = (datetime.now(timezone.utc) - traded_at_dt).total_seconds()
            if age_seconds < delay_sec:
                return False, "delay_not_elapsed", None, None
        except Exception:
            pass  # unparseable timestamp — skip delay gate

    # G4: max_trades_per_hour (in-memory; 0 = unlimited)
    # SELL/close events are exempt — position exits must not be rate-limited.
    max_per_hour = int(copy_bot.get("max_trades_per_hour") or 0)
    if max_per_hour > 0 and trade_side != "SELL" and _copy_bot_trades_this_hour(bot_id) >= max_per_hour:
        return False, "max_trades_per_hour_reached", None, None

    # G5: per-bot max_open_positions (DB query; BUY/entry only; 0 = unlimited)
    if trade_side in ("BUY", ""):
        max_open = int(copy_bot.get("max_open_positions") or 0)
        if max_open > 0:
            open_count = get_open_positions_count(bot_id)
            if open_count >= max_open:
                return False, "max_open_positions_reached", None, None

    # G6: Minimum market data required.
    # For BUY trades: need token_id OR market_slug.
    # For SELL trades: condition_id is also sufficient — close_matching_open_positions_on_exit
    # supports all three identifiers (token_id > market_slug > condition_id).  A SELL that
    # only carries condition_id must NOT be blocked here; it can still find and close an
    # open position using the condition_id fallback.
    _sell_has_id = (
        trade_side == "SELL" and bool(wallet_trade.get("condition_id"))
    )
    if not wallet_trade.get("token_id") and not wallet_trade.get("market_slug") and not _sell_has_id:
        if trade_side == "SELL":
            logging.warning(
                "SELL_BLOCKED_NO_MARKET_ID bot=%s wallet=%s trade=%s "
                "— SELL trade has no token_id, market_slug, or condition_id; "
                "cannot match any open copied_position. raw_keys=%s",
                _bot_label, _wallet_lbl,
                str(wallet_trade.get("source_trade_id", "?"))[:20],
                sorted(wallet_trade.keys()),
            )
        return False, "insufficient_market_data", None, None

    # G7: Price must be present and in range
    price = wallet_trade.get("price")
    if price is None:
        if trade_side == "SELL":
            logging.warning(
                "SELL_BLOCKED_NO_PRICE bot=%s wallet=%s trade=%s slug=%s token=%s "
                "— SELL trade has no price field; close_matching_open_positions_on_exit "
                "would also abort with SELL_MIRROR_NO_PRICE. "
                "raw_trade=%r",
                _bot_label, _wallet_lbl,
                str(wallet_trade.get("source_trade_id", "?"))[:20],
                wallet_trade.get("market_slug") or "?",
                str(wallet_trade.get("token_id") or "?")[:20],
                {k: wallet_trade.get(k) for k in
                 ("side", "price", "outcome", "market_slug", "token_id", "condition_id")},
            )
        return False, "unsupported_trade_shape", None, None
    try:
        submitted_price = float(price)
        if submitted_price <= 0 or submitted_price > 1:
            if trade_side == "SELL":
                logging.warning(
                    "SELL_BLOCKED_PRICE_RANGE bot=%s wallet=%s trade=%s slug=%s "
                    "price_raw=%r submitted_price=%.6f "
                    "— SELL price outside (0, 1]; blocking close mirror. "
                    "If source wallet redeemed at resolution this may be a REDEEM "
                    "event rather than a plain SELL — check raw_json.type.",
                    _bot_label, _wallet_lbl,
                    str(wallet_trade.get("source_trade_id", "?"))[:20],
                    wallet_trade.get("market_slug") or "?",
                    price, submitted_price,
                )
            return False, "unsupported_trade_shape", None, None
    except (TypeError, ValueError):
        return False, "unsupported_trade_shape", None, None

    submitted_size = compute_copy_size(copy_bot, wallet_trade, global_settings)

    # ══ PHASE 1 ADDITIVE GATES (G8–G13) ══════════════════════════════════════
    # All new gates below:
    #   • Only apply to BUY trades — SELL/close mirroring is NEVER blocked here.
    #   • Default to PASS when bot config flag is absent (backward-compatible).
    #   • Default to PASS when market/wallet data is unavailable (fail-open).
    #   • All skip reasons are new — do not clash with existing G1–G7 reasons.
    #
    # Bot config flags read (via copy_bots.*):
    #   fast_markets_only  bool  — only copy trades on FAST_MARKET markets
    #   block_blocked_markets bool — skip BLOCKED_MARKET (default True for BUYs)
    #   require_fast_copy  bool  — only copy wallets classified as FAST_COPY
    #   max_entry_age_minutes int — skip BUY if trade is older than this (0=off)
    # ─────────────────────────────────────────────────────────────────────────

    if trade_side != "SELL":
        # Compute market class once for all market-related gates.
        _market_class = classify_market(
            wallet_trade.get("market_slug"),
            wallet_trade.get("market_title"),
        )

        # G8: market_blocked — BLOCKED_MARKET markets are always skipped for BUY.
        # Controlled by copy_bots.block_blocked_markets (default True).
        _block_blocked = bool(copy_bot.get("block_blocked_markets", True))
        if _block_blocked and _market_class == "BLOCKED_MARKET":
            logging.warning(
                "COPY_GATE_G8_FAIL bot=%s wallet=%s trade=%s "
                "reason=market_blocked slug=%s class=%s",
                _bot_label, _wallet_lbl,
                str(wallet_trade.get("source_trade_id", "?"))[:20],
                wallet_trade.get("market_slug") or "?",
                _market_class,
            )
            return False, "market_blocked", None, None

        # G9: market_not_fast — SLOW_MARKET markets skipped when fast_markets_only=True.
        _fast_markets_only = bool(copy_bot.get("fast_markets_only", False))
        if _fast_markets_only and _market_class == "SLOW_MARKET":
            logging.info(
                "COPY_GATE_G9_FAIL bot=%s wallet=%s trade=%s "
                "reason=market_not_fast slug=%s class=%s fast_markets_only=True",
                _bot_label, _wallet_lbl,
                str(wallet_trade.get("source_trade_id", "?"))[:20],
                wallet_trade.get("market_slug") or "?",
                _market_class,
            )
            return False, "market_not_fast", None, None

        # G10/G11: wallet class gates — only run when require_fast_copy=True.
        _require_fast_copy = bool(copy_bot.get("require_fast_copy", False))
        if _require_fast_copy:
            _wallet_class = _get_wallet_class_from_metrics(
                str(wallet_trade.get("wallet_address") or "")
            )

            # G10: missing_fast_metrics — no wallet_metrics row at all.
            if _wallet_class is None:
                logging.info(
                    "COPY_GATE_G10_FAIL bot=%s wallet=%s trade=%s "
                    "reason=missing_fast_metrics require_fast_copy=True "
                    "— no wallet_metrics row; cannot verify wallet class",
                    _bot_label, _wallet_lbl,
                    str(wallet_trade.get("source_trade_id", "?"))[:20],
                )
                return False, "missing_fast_metrics", None, None

            # G11: wallet_unscorable — data exists but class is UNSCORABLE.
            if _wallet_class == "UNSCORABLE":
                logging.info(
                    "COPY_GATE_G11_FAIL bot=%s wallet=%s trade=%s "
                    "reason=wallet_unscorable wallet_class=UNSCORABLE require_fast_copy=True",
                    _bot_label, _wallet_lbl,
                    str(wallet_trade.get("source_trade_id", "?"))[:20],
                )
                return False, "wallet_unscorable", None, None

            # G12: wallet_not_fast_copy — wallet is classed but not FAST_COPY.
            if _wallet_class != "FAST_COPY":
                logging.info(
                    "COPY_GATE_G12_FAIL bot=%s wallet=%s trade=%s "
                    "reason=wallet_not_fast_copy wallet_class=%s require_fast_copy=True",
                    _bot_label, _wallet_lbl,
                    str(wallet_trade.get("source_trade_id", "?"))[:20],
                    _wallet_class,
                )
                return False, "wallet_not_fast_copy", None, None

        # G13: entry_too_late — BUY on FAST_MARKET is too old to copy.
        # max_entry_age_minutes=0 (default) disables this gate entirely.
        _max_age_min = int(copy_bot.get("max_entry_age_minutes") or 0)
        if _max_age_min > 0 and _market_class == "FAST_MARKET":
            _traded_at_str = str(wallet_trade.get("traded_at") or "")
            _traded_at_dt  = _parse_ts(_traded_at_str)
            if _traded_at_dt is not None:
                _age_min = (datetime.now(timezone.utc) - _traded_at_dt).total_seconds() / 60.0
                if _age_min > _max_age_min:
                    logging.info(
                        "COPY_GATE_G13_FAIL bot=%s wallet=%s trade=%s "
                        "reason=entry_too_late age_min=%.1f max=%s slug=%s class=%s",
                        _bot_label, _wallet_lbl,
                        str(wallet_trade.get("source_trade_id", "?"))[:20],
                        _age_min, _max_age_min,
                        wallet_trade.get("market_slug") or "?",
                        _market_class,
                    )
                    return False, "entry_too_late", None, None

    # ── All gates passed ──────────────────────────────────────────────────────
    logging.info(
        "COPY_SHARED_BRAIN_OK mode=%s bot=%s wallet=%s trade=%s "
        "side=%s size=%.4f price=%.4f",
        mode, _bot_label, _wallet_lbl,
        str(wallet_trade.get("source_trade_id", "?"))[:20],
        trade_side, submitted_size, submitted_price,
    )
    return True, None, submitted_size, submitted_price


# ── Copy trade loop ───────────────────────────────────────────────────────────

async def copy_trade_loop(trading_client: "ClobClient | None" = None) -> None:
    """
    Copy-trading ingestion and execution loop. Shared brain architecture.

    Runs every COPY_TRADE_LOOP_INTERVAL seconds alongside existing BTC tasks.
    Does NOT affect BTC strategy logic, bot_trades, or any BTC-specific tables.

    Architecture: one shared decision brain, one execution mode switch.
    ─────────────────────────────────────────────────────────────────────────
    SHARED BRAIN (evaluate_copy_trade_shared):
      Same gate logic for ALL bots, regardless of paper or live:
        G1  emergency_stop
        G2  closes_not_enabled
        G3  delay_not_elapsed
        G4  max_trades_per_hour
        G5  max_open_positions (per-bot)
        G6  insufficient_market_data
        G7  unsupported_trade_shape (price)
        → compute submitted_size + submitted_price

    EXECUTION MODE SWITCH (effective_live = live_session_active AND bot.arm_live):
      PAPER  (effective_live=False):
        → apply paper exposure cap
        → write copied_positions (mode=PAPER)

      LIVE   (effective_live=True):
        → evaluate_and_execute_live_copy_trade (live-only gates + CLOB):
            L8   live_global_hourly_cap
            L9   live_open_positions_limit
            L9b  live_max_exposure_reached
            L10  token_id required for CLOB
            → submit real CLOB GTC order
        → write copied_positions (mode=LIVE)

    Live eligibility is controlled ONLY by:
      • COPY_LIVE_ENABLED (env var)
      • copy_global_settings.live_on
      • copy_global_settings.emergency_stop
      • copy_bots.arm_live
    Legacy BTC strategy arming (paper_sniper, paper_fastloop) has no effect here.

    Per-tick flow:
      1. Load active tracked_wallets + enabled copy_bots + global_settings
      2. Derive live_bots (arm_live=True + session active) and paper_bots
      3. For each wallet: fetch + normalize + ingest wallet_trades
      4. For each bot watching this wallet:
           a. get_unevaluated_trades_for_bot()
           b. evaluate_copy_trade_shared() — identical for paper + live
           c. COPY_EXECUTION_PATH log — mode=paper|live
           d. Paper: exposure cap → write copied_positions
              Live:  live gates → CLOB → write copied_positions
           e. log_copy_attempt() (always, even skips)
      5. Update wallet_metrics for each wallet
      6. Log per-tick summary (paper + live positions opened, errors)
      7. Sleep COPY_TRADE_LOOP_INTERVAL seconds
    """
    # ── Build / version marker ────────────────────────────────────────────────
    # WARNING level — fires before any guard so it always appears in Railway on
    # startup regardless of COPY_TRADE_ENABLED.
    # live_routing=copy_bots.arm_live proves the new arm_live-based routing is
    # in this build.  If absent after restart: old code is still deployed.
    logging.warning(
        "COPY_WORKER_BUILD "
        "architecture=shared_copy_brain "
        "live_routing=copy_bots.arm_live "
        "legacy_btc_strategy_routing=disabled_for_copy_trading "
        "env_COPY_LIVE_ENABLED=%s "
        "env_COPY_TRADE_ENABLED=%s "
        "COPY_LIVE_MAX_TRADE_USD=%s "
        "COPY_LIVE_MAX_OPEN_POSITIONS=%s "
        "multi_live_bots=UNLIMITED",
        COPY_LIVE_ENABLED,
        COPY_TRADE_ENABLED,
        COPY_LIVE_MAX_TRADE_USD,
        COPY_LIVE_MAX_OPEN_POSITIONS,
    )

    if not COPY_TRADE_ENABLED:
        # Loop forever with a long sleep so _run_forever doesn't spin tightly.
        # Log at WARNING so this is visible in Railway even with log-level filters.
        while True:
            logging.warning(
                "COPY_TRADE_LOOP_DISABLED build=SHARED_BRAIN_V1 "
                "reason=COPY_TRADE_ENABLED_is_false "
                "action=sleeping_not_running "
                "fix=set_COPY_TRADE_ENABLED=true_in_Railway_env_vars",
            )
            await asyncio.sleep(300)  # re-log every 5 min so it stays visible
        return  # unreachable; satisfies type checkers

    # Read DB-side effective runtime config once at boot so operators can
    # confirm what the worker is actually using (live_on, emergency_stop,
    # exposure caps). If the read fails, log the failure but still boot.
    try:
        _boot_gs = load_copy_global_settings()
    except Exception:
        logging.exception("COPY_TRADE_LOOP_BOOT_GS_FAIL")
        _boot_gs = {}

    logging.info(
        "COPY_TRADE_LOOP_BOOT interval=%ss lookback=%sh fetch_limit=%s "
        "db_limit=%s live_enabled=%s live_on=%s emergency_stop=%s "
        "paper_max_exposure=%s live_max_exposure=%s "
        "live_max_trade_usd=%s live_max_open_pos=%s "
        "live_max_trades_per_hour=%s "
        "note_multi_live_bots=allowed close_path=exit_mirror+settlement",
        COPY_TRADE_LOOP_INTERVAL,
        COPY_TRADE_LOOKBACK_HOURS,
        COPY_WALLET_TRADE_FETCH_LIMIT,
        COPY_WALLET_TRADE_DB_LIMIT,
        COPY_LIVE_ENABLED,
        _boot_gs.get("live_on"),
        _boot_gs.get("emergency_stop"),
        _boot_gs.get("paper_max_exposure_usd"),
        _boot_gs.get("live_max_exposure_usd"),
        COPY_LIVE_MAX_TRADE_USD,
        COPY_LIVE_MAX_OPEN_POSITIONS,
        COPY_LIVE_MAX_TRADES_PER_HOUR,
    )

    # ── Loud warning if DB limit is set dangerously low via env var ──────────
    # A limit ≤ 200 hides SELL events for active wallets and causes missed
    # closes.  The Railway env var COPY_WALLET_TRADE_DB_LIMIT may override the
    # code default — if Railway logs show limit=200 at COPY_UNEVALUATED_LIMIT_HIT,
    # remove or raise that env var.
    _SAFE_DB_LIMIT = 500
    if 0 < COPY_WALLET_TRADE_DB_LIMIT < _SAFE_DB_LIMIT:
        logging.warning(
            "COPY_DB_LIMIT_TOO_LOW db_limit=%s safe_minimum=%s "
            "— SELL events for active wallets WILL be invisible at this limit. "
            "Remove COPY_WALLET_TRADE_DB_LIMIT from Railway env vars or raise "
            "it to at least %s.  Current value was likely set manually and "
            "overrides the code default of 1000.",
            COPY_WALLET_TRADE_DB_LIMIT, _SAFE_DB_LIMIT, _SAFE_DB_LIMIT,
        )

    while True:
        wallets        = load_tracked_wallets()
        all_bots       = load_enabled_copy_bots()
        global_settings = load_copy_global_settings()

        # ── Repeating build marker (WARNING so it's always visible in Railway) ─
        # Fires every tick — cannot be missed regardless of when you start watching.
        logging.warning(
            "COPY_WORKER_BUILD architecture=shared_copy_brain build=SHARED_BRAIN_V1 "
            "env_COPY_LIVE_ENABLED=%s env_COPY_TRADE_ENABLED=%s "
            "bots_loaded=%s wallets_loaded=%s "
            "db_limit=%s fetch_limit=%s lookback_hours=%s",
            COPY_LIVE_ENABLED,
            COPY_TRADE_ENABLED,
            len(all_bots),
            len(wallets),
            COPY_WALLET_TRADE_DB_LIMIT,
            COPY_WALLET_TRADE_FETCH_LIMIT,
            COPY_TRADE_LOOKBACK_HOURS,
        )

        # ── Log every enabled copy bot (WARNING for Railway visibility) ───────
        for _b in all_bots:
            logging.warning(
                "COPY_BOT_LOADED id=%s name=%s is_enabled=%s arm_live=%s mode=%s wallet=%s",
                str(_b.get("id", "?"))[:8],
                _b.get("name") or "(no name)",
                _b.get("is_enabled"),
                bool(_b.get("arm_live")),
                _b.get("mode") or "PAPER",
                str(_b.get("wallet_address") or "?")[:12],
            )

        # ── Paper reset check ─────────────────────────────────────────────────
        # Triggered by setting paper_reset_pending=True in copy_global_settings.
        # Runs before any trade evaluation so that the reset state is clean for
        # this tick. LIVE positions and bankroll are never affected.
        if global_settings.get("paper_reset_pending"):
            _execute_paper_reset()
            # Reload global_settings so this tick sees paper_reset_pending=False
            # and the updated exposure cap.
            global_settings = load_copy_global_settings()

        # ── Live-session active? ──────────────────────────────────────────────
        # Three ENV/DB conditions must ALL be true for ANY bot to go live this
        # tick.  This is evaluated once here so every bot/trade sees the same
        # snapshot of the master live state.
        #
        # IMPORTANT: copy_bots.mode is NOT used for routing.  arm_live is the
        # sole per-bot live switch — matching exactly what the BTCBOT UI counts
        # as "ARM LIVE Bots" / "Live Active Now".
        _live_session_active = (
            COPY_LIVE_ENABLED                                     # env var gate
            and bool(global_settings.get("live_on"))              # DB master switch
            and not bool(global_settings.get("emergency_stop"))   # safety stop
        )

        # live_bots = enabled bots that will execute LIVE this tick.
        # paper_bots = everything else (arm_live=False OR session not active).
        live_bots    = [b for b in all_bots if _live_session_active and bool(b.get("arm_live"))]
        live_bot_ids = [str(b["id"]) for b in live_bots]
        paper_bots   = [b for b in all_bots if not (_live_session_active and bool(b.get("arm_live")))]

        logging.warning(
            "SHARED_BRAIN_TICK live_on=%s arm_live_bots=%s effective_live_bots=%s",
            global_settings.get("live_on"),
            sum(1 for b in all_bots if bool(b.get("arm_live"))),
            [b.get("name") or str(b["id"])[:8] for b in live_bots] or "none",
        )

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
            "wallets=%s bots=%s paper_bots=%s live_bots=%s "
            "paper_max_exposure=%.0f live_max_exposure=%.0f",
            global_settings.get("emergency_stop"),
            global_settings.get("live_on"),
            total_wallets,
            len(all_bots),
            len(paper_bots),
            len(live_bots),
            float(global_settings.get("paper_max_exposure_usd") or 0),
            float(global_settings.get("live_max_exposure_usd") or 0),
        )

        # ── LIVE path diagnostics (every tick) ───────────────────────────────────
        # Routing source: copy_bots.arm_live (per-bot) + live_on (global DB) +
        # COPY_LIVE_ENABLED (env var).  copy_bots.mode is NOT used for routing.
        # live_bots = enabled bots with arm_live=True when session is active.
        # Emitted at WARNING when live_on=True but session is not active (mismatch).
        _diag_live_on_raw = global_settings.get("live_on")
        # Always WARNING — visible in Railway regardless of log-level filters.
        logging.warning(
            "COPY_LIVE_DIAG_TICK "
            "arming_source=copy_bots.arm_live "
            "env_COPY_LIVE_ENABLED=%s "
            "db_live_on=%r "
            "db_emergency_stop=%r "
            "live_session_active=%s "
            "trading_client_ok=%s "
            "armed_live_bot_count=%s live_bots=%s "
            "live_max_exposure_usd=%.0f",
            COPY_LIVE_ENABLED,
            _diag_live_on_raw,
            global_settings.get("emergency_stop"),
            _live_session_active,
            trading_client is not None,
            len(live_bots),
            [b.get("name") or str(b["id"])[:8] for b in live_bots] or "none",
            float(global_settings.get("live_max_exposure_usd") or 0),
        )

        for wallet in wallets:
            wallet_address = wallet["wallet_address"]
            wallet_label   = wallet_address[:10] + "..."

            try:
                # ── Step 1: Fetch raw activity ────────────────────────────
                raw_activities = await fetch_wallet_trades_for_address(wallet_address)

                # ── Step 2: Normalize + ingest wallet_trades ──────────────
                # Track every drop path so operators can see where rows go.
                newly_inserted: list[dict] = []
                _ingest_normalize_drop = 0
                _ingest_dup_or_fail    = 0
                for raw in raw_activities:
                    trade_row = normalize_activity_to_wallet_trade(raw, wallet_address)
                    if not trade_row:
                        _ingest_normalize_drop += 1
                        continue
                    upsert_market_cache(trade_row)
                    if insert_wallet_trade_if_new(trade_row):
                        newly_inserted.append(trade_row)
                        total_new_trades += 1
                    else:
                        _ingest_dup_or_fail += 1

                # Always emit an ingest summary — even when nothing is new.
                # This makes the difference between "wallet quiet" vs
                # "everything dropped" visible in Railway on every tick.
                logging.info(
                    "COPY_INGEST_SUMMARY wallet=%s raw_fetched=%s "
                    "new_inserted=%s normalize_drop=%s dup_or_fail=%s "
                    "fetch_limit=%s",
                    wallet_label,
                    len(raw_activities),
                    len(newly_inserted),
                    _ingest_normalize_drop,
                    _ingest_dup_or_fail,
                    COPY_WALLET_TRADE_FETCH_LIMIT,
                )
                if newly_inserted:
                    _new_sides = [
                        str(t.get("side") or "?").upper() for t in newly_inserted
                    ]
                    logging.info(
                        "COPY_TRADES_INGESTED wallet=%s new=%s raw_fetched=%s "
                        "sides=%s",
                        wallet_label, len(newly_inserted), len(raw_activities),
                        {s: _new_sides.count(s) for s in set(_new_sides)},
                    )
                    _new_sells = [t for t in newly_inserted if str(t.get("side") or "").upper() == "SELL"]
                    if _new_sells:
                        logging.warning(
                            "SELL_INGESTED wallet=%s sell_count=%s "
                            "slugs=%s tokens=%s "
                            "— new SELL events stored in wallet_trades; "
                            "will appear in next evaluate cycle",
                            wallet_label,
                            len(_new_sells),
                            [t.get("market_slug") or "?" for t in _new_sells[:5]],
                            [str(t.get("token_id") or "?")[:16] for t in _new_sells[:5]],
                        )
                if len(raw_activities) >= COPY_WALLET_TRADE_FETCH_LIMIT:
                    logging.warning(
                        "COPY_INGEST_FETCH_CAP_HIT wallet=%s fetch=%s limit=%s "
                        "— older activity may not be seen; consider raising "
                        "COPY_WALLET_TRADE_FETCH_LIMIT",
                        wallet_label,
                        len(raw_activities),
                        COPY_WALLET_TRADE_FETCH_LIMIT,
                    )

                # ── Step 3: Match copy bots + evaluate ───────────────────
                wallet_bots = [b for b in all_bots if b["wallet_address"] == wallet_address]

                for bot in wallet_bots:
                    bot_id    = str(bot["id"])
                    bot_label = bot.get("name") or bot_id[:8]
                    bot_mode  = str(bot.get("mode", "PAPER")).upper()  # informational only

                    # Effective live: arm_live (per-bot) + _live_session_active (global).
                    # This is the ONLY routing flag used — copy_bots.mode is ignored.
                    _effective_live = _live_session_active and bool(bot.get("arm_live"))

                    unevaluated = get_unevaluated_trades_for_bot(
                        wallet_address,
                        bot_id,
                        lookback_hours=COPY_TRADE_LOOKBACK_HOURS,
                        limit=COPY_WALLET_TRADE_DB_LIMIT,
                        copy_closes=bool(bot.get("copy_closes", True)),
                        bot_label=bot_label,
                    )
                    if not unevaluated:
                        continue

                    logging.info(
                        "COPY_BOT_EVAL bot=%s db_mode=%s arm_live=%s "
                        "effective_live=%s db_live_on=%r live_session_active=%s "
                        "wallet=%s unevaluated=%s db_limit=%s lookback_hours=%s",
                        bot_label, bot_mode, bool(bot.get("arm_live")),
                        _effective_live, global_settings.get("live_on"),
                        _live_session_active,
                        wallet_label, len(unevaluated),
                        COPY_WALLET_TRADE_DB_LIMIT, COPY_TRADE_LOOKBACK_HOURS,
                    )

                    # ── Paper cap top-of-loop lock ─────────────────────────
                    # Read paper exposure ONCE before the trade loop.
                    # If already at or above cap, all new BUY attempts this
                    # cycle are skipped immediately.  SELL/CLOSE trades are
                    # never affected by this flag.
                    _paper_cap_locked = False
                    if not _effective_live:
                        _loop_cap = float(
                            global_settings.get("paper_max_exposure_usd") or 0
                        )
                        if _loop_cap > 0:
                            _loop_exposure = _get_paper_exposure_simple()
                            if _loop_exposure >= _loop_cap:
                                _paper_cap_locked = True
                                logging.warning(
                                    "PAPER_CAP_LOCKED bot=%s exposure=%.4f cap=%.2f"
                                    " — skipping all new PAPER BUYs this cycle",
                                    bot_label, _loop_exposure, _loop_cap,
                                )

                    for wallet_trade in unevaluated:
                        trade_label     = str(wallet_trade.get("source_trade_id", "?"))[:20]
                        _prelim_side    = str(wallet_trade.get("side") or "").upper()
                        _is_buy_attempt = _prelim_side != "SELL"
                        _exec_mode      = "live" if _effective_live else "paper"

                        # Fast cap skip for paper BUY (before acquiring lock)
                        if not _effective_live and _paper_cap_locked and _is_buy_attempt:
                            logging.info(
                                "PAPER_CAP_SKIP bot=%s trade=%s reason=cap_locked",
                                bot_label, trade_label,
                            )
                            continue

                        # Single lock per mode (BUY only) — prevents concurrent
                        # coroutines from both passing the exposure gate before
                        # either commits its row.
                        _lock_ctx = (
                            _get_copy_buy_lock(_exec_mode.upper())
                            if _is_buy_attempt else nullcontext()
                        )
                        try:
                          async with _lock_ctx:

                            # ══ SHARED BRAIN ═════════════════════════════════
                            # Identical evaluation for paper AND live.
                            # Gates: emergency_stop, closes filter, delay,
                            #        rate_limit, max_open_positions,
                            #        market_data, price → compute size+price.
                            copied, skip_reason, submitted_size, submitted_price = (
                                evaluate_copy_trade_shared(
                                    bot, wallet_trade, global_settings,
                                    mode=_exec_mode,
                                )
                            )

                            if not copied:
                                log_copy_attempt(
                                    bot, wallet_trade, False, skip_reason,
                                    submitted_size, submitted_price, "SKIPPED",
                                )
                                total_attempts += 1
                                logging.info(
                                    "COPY_SHARED_BRAIN_SKIP bot=%s trade=%s "
                                    "mode=%s reason=%s",
                                    bot_label, trade_label,
                                    _exec_mode, skip_reason,
                                )
                                continue

                            trade_side = str(wallet_trade.get("side") or "").upper()

                            logging.warning(
                                "SHARED_BRAIN_EXECUTION mode=%s bot=%s reason=%s",
                                _exec_mode,
                                bot_label,
                                "effective_live_true" if _effective_live
                                else "effective_live_false",
                            )

                            # ══ PAPER EXECUTION ══════════════════════════════
                            if not _effective_live:
                                if trade_side == "SELL":
                                    order_status = "SOURCE_EXIT_MIRRORED"
                                    # ── SELL detected — log before mirror ────
                                    logging.warning(
                                        "SELL_DETECTED mode=paper bot=%s "
                                        "wallet=%s trade=%s slug=%s "
                                        "copy_closes=%s "
                                        "— shared brain passed; calling "
                                        "close_matching_open_positions_on_exit",
                                        bot_label, wallet_label, trade_label,
                                        wallet_trade.get("market_slug") or "?",
                                        bool(bot.get("copy_closes")),
                                    )
                                else:
                                    order_status = "PAPER_MATCHED"
                                log_copy_attempt(
                                    bot, wallet_trade, True, None,
                                    submitted_size, submitted_price, order_status,
                                )
                                total_attempts += 1

                                if trade_side == "SELL":
                                    # Source exit: close matching OPEN rows.
                                    n_closed = close_matching_open_positions_on_exit(
                                        bot, wallet_trade
                                    )
                                    if n_closed:
                                        update_wallet_metrics_for_address(wallet_address)
                                    total_paper_opened += n_closed
                                    logging.info(
                                        "COPY_EXIT_MIRROR_DONE bot=%s wallet=%s "
                                        "trade=%s slug=%s positions_closed=%s",
                                        bot_label, wallet_label, trade_label,
                                        wallet_trade.get("market_slug") or "?",
                                        n_closed,
                                    )
                                else:
                                    # BUY: paper-specific exposure hard guard.
                                    # Re-reads live DB so stale values can't bypass cap.
                                    _pg_cap  = float(
                                        global_settings.get("paper_max_exposure_usd") or 0
                                    )
                                    _pg_exp  = _get_paper_exposure_simple()
                                    _pg_proj = round(
                                        _pg_exp + (submitted_size or 0.0), 4
                                    )
                                    _should_open = (
                                        _pg_cap <= 0           # unlimited
                                        or _pg_proj <= _pg_cap # within cap
                                    )
                                    if not _should_open:
                                        logging.warning(
                                            "COPY_BUY_PREGUARD_BLOCKED mode=paper "
                                            "wallet=%s bot=%s "
                                            "current=%.4f size=%.4f "
                                            "projected=%.4f cap=%.2f decision=blocked",
                                            wallet_label, bot_label,
                                            _pg_exp, submitted_size or 0.0,
                                            _pg_proj, _pg_cap,
                                        )
                                    else:
                                        logging.info(
                                            "COPY_BUY_ALLOWED mode=paper "
                                            "wallet=%s bot=%s "
                                            "current=%.4f size=%.4f "
                                            "projected=%.4f cap=%.2f decision=allowed",
                                            wallet_label, bot_label,
                                            _pg_exp, submitted_size or 0.0,
                                            _pg_proj,
                                            _pg_cap if _pg_cap > 0
                                            else float("inf"),
                                        )
                                        _new_pos_id = open_copied_position(
                                            bot, wallet_trade,
                                            submitted_size, submitted_price,
                                            mode="PAPER",
                                        )
                                        if _new_pos_id is not None:
                                            _copy_bot_mark_trade(bot_id)
                                            total_paper_opened += 1
                                            _post_exp = _get_paper_exposure_simple()
                                            logging.info(
                                                "COPY_BUY_WRITTEN mode=paper "
                                                "wallet=%s bot=%s "
                                                "actual_opened_size=%.4f "
                                                "exposure_after_write=%.4f "
                                                "position_id=%s trade=%s",
                                                wallet_label, bot_label,
                                                submitted_size, _post_exp,
                                                _new_pos_id, trade_label,
                                            )
                                            logging.info(
                                                "COPY_PAPER_COPIED bot=%s "
                                                "wallet=%s trade=%s slug=%s "
                                                "side=%s outcome=%s "
                                                "size=%s price=%s",
                                                bot_label, wallet_label,
                                                trade_label,
                                                wallet_trade.get("market_slug")
                                                or "?",
                                                wallet_trade.get("side"),
                                                wallet_trade.get("outcome"),
                                                submitted_size, submitted_price,
                                            )
                                        else:
                                            logging.warning(
                                                "COPY_PAPER_BLOCKED_CAP "
                                                "bot=%s trade=%s size=%.4f "
                                                "error=insert_failed "
                                                "(see COPY_OPEN_POSITION_FAIL"
                                                " above)",
                                                bot_label, trade_label,
                                                submitted_size or 0.0,
                                            )

                            # ══ LIVE EXECUTION ═══════════════════════════════
                            # LIVE ON acts as an execution switch on the same
                            # shared brain.  Turning it on → live CLOB orders.
                            # Turning it off → falls back to paper execution.
                            else:
                                # Infrastructure checks gate the CLOB submission
                                # ONLY.  For SELL/close events, DB close mirroring
                                # is unconditional — the source wallet already
                                # exited, so the thesis is dead.  We ALWAYS close
                                # the DB row to prevent stuck-open positions even
                                # when CLOB infra is unavailable.
                                if not COPY_LIVE_ENABLED and trade_side != "SELL":
                                    logging.warning(
                                        "COPY_LIVE_SKIP_ENV_OFF bot=%s "
                                        "trade=%s — add COPY_LIVE_ENABLED=true"
                                        " env var and redeploy",
                                        bot_label, trade_label,
                                    )
                                    log_copy_attempt(
                                        bot, wallet_trade, False,
                                        "live_copy_globally_disabled",
                                        None, None, "SKIPPED",
                                    )
                                    total_attempts += 1
                                    continue

                                if not trading_client and trade_side != "SELL":
                                    logging.warning(
                                        "COPY_LIVE_SKIP_NO_CLIENT bot=%s "
                                        "trade=%s — trading_client is None "
                                        "(CLOB auth failed at startup?)",
                                        bot_label, trade_label,
                                    )
                                    log_copy_attempt(
                                        bot, wallet_trade, False,
                                        "live_client_unavailable",
                                        None, None, "SKIPPED",
                                    )
                                    total_attempts += 1
                                    continue

                                # Apply live-only gates + submit CLOB when infra
                                # is available.  For SELL trades reaching here with
                                # no CLOB infra, skip the CLOB but let DB close run.
                                _clob_ready = COPY_LIVE_ENABLED and bool(trading_client)
                                if _clob_ready:
                                    (
                                        live_ok,
                                        live_skip_reason,
                                        submitted_size,
                                        submitted_price,
                                        raw_response,
                                    ) = evaluate_and_execute_live_copy_trade(
                                        bot, wallet_trade, global_settings,
                                        trading_client, live_bot_ids,
                                        submitted_size, submitted_price,
                                    )
                                else:
                                    # CLOB infra unavailable (COPY_LIVE_ENABLED=False
                                    # or trading_client=None).  Only SELLs reach here
                                    # because BUYs were caught by the guards above.
                                    live_ok = False
                                    live_skip_reason = (
                                        "live_copy_globally_disabled"
                                        if not COPY_LIVE_ENABLED
                                        else "live_client_unavailable"
                                    )
                                    raw_response = {}
                                    logging.warning(
                                        "COPY_LIVE_SELL_CLOB_SKIP bot=%s trade=%s "
                                        "slug=%s reason=%s "
                                        "— CLOB infra unavailable; DB close mirror "
                                        "will still execute unconditionally below",
                                        bot_label, trade_label,
                                        wallet_trade.get("market_slug") or "?",
                                        live_skip_reason,
                                    )

                                if live_ok and trade_side == "SELL":
                                    order_status = "LIVE_EXIT_MIRRORED"
                                elif live_ok:
                                    order_status = "LIVE_MATCHED"
                                elif trade_side == "SELL":
                                    # CLOB failed but we will still close the
                                    # DB exit mirror below.
                                    order_status = "LIVE_EXIT_DB_ONLY"
                                else:
                                    order_status = "SKIPPED"

                                log_copy_attempt(
                                    bot, wallet_trade, live_ok,
                                    live_skip_reason,
                                    submitted_size, submitted_price,
                                    order_status, raw_response,
                                )
                                total_attempts += 1

                                # ── SELL/close: ALWAYS mirror the DB exit ──
                                # The source wallet has already sold; the thesis
                                # is dead.  We must close the DB row regardless
                                # of whether the CLOB SELL submission succeeded.
                                # CLOB submission is best-effort; operator can
                                # manually reconcile on Polymarket if the order
                                # failed (e.g. already filled, market gone, etc).
                                # This prevents stuck-open live positions when
                                # any live gate or order submission fails.
                                if trade_side == "SELL":
                                    logging.warning(
                                        "SELL_DETECTED mode=live bot=%s "
                                        "wallet=%s trade=%s slug=%s "
                                        "copy_closes=%s clob_ok=%s "
                                        "— calling close_matching_open_positions_on_exit",
                                        bot_label, wallet_label, trade_label,
                                        wallet_trade.get("market_slug") or "?",
                                        bool(bot.get("copy_closes")),
                                        live_ok,
                                    )
                                    n_closed = (
                                        close_matching_open_positions_on_exit(
                                            bot, wallet_trade
                                        )
                                    )
                                    if n_closed:
                                        update_wallet_metrics_for_address(
                                            wallet_address
                                        )
                                    if live_ok:
                                        logging.info(
                                            "COPY_LIVE_EXIT_MIRROR_DONE "
                                            "bot=%s wallet=%s trade=%s "
                                            "slug=%s positions_closed=%s "
                                            "clob=ok",
                                            bot_label, wallet_label,
                                            trade_label,
                                            wallet_trade.get("market_slug")
                                            or "?",
                                            n_closed,
                                        )
                                    else:
                                        logging.warning(
                                            "COPY_LIVE_EXIT_MIRROR_DB_ONLY "
                                            "bot=%s wallet=%s trade=%s "
                                            "slug=%s positions_closed=%s "
                                            "clob=failed clob_reason=%s "
                                            "— source exited, DB closed; "
                                            "CLOB SELL did not submit. "
                                            "Manual Polymarket reconcile may "
                                            "be needed.",
                                            bot_label, wallet_label,
                                            trade_label,
                                            wallet_trade.get("market_slug")
                                            or "?",
                                            n_closed, live_skip_reason,
                                        )
                                elif live_ok:
                                    # CLOB BUY submitted — write DB row.
                                    _live_pos_id = open_copied_position(
                                        bot, wallet_trade,
                                        submitted_size, submitted_price,
                                        mode="LIVE",
                                    )
                                    if _live_pos_id is not None:
                                        _copy_bot_mark_trade(bot_id)
                                        _mark_live_copy_trade()
                                        total_live_opened += 1
                                        logging.info(
                                            "COPY_LIVE_COPIED bot=%s "
                                            "wallet=%s trade=%s slug=%s "
                                            "side=%s outcome=%s "
                                            "size=%s price=%s "
                                            "position_id=%s",
                                            bot_label, wallet_label,
                                            trade_label,
                                            wallet_trade.get("market_slug")
                                            or "?",
                                            wallet_trade.get("side"),
                                            wallet_trade.get("outcome"),
                                            submitted_size, submitted_price,
                                            _live_pos_id,
                                        )
                                    else:
                                        logging.warning(
                                            "COPY_LIVE_POSITION_WRITE_FAIL"
                                            " bot=%s trade=%s size=%.4f"
                                            " — CLOB submitted but DB"
                                            " insert failed; manual"
                                            " reconciliation needed",
                                            bot_label, trade_label,
                                            submitted_size or 0.0,
                                        )
                                else:
                                    logging.info(
                                        "COPY_LIVE_SKIPPED bot=%s wallet=%s "
                                        "trade=%s reason=%s",
                                        bot_label, wallet_label,
                                        trade_label, live_skip_reason,
                                    )

                        except Exception:
                            logging.exception(
                                "COPY_EVALUATE_FAIL bot=%s trade=%s mode=%s",
                                bot_label, trade_label, _exec_mode,
                            )
                            total_errors += 1

                # ── Step 4: Update wallet metrics ─────────────────────────
                update_wallet_metrics_for_address(wallet_address)

            except Exception:
                logging.exception("COPY_WALLET_ERROR wallet=%s", wallet_label)
                total_errors += 1

        # ── Per-tick exposure snapshot (paper + live) ─────────────────────────
        # Uses the same RPC aggregate as Gate 9 / L9b.  Logged every tick
        # so the log stream shows a continuous exposure/headroom trace without
        # requiring a BUY event.  BLOCK = all new BUYs are currently rejected.
        _snap_paper_cap = float(global_settings.get("paper_max_exposure_usd") or 0)
        _snap_live_cap  = float(global_settings.get("live_max_exposure_usd") or 0)
        _snap_paper_exp = _get_paper_exposure_simple()
        if _snap_paper_cap > 0:
            _snap_headroom = max(0.0, round(_snap_paper_cap - _snap_paper_exp, 2))
            _snap_status   = "BLOCK" if _snap_paper_exp >= _snap_paper_cap else "OPEN"
            logging.info(
                "COPY_EXPOSURE_SNAPSHOT mode=paper "
                "open_exposure=%.2f cap=%.2f headroom=%.2f status=%s",
                _snap_paper_exp, _snap_paper_cap, _snap_headroom, _snap_status,
            )
        else:
            logging.info(
                "COPY_EXPOSURE_SNAPSHOT mode=paper open_exposure=%.2f cap=UNLIMITED",
                _snap_paper_exp,
            )

        logging.warning(
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


COPY_PAPER_BOT_ID = "copy_paper"


def _update_copy_paper_bankroll(
    pnl: float,
    pos_id: str,
    close_path: str = "settlement",
) -> None:
    """
    Apply a paper PnL delta to the copy_paper bankroll row in bot_settings.

    Reads the current paper_balance_usd and paper_pnl_usd for bot_id='copy_paper',
    adds pnl to both, writes the update, and logs with the required tag.

    Anti-double-count guarantee: this function is only called after the
    copied_positions row has already been updated to status='CLOSED' in the DB.
    The settlement loop only loads OPEN positions, and the exit-mirror query also
    only matches OPEN positions — so the same position can never trigger a second
    bankroll update.

    close_path identifies which code path triggered the close (for log context):
      "settlement"  — copy_settlement_loop (market resolution)
      "exit_mirror" — close_matching_open_positions_on_exit (source wallet SELL)
    """
    try:
        resp = (
            supabase.table("bot_settings")
            .select("paper_balance_usd, paper_pnl_usd")
            .eq("bot_id", COPY_PAPER_BOT_ID)
            .limit(1)
            .execute()
        )
        row = (resp.data or [None])[0]
        old_balance = float_or_none(row.get("paper_balance_usd") if row else None) or 0.0
        old_pnl     = float_or_none(row.get("paper_pnl_usd")     if row else None) or 0.0

        new_balance = round(old_balance + pnl, 6)
        new_pnl     = round(old_pnl     + pnl, 6)

        payload = {
            "paper_balance_usd": new_balance,
            "paper_pnl_usd":     new_pnl,
            "updated_at":        utc_now_iso(),
        }
        if row:
            supabase.table("bot_settings").update(payload).eq("bot_id", COPY_PAPER_BOT_ID).execute()
        else:
            supabase.table("bot_settings").insert(
                {"bot_id": COPY_PAPER_BOT_ID, **payload}
            ).execute()

        logging.info(
            "COPY_PAPER_BANKROLL_UPDATED pos=%s path=%s pnl=%+.4f "
            "old_balance=%.4f new_balance=%.4f old_pnl=%.4f new_pnl=%.4f",
            pos_id[:8] if pos_id else "?",
            close_path,
            pnl,
            old_balance,
            new_balance,
            old_pnl,
            new_pnl,
        )
    except Exception:
        logging.exception(
            "COPY_PAPER_BANKROLL_UPDATE_FAIL pos=%s path=%s pnl=%s",
            pos_id[:8] if pos_id else "?",
            close_path,
            pnl,
        )


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

    _settle_now = utc_now_iso()
    updates = {
        "status": "CLOSED",
        "exit_price": exit_price,
        "pnl": pnl,
        "closed_at": closed_at,
        "raw_json": {
            **(pos.get("raw_json") or {}),
            # Standardized top-level close_reason (Phase 2)
            "close_reason": CLOSE_REASON_SETTLED_MARKET,
            # Detailed settlement sub-object (preserved for backward compatibility)
            "settlement": {
                "resolved": resolution_data.get("resolved"),
                "resolution_outcome": resolution_data.get("resolution_outcome"),
                "exit_price": exit_price,
                "pnl": pnl,
                "settled_by": "copy_settlement_loop",
                "settled_at": _settle_now,
            },
        },
    }
    try:
        supabase.table("copied_positions").update(updates).eq("id", pos_id).execute()
        logging.info(
            "COPY_POSITION_CLOSED pos=%s slug=%s outcome=%s resolution=%s "
            "exit_price=%s entry_price=%s size=%s pnl=%s close_reason=%s",
            pos_id[:8],
            pos.get("market_slug") or "?",
            pos.get("outcome"),
            resolution_data.get("resolution_outcome"),
            exit_price,
            entry_price,
            size,
            pnl,
            CLOSE_REASON_SETTLED_MARKET,
        )
        # Best-effort write to dedicated close_reason column (Phase 2 migration)
        _try_write_close_reason_col(pos_id, CLOSE_REASON_SETTLED_MARKET)
        # Update copy_paper bankroll — paper positions only.
        # Live positions are tracked separately (real balance, not paper).
        # Default to paper=True so pre-mode-tag positions are also credited.
        raw_json = pos.get("raw_json") or {}
        is_paper = raw_json.get("paper", True)
        if is_paper and pnl != 0.0:
            _update_copy_paper_bankroll(pnl, pos_id, close_path="settlement")
    except Exception:
        logging.exception("COPY_SETTLE_CLOSE_POSITION_FAIL pos=%s", pos_id)


# ── Paper reset ───────────────────────────────────────────────────────────────

def _execute_paper_reset() -> int:
    """
    Execute a clean paper reset triggered by paper_reset_pending=True in
    copy_global_settings.

    Cancels ALL OPEN copied_positions that are NOT owned by a confirmed LIVE-mode
    copy bot.  This includes:
      - Positions owned by known PAPER-mode bots (enabled or disabled)
      - Positions owned by bots that have since been DELETED (orphaned rows)
      - Positions with a null or unrecognised copy_bot_id
    LIVE positions are identified by exclusion and are NEVER touched.

    Strategy (exclusion-based, not whitelist):
      Previous approach:  cancel where copy_bot_id IN (paper_bot_ids)
                          — misses deleted-bot rows and null copy_bot_id rows
      New approach:       load ALL OPEN rows, classify in Python,
                          cancel by primary-key batch — catches everything

    Steps:
      1. Load ALL copy_bots to build the LIVE bot ID exclusion set.
      2. Count ALL OPEN copied_positions (pre-reset audit).
      3. Fetch ALL OPEN position IDs (up to 50 000).
      4. Classify in Python: skip confirmed-LIVE positions, queue everything else.
      5. Batch-cancel queued positions by primary key (500 per request).
      6. Verify: count remaining OPEN non-LIVE positions; warn if > 0.
      7. Reset bot_settings for bot_id='copy_paper'.
      8. Clear paper_reset_pending in copy_global_settings.

    Returns the number of positions cancelled.
    Logs COPY_PAPER_RESET_* tags throughout.
    """
    _CANCEL_BATCH = 500
    now_ts = utc_now_iso()

    logging.info("COPY_PAPER_RESET_START — hard reset of all OPEN paper positions")

    # ── Step 1: Build LIVE bot exclusion set ─────────────────────────────────
    # Fetch ALL copy_bots (enabled + disabled) so we know which IDs are LIVE.
    try:
        all_bots_resp = supabase.table("copy_bots").select("id, mode, name").execute()
        all_bots_rows = all_bots_resp.data or []
    except Exception:
        logging.exception("COPY_PAPER_RESET_FAIL step=load_bots")
        return 0

    live_bot_ids: set[str] = {
        str(b["id"])
        for b in all_bots_rows
        if str(b.get("mode", "PAPER")).upper() == "LIVE"
    }
    paper_bot_ids: set[str] = {
        str(b["id"])
        for b in all_bots_rows
        if str(b.get("mode", "PAPER")).upper() == "PAPER"
    }
    paper_bot_names = [
        b.get("name") or str(b["id"])[:8]
        for b in all_bots_rows
        if str(b.get("mode", "PAPER")).upper() == "PAPER"
    ]
    logging.info(
        "COPY_PAPER_RESET_BOTS paper=%s live=%s live_ids=%s paper_names=%s",
        len(paper_bot_ids),
        len(live_bot_ids),
        list(live_bot_ids),
        paper_bot_names,
    )

    # ── Step 2: Pre-reset audit count ────────────────────────────────────────
    try:
        pre_resp = (
            supabase.table("copied_positions")
            .select("id", count="exact")
            .eq("status", "OPEN")
            .execute()
        )
        total_open_before = pre_resp.count or 0
    except Exception:
        logging.exception("COPY_PAPER_RESET_FAIL step=pre_count")
        total_open_before = -1

    logging.info("COPY_PAPER_RESET_PRE_COUNT total_open=%s", total_open_before)

    # ── Step 3: Fetch ALL OPEN position IDs ──────────────────────────────────
    # Select only the columns needed for classification (id + copy_bot_id).
    # limit=50000 to handle large position counts without Supabase's default
    # 1000-row page cap silently truncating the result.
    try:
        open_resp = (
            supabase.table("copied_positions")
            .select("id, copy_bot_id")
            .eq("status", "OPEN")
            .limit(50000)
            .execute()
        )
        all_open_rows = open_resp.data or []
    except Exception:
        logging.exception("COPY_PAPER_RESET_FAIL step=load_open_positions")
        return 0

    if len(all_open_rows) == 50000:
        logging.warning(
            "COPY_PAPER_RESET_LIMIT_HIT — 50000 row limit reached; "
            "some positions may be missed. Run reset again if needed."
        )

    # ── Step 4: Classify in Python ────────────────────────────────────────────
    # Exclusion: skip only confirmed LIVE-bot positions.
    # Everything else (known paper, orphaned, null copy_bot_id) is cancelled.
    to_cancel_ids: list[str] = []
    live_skipped    = 0
    orphaned_count  = 0

    for row in all_open_rows:
        bid = str(row.get("copy_bot_id") or "")
        if bid in live_bot_ids:
            live_skipped += 1
            continue
        to_cancel_ids.append(str(row["id"]))
        if bid and bid not in paper_bot_ids:
            orphaned_count += 1   # non-null copy_bot_id not in any current bot

    null_count = sum(1 for r in all_open_rows if not r.get("copy_bot_id"))

    logging.info(
        "COPY_PAPER_RESET_CLASSIFY fetched=%s to_cancel=%s "
        "live_skipped=%s orphaned=%s null_bot_id=%s",
        len(all_open_rows),
        len(to_cancel_ids),
        live_skipped,
        orphaned_count,
        null_count,
    )

    # ── Step 5: Batch-cancel by primary key ──────────────────────────────────
    # Use position IDs (not copy_bot_id) so orphaned and null rows are covered.
    # Batched in groups of _CANCEL_BATCH to stay well under URL length limits.
    cancelled_count = 0
    total_batches   = -(-len(to_cancel_ids) // _CANCEL_BATCH) if to_cancel_ids else 0

    for batch_num, i in enumerate(range(0, len(to_cancel_ids), _CANCEL_BATCH), start=1):
        batch = to_cancel_ids[i : i + _CANCEL_BATCH]
        try:
            supabase.table("copied_positions").update(
                {"status": "CANCELLED", "closed_at": now_ts, "close_reason": CLOSE_REASON_MANUAL_RESET}
            ).in_("id", batch).execute()
            cancelled_count += len(batch)
            logging.info(
                "COPY_PAPER_RESET_BATCH %s/%s cancelled=%s",
                batch_num, total_batches, len(batch),
            )
        except Exception:
            logging.exception(
                "COPY_PAPER_RESET_FAIL step=cancel_batch batch=%s/%s",
                batch_num, total_batches,
            )

    logging.info(
        "COPY_PAPER_RESET_CANCELLED total_cancelled=%s",
        cancelled_count,
    )

    # ── Step 6: Verify — count remaining non-LIVE OPEN positions ─────────────
    try:
        post_resp = (
            supabase.table("copied_positions")
            .select("id", count="exact")
            .eq("status", "OPEN")
            .execute()
        )
        total_open_after = post_resp.count or 0
        expected_open    = live_skipped   # only LIVE positions should remain OPEN

        if total_open_after > expected_open:
            logging.warning(
                "COPY_PAPER_RESET_INCOMPLETE "
                "total_open_after=%s live_expected=%s unexplained_remaining=%s "
                "— some OPEN rows were not cancelled; inspect copied_positions",
                total_open_after,
                expected_open,
                total_open_after - expected_open,
            )
        else:
            logging.info(
                "COPY_PAPER_RESET_VERIFIED "
                "total_open_after=%s live_positions=%s paper_open=0",
                total_open_after,
                live_skipped,
            )
    except Exception:
        logging.exception("COPY_PAPER_RESET_FAIL step=post_verify")

    # ── Step 7: Reset paper bankroll ─────────────────────────────────────────
    paper_start_balance = 1000.0
    bankroll_payload = {
        "paper_balance_usd": paper_start_balance,
        "paper_pnl_usd":     0.0,
        "updated_at":        now_ts,
    }
    try:
        check_resp = (
            supabase.table("bot_settings")
            .select("bot_id")
            .eq("bot_id", COPY_PAPER_BOT_ID)
            .limit(1)
            .execute()
        )
        if check_resp.data:
            supabase.table("bot_settings").update(bankroll_payload).eq("bot_id", COPY_PAPER_BOT_ID).execute()
        else:
            supabase.table("bot_settings").insert({"bot_id": COPY_PAPER_BOT_ID, **bankroll_payload}).execute()
        logging.info("COPY_PAPER_RESET_BANKROLL balance=%.2f pnl=0.00", paper_start_balance)
    except Exception:
        logging.exception("COPY_PAPER_RESET_FAIL step=reset_bankroll")

    # ── Step 8: Clear the reset flag ─────────────────────────────────────────
    try:
        supabase.table("copy_global_settings").update({
            "paper_reset_pending": False,
            "paper_reset_at":      now_ts,
        }).eq("id", 1).execute()
        logging.info("COPY_PAPER_RESET_FLAG_CLEARED")
    except Exception:
        logging.exception("COPY_PAPER_RESET_FAIL step=clear_flag")

    logging.info(
        "COPY_PAPER_RESET_DONE "
        "pre_open=%s cancelled=%s orphaned=%s null_bot_id=%s new_balance=%.2f",
        total_open_before,
        cancelled_count,
        orphaned_count,
        null_count,
        paper_start_balance,
    )
    return cancelled_count


# ── Auto-profit / max-hold exit loop ─────────────────────────────────────────
#
# Scans OPEN copied_positions on a background loop and closes positions early
# when per-bot exit rules (take_profit_pct / max_hold_minutes) are met.
#
# COMPLETELY ISOLATED from:
#   • copy trading ingestion   (copy_trade_loop, get_unevaluated_trades_for_bot)
#   • source-wallet SELL close (close_matching_open_positions_on_exit)
#   • market-resolution settle (copy_settlement_loop / close_copied_position)
#
# Settings read from the copy_bot row (copy_bots table, select("*")):
#   exit_mode         text     DEFAULT 'mirror_only'
#                              allowed: mirror_only | auto_profit | auto_profit_max_hold
#   take_profit_pct   numeric  target profit % (e.g. 20 = 20 %)
#   max_hold_minutes  numeric  max age in minutes before forced close
#
# These columns must be added to copy_bots in Supabase (see deploy steps).
# Until the columns exist, copy_bot.get("exit_mode") returns None which
# defaults to "mirror_only" — existing behavior is fully preserved.
#
# ─────────────────────────────────────────────────────────────────────────────


def _copy_auto_exit_fetch_mark_price_sync(pos: dict) -> float | None:
    """
    Fetch the current market price for the outcome held in this position.

    Returns a probability in [0.0, 1.0] or None on failure.

    Attempts:
      1. Gamma /markets?clob_token_ids={token_id}  — most specific
      2. Gamma /markets?condition_id={condition_id}
      3. Gamma events/slug/{slug}  — fallback via _ema5m_fetch_market_prices_sync

    Does NOT use the resolution cache from the settlement loop — prices are
    fetched live so the auto-exit loop sees the real current mark price.
    """
    token_id     = pos.get("token_id")
    condition_id = pos.get("condition_id")
    market_slug  = pos.get("market_slug")
    outcome      = str(pos.get("outcome") or "").upper()

    def _extract_price(market_obj: dict) -> float | None:
        op       = market_obj.get("outcomePrices") or []
        outcomes = market_obj.get("outcomes") or []
        # Match by outcome name first
        for i, o in enumerate(outcomes):
            if str(o).upper() == outcome and i < len(op):
                try:
                    return float(op[i])
                except (TypeError, ValueError):
                    pass
        # Positional fallback
        if outcome == "YES" and op:
            try:
                return float(op[0])
            except (TypeError, ValueError):
                pass
        if outcome == "NO" and len(op) > 1:
            try:
                return float(op[1])
            except (TypeError, ValueError):
                pass
        # lastPrice (some markets)
        lp = market_obj.get("lastPrice")
        if lp:
            try:
                return float(lp)
            except (TypeError, ValueError):
                pass
        return None

    _hdrs    = {"User-Agent": "FastLoopWorker/1.0"}
    _pos_tag = str(pos.get("id") or "?")[:8]
    _attempts_tried: list[str] = []

    # ── Attempt 1: by token_id ────────────────────────────────────────────
    if token_id:
        _attempts_tried.append("token_id")
        try:
            url = f"{GAMMA_API_BASE}/markets?clob_token_ids={token_id}"
            req = request.Request(url, headers=_hdrs)
            with request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read())
            markets = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
            for m in markets:
                price = _extract_price(m)
                if price is not None:
                    logging.info(
                        "COPY_MARK_PRICE_OK pos=%s slug=%s outcome=%s "
                        "price=%.4f source=token_id",
                        _pos_tag, market_slug or "?", outcome, price,
                    )
                    return price
            logging.info(
                "COPY_MARK_PRICE_MISS pos=%s slug=%s source=token_id "
                "— no usable price in %s market objects",
                _pos_tag, market_slug or "?", len(markets),
            )
        except Exception as exc:
            logging.info(
                "COPY_MARK_PRICE_FAIL pos=%s slug=%s source=token_id err=%s",
                _pos_tag, market_slug or "?", exc,
            )

    # ── Attempt 2: by condition_id ────────────────────────────────────────
    if condition_id:
        _attempts_tried.append("condition_id")
        try:
            url = f"{GAMMA_API_BASE}/markets?condition_id={condition_id}"
            req = request.Request(url, headers=_hdrs)
            with request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read())
            markets = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
            for m in markets:
                price = _extract_price(m)
                if price is not None:
                    logging.info(
                        "COPY_MARK_PRICE_OK pos=%s slug=%s outcome=%s "
                        "price=%.4f source=condition_id",
                        _pos_tag, market_slug or "?", outcome, price,
                    )
                    return price
            logging.info(
                "COPY_MARK_PRICE_MISS pos=%s slug=%s source=condition_id "
                "— no usable price in %s market objects",
                _pos_tag, market_slug or "?", len(markets),
            )
        except Exception as exc:
            logging.info(
                "COPY_MARK_PRICE_FAIL pos=%s slug=%s source=condition_id err=%s",
                _pos_tag, market_slug or "?", exc,
            )

    # ── Attempt 3: by slug ────────────────────────────────────────────────
    if market_slug:
        _attempts_tried.append("slug")
        try:
            yes_p, no_p = _ema5m_fetch_market_prices_sync(market_slug)
            if outcome == "YES" and yes_p not in (None, 0.50):
                logging.info(
                    "COPY_MARK_PRICE_OK pos=%s slug=%s outcome=YES "
                    "price=%.4f source=slug",
                    _pos_tag, market_slug, yes_p,
                )
                return yes_p
            if outcome == "NO" and no_p not in (None, 0.50):
                logging.info(
                    "COPY_MARK_PRICE_OK pos=%s slug=%s outcome=NO "
                    "price=%.4f source=slug",
                    _pos_tag, market_slug, no_p,
                )
                return no_p
            logging.info(
                "COPY_MARK_PRICE_MISS pos=%s slug=%s outcome=%s source=slug "
                "yes_p=%s no_p=%s — prices are None or 0.50 (ambiguous)",
                _pos_tag, market_slug, outcome, yes_p, no_p,
            )
        except Exception as exc:
            logging.info(
                "COPY_MARK_PRICE_FAIL pos=%s slug=%s source=slug err=%s",
                _pos_tag, market_slug or "?", exc,
            )

    # All attempts exhausted — log clearly so operators know why close may not fire
    logging.warning(
        "COPY_MARK_PRICE_UNAVAILABLE pos=%s slug=%s outcome=%s "
        "token_id_present=%s condition_id_present=%s attempts_tried=%s "
        "— could not fetch mark price from any Gamma API source. "
        "TP exit blocked this tick. "
        "max-hold exit will use entry_price fallback if max_hold triggered. "
        "Check: is token_id populated on copied_positions? "
        "Is the market slug correct? Is Gamma API reachable?",
        _pos_tag,
        market_slug or "NONE",
        outcome,
        bool(token_id),
        bool(condition_id),
        _attempts_tried or ["none_tried"],
    )
    return None


def _try_write_close_reason_col(pos_id: str, close_reason: str) -> None:
    """
    Attempt to write the standardized close_reason to the dedicated column on
    copied_positions.  Fails silently if the column does not yet exist.

    This is a best-effort write — the canonical close_reason lives in
    raw_json["close_reason"] which is always written by the main close update.
    The dedicated column is for SQL queries and dashboards after the migration.

    Run the Phase 2 migration before this has any effect:
      ALTER TABLE copied_positions ADD COLUMN IF NOT EXISTS close_reason text;

    Log tags:
      COPY_CLOSE_REASON_WRITE_OK   — dedicated column write succeeded
      COPY_CLOSE_REASON_COL_WRITE_FAIL — non-schema error during write
    """
    try:
        supabase.table("copied_positions").update(
            {"close_reason": close_reason}
        ).eq("id", pos_id).execute()
        logging.info(
            "COPY_CLOSE_REASON_WRITE_OK pos=%s reason=%s",
            pos_id[:8], close_reason,
        )
    except Exception as exc:
        exc_str = str(exc).lower()
        if any(kw in exc_str for kw in ("close_reason", "column", "42703", "schema")):
            pass  # column not yet migrated — expected pre-migration; raw_json is canonical
        else:
            logging.info(
                "COPY_CLOSE_REASON_COL_WRITE_FAIL pos=%s reason=%s err=%s",
                pos_id[:8], close_reason, exc,
            )


def _copy_auto_exit_close_position_sync(
    pos: dict,
    exit_price: float,
    reason: str,
    fallback_price: bool = False,
) -> bool:
    """
    Close a single OPEN copied_positions row via auto-exit logic.

    Only called for PAPER positions — live auto-exit is blocked upstream.

    Uses .eq("status", "OPEN") as a concurrency guard: if another path
    (settlement loop, source wallet SELL) closed the position between
    load and this update, the DB update matches 0 rows and we log
    COPY_EXIT_ALREADY_CLOSED cleanly.

    reason: CLOSE_REASON_AUTO_PROFIT | CLOSE_REASON_MAX_HOLD
      (constants defined in worker_config.py)

    fallback_price: True when mark_price was unavailable and entry_price
      is used as the exit_price (max_hold path only).  Written into
      raw_json["auto_exit"]["fallback_close_no_mark_price"] for traceability.

    Phase 2: writes standardized close_reason at top level of raw_json
    and attempts to write the dedicated close_reason column.
    Phase 3: records fallback_close_no_mark_price in raw_json sub-object.
    """
    pos_id      = str(pos.get("id") or "")
    entry_price = float_or_none(pos.get("entry_price")) or 0.0
    size        = float_or_none(pos.get("size")) or 0.0
    _now_ts     = utc_now_iso()

    pnl = (
        round(size * (exit_price - entry_price) / entry_price, 6)
        if entry_price > 0
        else 0.0
    )

    updates = {
        "status":     "CLOSED",
        "exit_price": exit_price,
        "pnl":        pnl,
        "closed_at":  _now_ts,
        "raw_json": {
            **(pos.get("raw_json") or {}),
            # Standardized top-level close_reason (Phase 2) — read by all paths
            "close_reason": reason,
            # Detailed sub-object for this close path (preserved from Phase 1)
            "auto_exit": {
                "reason":     reason,
                "exit_price": exit_price,
                "pnl":        pnl,
                "closed_at":  _now_ts,
                # Phase 3: set when mark_price was unavailable and entry_price
                # was used as a neutral fallback (max_hold path only).
                "fallback_close_no_mark_price": fallback_price,
            },
        },
    }

    try:
        resp = (
            supabase.table("copied_positions")
            .update(updates)
            .eq("id", pos_id)
            .eq("status", "OPEN")          # concurrency guard
            .execute()
        )

        if not (resp.data):
            logging.warning(
                "COPY_EXIT_ALREADY_CLOSED pos=%s reason=%s "
                "— DB update matched 0 OPEN rows; position already closed elsewhere",
                pos_id[:8], reason,
            )
            return False

        logging.warning(
            "COPY_EXIT_CLOSE_OK pos=%s slug=%s reason=%s "
            "entry=%.4f exit=%.4f size=%.4f pnl=%+.4f fallback_price=%s",
            pos_id[:8],
            pos.get("market_slug") or "?",
            reason,
            entry_price, exit_price, size, pnl, fallback_price,
        )

        if fallback_price and reason == CLOSE_REASON_MAX_HOLD:
            logging.warning(
                "COPY_MAX_HOLD_FALLBACK_CLOSE pos=%s slug=%s "
                "— position force-closed by max_hold using entry_price=%.4f "
                "as fallback exit (mark_price was unavailable). "
                "PnL recorded as %.4f (net-zero). "
                "raw_json.fallback_close_no_mark_price=true",
                pos_id[:8], pos.get("market_slug") or "?",
                entry_price, pnl,
            )

        # Best-effort write to dedicated close_reason column (Phase 2 migration)
        _try_write_close_reason_col(pos_id, reason)

        raw_json = pos.get("raw_json") or {}
        is_paper = raw_json.get("paper", True)
        if is_paper and pnl != 0.0:
            _update_copy_paper_bankroll(pnl, pos_id, close_path=f"auto_exit_{reason}")

        return True

    except Exception:
        logging.warning(
            "COPY_EXIT_CLOSE_FAIL pos=%s reason=%s exit_price=%.4f pnl=%+.4f",
            pos_id[:8], reason, exit_price, pnl,
        )
        logging.exception("COPY_EXIT_CLOSE_FAIL detail pos=%s", pos_id[:8])
        return False


async def copy_auto_exit_loop() -> None:
    """
    Auto-profit / max-hold background scanner for OPEN copied positions.

    Runs every COPY_AUTO_EXIT_LOOP_INTERVAL seconds.
    Completely isolated from copy trade ingestion, source-wallet SELL mirroring,
    and the settlement loop.

    Per-tick flow:
      1. Load all enabled copy_bots → build {copy_bot_id: bot} lookup map.
      2. Load OPEN copied_positions (up to COPY_SETTLEMENT_BATCH_SIZE).
      3. For each position:
           a. Look up its bot's exit_mode (defaults to mirror_only if column absent).
           b. Log COPY_EXIT_MODE.
           c. If exit_mode = mirror_only → skip (keep existing close path).
           d. Compute hold age (minutes since opened_at).
           e. Fetch current mark price from Gamma API.
           f. Compute profit_pct = (mark - entry) / entry * 100.
           g. Log COPY_EXIT_TP_CHECK.
           h. If exit_mode = auto_profit or auto_profit_max_hold:
                - profit_pct >= take_profit_pct → close, reason=auto_profit
           i. If exit_mode = auto_profit_max_hold:
                - hold_min >= max_hold_minutes (and TP not hit) → close, reason=max_hold
           j. If close triggered and position is live → log COPY_EXIT_SKIP_LIVE_UNSUPPORTED.
           k. Otherwise close and log COPY_EXIT_CLOSE_OK / COPY_EXIT_CLOSE_FAIL.

    Source-wallet SELL arriving after auto-exit:
      close_matching_open_positions_on_exit queries WHERE status=OPEN so it
      naturally finds 0 rows and logs SELL_MIRROR_NO_MATCH — no special
      handling needed; the existing path is already concurrency-safe.
    """
    if not COPY_TRADE_ENABLED:
        logging.info(
            "COPY_AUTO_EXIT_LOOP disabled via COPY_TRADE_ENABLED=false — exiting task"
        )
        return

    logging.warning(
        "COPY_AUTO_EXIT_LOOP_BOOT interval=%ss batch=%s "
        "— reads exit_mode/take_profit_pct/max_hold_minutes from copy_bots table",
        COPY_AUTO_EXIT_LOOP_INTERVAL,
        COPY_SETTLEMENT_BATCH_SIZE,
    )

    while True:
        try:
            # ── 1. Load bots into a lookup map ────────────────────────────────
            bots    = load_enabled_copy_bots()
            bot_map = {str(b["id"]): b for b in bots}

            # ── 2. Load OPEN positions ────────────────────────────────────────
            open_positions = load_open_copied_positions(COPY_SETTLEMENT_BATCH_SIZE)

            checked   = 0
            tp_closed = 0
            mh_closed = 0
            skipped   = 0

            for pos in open_positions:
                pos_id      = str(pos.get("id") or "")
                copy_bot_id = str(pos.get("copy_bot_id") or "")

                try:
                    # ── 3a. Look up bot ───────────────────────────────────────
                    bot = bot_map.get(copy_bot_id)
                    if bot is None:
                        # Bot disabled or deleted — leave position open
                        skipped += 1
                        continue

                    # ── 3b. Read exit settings ────────────────────────────────
                    exit_mode       = str(
                        bot.get("exit_mode") or "mirror_only"
                    ).strip().lower()
                    take_profit_pct = float_or_none(bot.get("take_profit_pct"))
                    max_hold_min    = float_or_none(bot.get("max_hold_minutes"))

                    raw_json   = pos.get("raw_json") or {}
                    is_live    = bool(raw_json.get("live", False))
                    entry_price = float_or_none(pos.get("entry_price")) or 0.0
                    pos_slug    = pos.get("market_slug") or "?"
                    pos_wallet  = str(pos.get("wallet_address") or "?")[:12]
                    opened_at_str = pos.get("opened_at") or "?"

                    # ── 3c. mirror_only gate ──────────────────────────────────
                    if exit_mode == "mirror_only":
                        logging.info(
                            "COPY_AUTO_EXIT_POSITION_EVAL pos=%s bot=%s wallet=%s "
                            "slug=%s opened_at=%s exit_mode=mirror_only "
                            "entry=%.4f is_live=%s action=skip_mirror_only",
                            pos_id[:8], copy_bot_id[:8], pos_wallet,
                            pos_slug, opened_at_str, entry_price, is_live,
                        )
                        skipped += 1
                        continue

                    # ── 3d. Hold age ──────────────────────────────────────────
                    opened_dt = _parse_ts(pos.get("opened_at"))
                    hold_min: float | None = None
                    if opened_dt:
                        hold_min = round(
                            (datetime.now(timezone.utc) - opened_dt).total_seconds() / 60,
                            1,
                        )

                    # ── 3e. Fetch current mark price ──────────────────────────
                    mark_price = await asyncio.to_thread(
                        _copy_auto_exit_fetch_mark_price_sync, pos
                    )

                    # ── 3f. Profit % ──────────────────────────────────────────
                    profit_pct: float | None = None
                    if mark_price is not None and entry_price > 0:
                        profit_pct = (mark_price - entry_price) / entry_price * 100.0

                    # ── 3g. Per-position structured diagnostic log ─────────────
                    # COPY_AUTO_EXIT_POSITION_EVAL consolidates all evaluation
                    # inputs in one searchable log line for every non-mirror position.
                    _tp_condition_met  = (
                        take_profit_pct is not None
                        and profit_pct   is not None
                        and profit_pct >= take_profit_pct
                    )
                    _mh_condition_met = (
                        max_hold_min is not None
                        and hold_min is not None
                        and hold_min >= max_hold_min
                    )
                    logging.warning(
                        "COPY_AUTO_EXIT_POSITION_EVAL "
                        "pos=%s bot=%s wallet=%s slug=%s "
                        "opened_at=%s hold_min=%s "
                        "entry=%.4f mark=%s profit_pct=%s "
                        "exit_mode=%s take_profit_pct=%s max_hold_min=%s "
                        "tp_condition=%s mh_condition=%s is_live=%s",
                        pos_id[:8], copy_bot_id[:8], pos_wallet, pos_slug,
                        opened_at_str,
                        f"{hold_min:.1f}" if hold_min is not None else "N/A",
                        entry_price,
                        f"{mark_price:.4f}" if mark_price is not None else "N/A",
                        f"{profit_pct:.1f}%" if profit_pct is not None else "N/A",
                        exit_mode, take_profit_pct, max_hold_min,
                        _tp_condition_met, _mh_condition_met, is_live,
                    )

                    # Legacy per-strategy check log kept for Railway search compatibility
                    logging.warning(
                        "COPY_EXIT_TP_CHECK pos=%s slug=%s exit_mode=%s "
                        "entry=%.4f mark=%s profit_pct=%s tp_target=%s "
                        "hold_min=%s max_hold_min=%s",
                        pos_id[:8], pos_slug, exit_mode,
                        entry_price,
                        f"{mark_price:.4f}" if mark_price is not None else "N/A",
                        f"{profit_pct:.1f}%" if profit_pct is not None else "N/A",
                        take_profit_pct,
                        f"{hold_min:.1f}" if hold_min is not None else "N/A",
                        max_hold_min,
                    )

                    checked += 1

                    # ── 3h. Take-profit check ─────────────────────────────────
                    # HARDENED (Phase 2): TP requires a real mark_price.
                    # If the Gamma API returned None, we cannot verify the
                    # profit target was reached — skip TP exit this tick.
                    # max_hold is still evaluated independently below.
                    close_reason: str | None = None

                    if exit_mode in ("auto_profit", "auto_profit_max_hold"):
                        if mark_price is None:
                            logging.warning(
                                "COPY_EXIT_TP_SKIP_NO_PRICE pos=%s slug=%s "
                                "— mark_price unavailable; TP exit skipped this tick. "
                                "Will retry next auto-exit loop cycle.",
                                pos_id[:8], pos_slug,
                            )
                        elif _tp_condition_met:
                            close_reason = CLOSE_REASON_AUTO_PROFIT
                            logging.warning(
                                "COPY_EXIT_TP_HIT pos=%s slug=%s "
                                "profit_pct=%.1f%% tp_target=%.1f%% "
                                "entry=%.4f mark=%.4f",
                                pos_id[:8], pos_slug,
                                profit_pct,
                                take_profit_pct,
                                entry_price,
                                mark_price,
                            )

                    # ── 3i. Max-hold check ────────────────────────────────────
                    # HARDENED (Phase 2): max_hold fires even when mark_price is
                    # unavailable — the position must be closed after max hold time
                    # regardless of price. Entry_price is used as the exit_price
                    # fallback (net-zero PnL) and logged clearly.
                    if exit_mode == "auto_profit_max_hold" and close_reason is None:
                        if _mh_condition_met:
                            close_reason = CLOSE_REASON_MAX_HOLD
                            if mark_price is None:
                                logging.warning(
                                    "COPY_EXIT_MAX_HOLD_HIT_NO_PRICE pos=%s slug=%s "
                                    "hold_min=%.1f max_hold_min=%.1f "
                                    "— mark_price unavailable; will exit at entry_price "
                                    "(net-zero PnL) to enforce max-hold discipline",
                                    pos_id[:8], pos_slug,
                                    hold_min, max_hold_min,
                                )
                                logging.warning(
                                    "COPY_MAX_HOLD_FALLBACK_NO_MARK pos=%s slug=%s "
                                    "hold_min=%.1f token_id=%s condition_id=%s "
                                    "— all Gamma price sources failed; "
                                    "entry_price will be used as fallback exit_price",
                                    pos_id[:8], pos_slug,
                                    hold_min,
                                    str(pos.get("token_id") or "NONE")[:16],
                                    str(pos.get("condition_id") or "NONE")[:16],
                                )
                            else:
                                logging.warning(
                                    "COPY_EXIT_MAX_HOLD_HIT pos=%s slug=%s "
                                    "hold_min=%.1f max_hold_min=%.1f "
                                    "entry=%.4f mark=%.4f",
                                    pos_id[:8], pos_slug,
                                    hold_min, max_hold_min,
                                    entry_price, mark_price,
                                )

                    if close_reason is None:
                        logging.info(
                            "COPY_AUTO_EXIT_NO_TRIGGER pos=%s slug=%s "
                            "exit_mode=%s profit_pct=%s hold_min=%s "
                            "— neither TP nor max-hold condition met",
                            pos_id[:8], pos_slug, exit_mode,
                            f"{profit_pct:.1f}%" if profit_pct is not None else "N/A",
                            f"{hold_min:.1f}" if hold_min is not None else "N/A",
                        )
                        skipped += 1
                        continue

                    # ── 3j. Live block ────────────────────────────────────────
                    if is_live:
                        logging.warning(
                            "COPY_EXIT_SKIP_LIVE_UNSUPPORTED pos=%s slug=%s reason=%s "
                            "— live auto-exit is not yet implemented; "
                            "position remains open until source wallet SELL or settlement",
                            pos_id[:8], pos_slug, close_reason,
                        )
                        skipped += 1
                        continue

                    # ── 3k. Close position (PAPER only) ──────────────────────
                    # For TP: mark_price guaranteed non-None (guard above).
                    # For max_hold: falls back to entry_price if mark_price is None.
                    exit_price = mark_price if mark_price is not None else entry_price
                    _using_fallback_price = mark_price is None
                    logging.warning(
                        "COPY_AUTO_EXIT_ACTION pos=%s slug=%s reason=%s "
                        "entry=%.4f exit=%.4f fallback_price=%s "
                        "— triggering paper close now",
                        pos_id[:8], pos_slug, close_reason,
                        entry_price, exit_price, _using_fallback_price,
                    )
                    ok = await asyncio.to_thread(
                        _copy_auto_exit_close_position_sync,
                        pos,
                        exit_price,
                        close_reason,
                        _using_fallback_price,  # Phase 3: recorded in raw_json
                    )
                    if ok:
                        if close_reason == CLOSE_REASON_AUTO_PROFIT:
                            tp_closed += 1
                        else:
                            mh_closed += 1
                    # else: already-closed case logged inside helper

                except Exception:
                    logging.exception("COPY_AUTO_EXIT_POSITION_FAIL pos=%s", pos_id)

            logging.warning(
                "COPY_AUTO_EXIT_TICK_DONE scanned=%s checked=%s "
                "tp_closed=%s mh_closed=%s skipped=%s",
                len(open_positions), checked,
                tp_closed, mh_closed, skipped,
            )

        except Exception:
            logging.exception("COPY_AUTO_EXIT_LOOP_ERROR")

        await asyncio.sleep(COPY_AUTO_EXIT_LOOP_INTERVAL)


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
        all_bots_for_audit = load_enabled_copy_bots()
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

            # Compute position age once for logging
            pos_age_min: float | None = None
            opened_dt = _parse_ts(pos.get("opened_at"))
            if opened_dt:
                pos_age_min = round(
                    (datetime.now(timezone.utc) - opened_dt).total_seconds() / 60, 1
                )

            try:
                # ── Step 1: Build market lookup key ───────────────────────
                condition_id = pos.get("condition_id")
                market_slug  = pos.get("market_slug")
                token_id     = pos.get("token_id")

                market_key = condition_id or market_slug or token_id
                if not market_key:
                    logging.info(
                        "COPY_SETTLE_SKIP pos=%s reason=no_market_key "
                        "age_min=%s slug=%s outcome=%s "
                        "(no condition_id, market_slug, or token_id on this position)",
                        pos_id[:8], pos_age_min,
                        market_slug or "—", pos.get("outcome") or "—",
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
                    # Previously logging.debug (invisible in Railway) — now INFO.
                    # This is the most common failure path; must be visible.
                    logging.info(
                        "COPY_SETTLE_SKIP pos=%s reason=gamma_api_no_data "
                        "age_min=%s slug=%s cid=%s token=%s "
                        "(Gamma API returned no market data — market may be very new, "
                        "slug may be missing, or API is temporarily unavailable)",
                        pos_id[:8], pos_age_min,
                        market_slug or "—",
                        str(condition_id or "—")[:16],
                        str(token_id or "—")[:16],
                    )
                    skipped_no_data += 1
                    continue

                # ── Step 3: Check whether market has resolved ─────────────
                if not resolution_data.get("resolved"):
                    logging.info(
                        "COPY_SETTLE_SKIP pos=%s reason=market_unresolved "
                        "age_min=%s slug=%s active=%s resolution=%s",
                        pos_id[:8], pos_age_min,
                        market_slug or "—",
                        resolution_data.get("active"),
                        resolution_data.get("resolution_outcome") or "pending",
                    )
                    skipped_unresolved += 1
                    continue

                # ── Step 4: Compute exit price ────────────────────────────
                exit_price = compute_settlement_exit_price(pos, resolution_data)
                if exit_price is None:
                    if str(pos.get("side") or "BUY").upper() == "SELL":
                        logging.info(
                            "COPY_SETTLE_SKIP pos=%s reason=sell_position_not_settled "
                            "age_min=%s slug=%s — SELL-side settlement not yet implemented",
                            pos_id[:8], pos_age_min, market_slug or "—",
                        )
                        skipped_sell += 1
                    else:
                        logging.info(
                            "COPY_SETTLE_SKIP pos=%s reason=cannot_determine_exit_price "
                            "age_min=%s slug=%s outcome=%s resolution=%s "
                            "yes_token=%s no_token=%s pos_token=%s",
                            pos_id[:8], pos_age_min,
                            market_slug or "—",
                            pos.get("outcome") or "—",
                            resolution_data.get("resolution_outcome") or "—",
                            str(resolution_data.get("yes_token_id") or "—")[:12],
                            str(resolution_data.get("no_token_id") or "—")[:12],
                            str(token_id or "—")[:12],
                        )
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

        # Per-bot position audit — runs at most once every 10 minutes regardless of
        # how frequently the settlement loop fires, to avoid DB spam.
        _audit_last_ts, _ = log_throttle_state.get("copy_bot_position_audit", (0, None))
        if int(time()) - _audit_last_ts >= 600:
            log_throttle_state["copy_bot_position_audit"] = (int(time()), None)
            log_per_bot_position_audit(all_bots_for_audit)

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

async def copy_diag_loop() -> None:
    """
    Lightweight diagnostic loop — no DB calls, no external deps.
    Fires every 10 seconds at WARNING so the copy-brain build is always visible
    in Railway logs regardless of COPY_TRADE_ENABLED or any DB connectivity.
    """
    while True:
        logging.warning(
            "SHARED_BRAIN_ACTIVE build=SHARED_BRAIN_V1 "
            "env_COPY_TRADE_ENABLED=%s env_COPY_LIVE_ENABLED=%s",
            COPY_TRADE_ENABLED,
            COPY_LIVE_ENABLED,
        )
        await asyncio.sleep(10)


# ══════════════════════════════════════════════════════════════════════════════
# LEADERBOARD WALLET DISCOVERY PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
#
# Data flow:
#   Polymarket leaderboard API (crypto / today / profit)
#     └─ _fetch_leaderboard_page_sync()    — HTTP GET per page
#     └─ _normalize_leaderboard_row()      — extract address, rank, profit
#     └─ upsert_candidate_wallet()         — write to candidate_wallets
#     └─ _enrich_candidate_wallet()        — fetch recent activity, compute score
#     └─ candidate_wallets table           — BTCBOT reads for Hot Wallet suggestions
#
# candidate_wallets is separate from tracked_wallets.
# A human or automation must promote a candidate to tracked_wallets to start
# copy trading it.  The table is purely a discovery/ranking surface.
#
# Columns written:
#   wallet_address, display_name, rank, daily_profit, daily_volume,
#   source, fetched_at,
#   recent_trade_count, trades_per_day, avg_hold_minutes,
#   exit_before_resolution_rate, recent_pnl, copy_score, enriched_at,
#   is_tracked, status, updated_at
# ══════════════════════════════════════════════════════════════════════════════


def _fetch_leaderboard_page_sync(offset: int, limit: int) -> list[dict]:
    """
    Fetch one page of the Polymarket Crypto/Today/Profit leaderboard.

    Endpoint: GET {COPY_DATA_API_BASE}/leaderboard
    Params:   timeframe, categoryType, sortBy=profit, limit, offset

    Handles both list and dict-wrapped response shapes.
    Returns [] on any error — caller skips empty pages.
    """
    url = (
        f"{COPY_DATA_API_BASE}/leaderboard"
        f"?timeframe={LEADERBOARD_TIMEFRAME}"
        f"&categoryType={LEADERBOARD_CATEGORY.upper()}"
        f"&sortBy=profit"
        f"&limit={limit}"
        f"&offset={offset}"
    )
    try:
        req = request.Request(url, headers={"Accept": "application/json", "User-Agent": "FastLoopWorker/1.0"})
        with request.urlopen(req, timeout=15) as resp:
            raw = json.loads(resp.read())
        if isinstance(raw, list):
            return raw
        # Common wrapped shapes
        for key in ("data", "results", "leaderboard", "users", "entries"):
            if isinstance(raw.get(key), list):
                return raw[key]
        logging.warning(
            "LEADERBOARD_UNKNOWN_SHAPE offset=%s keys=%s",
            offset, list(raw.keys())[:8] if isinstance(raw, dict) else type(raw),
        )
        return []
    except Exception as exc:
        logging.warning(
            "LEADERBOARD_FETCH_FAIL offset=%s url=%s err=%s",
            offset, url, exc,
        )
        return []


def _safe_float(val: object) -> "float | None":
    """Parse a value to float, return None if unparseable."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _normalize_leaderboard_row(
    row: dict,
    rank: int,
    fetched_at: str,
) -> "dict | None":
    """
    Normalise a raw leaderboard API row to the candidate_wallets schema.

    Returns None if the row lacks a usable wallet address.
    Accepts both Polymarket data-API field names and alternative spellings.
    """
    wallet = (
        row.get("proxyWallet")
        or row.get("userId")
        or row.get("user")
        or row.get("address")
        or row.get("wallet_address")
        or row.get("walletAddress")
    )
    if not wallet or not str(wallet).startswith("0x"):
        return None

    return {
        "wallet_address": str(wallet).lower(),
        "display_name": (
            row.get("name")
            or row.get("displayName")
            or row.get("username")
            or row.get("pseudonym")
        ),
        "rank": int(row.get("position") or row.get("rank") or rank),
        "daily_profit": _safe_float(
            row.get("profit") or row.get("pnl") or row.get("dailyProfit")
        ),
        "daily_volume": _safe_float(
            row.get("volume") or row.get("dailyVolume") or row.get("amountBet")
        ),
        "source":     "leaderboard_crypto_today_profit",
        "fetched_at": fetched_at,
    }


def _compute_candidate_copy_score(
    trades_per_day: float,
    avg_hold_minutes: float,
    exit_before_resolution_rate: float,
    recent_pnl: float,
    recent_trade_count: int,
) -> float:
    """
    Composite copy suitability score in the range 0–100.

    Designed for the leaderboard discovery context where we have no
    historical copy-position data — all inputs come from recent raw activity.

    Component weights:

      activity_score       (25 pts) — trades_per_day: 0 → 0, 5+ → 1.0
                                      Fast traders are more copyable; slow
                                      traders (1 trade/week) score near 0.
                                      Formula: clamp(trades_per_day / 5, 0, 1)

      hold_score           (25 pts) — avg_hold_minutes: rewards short holds.
                                      <30 min → 1.0, 60 min → 0.5, 240+ → 0.
                                      Formula: clamp(1 - avg_hold_minutes/240, 0, 1)
                                      Rationale: copy value decays if we can only
                                      fill AFTER the source has already exited.

      exit_before_res      (25 pts) — fraction exiting before market resolves.
                                      1.0 → full score, 0 → 0.
                                      Rationale: wallets that hold to resolution
                                      give copy traders no exit signal.

      pnl_score            (15 pts) — recent_pnl normalised: -$50 → 0, $100 → 1.
                                      Formula: clamp((recent_pnl+50)/150, 0, 1)

      volume_score         (10 pts) — recent_trade_count: 0 → 0, 20+ → 1.0.
                                      Formula: clamp(recent_trade_count/20, 0, 1)
                                      Sanity check: low-count wallets get capped.
    """
    def _clamp(v: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, v))

    activity_score  = _clamp(trades_per_day / 5.0,              0.0, 1.0)
    hold_score      = _clamp(1.0 - avg_hold_minutes / 240.0,    0.0, 1.0)
    exit_score      = _clamp(exit_before_resolution_rate,        0.0, 1.0)
    pnl_score       = _clamp((recent_pnl + 50.0) / 150.0,       0.0, 1.0)
    volume_score    = _clamp(recent_trade_count / 20.0,          0.0, 1.0)

    raw = (
        activity_score * 25.0
        + hold_score   * 25.0
        + exit_score   * 25.0
        + pnl_score    * 15.0
        + volume_score * 10.0
    )
    return round(raw, 2)


def _enrich_candidate_wallet(
    wallet_address: str,
    activities: list[dict],
    fetched_at: str,
) -> dict:
    """
    Compute enrichment metrics for a candidate from its recent activity.

    Activities come from the same Polymarket data API used by copy_trade_loop.
    No DB queries here — enrichment is pure computation on the raw activity list.

    Returns a dict of enrichment columns ready to merge into a candidate row.

    Metrics computed:
      recent_trade_count          — len(activities)
      trades_per_day              — based on span between first and last trade
      avg_hold_minutes            — average gap between consecutive BUY and SELL
                                    on the same market/token within the window
      exit_before_resolution_rate — fraction of closed trades where the SELL
                                    happened before we see a resolution event
                                    (approximated: SELL price between 0.02–0.98
                                    suggests pre-resolution exit)
      recent_pnl                  — sum of (price × size) for SELLs minus BUYs
                                    (notional, not realised PnL)
      copy_score                  — _compute_candidate_copy_score(...)
    """
    now_utc = datetime.now(timezone.utc)
    enriched_at = now_utc.isoformat()

    recent_trade_count = len(activities)
    if not activities:
        return {
            "recent_trade_count": 0,
            "trades_per_day": 0.0,
            "avg_hold_minutes": 0.0,
            "exit_before_resolution_rate": 0.0,
            "recent_pnl": 0.0,
            "copy_score": 0.0,
            "enriched_at": enriched_at,
        }

    # ── Parse timestamps for span computation ─────────────────────────────────
    trade_times: list[datetime] = []
    for act in activities:
        ts_raw = (
            act.get("timestamp") or act.get("match_time")
            or act.get("created_at") or act.get("createdAt")
        )
        if ts_raw:
            try:
                if isinstance(ts_raw, (int, float)):
                    ts_val = ts_raw / 1000 if ts_raw > 1e12 else ts_raw
                    trade_times.append(
                        datetime.fromtimestamp(ts_val, tz=timezone.utc)
                    )
                else:
                    trade_times.append(
                        datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
                    )
            except Exception:
                pass

    if trade_times:
        span_days = (
            (max(trade_times) - min(trade_times)).total_seconds() / 86400
        )
        trades_per_day = round(
            recent_trade_count / max(span_days, 1.0), 2
        )
    else:
        trades_per_day = 0.0

    # ── avg_hold_minutes via BUY→SELL pairing per market ─────────────────────
    # Group trades by token_id / market identifier, compute time between
    # consecutive BUY and SELL on the same market.
    from collections import defaultdict
    buys_by_market: dict[str, list[datetime]] = defaultdict(list)
    hold_durations: list[float] = []
    pre_resolution_exits = 0
    total_exits = 0

    for act in activities:
        side_raw = str(act.get("side") or "").strip().upper()
        market_key = (
            act.get("tokenId") or act.get("token_id") or act.get("asset_id")
            or act.get("conditionId") or act.get("condition_id")
            or act.get("market") or ""
        )
        price_raw = act.get("price") or act.get("avgPrice") or act.get("avg_price")
        try:
            price_f = float(price_raw) if price_raw is not None else None
        except (TypeError, ValueError):
            price_f = None
        size_raw = act.get("shares") or act.get("size") or act.get("quantity")
        try:
            size_f = float(size_raw) if size_raw is not None else None
        except (TypeError, ValueError):
            size_f = None

        ts_raw = (
            act.get("timestamp") or act.get("match_time")
            or act.get("created_at") or act.get("createdAt")
        )
        act_dt: "datetime | None" = None
        if ts_raw:
            try:
                if isinstance(ts_raw, (int, float)):
                    ts_val = ts_raw / 1000 if ts_raw > 1e12 else ts_raw
                    act_dt = datetime.fromtimestamp(ts_val, tz=timezone.utc)
                else:
                    act_dt = datetime.fromisoformat(
                        str(ts_raw).replace("Z", "+00:00")
                    )
            except Exception:
                pass

        if side_raw in ("BUY", "ENTER", "LONG") and market_key and act_dt:
            buys_by_market[market_key].append(act_dt)

        elif side_raw in ("SELL", "EXIT", "SHORT") and market_key and act_dt:
            total_exits += 1
            # Pre-resolution exit: price is between 0.02 and 0.98
            if price_f is not None and 0.02 < price_f < 0.98:
                pre_resolution_exits += 1
            # Match against an open BUY for hold duration
            if buys_by_market[market_key]:
                buy_dt = buys_by_market[market_key].pop(0)
                if act_dt > buy_dt:
                    hold_durations.append(
                        (act_dt - buy_dt).total_seconds() / 60.0
                    )

    avg_hold_minutes = (
        round(sum(hold_durations) / len(hold_durations), 2)
        if hold_durations else 0.0
    )
    exit_before_resolution_rate = (
        round(pre_resolution_exits / total_exits, 4) if total_exits > 0 else 0.0
    )

    # ── recent_pnl (notional) ─────────────────────────────────────────────────
    # Sum of SELL notionals minus BUY notionals = crude net flow proxy.
    # Positive = net seller (realised value); negative = net buyer (deployed).
    recent_pnl = 0.0
    for act in activities:
        side_raw = str(act.get("side") or "").strip().upper()
        price_raw = act.get("price") or act.get("avgPrice") or act.get("avg_price")
        size_raw  = act.get("shares") or act.get("size") or act.get("quantity")
        try:
            p = float(price_raw) if price_raw is not None else 0.0
        except (TypeError, ValueError):
            p = 0.0
        try:
            s = float(size_raw) if size_raw is not None else 0.0
        except (TypeError, ValueError):
            s = 0.0
        notional = p * s
        if side_raw in ("SELL", "EXIT", "SHORT"):
            recent_pnl += notional
        elif side_raw in ("BUY", "ENTER", "LONG"):
            recent_pnl -= notional

    recent_pnl = round(recent_pnl, 4)

    # ── copy_score ────────────────────────────────────────────────────────────
    copy_score = _compute_candidate_copy_score(
        trades_per_day=trades_per_day,
        avg_hold_minutes=avg_hold_minutes,
        exit_before_resolution_rate=exit_before_resolution_rate,
        recent_pnl=recent_pnl,
        recent_trade_count=recent_trade_count,
    )

    return {
        "recent_trade_count":          recent_trade_count,
        "trades_per_day":              trades_per_day,
        "avg_hold_minutes":            avg_hold_minutes,
        "exit_before_resolution_rate": exit_before_resolution_rate,
        "recent_pnl":                  recent_pnl,
        "copy_score":                  copy_score,
        "enriched_at":                 enriched_at,
    }


def _load_tracked_wallet_addresses() -> set[str]:
    """Return the set of lower-cased wallet addresses already in tracked_wallets."""
    try:
        resp = (
            supabase.table("tracked_wallets")
            .select("wallet_address")
            .execute()
        )
        return {str(r["wallet_address"]).lower() for r in (resp.data or [])}
    except Exception:
        logging.exception("LEADERBOARD_LOAD_TRACKED_WALLETS_FAIL")
        return set()


def _upsert_candidate_wallet(candidate: dict) -> bool:
    """
    Upsert a single row into candidate_wallets.
    Returns True on success, False on failure.
    Conflict target: wallet_address.
    """
    try:
        candidate["updated_at"] = utc_now_iso()
        supabase.table("candidate_wallets").upsert(
            candidate, on_conflict="wallet_address"
        ).execute()
        return True
    except Exception:
        logging.exception(
            "LEADERBOARD_UPSERT_FAIL wallet=%s",
            str(candidate.get("wallet_address", "?"))[:12],
        )
        return False


async def leaderboard_ingest_loop() -> None:
    """
    Periodically scrape the Polymarket Crypto/Today/Profit leaderboard and
    write new candidates to candidate_wallets for Hot Wallet discovery.

    Loop cadence: LEADERBOARD_INGEST_INTERVAL seconds (default 3600 = 1 hour).
    Pages: up to LEADERBOARD_MAX_PAGES × LEADERBOARD_PAGE_SIZE rows per scan.

    For each wallet found:
      1. Normalise fields from the raw API row.
      2. Mark is_tracked = True if address is already in tracked_wallets.
      3. Upsert into candidate_wallets (updates leaderboard snapshot).
      4. Fetch recent activity and compute enrichment + copy_score.
      5. Upsert enrichment fields back into candidate_wallets.

    Logs (all prefixed LEADERBOARD_*):
      LEADERBOARD_INGEST_BOOT   — on startup, shows effective config
      LEADERBOARD_SCAN_START    — each scan begins
      LEADERBOARD_PAGE_FETCHED  — per page: count, offset
      LEADERBOARD_PAGE_EMPTY    — page returned 0 rows → stop paginating
      LEADERBOARD_SCAN_SUMMARY  — total discovered / inserted / skipped
      LEADERBOARD_ENRICH_START  — enrichment pass begins
      LEADERBOARD_ENRICH_DONE   — per wallet: score, hold time, trades/day
      LEADERBOARD_ENRICH_FAIL   — enrichment fetch failed for wallet
      LEADERBOARD_HOT_CANDIDATE — wallet scored above LEADERBOARD_MIN_COPY_SCORE
      LEADERBOARD_SCAN_DONE     — scan complete, sleeping until next interval
    """
    if not LEADERBOARD_INGEST_ENABLED:
        logging.warning(
            "LEADERBOARD_INGEST_DISABLED — set LEADERBOARD_INGEST_ENABLED=true "
            "to enable wallet discovery from Polymarket leaderboard"
        )
        while True:
            await asyncio.sleep(3600)
        return  # unreachable

    logging.warning(
        "LEADERBOARD_INGEST_BOOT interval=%ss max_pages=%s page_size=%s "
        "category=%s timeframe=%s min_copy_score=%.1f enrich_limit=%s",
        LEADERBOARD_INGEST_INTERVAL,
        LEADERBOARD_MAX_PAGES,
        LEADERBOARD_PAGE_SIZE,
        LEADERBOARD_CATEGORY,
        LEADERBOARD_TIMEFRAME,
        LEADERBOARD_MIN_COPY_SCORE,
        LEADERBOARD_ENRICH_LIMIT,
    )

    while True:
        try:
            await _run_leaderboard_scan()
        except Exception:
            logging.exception("LEADERBOARD_SCAN_UNHANDLED_ERROR")

        await asyncio.sleep(LEADERBOARD_INGEST_INTERVAL)


async def _run_leaderboard_scan() -> None:
    """Execute one full leaderboard scan + enrichment pass."""
    scan_start = datetime.now(timezone.utc)
    fetched_at = scan_start.isoformat()

    logging.warning(
        "LEADERBOARD_SCAN_START category=%s timeframe=%s max_pages=%s page_size=%s",
        LEADERBOARD_CATEGORY, LEADERBOARD_TIMEFRAME,
        LEADERBOARD_MAX_PAGES, LEADERBOARD_PAGE_SIZE,
    )

    # ── Step 1: Load currently tracked wallet addresses for dedup ─────────────
    tracked_addresses = await asyncio.to_thread(_load_tracked_wallet_addresses)

    # ── Step 2: Paginate through leaderboard ──────────────────────────────────
    all_candidates: list[dict] = []
    global_rank = 0

    for page_num in range(LEADERBOARD_MAX_PAGES):
        offset = page_num * LEADERBOARD_PAGE_SIZE
        rows = await asyncio.to_thread(
            _fetch_leaderboard_page_sync, offset, LEADERBOARD_PAGE_SIZE
        )

        if not rows:
            logging.info(
                "LEADERBOARD_PAGE_EMPTY page=%s offset=%s — stopping pagination",
                page_num + 1, offset,
            )
            break

        logging.info(
            "LEADERBOARD_PAGE_FETCHED page=%s offset=%s rows=%s",
            page_num + 1, offset, len(rows),
        )

        for row in rows:
            global_rank += 1
            candidate = _normalize_leaderboard_row(row, global_rank, fetched_at)
            if not candidate:
                continue
            candidate["is_tracked"] = (
                candidate["wallet_address"] in tracked_addresses
            )
            candidate["status"] = (
                "tracked" if candidate["is_tracked"] else "candidate"
            )
            all_candidates.append(candidate)

        # If page was shorter than a full page, we've hit the end.
        if len(rows) < LEADERBOARD_PAGE_SIZE:
            logging.info(
                "LEADERBOARD_PAGE_PARTIAL page=%s rows=%s < page_size=%s "
                "— end of leaderboard reached",
                page_num + 1, len(rows), LEADERBOARD_PAGE_SIZE,
            )
            break

    # ── Step 3: Upsert snapshot rows ──────────────────────────────────────────
    inserted = skipped_tracked = upsert_failed = 0
    new_candidate_wallets: list[str] = []   # wallets needing enrichment

    for candidate in all_candidates:
        ok = await asyncio.to_thread(_upsert_candidate_wallet, candidate)
        if not ok:
            upsert_failed += 1
            continue
        if candidate["is_tracked"]:
            skipped_tracked += 1
            logging.info(
                "LEADERBOARD_ALREADY_TRACKED wallet=%s rank=%s — skipped "
                "(already in tracked_wallets)",
                candidate["wallet_address"][:12], candidate.get("rank"),
            )
        else:
            inserted += 1
            new_candidate_wallets.append(candidate["wallet_address"])

    logging.warning(
        "LEADERBOARD_SCAN_SUMMARY pages_fetched=%s wallets_discovered=%s "
        "inserted_or_updated=%s already_tracked=%s upsert_failed=%s",
        min(LEADERBOARD_MAX_PAGES, (global_rank // LEADERBOARD_PAGE_SIZE) + 1),
        len(all_candidates),
        inserted,
        skipped_tracked,
        upsert_failed,
    )

    # ── Step 4: Enrich new/updated candidates ─────────────────────────────────
    if not new_candidate_wallets:
        logging.info("LEADERBOARD_ENRICH_SKIP — no new candidates to enrich")
    else:
        logging.warning(
            "LEADERBOARD_ENRICH_START candidates=%s fetch_limit=%s",
            len(new_candidate_wallets), LEADERBOARD_ENRICH_LIMIT,
        )
        enrich_ok = enrich_failed = hot_count = 0

        for wallet_address in new_candidate_wallets:
            try:
                activities = await asyncio.to_thread(
                    _fetch_wallet_activity_sync,
                    wallet_address,
                    LEADERBOARD_ENRICH_LIMIT,
                )
                enrichment = _enrich_candidate_wallet(
                    wallet_address, activities, fetched_at
                )
                ok = await asyncio.to_thread(
                    _upsert_candidate_wallet,
                    {"wallet_address": wallet_address, **enrichment},
                )
                if ok:
                    enrich_ok += 1
                    score = enrichment.get("copy_score", 0.0)
                    logging.info(
                        "LEADERBOARD_ENRICH_DONE wallet=%s "
                        "copy_score=%.1f trades_per_day=%.2f "
                        "avg_hold_min=%.1f exit_before_res=%.2f recent_pnl=%.2f",
                        wallet_address[:12],
                        score,
                        enrichment.get("trades_per_day", 0.0),
                        enrichment.get("avg_hold_minutes", 0.0),
                        enrichment.get("exit_before_resolution_rate", 0.0),
                        enrichment.get("recent_pnl", 0.0),
                    )
                    if score >= LEADERBOARD_MIN_COPY_SCORE:
                        hot_count += 1
                        logging.warning(
                            "LEADERBOARD_HOT_CANDIDATE wallet=%s "
                            "copy_score=%.1f trades_per_day=%.2f "
                            "avg_hold_min=%.1f exit_before_res=%.2f "
                            "— above min_copy_score=%.1f; "
                            "consider adding to tracked_wallets",
                            wallet_address[:12],
                            score,
                            enrichment.get("trades_per_day", 0.0),
                            enrichment.get("avg_hold_minutes", 0.0),
                            enrichment.get("exit_before_resolution_rate", 0.0),
                            LEADERBOARD_MIN_COPY_SCORE,
                        )
                else:
                    enrich_failed += 1

            except Exception:
                enrich_failed += 1
                logging.exception(
                    "LEADERBOARD_ENRICH_FAIL wallet=%s", wallet_address[:12]
                )

        logging.warning(
            "LEADERBOARD_ENRICH_SUMMARY enriched=%s failed=%s "
            "hot_candidates=%s min_score=%.1f",
            enrich_ok, enrich_failed, hot_count, LEADERBOARD_MIN_COPY_SCORE,
        )

    elapsed = (datetime.now(timezone.utc) - scan_start).total_seconds()
    logging.warning(
        "LEADERBOARD_SCAN_DONE elapsed_s=%.1f next_scan_in=%ss",
        elapsed, LEADERBOARD_INGEST_INTERVAL,
    )


# ─────────────────────────────────────────────────────────────────────────────
# ── EMA_5M_BTC  —  5-minute BTC EMA paper strategy ───────────────────────────
#
# Isolated from all copy-trading and existing BTC strategy logic.
# Does NOT modify heartbeat_loop, copy_trade_loop, wallet logic, or any
# existing strategy.  Only reads from Binance + Gamma, writes to paper_positions.
#
# Data flow:
#   Binance 5m klines (REST, no auth)
#     → compute EMA 9 + EMA 200 from closed candle closes
#     → YES if close > EMA9 and close > EMA200
#     → NO  if close < EMA9 and close < EMA200
#     → NONE otherwise (no trade)
#   Gamma API → YES/NO market prices for current btc-updown-5m-{ts} slug
#   paper_positions (Supabase) → OPEN row if signal is YES or NO
#
# Duplicate prevention: has_open_paper_position_for_strategy() called before
# every insert; one open position per (slug, strategy_id, bot_id) at most.
# ─────────────────────────────────────────────────────────────────────────────

_BINANCE_5M_URL = (
    "https://api.binance.com/api/v3/klines"
    "?symbol=BTCUSDT&interval=5m&limit=250"
)


def _ema5m_fetch_closes_sync() -> list[float] | None:
    """
    Fetch BTC/USDT 5-minute klines from the Binance public REST API.

    Returns a list of closing prices, oldest first, newest last.
    The last row returned by Binance is the currently-forming (not yet closed)
    candle and is always excluded — only fully closed candles are returned.

    Returns None on any network or parse error.
    """
    try:
        req = request.Request(_BINANCE_5M_URL, headers={"User-Agent": "FastLoopWorker/1.0"})
        with request.urlopen(req, timeout=10) as resp:
            rows = json.loads(resp.read())
        # Binance kline row: [open_time, open, high, low, CLOSE, vol, close_time, ...]
        # rows[-1] is the current open (unfinalised) candle — drop it.
        closes = [float(row[4]) for row in rows[:-1]]
        return closes
    except Exception:
        logging.exception("EMA_FETCH_CANDLES_FAIL")
        return None


def _ema5m_compute(closes: list[float], period: int) -> float | None:
    """
    Standard exponential moving average.

    Seeds from the simple average of the first `period` values, then applies
    the standard multiplier k = 2 / (period + 1) to each subsequent close.
    Returns None when fewer data points exist than the period requires.
    """
    if len(closes) < period:
        return None
    k = 2.0 / (period + 1)
    ema = sum(closes[:period]) / period          # SMA seed
    for price in closes[period:]:
        ema = price * k + ema * (1.0 - k)
    return round(ema, 4)


def _ema5m_signal(closes: list[float]) -> tuple[str, float | None, float | None]:
    """
    Compute EMA9 and EMA200 and derive the trading signal.

    Returns (signal, ema9, ema200) where signal ∈ {"YES", "NO", "NONE"}.

    Rules:
      YES  — last close > EMA9  AND  last close > EMA200  (BTC trending up)
      NO   — last close < EMA9  AND  last close < EMA200  (BTC trending down)
      NONE — price is between the two EMAs, or data insufficient
    """
    ema9   = _ema5m_compute(closes, 9)
    ema200 = _ema5m_compute(closes, 200)
    if ema9 is None or ema200 is None or not closes:
        return "NONE", ema9, ema200
    price = closes[-1]
    if price > ema9 and price > ema200:
        return "YES", ema9, ema200
    if price < ema9 and price < ema200:
        return "NO", ema9, ema200
    return "NONE", ema9, ema200


def _ema5m_fetch_market_prices_sync(slug: str) -> tuple[float, float]:
    """
    Fetch current YES / NO token prices from the Gamma API for a given slug.

    Returns (yes_price, no_price).  Defaults to (0.50, 0.50) on any failure
    so the paper position is still created with a sensible entry price.
    """
    try:
        url = f"{GAMMA_API_BASE}/events/slug/{slug}"
        req = request.Request(url, headers={"User-Agent": "FastLoopWorker/1.0"})
        try:
            with request.urlopen(req, timeout=8) as resp:
                event = json.loads(resp.read())
        except HTTPError as exc:
            if exc.code == 404:
                return 0.50, 0.50
            raise
        markets = event.get("markets") or []
        yes_price = no_price = 0.50
        for m in markets:
            outcome = str(m.get("outcome") or "").upper()
            try:
                # outcomePrices is a list — index 0 = YES probability
                op = m.get("outcomePrices") or []
                lp = m.get("lastPrice")
                if outcome == "YES":
                    yes_price = float(op[0]) if op else float(lp or 0.50)
                elif outcome == "NO":
                    no_price  = float(op[0]) if op else float(lp or 0.50)
            except (TypeError, ValueError, IndexError):
                pass
        return yes_price, no_price
    except Exception:
        logging.exception("EMA_MARKET_PRICE_FAIL slug=%s", slug)
        return 0.50, 0.50


def _ema5m_get_paper_exposure_sync() -> tuple[int, float]:
    """
    Return (open_position_count, total_exposure_usd) for OPEN btc_5m_ema paper positions.

    Only queries paper_positions WHERE bot_id = EMA_5M_BOT_ID AND status = 'OPEN'.
    Completely isolated from copy-trading — does not read copied_positions or any
    copy-trading table.

    Returns (0, 0.0) on any DB error so a transient failure does not permanently
    block new entries (fail-open for exposure query, cap still enforced on projected).
    """
    try:
        resp = (
            supabase.table("paper_positions")
            .select("size_usd")
            .eq("bot_id", EMA_5M_BOT_ID)
            .eq("status", "OPEN")
            .execute()
        )
        rows  = resp.data or []
        count = len(rows)
        total = round(sum(float(r.get("size_usd") or 0.0) for r in rows), 4)
        return count, total
    except Exception:
        logging.exception("EMA_EXPOSURE_QUERY_FAIL bot_id=%s", EMA_5M_BOT_ID)
        return 0, 0.0


def _ema5m_apply_realized_pnl_sync(
    pnl_usd: float,
    pos_id: str,
    slug: str,
) -> None:
    """
    Apply realized PnL from a closed btc_5m_ema paper position to bot_settings.

    Wraps update_bot_settings_with_realized_pnl with EMA-specific WARNING logs:
      EMA_BALANCE_BEFORE  — balance and cumulative PnL before this close
      EMA_POSITION_CLOSE_PNL — the PnL delta for this specific position
      EMA_BALANCE_AFTER   — balance and cumulative PnL after this close

    Completely isolated from copy trading — only touches bot_id='btc_5m_ema'.
    """
    short_id = pos_id[:8] if pos_id else "?"

    # Read current balance so we can log BEFORE state
    try:
        resp = (
            supabase.table("bot_settings")
            .select("paper_balance_usd, paper_pnl_usd")
            .eq("bot_id", EMA_5M_BOT_ID)
            .limit(1)
            .execute()
        )
        row            = (resp.data or [None])[0]
        balance_before = float(row.get("paper_balance_usd") or 0.0) if row else 0.0
        pnl_before     = float(row.get("paper_pnl_usd")     or 0.0) if row else 0.0
    except Exception:
        balance_before = 0.0
        pnl_before     = 0.0

    logging.warning(
        "EMA_BALANCE_BEFORE bot_id=%s pos=%s slug=%s "
        "paper_balance_usd=%.4f paper_pnl_usd=%.4f",
        EMA_5M_BOT_ID, short_id, slug,
        balance_before, pnl_before,
    )
    logging.warning(
        "EMA_POSITION_CLOSE_PNL bot_id=%s pos=%s slug=%s pnl_usd=%+.4f",
        EMA_5M_BOT_ID, short_id, slug, pnl_usd,
    )

    # Apply the delta via the shared helper (handles insert-if-row-missing)
    update_bot_settings_with_realized_pnl(EMA_5M_BOT_ID, pnl_usd)

    logging.warning(
        "EMA_BALANCE_AFTER bot_id=%s pos=%s slug=%s "
        "pnl_delta=%+.4f paper_balance_usd=%.4f paper_pnl_usd=%.4f",
        EMA_5M_BOT_ID, short_id, slug,
        pnl_usd,
        balance_before + pnl_usd,
        pnl_before     + pnl_usd,
    )


def _ema5m_upsert_telemetry_sync(
    slug: str,
    signal: str,
    ema9: float | None,
    ema200: float | None,
    last_close: float | None,
    open_exposure_usd: float | None = None,
    open_position_count: int | None = None,
) -> None:
    """
    Write EMA signal telemetry to bot_settings.strategy_settings for bot_id=EMA_5M_BOT_ID.

    Called on EVERY tick — even when the strategy is disabled or signal is NONE —
    so the BTCBOT card always shows the latest EMA values without any trade being placed.

    JSON shape written to strategy_settings (all fields the BTCBOT card uses):
      {
        "ema9":                    <float | null>,
        "ema200":                  <float | null>,
        "last_close":              <float | null>,
        "signal":                  "YES" | "NO" | "NONE",
        "updated_at":              "<iso-timestamp>",
        "market_slug":             "<current slug>",
        "open_exposure_usd":       <float>,    live open notional (btc_5m_ema only)
        "open_position_count":     <int>,      live open count
        "realized_pnl_usd":        <float>,    cumulative P/L from paper_pnl_usd column
        "paper_balance_usd_snapshot": <float>, snapshot of current paper balance
        "paper_max_exposure_usd":  100.0,      (preserved from existing row)
        "live_max_exposure_usd":   0.0         (preserved from existing row)
      }

    Uses read-then-update-or-insert pattern.

    Auto-create safe defaults (row does not yet exist):
      is_enabled              = false   ← SAFE: must be explicitly enabled via card
      mode                    = PAPER
      arm_live                = false
      trade_size_usd          = 10.0
      paper_balance_usd       = 100.0
      paper_max_exposure_usd  = 100.0  (in strategy_settings JSON)
      live_max_exposure_usd   = 0.0    (in strategy_settings JSON)
    """
    # Base telemetry — always updated
    telemetry: dict = {
        "ema9":        ema9,
        "ema200":      ema200,
        "last_close":  last_close,
        "signal":      signal,
        "updated_at":  utc_now_iso(),
        "market_slug": slug,
    }
    # Exposure metrics from this tick (optional — only present when loop fetched them)
    if open_exposure_usd is not None:
        telemetry["open_exposure_usd"] = open_exposure_usd
    if open_position_count is not None:
        telemetry["open_position_count"] = open_position_count

    try:
        # Read the existing row — we need strategy_settings (to preserve operator caps)
        # and the accounting columns (paper_pnl_usd, paper_balance_usd) for the snapshot.
        existing_resp = (
            supabase.table("bot_settings")
            .select("strategy_settings, paper_pnl_usd, paper_balance_usd")
            .eq("bot_id", EMA_5M_BOT_ID)
            .limit(1)
            .execute()
        )
        existing_row = (existing_resp.data or [None])[0]

        if existing_row is None:
            # ── Row does not exist — create with safe defaults ────────────────
            new_ss = {
                **telemetry,
                "realized_pnl_usd":           0.0,
                "paper_balance_usd_snapshot":  100.0,
                "paper_max_exposure_usd":      100.0,
                "live_max_exposure_usd":       0.0,
            }
            supabase.table("bot_settings").insert({
                "bot_id":            EMA_5M_BOT_ID,
                "is_enabled":        False,
                "mode":              "PAPER",
                "arm_live":          False,
                "trade_size_usd":    10.0,
                "paper_balance_usd": 100.0,
                "strategy_settings": new_ss,
            }).execute()
            logging.warning(
                "EMA_STRATEGY_STATE_WRITE bot_id=%s signal=%s "
                "ema9=%s ema200=%s last_close=%s slug=%s action=row_created_with_safe_defaults",
                EMA_5M_BOT_ID, signal, ema9, ema200, last_close, slug,
            )
            return

        # ── Row exists — enrich telemetry with live accounting snapshot ───────
        realized_pnl = float_or_none(existing_row.get("paper_pnl_usd"))
        balance_snap = float_or_none(existing_row.get("paper_balance_usd"))
        if realized_pnl is not None:
            telemetry["realized_pnl_usd"] = realized_pnl
        if balance_snap is not None:
            telemetry["paper_balance_usd_snapshot"] = balance_snap

        # Merge: operator-set caps/values survive, telemetry fields are overwritten.
        raw_ss = existing_row.get("strategy_settings") or {}
        if isinstance(raw_ss, str):
            try:
                raw_ss = json.loads(raw_ss)
            except json.JSONDecodeError:
                raw_ss = {}
        merged_ss = {**raw_ss, **telemetry}

        supabase.table("bot_settings").update(
            {"strategy_settings": merged_ss, "updated_at": utc_now_iso()}
        ).eq("bot_id", EMA_5M_BOT_ID).execute()

        logging.warning(
            "EMA_STRATEGY_STATE_WRITE bot_id=%s signal=%s "
            "ema9=%s ema200=%s last_close=%s slug=%s "
            "open_exposure_usd=%s realized_pnl_usd=%s paper_balance_usd=%s",
            EMA_5M_BOT_ID, signal, ema9, ema200, last_close, slug,
            open_exposure_usd, realized_pnl, balance_snap,
        )
    except Exception:
        logging.warning(
            "EMA_STRATEGY_STATE_WRITE_FAIL bot_id=%s slug=%s signal=%s",
            EMA_5M_BOT_ID, slug, signal,
        )
        logging.exception(
            "EMA_STRATEGY_STATE_WRITE_FAIL detail bot_id=%s slug=%s",
            EMA_5M_BOT_ID, slug,
        )


async def ema_5m_btc_loop() -> None:
    """
    EMA_5M_BTC paper strategy main loop.

    Wired to bot_settings row  bot_id = EMA_5M_BOT_ID ("btc_5m_ema").

    Per-tick flow:
      1.  Read  bot_settings for EMA_5M_BOT_ID (is_enabled, mode, trade_size_usd, arm_live).
      2.  Compute current btc-updown-5m-{ts} slug.
      3.  Fetch 249 closed BTC/USDT 5m candles from Binance.
      4.  Compute EMA 9 and EMA 200 from closing prices.
      5.  Derive signal: YES / NO / NONE.
      6.  Write telemetry to bot_settings.strategy_settings (ALWAYS, even if disabled).
      7.  Skip trade entry if is_enabled=False  → log EMA_STRATEGY_DISABLED.
      8.  Skip trade entry if signal == NONE.
      9.  Skip trade entry if entry cutoff reached.
      10. Skip trade entry if mode == LIVE  → log EMA_STRATEGY_LIVE_BLOCKED.
      11. Skip trade entry if position already open for this slug  → log EMA_STRATEGY_SKIP.
      12. Fetch YES/NO prices from Gamma, insert paper_positions row.

    All state is in the DB — loop is stateless between restarts.
    """
    if not EMA_5M_ENABLED:
        logging.warning(
            "EMA_5M_BTC_DISABLED env — set EMA_5M_ENABLED=true to activate; "
            "sleeping indefinitely"
        )
        while True:
            await asyncio.sleep(3600)
        return  # unreachable; satisfies type checkers

    logging.warning(
        "EMA_5M_BTC_BOOT bot_id=%s strategy_id=%s slug_prefix=%s "
        "loop_interval=%ss entry_cutoff=%ss "
        "— runtime settings read from bot_settings each tick",
        EMA_5M_BOT_ID,
        EMA_5M_STRATEGY_ID,
        EMA_5M_SLUG_PREFIX,
        EMA_5M_LOOP_INTERVAL,
        EMA_5M_ENTRY_CUTOFF_SECONDS,
    )

    while True:
        try:
            # ── 1. Read settings from bot_settings ───────────────────────────
            # read_strategy_settings fetches is_enabled, mode, trade_size_usd,
            # arm_live, paper_balance_usd, AND strategy_settings JSON (which
            # holds the EMA-specific exposure caps).
            settings      = read_strategy_settings(EMA_5M_BOT_ID)
            is_enabled    = bool(settings.get("is_enabled", False))
            mode          = str(settings.get("mode") or "PAPER").upper()
            trade_size    = float(settings.get("trade_size_usd") or EMA_5M_TRADE_SIZE_USD)
            arm_live      = bool(settings.get("arm_live", False))

            # Exposure caps live in strategy_settings JSON so they require no
            # schema change and are completely isolated from copy-trading caps.
            _inner_ss          = settings.get("strategy_settings") or {}
            paper_max_exposure = float(_inner_ss.get("paper_max_exposure_usd") or 100.0)
            live_max_exposure  = float(_inner_ss.get("live_max_exposure_usd")  or 0.0)

            logging.warning(
                "EMA_STRATEGY_SETTINGS_LOADED bot_id=%s is_enabled=%s "
                "mode=%s trade_size_usd=%.2f arm_live=%s "
                "paper_max_exposure_usd=%.2f live_max_exposure_usd=%.2f",
                EMA_5M_BOT_ID, is_enabled, mode, trade_size, arm_live,
                paper_max_exposure, live_max_exposure,
            )

            # ── 2. Compute current market slug ────────────────────────────────
            now       = int(time())
            period    = 300                          # 5-minute window in seconds
            start_ts  = (now // period) * period
            end_ts    = start_ts + period
            remaining = end_ts - now
            slug      = f"{EMA_5M_SLUG_PREFIX}-{start_ts}"

            # ── 3. Fetch closed candles (blocking I/O in thread) ─────────────
            closes = await asyncio.to_thread(_ema5m_fetch_closes_sync)
            if not closes or len(closes) < 201:
                logging.warning(
                    "EMA_STRATEGY_SKIP slug=%s reason=insufficient_candles "
                    "candles=%s need=201",
                    slug, len(closes) if closes else 0,
                )
                await asyncio.sleep(EMA_5M_LOOP_INTERVAL)
                continue

            # ── 4 + 5. Compute EMA9, EMA200 and derive signal ────────────────
            signal, ema9, ema200 = _ema5m_signal(closes)
            btc_price = closes[-1]

            logging.warning(
                "EMA_STRATEGY_BAR slug=%s candles=%s btc_price=%.2f "
                "ema9=%.2f ema200=%.2f signal=%s",
                slug, len(closes), btc_price,
                ema9 or 0.0, ema200 or 0.0, signal,
            )
            logging.warning(
                "EMA_STRATEGY_SIGNAL slug=%s signal=%s "
                "btc_price=%.2f ema9=%.2f ema200=%.2f",
                slug, signal, btc_price, ema9 or 0.0, ema200 or 0.0,
            )

            # ── 5.5. Fetch open exposure (one query per tick, reused for both ─────
            #        telemetry and the cap gate at step 11b)
            open_pos_count, current_exposure = await asyncio.to_thread(
                _ema5m_get_paper_exposure_sync
            )
            logging.warning(
                "EMA_EXPOSURE_SUMMARY bot_id=%s slug=%s "
                "open_positions=%s open_exposure_usd=%.2f "
                "cap=%.2f remaining_cap=%.2f",
                EMA_5M_BOT_ID, slug,
                open_pos_count, current_exposure,
                paper_max_exposure,
                max(0.0, paper_max_exposure - current_exposure),
            )

            # ── 6. Write telemetry (always — card reads this regardless of is_enabled) ──
            await asyncio.to_thread(
                _ema5m_upsert_telemetry_sync,
                slug, signal, ema9, ema200, btc_price,
                current_exposure,   # open_exposure_usd for strategy_settings
                open_pos_count,     # open_position_count for strategy_settings
            )

            # ── 7. Disabled gate ──────────────────────────────────────────────
            if not is_enabled:
                logging.warning(
                    "EMA_STRATEGY_DISABLED bot_id=%s slug=%s signal=%s "
                    "— is_enabled=False in bot_settings; "
                    "telemetry written to card, no trade placed",
                    EMA_5M_BOT_ID, slug, signal,
                )
                await asyncio.sleep(EMA_5M_LOOP_INTERVAL)
                continue

            # ── 8. NONE signal gate ───────────────────────────────────────────
            if signal == "NONE":
                await asyncio.sleep(EMA_5M_LOOP_INTERVAL)
                continue

            # ── 9. Entry cutoff gate ──────────────────────────────────────────
            if remaining < EMA_5M_ENTRY_CUTOFF_SECONDS:
                logging.warning(
                    "EMA_STRATEGY_SKIP slug=%s reason=entry_cutoff "
                    "remaining_s=%s cutoff_s=%s",
                    slug, remaining, EMA_5M_ENTRY_CUTOFF_SECONDS,
                )
                await asyncio.sleep(EMA_5M_LOOP_INTERVAL)
                continue

            # ── 10. Live mode gate (not yet implemented) ──────────────────────
            if mode == "LIVE":
                logging.warning(
                    "EMA_STRATEGY_LIVE_BLOCKED bot_id=%s slug=%s signal=%s "
                    "— mode=LIVE in bot_settings but live execution is not yet "
                    "implemented for EMA_5M_BTC; set mode=PAPER to trade",
                    EMA_5M_BOT_ID, slug, signal,
                )
                await asyncio.sleep(EMA_5M_LOOP_INTERVAL)
                continue

            # ── 11. Duplicate entry prevention ────────────────────────────────
            already_open = await has_open_paper_position_for_strategy(
                slug, EMA_5M_STRATEGY_ID, EMA_5M_BOT_ID
            )
            if already_open:
                logging.warning(
                    "EMA_STRATEGY_SKIP slug=%s reason=already_open signal=%s "
                    "— OPEN position already exists for this market+strategy",
                    slug, signal,
                )
                await asyncio.sleep(EMA_5M_LOOP_INTERVAL)
                continue

            # ── 11b. Exposure cap check (btc_5m_ema bucket only) ─────────────
            # current_exposure already fetched at step 5.5 — no extra DB query.
            projected_exposure = current_exposure + trade_size

            logging.warning(
                "EMA_EXPOSURE_CHECK bot_id=%s slug=%s "
                "current_exposure=%.2f trade_size=%.2f "
                "projected=%.2f cap=%.2f",
                EMA_5M_BOT_ID, slug,
                current_exposure, trade_size,
                projected_exposure, paper_max_exposure,
            )

            if paper_max_exposure > 0 and projected_exposure > paper_max_exposure:
                logging.warning(
                    "EMA_EXPOSURE_BLOCKED bot_id=%s slug=%s signal=%s "
                    "current_exposure=%.2f trade_size=%.2f "
                    "projected=%.2f cap=%.2f "
                    "— trade blocked; reduce open positions or raise "
                    "paper_max_exposure_usd in strategy_settings",
                    EMA_5M_BOT_ID, slug, signal,
                    current_exposure, trade_size,
                    projected_exposure, paper_max_exposure,
                )
                await asyncio.sleep(EMA_5M_LOOP_INTERVAL)
                continue

            logging.warning(
                "EMA_EXPOSURE_OK bot_id=%s slug=%s signal=%s "
                "current_exposure=%.2f trade_size=%.2f "
                "projected=%.2f cap=%.2f — proceeding to entry",
                EMA_5M_BOT_ID, slug, signal,
                current_exposure, trade_size,
                projected_exposure, paper_max_exposure,
            )

            # ── 12. Fetch market prices and place PAPER position ──────────────
            yes_price, no_price = await asyncio.to_thread(
                _ema5m_fetch_market_prices_sync, slug
            )
            side        = "yes" if signal == "YES" else "no"
            entry_price = yes_price if side == "yes" else no_price
            if not (0.01 < entry_price < 0.99):
                entry_price = 0.50
            shares = round(trade_size / entry_price, 4)

            logging.warning(
                "EMA_STRATEGY_PAPER_ENTRY slug=%s signal=%s side=%s "
                "entry_price=%.4f size_usd=%.2f shares=%.4f "
                "ema9=%.2f ema200=%.2f btc_price=%.2f "
                "exposure_after=%.2f cap=%.2f",
                slug, signal, side,
                entry_price, trade_size, shares,
                ema9 or 0.0, ema200 or 0.0, btc_price,
                projected_exposure, paper_max_exposure,
            )

            await insert_paper_position_row(
                EMA_5M_BOT_ID,
                EMA_5M_STRATEGY_ID,
                slug,
                side,
                entry_price,
                trade_size,
                shares,
                start_ts,
            )

        except Exception:
            logging.exception("EMA_5M_LOOP_ERROR")

        await asyncio.sleep(EMA_5M_LOOP_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────


async def main():
    # ── Unmistakable startup marker in the running event loop ─────────────────
    # Fires from main() — same scope as heartbeat_loop and all other tasks.
    # This is the definitive proof that the shared-brain code is executing.
    logging.warning(
        "COPY_WORKER_BUILD architecture=shared_copy_brain build=SHARED_BRAIN_V1 "
        "from=main_entrypoint "
        "env_COPY_TRADE_ENABLED=%s env_COPY_LIVE_ENABLED=%s",
        COPY_TRADE_ENABLED,
        COPY_LIVE_ENABLED,
    )
    trading_client = build_trading_client()
    tasks = []
    # ── BTC strategy tasks (existing — do not reorder or remove) ──────────────
    tasks.append(asyncio.create_task(_run_forever("rotate_loop", rotate_loop)))
    tasks.append(asyncio.create_task(_run_forever("paper_settlement_loop", paper_settlement_loop)))
    tasks.append(asyncio.create_task(_run_forever("live_balance_loop", live_balance_loop, trading_client)))
    tasks.append(asyncio.create_task(_run_forever("scan_loop", scan_loop)))
    tasks.append(asyncio.create_task(_run_forever("heartbeat_loop", heartbeat_loop, trading_client)))
    # ── Copy-trading tasks (additive — isolated from BTC strategy tasks) ──────
    # copy_diag_loop: lightweight heartbeat — no DB, no deps, always runs.
    #   Logs COPY_BRAIN_ALIVE every 10s so the build is always visible in Railway.
    # copy_trade_loop: shared-brain decision + paper/live execution.
    # copy_settlement_loop: resolves expired paper positions via Gamma API.
    # copy_auto_exit_loop: TP / max-hold auto-close for OPEN copied positions.
    # leaderboard_ingest_loop: scrapes Polymarket leaderboard → candidate_wallets.
    tasks.append(asyncio.create_task(_run_forever("copy_diag_loop", copy_diag_loop)))
    tasks.append(asyncio.create_task(_run_forever("copy_trade_loop", copy_trade_loop, trading_client)))
    tasks.append(asyncio.create_task(_run_forever("copy_settlement_loop", copy_settlement_loop)))
    tasks.append(asyncio.create_task(_run_forever("copy_auto_exit_loop", copy_auto_exit_loop)))
    tasks.append(asyncio.create_task(_run_forever("leaderboard_ingest_loop", leaderboard_ingest_loop)))
    # ── EMA_5M_BTC strategy (isolated — paper only, btc-updown-5m-* markets) ─
    tasks.append(asyncio.create_task(_run_forever("ema_5m_btc_loop", ema_5m_btc_loop)))
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
