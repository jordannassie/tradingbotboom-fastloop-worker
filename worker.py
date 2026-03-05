import asyncio
import base64
import json
import logging
import os
from collections import deque, defaultdict
from contextlib import suppress
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
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

HOST = "https://clob.polymarket.com"
CHAIN_ID = 137
WS_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
COINBASE_SPOT_URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
STRATEGY_FASTLOOP = "FASTLOOP"
STRATEGY_SNIPER = "SNIPER"
STRATEGY_COPY = "COPY"
STRATEGY_SCALPER = "SCALPER"
SNIPER_SECONDS_THRESHOLD = 30
DEFAULT_PAPER_START_BALANCE = 1000.0
SNIPER_SIZE_CAP_USD = float(os.getenv("SNIPER_SIZE_CAP_USD", "500"))
COPY_SIZE_CAP_USD = float(os.getenv("COPY_SIZE_CAP_USD", "25"))

STRATEGY_FASTLOOP_BOT_ID = "paper_fastloop"
STRATEGY_SNIPER_BOT_ID = "paper_sniper"
STRATEGY_COPY_BOT_ID = "paper_copy"
STRATEGY_SCALPER_BOT_ID = "paper_scalper"
LIVE_MASTER_BOT_ID = "live"

STRATEGY_TO_BOT_ID = {
    STRATEGY_FASTLOOP: STRATEGY_FASTLOOP_BOT_ID,
    STRATEGY_SNIPER: STRATEGY_SNIPER_BOT_ID,
    STRATEGY_COPY: STRATEGY_COPY_BOT_ID,
    STRATEGY_SCALPER: STRATEGY_SCALPER_BOT_ID,
}

COPY_TARGET_WALLET = os.getenv(
    "COPY_TARGET_WALLET", "0xd0d6053c3c37e727402d84c14069780d360993aa"
)
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
PAPER_BANKROLL_SHARED_ENABLED = os.getenv("PAPER_BANKROLL_SHARED", "false").strip().lower() in (
    "1",
    "true",
    "yes",
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
PM_ACCESS_KEY = os.getenv("PM_ACCESS_KEY")
PM_ED25519_PRIVATE_KEY_B64 = os.getenv("PM_ED25519_PRIVATE_KEY_B64")
PM_ACCOUNT_HOST = os.getenv("PM_ACCOUNT_HOST", "https://api.polymarket.us")
MIN_ORDER_USD = float(os.getenv("MIN_ORDER_USD", "2.0"))
MIN_ORDER_SHARES = float(os.getenv("MIN_ORDER_SHARES", "5.0"))
ENTRY_CUTOFF_SECONDS = int(os.getenv("ENTRY_CUTOFF_SECONDS", "180"))
FORCE_EXIT_SECONDS = int(os.getenv("FORCE_EXIT_SECONDS", "150"))
EXIT_LADDER_MAX_STEPS = int(os.getenv("EXIT_LADDER_MAX_STEPS", "8"))
EXIT_LADDER_STEP_SECONDS = int(os.getenv("EXIT_LADDER_STEP_SECONDS", "2"))
EXIT_LADDER_PRICE_IMPROVE_CENTS = float(os.getenv("EXIT_LADDER_PRICE_IMPROVE_CENTS", "1.0"))
FAST_TP_CENTS = float(os.getenv("FAST_TP_CENTS", "0.05"))
FAST_SL_CENTS = float(os.getenv("FAST_SL_CENTS", "0.05"))
FAST_MAX_HOLD_SECONDS = int(os.getenv("FAST_MAX_HOLD_SECONDS", "120"))
SNIPE_TP_CENTS = float(os.getenv("SNIPE_TP_CENTS", "0.1"))
SNIPE_SL_CENTS = float(os.getenv("SNIPE_SL_CENTS", "0.1"))
SNIPE_MAX_HOLD_SECONDS = int(os.getenv("SNIPE_MAX_HOLD_SECONDS", "240"))
LOW_FUNDS_SKIP_USD = float(os.getenv("LOW_FUNDS_SKIP_USD", "4.0"))
LIVE_MIN_AVAILABLE_USD = float(os.getenv("LIVE_MIN_AVAILABLE_USD", "5.0"))
PAPER_MIN_AVAILABLE_USD = float(os.getenv("PAPER_MIN_AVAILABLE_USD", "5.0"))
logging.info("WORKER_BOOT build=LIVE_BANKROLL_V3")
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

LIVE_WALLET_ADDRESS_EXPECTED = os.getenv("LIVE_WALLET_ADDRESS_EXPECTED")
LIVE_BANKROLL_REFRESH_SECONDS = int(os.getenv("LIVE_BANKROLL_REFRESH_SECONDS", "30"))
LIVE_TEST_ORDER_USD = (
    float(os.getenv("LIVE_TEST_ORDER_USD")) if os.getenv("LIVE_TEST_ORDER_USD") else None
)
LIVE_USDC_DECIMALS = int(os.getenv("LIVE_USDC_DECIMALS", "6"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
BOT_ID = os.getenv("BOT_ID", "default")

YES_TOKEN_ID = os.getenv("YES_TOKEN_ID")
NO_TOKEN_ID = os.getenv("NO_TOKEN_ID")

PRIVATE_KEY = os.getenv("PRIVATE_KEY")
SIGNATURE_TYPE = os.getenv("SIGNATURE_TYPE", "0")
FUNDER = os.getenv("FUNDER")  # optional
KILL_SWITCH = os.getenv("KILL_SWITCH", "false").lower() == "true"

current_slug = None
current_yes_token = YES_TOKEN_ID
current_no_token = NO_TOKEN_ID
HAVE_PRIVATE_KEY = bool(PRIVATE_KEY)
rotating = False
ws_task = None
live_balance_task = None
scan_task = None
copy_watch_task = None
HAS_PAPER_START_BALANCE_COLUMN: bool | None = None

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
copy_last_seen_trade_ids = deque()
copy_seen_trade_id_set = set()
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

SHARE_QUANT = Decimal("0.0001")
DEFAULT_TICK = Decimal("0.01")

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
    STRATEGY_COPY: deque(),
}
strategy_missing_rows: set[str] = set()
live_master_warned = False
global_trade_mode_cache: str | None = None
last_copy_schema_log_ts = 0
last_copy_target_log_ts = 0
schema_probe_last_ts = 0
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
live_wallet_ok = False
live_signer_address: str | None = None
live_funder_address: str | None = None
last_live_positions_snapshot_ts = 0
trades_auth_mode_logged = False
trades_sample_logged = False
live_order_tracker: dict[str, dict[str, object]] = {}
tracker_enabled_logged = False
live_positions: dict[str, float] = {}

EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "0.004"))
TRADE_SIZE = float(os.getenv("TRADE_SIZE", "5"))
MAX_TRADES_PER_HOUR = max(1, int(os.getenv("MAX_TRADES_PER_HOUR", "30")))
MAX_RUNTIME_TRADES = max(1, int(os.getenv("MAX_RUNTIME_TRADES", "200")))
MAX_CONSECUTIVE_ERRORS = 5
AUTO_ROTATE_ENV = os.getenv("AUTO_ROTATE", "true")
AUTO_ROTATE_ENABLED = AUTO_ROTATE_ENV.strip().lower() in ("1", "true", "yes", "y")
MARKET_SLUG_PREFIX = os.getenv("MARKET_SLUG_PREFIX", "btc-updown-5m")
MARKET_SLUG_PREFIXES_ENV = os.getenv("MARKET_SLUG_PREFIXES", "")
MARKET_SLUG_PREFIXES = [
    prefix.strip()
    for prefix in MARKET_SLUG_PREFIXES_ENV.split(",")
    if prefix.strip()
]
if not MARKET_SLUG_PREFIXES:
    MARKET_SLUG_PREFIXES = [MARKET_SLUG_PREFIX]
logging.info("MARKET_SLUG_PREFIXES parsed: %s", MARKET_SLUG_PREFIXES)
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "300"))
current_interval_seconds = INTERVAL_SECONDS
current_prefix = MARKET_SLUG_PREFIXES[0] if MARKET_SLUG_PREFIXES else MARKET_SLUG_PREFIX
ROTATE_POLL_SECONDS = int(os.getenv("ROTATE_POLL_SECONDS", "10"))
ROTATE_LOOKAHEAD_SECONDS = int(os.getenv("ROTATE_LOOKAHEAD_SECONDS", "20"))

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


def get_live_positions_truth(
    client: ClobClient | None, signer_address: str | None
) -> dict[str, float]:
    tracker = get_live_tracker_snapshot()
    if tracker:
        logging.info(
            "LIVE_POSITIONS_SOURCE source=TRACKER tokens=%s", len(tracker)
        )
        return tracker
    logging.info("LIVE_POSITIONS_SOURCE source=TRACKER_EMPTY")
    positions = get_live_token_holdings_truth(client, signer_address)
    if positions:
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
    positions_truth = get_live_positions_truth(client, signer)
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
    if not value:
        return ""
    try:
        return str(value).strip().lower()
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


def record_live_order_fill(
    token_id: str,
    order_side: str,
    shares: float,
    price: float,
    strategy: str | None,
    slug: str | None,
    reason: str,
    ts: int,
) -> None:
    if not token_id or shares <= 0:
        return
    entry = live_order_tracker.setdefault(
        token_id,
        {
            "net_shares": 0.0,
            "last_update_ts": ts,
            "events": [],
        },
    )
    net = entry["net_shares"]
    if order_side.upper() == "BUY":
        net += shares
    else:
        net -= shares
    if abs(net) < 1e-6:
        net = 0.0
    entry["net_shares"] = net
    entry["last_update_ts"] = ts
    events = entry["events"]
    events.append(
        {
            "ts": ts,
            "token_id": token_id,
            "order_side": order_side,
            "shares": shares,
            "price": price,
            "strategy": strategy or "unknown",
            "slug": slug or "none",
            "reason": reason,
        }
    )
    if len(events) > 50:
        del events[0]
    logging.info(
        "LIVE_TRACKER_EVENT token_id=%s order_side=%s shares=%.4f price=%.4f net=%.4f strategy=%s slug=%s reason=%s",
        token_id,
        order_side,
        shares,
        price,
        net,
        strategy or "unknown",
        slug or "none",
        reason,
    )


def get_live_tracker_snapshot() -> dict[str, float]:
    snapshot = {
        token: entry["net_shares"]
        for token, entry in live_order_tracker.items()
        if abs(entry["net_shares"]) > 0.01
    }
    tokens = list(snapshot.items())[:3]
    logging.info(
        "LIVE_TRACKER_SNAPSHOT tokens=%s example=%s",
        len(snapshot),
        [(token, round(shares, 4)) for token, shares in tokens],
    )
    global tracker_enabled_logged
    if not tracker_enabled_logged:
        logging.info("LIVE_TRACKER_ENABLED enabled=true")
        tracker_enabled_logged = True
    return snapshot


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
        logging.info(
            "LIVE_HOLDINGS_FROM_FILLS_NO_MATCH signer=%s hint=check LIVE_TRADES_SCHEMA_HINT fields sample_owner=%s sample_maker=%s",
            signer_address,
            sample.get("owner"),
            sample.get("maker"),
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
    positions_truth = get_live_positions_truth(client, signer)
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
        positions_after = get_live_positions_truth(client, signer)
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
    positions_truth = get_live_positions_truth(client, signer)
    if not positions_truth:
        logging.info(
            "LIVE_FORCE_EXIT_SKIP_NO_POSITIONS token_id=%s reason=%s",
            token_id,
            reason,
        )
        return True
    current_holdings = positions_truth.get(token_id, 0.0)
    if current_holdings <= 0.01:
        logging.info(
            "LIVE_FORCE_EXIT_SKIP_NO_POSITIONS token_id=%s shares=%s reason=%s",
            token_id,
            current_holdings,
            reason,
        )
        return True
    shares_to_close = min(shares, current_holdings)
    if shares_to_close <= 0.01:
        logging.info(
            "LIVE_FORCE_EXIT_SKIP_NO_POSITIONS token_id=%s shares_to_close=%s reason=%s",
            token_id,
            shares_to_close,
            reason,
        )
        return True
    close_side = "SELL"
    try:
        price_base = base_price or get_token_midprice(client, token_id)
        if not price_base:
            logging.warning("LIVE_EXIT_NO_PRICE token_id=%s", token_id)
            return False

        improve = EXIT_LADDER_PRICE_IMPROVE_CENTS / 100.0
        shares_now = shares
        for step in range(1, EXIT_LADDER_MAX_STEPS + 1):
            if close_side == "SELL":
                price = max(
                    0.01, price_base - 0.01 - (step - 1) * improve
                )
            else:
                price = min(
                    0.99, price_base + 0.01 + (step - 1) * improve
                )

            params = build_exit_order_params(token_id, shares_to_close, close_side, price)
            if params["size_usd"] <= 0:
                logging.warning(
                    "LIVE_EXIT_SKIP_ZERO_SIZE token_id=%s price=%s",
                    token_id,
                    price,
                )
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
            )

            logging.info(
                "LIVE_EXIT_STEP token_id=%s close_side=%s shares=%s price=%s step=%s/%s reason=%s success=%s",
                token_id,
                close_side,
                shares_abs,
                price,
                step,
                EXIT_LADDER_MAX_STEPS,
                reason,
                success,
            )

        await asyncio.sleep(EXIT_LADDER_STEP_SECONDS)
        signer = live_signer_address or live_funder_address
        positions_truth = get_live_positions_truth(client, signer)
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
    global live_wallet_ok, live_signer_address, live_funder_address
    if not client:
        return False
    signer = client.get_address()
    funder_addr = FUNDER if FUNDER else signer
    live_signer_address = signer
    live_funder_address = funder_addr
    expected = (LIVE_WALLET_ADDRESS_EXPECTED or "").lower()
    match = False
    if expected:
        match = any(
            addr and addr.lower() == expected
            for addr in (live_signer_address, live_funder_address)
        )
    else:
        match = True
    logging.info(
        "LIVE_WALLET_CHECK expected=%s signer=%s funder=%s match=%s",
        LIVE_WALLET_ADDRESS_EXPECTED,
        live_signer_address,
        live_funder_address,
        match,
    )
    live_wallet_ok = match
    return match


def refresh_live_bankroll_usd_if_needed(
    client: ClobClient | None, force: bool = False
) -> tuple[float | None, float | None]:
    global last_live_bankroll_refresh_ts, live_balance_cache, live_allowance_cache
    now_ts = int(time())
    if not live_wallet_ok:
        logging.warning(
            "LIVE_BLOCK_WALLET_MISMATCH expected=%s signer=%s funder=%s",
            LIVE_WALLET_ADDRESS_EXPECTED,
            live_signer_address,
            live_funder_address,
        )
        return live_balance_cache, live_allowance_cache
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
        "paper_balance_usd": 0.0,
        "arm_live": False,
        "copy_target_wallet": COPY_TARGET_WALLET,
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

        settings = {
            "bot_id": bot_id,
            "is_enabled": bool(row.get("is_enabled")),
            "mode": (row.get("mode") or "PAPER").upper(),
            "edge_threshold": float_or_none(row.get("edge_threshold")) or EDGE_THRESHOLD,
            "trade_size_usd": float_or_none(row.get("trade_size_usd")) or TRADE_SIZE,
            "max_trades_per_hour": int(row.get("max_trades_per_hour") or MAX_TRADES_PER_HOUR),
            "paper_balance_usd": float_or_none(row.get("paper_balance_usd")) or 0.0,
            "arm_live": bool(row.get("arm_live")),
            "strategy_settings": parsed_strategy,
        }
        if bot_id == STRATEGY_COPY_BOT_ID:
            settings["copy_target_wallet"] = parsed_strategy.get("copy_target_wallet") or COPY_TARGET_WALLET
        logging.info(
            "Loaded strategy settings bot_id=%s is_enabled=%s edge_threshold=%s arm_live=%s",
            bot_id,
            settings["is_enabled"],
            settings["edge_threshold"],
            settings["arm_live"],
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
        logging.info("Live master fetched: live_master_enabled=%s", enabled)
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


def sync_live_bankroll(client: ClobClient | None) -> tuple[float | None, float | None]:
    global live_balance_cache, live_allowance_cache, last_live_bankroll_log_ts
    if not client:
        return None, None
    buying_power = fetch_account_buying_power_usd()
    allowance = None
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
    if patch_payload:
        logging.info("LIVE_BANKROLL_PATCH_KEYS_SYNC keys=%s", list(patch_payload.keys()))
        try:
            resp_update = (
                supabase.table("bot_settings")
                .update(patch_payload)
                .eq("bot_id", LIVE_MASTER_BOT_ID)
                .execute()
            )
            status = getattr(resp_update, "status_code", None)
            ok = status in (200, 201)
            logging.info("LIVE_BANKROLL_WRITE_SYNC ok=%s status_code=%s", ok, status)
        except Exception:
            logging.exception("Failed updating live_balance_usd")
    if allowance is not None:
        live_allowance_cache = allowance
        persist_live_strategy_settings(None, allowance)
    now_ts = int(time())
    if now_ts - last_live_bankroll_log_ts >= 60:
        last_live_bankroll_log_ts = now_ts
        logging.info("LIVE_BANKROLL balance_usd=%s allowance_usd=%s", balance, allowance)
    return balance, allowance


def compute_strategy_size(settings: dict[str, object], strategy_id: str, mode: str) -> float:
    base_size = max(settings["trade_size_usd"], 0.0)
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
    if strategy_id == STRATEGY_COPY:
        if size > COPY_SIZE_CAP_USD:
            cap_applied = True
            cap_value = COPY_SIZE_CAP_USD
            logging.info(
                "COPY_SIZE_CAP_APPLIED wanted=%s capped=%s",
                size,
                COPY_SIZE_CAP_USD,
            )
            size = COPY_SIZE_CAP_USD
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
    if strategy_id == STRATEGY_COPY and size > COPY_SIZE_CAP_USD:
        cap_applied = True
        cap_value = COPY_SIZE_CAP_USD
        logging.info(
            "COPY_SIZE_CAP_APPLIED live wanted=%s capped=%s",
            size,
            COPY_SIZE_CAP_USD,
        )
        size = COPY_SIZE_CAP_USD
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


def create_copy_paper_position(
    market_slug: str,
    side: str,
    entry_price: float,
    size_usd: float,
    shares: float,
    meta: dict[str, object],
) -> bool:
    start_ts = slug_start_timestamp(market_slug)
    if start_ts is None:
        logging.warning("COPY_BUY_MIRROR skip slug=%s start_ts missing", market_slug)
        return False

    position_payload = {
        "bot_id": STRATEGY_COPY_BOT_ID,
        "market_slug": market_slug,
        "side": side,
        "entry_price": entry_price,
        "size_usd": size_usd,
        "shares": shares,
        "start_ts": start_ts,
        "end_ts": start_ts + current_interval_seconds,
        "status": "OPEN",
        "strategy_id": STRATEGY_COPY,
        "meta": meta,
    }
    position_payload["meta"].setdefault("timestamp", utc_now_iso())
    try:
        supabase.table("paper_positions").insert(position_payload).execute()
        logging.info(
            "COPY_BUY_MIRROR slug=%s side=%s price=%s size_usd=%s shares=%s meta=%s",
            market_slug,
            side.upper(),
            entry_price,
            size_usd,
            shares,
            {k: position_payload["meta"].get(k) for k in ("target_trade_id", "target_wallet")},
        )
        return True
    except Exception:
        logging.exception("Failed inserting COPY paper_positions row")
        return False


def get_copy_open_position(market_slug: str, side: str):
    try:
        resp = (
            supabase.table("paper_positions")
            .select("id, shares, size_usd, entry_price, start_ts, market_slug, side")
            .eq("bot_id", STRATEGY_COPY_BOT_ID)
            .eq("strategy_id", STRATEGY_COPY)
            .eq("status", "OPEN")
            .eq("market_slug", market_slug)
            .eq("side", side)
            .order("start_ts", {"ascending": True})
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        return rows[0] if rows else None
    except Exception:
        logging.exception("Failed querying COPY open position slug=%s side=%s", market_slug, side)
        return None


def approx_mid_price():
    ya = best_quotes["yes"]["ask"]
    na = best_quotes["no"]["ask"]
    if ya is None or na is None:
        return None
    return (ya + na) / 2


def close_copy_position(position, exit_price: float, approx: bool):
    row_id = position.get("id")
    shares = float_or_none(position.get("shares")) or 0.0
    size_usd = float_or_none(position.get("size_usd")) or 0.0
    if shares <= 0 or size_usd <= 0:
        logging.warning("COPY_CLOSE skip invalid shares/size id=%s shares=%s size=%s", row_id, shares, size_usd)
        return None
    payout = shares * exit_price
    pnl_usd = payout - size_usd
    updates = {
        "status": "CLOSED",
        "end_price": exit_price,
        "pnl_usd": pnl_usd,
        "resolved_side": position.get("side"),
        "closed_at": utc_now_iso(),
    }
    try:
        supabase.table("paper_positions").update(updates).eq("id", row_id).execute()
        logging.info(
            "COPY_SELL_MIRROR slug=%s side=%s exit_price=%s pnl_usd=%s approx=%s",
            position.get("market_slug"),
            position.get("side"),
            exit_price,
            pnl_usd,
            approx,
        )
    except Exception:
        logging.exception("Failed closing COPY position id=%s", row_id)

def interval_from_prefix(prefix: str) -> int:
    if "-15m" in prefix:
        return 900
    if "-5m" in prefix:
        return 300
    return INTERVAL_SECONDS


def should_trade_strategy(settings: dict[str, object], strategy_id: str, edge: float | None) -> bool:
    if not settings["is_enabled"]:
        return False
    if settings["mode"] != "PAPER":
        return False
    if edge is None or edge < settings["edge_threshold"]:
        return False
    if not has_strategy_trade_capacity(
        strategy_id, 2, settings["max_trades_per_hour"]
    ):
        logging.info(
            "Rate limit reached for %s max_trades_per_hour=%s",
            strategy_id,
            settings["max_trades_per_hour"],
        )
        return False
    return True


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
) -> bool:
    if not current_slug:
        logging.warning("Missing slug; skipping strategy %s", strategy_id)
        return False
    paper_side = "yes" if ya <= na else "no"
    entry_price = ya if paper_side == "yes" else na
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
    if route_live:
        logging.info(
            "Live gating requested for strategy=%s arm_live=%s live_master_enabled=%s",
            strategy_id,
            settings["arm_live"],
            live_master_enabled,
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
                if not executed_live:
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
                )

    mark_strategy_trade_attempts(strategy_id, 2)
    return True


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    meta = {**meta_template(edge, ya, na), "token_id": token_id, "side_label": side_label}
    if response is not None:
        meta["response"] = response
    if error is not None:
        meta["error"] = error
    meta["slug"] = current_slug
    if strategy_id:
        meta["strategy_id"] = strategy_id

    payload = {
        "bot_id": BOT_ID,
        "market": "FASTLOOP",
        "side": side_label,
        "price": price,
        "size": trade_size,
        "status": status,
        "meta": meta,
    }
    try:
        supabase.table("bot_trades").insert(payload).execute()
    except Exception:
        logging.exception("Failed inserting bot_trades row")


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


def track_copy_trade_id(trade_id: str) -> bool:
    if trade_id in copy_seen_trade_id_set:
        return False
    copy_last_seen_trade_ids.append(trade_id)
    copy_seen_trade_id_set.add(trade_id)
    if len(copy_last_seen_trade_ids) > 500:
        old = copy_last_seen_trade_ids.popleft()
        copy_seen_trade_id_set.discard(old)
    return True


def log_copy_schema_sample(raw: dict[str, object]) -> None:
    global last_copy_schema_log_ts
    now_ts = time()
    if now_ts - last_copy_schema_log_ts < 60:
        return
    last_copy_schema_log_ts = now_ts
    sample_keys = list(raw.keys())
    sample = {}
    for key in sample_keys[:5]:
        value = raw.get(key)
        sample[key] = str(value)[:120]
    logging.info(
        "COPY_FEED_SCHEMA sample_keys=%s sample_item=%s",
        sample_keys,
        json.dumps(sample, ensure_ascii=False),
    )


def normalize_copy_trade(raw: object) -> dict[str, object] | None:
    if not isinstance(raw, dict):
        logging.info(
            "COPY_FEED_BAD_ITEM kind=%s value=%s",
            type(raw).__name__,
            str(raw)[:100],
        )
        return None
    id_keys = ["id", "tradeId", "txHash", "transactionHash", "hash"]
    action_keys = ["action", "type", "direction", "sideAction", "tradeType"]
    side_keys = ["side", "outcome", "outcomeSide", "tokenSide", "betSide"]
    price_keys = ["price", "avgPrice", "avg_price", "fillPrice", "executionPrice"]
    size_keys = ["size", "sizeUsd", "amount", "notional", "usdAmount", "value"]
    slug_keys = ["market_slug", "marketSlug", "slug", "market", "event", "eventSlug", "ticker", "conditionId", "condition_id"]
    trade_id = next((raw.get(k) for k in id_keys if raw.get(k)), None)
    action_val = next((raw.get(k) for k in action_keys if raw.get(k)), None)
    slug = next((raw.get(k) for k in slug_keys if raw.get(k)), None)
    price = next((float_or_none(raw.get(k)) for k in price_keys if raw.get(k)), None)
    size = next((float_or_none(raw.get(k)) for k in size_keys if raw.get(k)), None)
    side = next((raw.get(k) for k in side_keys if raw.get(k)), None)
    condition_id = raw.get("conditionId") or raw.get("condition_id")
    asset_val = raw.get("asset") or raw.get("assetId") or raw.get("asset_id")
    if isinstance(side, str):
        sval = side.strip().lower()
        if sval in ("yes", "no"):
            side = sval.upper()
        elif "yes" in sval or "up" in sval:
            side = "YES"
        elif "no" in sval or "down" in sval:
            side = "NO"
        else:
            side = side.upper()
    action = (action_val or "").upper()
    if not action:
        if raw.get("isBuy"):
            action = "BUY"
        elif raw.get("isSell"):
            action = "SELL"
        else:
            for key in ("tradeType", "activityType", "eventType"):
                value = raw.get(key)
                if value:
                    action = str(value).upper()
                    break
            if not action:
                maker_side = raw.get("makerSide")
                taker_side = raw.get("takerSide")
                if taker_side and maker_side:
                    action = "BUY" if str(taker_side).upper() == "BUY" else "SELL"
                elif maker_side:
                    action = "BUY" if str(maker_side).upper() == "BUY" else "SELL"
    if not action:
        action = "BUY"
    if not slug and condition_id:
        slug = str(condition_id)
        log_copy_schema_sample(raw)
    if not slug and asset_val:
        slug = str(asset_val)
    if not trade_id:
        components = []
        if condition_id:
            components.append(str(condition_id))
        if asset_val:
            components.append(str(asset_val))
        if slug:
            components.append(str(slug))
        if side:
            components.append(str(side))
        if size is not None:
            components.append(str(size))
        proxy_wallet = raw.get("proxyWallet")
        if proxy_wallet:
            components.append(str(proxy_wallet))
        ts = raw.get("timestamp") or raw.get("createdAt")
        if ts:
            components.append(str(ts))
        trade_id = ":".join([c for c in components if c])
    missing = []
    if not trade_id:
        missing.append("id")
    if not action:
        missing.append("action")
    if not slug:
        missing.append("slug")
    if price is None:
        missing.append("price")
    if size is None:
        missing.append("size")
    if missing:
        sample_keys = list(raw.keys())[:5]
        logging.info(
            "COPY_FEED_SKIP missing_fields=%s sample_keys=%s",
            missing,
            sample_keys,
        )
        return None
    return {
        "id": trade_id,
        "action": action,
        "side": (side or "").upper(),
        "price": price,
        "size": size,
        "slug": slug,
        **raw,
    }


def log_schema_probe(url: str, data: object) -> None:
    global schema_probe_last_ts
    now_ts = time()
    if now_ts - schema_probe_last_ts < 60:
        return
    schema_probe_last_ts = now_ts
    if isinstance(data, dict):
        keys = list(data.keys())
        logging.info("COPY_SCHEMA url=%s top_keys=%s type=dict", url, keys)
        list_path: str | None = None
        list_items: list | None = None
        for key in ("trades", "data", "items", "activities", "events"):
            value = data.get(key)
            if isinstance(value, list):
                list_path = key
                list_items = value
                break
        if not list_path:
            for key, value in data.items():
                if isinstance(value, list):
                    list_path = key
                    list_items = value
                    break
        if list_path:
            logging.info(
                "COPY_SCHEMA items_path=%s items_count=%d",
                list_path,
                len(list_items or []),
            )
            sample = (list_items or [None])[0]
            if isinstance(sample, dict):
                logging.info(
                    "COPY_SCHEMA sample_item_keys=%s sample_item_str=%s",
                    list(sample.keys()),
                    json.dumps(sample)[:500],
                )
        else:
            logging.info("COPY_SCHEMA url=%s items_path=none", url)
    elif isinstance(data, list):
        logging.info("COPY_SCHEMA url=%s top_keys=[] type=list items_count=%d", url, len(data))
        if data:
            sample = data[0]
            if isinstance(sample, dict):
                logging.info(
                    "COPY_SCHEMA sample_item_keys=%s sample_item_str=%s",
                    list(sample.keys()),
                    json.dumps(sample)[:500],
                )
            else:
                logging.info(
                    "COPY_FEED_BAD_ITEM kind=%s value=%s",
                    type(sample).__name__,
                    str(sample)[:100],
                )
    else:
        logging.info("COPY_SCHEMA url=%s type=%s", url, type(data).__name__)


def extract_trade_list(data: object) -> tuple[list[object], str | None]:
    if isinstance(data, list):
        return data, "root"
    if isinstance(data, dict):
        for key in ("trades", "data", "items", "activities", "events"):
            value = data.get(key)
            if isinstance(value, list):
                return value, key
        for key, value in data.items():
            if isinstance(value, list):
                return value, key
    return [], None


def fetch_copy_feed(wallet: str) -> list[dict]:
    if not wallet:
        return []
    base = "https://data-api.polymarket.com"
    candidates = [
        f"{base}/trades?user={parse.quote(wallet)}&limit=50&takerOnly=false",
        f"{base}/activity?user={parse.quote(wallet)}&limit=50&type=TRADE",
        f"{base}/users/{parse.quote(wallet)}/trades?limit=50&offset=0",
    ]
    tried_urls = []
    for url in candidates:
        logging.info("COPY_FEED_TRY url=%s", url)
        try:
            req = request.Request(url, headers={"User-Agent": "FastLoopWorker/1.0"})
            with request.urlopen(req, timeout=5) as resp:
                status = getattr(resp, "status", None)
                if status not in (None, 200):
                    tried_urls.append(url)
                    logging.warning("COPY_FEED_FAIL url=%s err=status=%s", url, status)
                    continue
                data = json.loads(resp.read())
        except Exception as exc:
            tried_urls.append(url)
            logging.warning("COPY_FEED_FAIL url=%s err=%s", url, exc)
            continue
        log_schema_probe(url, data)
        trades_raw, path = extract_trade_list(data)
        if not trades_raw:
            tried_urls.append(url)
            logging.warning("COPY_FEED_FAIL url=%s err=unexpected schema", url)
            continue
        normalized = []
        for item in trades_raw:
            entry = normalize_copy_trade(item)
            if entry:
                normalized.append(entry)
        if not normalized:
            tried_urls.append(url)
            logging.warning("COPY_FEED_FAIL url=%s err=unexpected schema", url)
            continue
        logging.info(
            "COPY_FEED_OK normalized=%d raw_items=%d path=%s",
            len(normalized),
            len(trades_raw),
            path or "unknown",
        )
        return normalized
    logging.warning("COPY_FEED_UNAVAILABLE tried=%d urls=%s", len(tried_urls), candidates)
    return []


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

    trades = data
    if isinstance(data, dict):
        if "trades" in data and isinstance(data["trades"], list):
            trades = data["trades"]
        elif "data" in data and isinstance(data["data"], list):
            trades = data["data"]
        else:
            trades = []
    if not isinstance(trades, list):
        return []
    return trades


def slug_from_start(target_start):
    return f"{MARKET_SLUG_PREFIX}-{target_start}"


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
        sync_live_bankroll(client)
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


async def copy_watch_loop():
    while True:
        settings = read_strategy_settings(STRATEGY_COPY_BOT_ID)
        if settings["is_enabled"]:
            target_wallet = settings.get("copy_target_wallet") or COPY_TARGET_WALLET
            now = int(time())
            global last_copy_target_log_ts
            if now - last_copy_target_log_ts >= 60:
                logging.info("COPY_TARGET wallet=%s", target_wallet)
                last_copy_target_log_ts = now
            trades = fetch_copy_feed(target_wallet)
            for trade in trades:
                trade_id = trade.get("id") or trade.get("tradeId") or trade.get("hash")
                if not trade_id:
                    continue
                if not track_copy_trade_id(trade_id):
                    continue
                market_slug = trade.get("slug") or trade.get("marketSlug") or trade.get("market") or "none"
                trade_side = (trade.get("outcome") or trade.get("side") or trade.get("symbol") or "").lower()
                if trade_side not in {"yes", "no"}:
                    trade_side = (trade.get("type") or "unknown").lower()
                action = (trade.get("action") or trade.get("type") or trade.get("direction") or "unknown").upper()
                size = float_or_none(trade.get("size") or trade.get("amount") or trade.get("sizeUsd") or 0.0)
                price = float_or_none(trade.get("price") or trade.get("executionPrice"))
                if action == "BUY":
                    if not price or not market_slug:
                        continue
                    if not has_strategy_trade_capacity(
                        STRATEGY_COPY, 2, settings["max_trades_per_hour"]
                    ):
                        logging.info(
                            "Rate limit reached for COPY max_trades_per_hour=%s",
                            settings["max_trades_per_hour"],
                        )
                        continue
                    size_usd = compute_strategy_size(settings, STRATEGY_COPY, settings.get("mode", "PAPER").upper())
                    shares = compute_shares_from_size(size_usd, price)
                    if size_usd <= 0 or shares <= 0:
                        logging.warning(
                            "COPY_BUY_MIRROR skip trade_id=%s size_usd=%s shares=%s",
                            trade_id,
                            size_usd,
                            shares,
                        )
                        continue
                    if not create_copy_paper_position(
                        market_slug,
                        trade_side,
                        price,
                        size_usd,
                        shares,
                        {
                            "target_wallet": COPY_TARGET_WALLET,
                            "target_trade_id": trade_id,
                            "target_price": price,
                            "target_size": size,
                        },
                    ):
                        continue
                    mark_strategy_trade_attempts(STRATEGY_COPY, 2)
                else:
                    logging.info(
                        "COPY_SELL_SEEN trade_id=%s slug=%s side=%s price=%s size=%s",
                        trade_id,
                        market_slug,
                        trade_side,
                        price,
                        size,
                    )
                    if market_slug:
                        position = get_copy_open_position(market_slug, trade_side)
                        if position:
                            exit_price = price
                            approx = False
                            if exit_price is None:
                                exit_price = approx_mid_price() or price or 0.0
                                approx = True
                            if exit_price and exit_price > 0:
                                close_copy_position(position, exit_price, approx)
                logging.info(
                    "COPY_FEED trade_id=%s side=%s action=%s size=%s price=%s market_slug=%s",
                    trade_id,
                    trade_side,
                    action,
                    size,
                    price,
                    market_slug,
                )
        await asyncio.sleep(10)


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

    paper_side = "yes" if ya <= na else "no"
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
        "side": "BUY_BOTH",
        "price": total_ask,
        "size": size_usd,
        "status": "PAPER_DECISION",
        "strategy_id": strategy_id,
        "meta": meta,
    }

    try:
        supabase.table("bot_trades").insert(paper_payload).execute()
    except Exception:
        logging.exception("Failed inserting PAPER_DECISION for strategy %s", strategy_id)

    position_payload = {
        "bot_id": bot_id_override,
        "market_slug": current_slug,
        "side": paper_side,
        "entry_price": entry_price,
        "size_usd": size_usd,
        "shares": shares,
        "start_ts": start_ts,
        "end_ts": start_ts + current_interval_seconds,
        "status": "OPEN",
        "strategy_id": strategy_id,
    }

    start_price_at_open = await fetch_btc_spot_price()
    if start_price_at_open is not None:
        position_payload["start_price"] = start_price_at_open
    else:
        logging.warning(
            "Unable to fetch BTC spot price for paper_positions start slug=%s strategy_id=%s",
            current_slug,
            strategy_id,
        )

    try:
        supabase.table("paper_positions").insert(position_payload).execute()
        logging.info(
            "PAPER_OPEN bot_id=%s strategy_id=%s slug=%s end_ts=%s",
            bot_id_override,
            strategy_id,
            position_payload["market_slug"],
            position_payload["end_ts"],
        )
    except Exception:
        logging.exception(
            "Failed inserting paper_positions row for strategy_id=%s",
            strategy_id,
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



def update_best_quotes(asset_id, bid, ask):
    side = ASSET_TO_SIDE.get(asset_id)
    if not side:
        return
    if bid is not None:
        best_quotes[side]["bid"] = bid
    if ask is not None:
        best_quotes[side]["ask"] = ask


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
    global consecutive_trade_errors, last_trade_error, paused_due_to_errors, last_live_order_400_body

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
    if float(shares_q) < MIN_ORDER_SHARES:
        logging.warning(
            "MIN_SHARES_SKIP mode=LIVE strategy=%s price=%s size_usd=%s shares=%s min_shares=%s",
            strategy_id or "unknown",
            price_q,
            trade_size,
            shares_q,
            MIN_ORDER_SHARES,
        )
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
        record_live_order_fill(
            token_id,
            order_side_normalized,
            float(shares_q),
            float(price_q),
            strategy_id,
            current_slug,
            reason,
            int(time()),
        )
        now_ts = int(time())
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


async def heartbeat_loop(client: ClobClient | None):
    global paused_due_to_max_trades, trade_triggers, rotating, last_paper_skip_ts
    global last_live_positions_snapshot_ts

    while True:
        now_ts = int(time())
        sniper_settings = read_strategy_settings(STRATEGY_SNIPER_BOT_ID)
        fastloop_settings = read_strategy_settings(STRATEGY_FASTLOOP_BOT_ID)
        copy_settings = read_strategy_settings(STRATEGY_COPY_BOT_ID)
        scalper_settings = read_strategy_settings(STRATEGY_SCALPER_BOT_ID)
        trade_mode = current_global_trade_mode()
        copy_settings = read_strategy_settings(STRATEGY_COPY_BOT_ID)
        scalper_settings = read_strategy_settings(STRATEGY_SCALPER_BOT_ID)
        logging.info(
            "Loaded strategy settings bot_id=%s is_enabled=%s arm_live=%s",
            STRATEGY_COPY_BOT_ID,
            copy_settings["is_enabled"],
            copy_settings["arm_live"],
        )
        logging.info(
            "Loaded strategy settings bot_id=%s is_enabled=%s arm_live=%s",
            STRATEGY_SCALPER_BOT_ID,
            scalper_settings["is_enabled"],
            scalper_settings["arm_live"],
        )
        live_master_enabled = read_live_master_enabled()
        is_enabled_combined = sniper_settings["is_enabled"] or fastloop_settings["is_enabled"]
        mode = fastloop_settings["mode"]
        edge_threshold = min(sniper_settings["edge_threshold"], fastloop_settings["edge_threshold"])
        arm_live_active = sniper_settings["arm_live"] or fastloop_settings["arm_live"]
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
            and (sniper_settings["arm_live"] or fastloop_settings["arm_live"])
            and time_to_end is not None
        ):
                if should_force_exit(time_to_end, FORCE_EXIT_SECONDS):
                    force_exit_triggered = True
                    logging.info(
                        "LIVE_FORCE_EXIT_TRIGGER slug=%s time_to_end=%s",
                        current_slug,
                        time_to_end,
                    )
                    signer = live_signer_address or live_funder_address
                    positions_truth = get_live_positions_truth(client, signer)
                    if not positions_truth:
                        logging.info(
                            "LIVE_FORCE_EXIT_SKIP_NO_POSITIONS slug=%s",
                            current_slug,
                        )
                        continue
                    for token_id, shares in positions_truth.items():
                        if shares <= 0.01:
                            logging.info(
                                "LIVE_FORCE_EXIT_SKIP_NO_HOLDINGS token_id=%s shares=%s",
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
            f"sniper_enabled={sniper_settings['is_enabled']} fastloop_enabled={fastloop_settings['is_enabled']}"
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

        if trading_condition:
            sniper_traded = False
            sniper_time_to_end = time_to_end or 0
            if should_trade_strategy(sniper_settings, STRATEGY_SNIPER, edge):
                if entry_cutoff_active:
                    logging.info(
                        "ENTRY_CUTOFF_SKIP mode=PAPER strategy=%s time_to_end=%s",
                        STRATEGY_SNIPER,
                        sniper_time_to_end,
                    )
                elif should_skip_low_funds(sniper_settings["paper_balance_usd"]):
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
            if sniper_traded and trade_mode == "ONE":
                logging.info(
                    "TRADE_MODE_BLOCK trade_mode=ONE blocked_strategy=%s by_strategy=%s slug=%s",
                    STRATEGY_FASTLOOP,
                    STRATEGY_SNIPER,
                    current_slug or "none",
                )
            else:
                fast_time_to_end = time_to_end or 0
                if should_trade_strategy(fastloop_settings, STRATEGY_FASTLOOP, edge):
                    if entry_cutoff_active:
                        logging.info(
                            "ENTRY_CUTOFF_SKIP mode=PAPER strategy=%s time_to_end=%s",
                            STRATEGY_FASTLOOP,
                            fast_time_to_end,
                        )
                    elif should_skip_low_funds(fastloop_settings["paper_balance_usd"]):
                        pass
                    else:
                        await execute_strategy(
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
        await asyncio.sleep(5)
        if rotating:
            rotating = False


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
                        STRATEGY_COPY_BOT_ID,
                        STRATEGY_SCALPER_BOT_ID,
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
                counts = {bot_id: sum(1 for r in rows if r.get("bot_id") == bot_id) for bot_id in [
                    STRATEGY_FASTLOOP_BOT_ID,
                    STRATEGY_SNIPER_BOT_ID,
                    STRATEGY_COPY_BOT_ID,
                    STRATEGY_SCALPER_BOT_ID,
                    BOT_ID,
                ]}
                logging.info(
                    "Found OPEN paper_positions %s total=%d",
                    ", ".join(f"{bot_id}={counts[bot_id]}" for bot_id in counts),
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
                "bot_id": BOT_ID,
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
            except Exception:
                logging.exception("Failed inserting PAPER_CLOSED bot_trades row")

        if rows:
            update_paper_settings_from_positions()

        await asyncio.sleep(15)


async def main():
    trading_client = build_trading_client()
    rotator = asyncio.create_task(rotate_loop())
    settlement = asyncio.create_task(paper_settlement_loop())
    global live_balance_task, scan_task
    live_balance_task = asyncio.create_task(live_balance_loop(trading_client))
    scan_task = asyncio.create_task(scan_loop())
    global copy_watch_task
    copy_watch_task = asyncio.create_task(copy_watch_loop())
    restart_ws_task()
    try:
        await heartbeat_loop(trading_client)
    finally:
        rotator.cancel()
        settlement.cancel()
        if live_balance_task:
            live_balance_task.cancel()
        if scan_task:
            scan_task.cancel()
        if copy_watch_task:
            copy_watch_task.cancel()
        if ws_task:
            ws_task.cancel()
        with suppress(asyncio.CancelledError):
            await rotator
        with suppress(asyncio.CancelledError):
            if ws_task:
                await ws_task
        with suppress(asyncio.CancelledError):
            await settlement
        with suppress(asyncio.CancelledError):
            if live_balance_task:
                await live_balance_task
        with suppress(asyncio.CancelledError):
            if copy_watch_task:
                await copy_watch_task
        with suppress(asyncio.CancelledError):
            if scan_task:
                await scan_task


if __name__ == "__main__":
    asyncio.run(main())
