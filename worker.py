import asyncio
import json
import logging
import os
from collections import deque
from contextlib import suppress
from datetime import datetime, timezone
from math import floor
from time import time
from urllib import parse, request

import websockets
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY
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

STRATEGY_FASTLOOP_BOT_ID = "paper_fastloop"
STRATEGY_SNIPER_BOT_ID = "paper_sniper"
STRATEGY_COPY_BOT_ID = "paper_copy"
STRATEGY_SCALPER_BOT_ID = "paper_scalper"
LIVE_MASTER_BOT_ID = "live"

COPY_TARGET_WALLET = os.getenv(
    "COPY_TARGET_WALLET", "0xd0d6053c3c37e727402d84c14069780d360993aa"
)
GAMMA_API_BASE = "https://gamma-api.polymarket.com"

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

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

refresh_asset_map()
strategy_trade_timestamps = {
    STRATEGY_SNIPER: deque(),
    STRATEGY_FASTLOOP: deque(),
}
strategy_missing_rows: set[str] = set()
live_master_warned = False
consecutive_trade_errors = 0
last_trade_error = None
paused_due_to_errors = False
paused_due_to_max_trades = False
trade_triggers = 0  # counts 2-leg attempts (YES+NO)
last_paper_skip_ts = 0

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


def float_or_none(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


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
    }
    columns = "is_enabled, mode, edge_threshold, trade_size_usd, max_trades_per_hour, paper_balance_usd, arm_live"
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
        settings = {
            "bot_id": bot_id,
            "is_enabled": bool(row.get("is_enabled")),
            "mode": (row.get("mode") or "PAPER").upper(),
            "edge_threshold": float_or_none(row.get("edge_threshold")) or EDGE_THRESHOLD,
            "trade_size_usd": float_or_none(row.get("trade_size_usd")) or TRADE_SIZE,
            "max_trades_per_hour": int(row.get("max_trades_per_hour") or MAX_TRADES_PER_HOUR),
            "paper_balance_usd": float_or_none(row.get("paper_balance_usd")) or 0.0,
            "arm_live": bool(row.get("arm_live")),
        }
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


def compute_strategy_size(settings: dict[str, object], strategy_id: str) -> float:
    base_size = max(settings["trade_size_usd"], 0.0)
    if strategy_id == STRATEGY_SNIPER:
        paper_balance = settings.get("paper_balance_usd") or 0.0
        if base_size <= 1 and paper_balance > 0:
            base_size = paper_balance * base_size
        size = min(base_size, SNIPER_SIZE_CAP_USD)
    else:
        size = base_size
    return size


def compute_shares_from_size(size_usd: float, price: float) -> float:
    if not price or price <= 0:
        return 0.0
    raw = size_usd / price
    if raw <= 0:
        return 0.0
    return floor(raw * 1e8) / 1e8


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
) -> bool:
    if not current_slug:
        logging.warning("Missing slug; skipping strategy %s", strategy_id)
        return False
    paper_side = "yes" if ya <= na else "no"
    entry_price = ya if paper_side == "yes" else na
    if entry_price is None or entry_price <= 0:
        logging.warning("Invalid entry price for %s slug=%s", strategy_id, current_slug)
        return False

    size_usd = compute_strategy_size(settings, strategy_id)
    shares = compute_shares_from_size(size_usd, entry_price)
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

    route_live = live_master_enabled and settings["arm_live"]
    executed_live = False
    if route_live:
        logging.info(
            "Live gating requested for strategy=%s arm_live=%s live_master_enabled=%s",
            strategy_id,
            settings["arm_live"],
            live_master_enabled,
        )
        executed_live = execute_live_strategy(client, strategy_id, edge, ya, na, size_usd)
        if not executed_live:
            logging.info(
                "Falling back to PAPER for strategy=%s live_master_enabled=%s client=%s",
                strategy_id,
                live_master_enabled,
                "available" if client else "missing",
            )

    if not executed_live:
        logging.info(
            "PAPER_EXEC strategy=%s slug=%s size_usd=%s",
            strategy_id,
            current_slug,
            size_usd,
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


def fetch_copy_feed() -> list[dict]:
    if not COPY_TARGET_WALLET:
        return []
    url = f"{GAMMA_API_BASE}/trades?accountId={parse.quote(COPY_TARGET_WALLET)}&limit=50"
    try:
        req = request.Request(url, headers={"User-Agent": "FastLoopWorker/1.0"})
        with request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
    except Exception as exc:
        logging.warning("COPY_FEED_UNAVAILABLE url=%s error=%s", url, exc)
        return []

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
    if not client:
        return None
    try:
        resp = client.get_balance(asset_type="COLLATERAL")
        if not resp:
            return None
        for key in ("balance", "collateral_balance", "amount", "collateralBalance"):
            value = resp.get(key)
            if value is not None:
                return float(value)
        return None
    except Exception:
        logging.exception("Failed fetching live collateral balance")
        return None


async def live_balance_loop(client: ClobClient | None):
    while True:
        balance = get_live_balance_usd(client)
        if balance is not None:
            try:
                supabase.table("bot_settings").update(
                    {
                        "live_balance_usd": balance,
                        "live_updated_at": utc_now_iso(),
                    }
                ).eq("bot_id", BOT_ID).execute()
            except Exception:
                logging.exception("Failed updating live balance bot_settings")
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
            trades = fetch_copy_feed()
            for trade in trades:
                trade_id = trade.get("id") or trade.get("tradeId") or trade.get("hash")
                if not trade_id:
                    continue
                if not track_copy_trade_id(trade_id):
                    continue
                market_slug = trade.get("slug") or trade.get("marketSlug") or trade.get("market") or "none"
                trade_side = (trade.get("outcome") or trade.get("side") or trade.get("symbol") or "").upper()
                if trade_side not in {"YES", "NO"}:
                    trade_side = trade.get("type", "UNKNOWN").upper()
                action = (trade.get("action") or trade.get("type") or trade.get("direction") or "UNKNOWN").upper()
                size = float_or_none(trade.get("size") or trade.get("amount") or trade.get("sizeUsd") or 0.0)
                price = float_or_none(trade.get("price") or trade.get("executionPrice"))
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

    strategy_bot_id = (
        STRATEGY_SNIPER_BOT_ID if strategy_id == STRATEGY_SNIPER else STRATEGY_FASTLOOP_BOT_ID
    )
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

    paper_payload = {
        "bot_id": BOT_ID,
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
        "bot_id": BOT_ID,
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
            "Inserted OPEN paper_positions row slug=%s side=%s strategy_id=%s",
            current_slug,
            paper_side,
            strategy_id,
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
):
    global consecutive_trade_errors, last_trade_error, paused_due_to_errors

    try:
        order = OrderArgs(token_id=token_id, price=price, size=trade_size, side=BUY)
        signed = client.create_order(order)
        resp = client.post_order(signed, OrderType.FAK)
        record_trade(
            token_id,
            side_label,
            "SUBMITTED",
            price,
            edge,
            ya,
            na,
            trade_size,
            strategy_id=strategy_id,
            response=resp,
        )
        consecutive_trade_errors = 0
        last_trade_error = None
    except Exception as exc:
        consecutive_trade_errors += 1
        last_trade_error = str(exc)[:512]
        record_trade(
            token_id,
            side_label,
            "ERROR",
            price,
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


async def heartbeat_loop(client: ClobClient | None):
    global paused_due_to_max_trades, trade_triggers, rotating, last_paper_skip_ts

    while True:
        now_ts = int(time())
        sniper_settings = read_strategy_settings(STRATEGY_SNIPER_BOT_ID)
        fastloop_settings = read_strategy_settings(STRATEGY_FASTLOOP_BOT_ID)
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

        ya = best_quotes["yes"]["ask"]
        na = best_quotes["no"]["ask"]
        yb = best_quotes["yes"]["bid"]
        nb = best_quotes["no"]["bid"]

        total_ask = (ya + na) if (ya is not None and na is not None) else None
        edge = (1.0 - total_ask) if (total_ask is not None) else None

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
            if should_trade_strategy(sniper_settings, STRATEGY_SNIPER, edge):
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
                )
            if sniper_traded:
                logging.info("SNIPER blocked FASTLOOP slug=%s", current_slug or "none")
            else:
                if should_trade_strategy(fastloop_settings, STRATEGY_FASTLOOP, edge):
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
                .in_("bot_id", [STRATEGY_FASTLOOP_BOT_ID, STRATEGY_SNIPER_BOT_ID])
                .eq("status", "OPEN")
                .lte("end_ts", now_ts)
                .execute()
            )
            rows = resp.data or []
        except Exception:
            logging.exception("Failed querying OPEN paper_positions")
            rows = []

        if rows:
            fastloop_count = sum(1 for r in rows if r.get("bot_id") == STRATEGY_FASTLOOP_BOT_ID)
            sniper_count = sum(1 for r in rows if r.get("bot_id") == STRATEGY_SNIPER_BOT_ID)
            logging.info(
                "Found OPEN paper_positions fastloop=%d sniper=%d total=%d",
                fastloop_count,
                sniper_count,
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
