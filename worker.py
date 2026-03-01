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
trade_timestamps = deque()
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
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "300"))
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
    "Rotation config AUTO_ROTATE_ENABLED=%s AUTO_ROTATE_RAW=%s MARKET_SLUG_PREFIX=%s INTERVAL_SECONDS=%s ROTATE_POLL_SECONDS=%s ROTATE_LOOKAHEAD_SECONDS=%s",
    AUTO_ROTATE_ENABLED,
    AUTO_ROTATE_ENV,
    MARKET_SLUG_PREFIX,
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


def read_is_enabled() -> bool:
    settings = read_bot_settings()
    return settings["is_enabled"]


def read_bot_settings() -> dict[str, object]:
    defaults = {
        "is_enabled": False,
        "mode": "PAPER",
        "edge_threshold": EDGE_THRESHOLD,
        "trade_size_usd": TRADE_SIZE,
        "max_trades_per_hour": MAX_TRADES_PER_HOUR,
    }
    try:
        resp = (
            supabase.table("bot_settings")
            .select("is_enabled, mode, edge_threshold, trade_size_usd, max_trades_per_hour")
            .eq("bot_id", BOT_ID)
            .limit(1)
            .execute()
        )
        data = resp.data or []
        if not data:
            return defaults
        row = data[0]
        return {
            "is_enabled": bool(row.get("is_enabled")),
            "mode": (row.get("mode") or "PAPER").upper(),
            "edge_threshold": float_or_none(row.get("edge_threshold")) or EDGE_THRESHOLD,
            "trade_size_usd": float_or_none(row.get("trade_size_usd")) or TRADE_SIZE,
            "max_trades_per_hour": int(row.get("max_trades_per_hour") or MAX_TRADES_PER_HOUR),
        }
    except Exception:
        logging.exception("Failed reading bot_settings")
        return defaults


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
):
    meta = {**meta_template(edge, ya, na), "token_id": token_id, "side_label": side_label}
    if response is not None:
        meta["response"] = response
    if error is not None:
        meta["error"] = error
    meta["slug"] = current_slug

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


def compute_target_start(now_epoch):
    interval = INTERVAL_SECONDS
    start = floor(now_epoch / interval) * interval
    if (start + interval - now_epoch) <= ROTATE_LOOKAHEAD_SECONDS:
        return start + interval
    return start


def slug_from_start(target_start):
    return f"{MARKET_SLUG_PREFIX}-{target_start}"


def restart_ws_task():
    global ws_task
    if ws_task:
        ws_task.cancel()
    ws_task = asyncio.create_task(market_listener())
    return ws_task


async def rotate_loop():
    if not AUTO_ROTATE_ENABLED:
        return
    global current_slug, current_yes_token, current_no_token, rotating
    current_slug = None
    while True:
        now = int(time())
        target_start = compute_target_start(now)
        logging.info(
            "ROTATE_TICK now=%s target_start=%s current_slug=%s",
            now,
            target_start,
            current_slug or "none",
        )
        await asyncio.sleep(0)
        try:
            candidates = [
                target_start,
                target_start - INTERVAL_SECONDS,
                target_start + INTERVAL_SECONDS,
            ]
            found_market = None
            found_slug = None
            for candidate in candidates:
                if candidate < 0:
                    continue
                slug = slug_from_start(candidate)
                logging.info("ROTATE_FETCH_START slug=%s", slug)
                start_fetch = time()
                market = None
                found = False
                try:
                    market = await fetch_event_by_slug_async(slug)
                    found = bool(market)
                except Exception:
                    logging.exception("ROTATE_ERROR")
                    found = False
                duration_ms = int((time() - start_fetch) * 1000)
                logging.info(
                    "ROTATE_FETCH_DONE slug=%s found=%s duration_ms=%s",
                    slug,
                    "true" if found else "false",
                    duration_ms,
                )
                if found:
                    found_market = market
                    found_slug = slug
                    break

            if found_market and found_slug != current_slug:
                current_slug = found_slug
                outcomes = [o.lower() for o in found_market["outcomes"]]
                clobs = found_market["clobTokenIds"]
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
        except Exception:
            logging.exception("ROTATE_ERROR")
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


def prune_trade_history():
    cutoff = datetime.now(timezone.utc).timestamp() - 3600
    while trade_timestamps and trade_timestamps[0] < cutoff:
        trade_timestamps.popleft()


def has_trade_capacity(required=2, max_per_hour=MAX_TRADES_PER_HOUR) -> bool:
    prune_trade_history()
    return (len(trade_timestamps) + required) <= max_per_hour


def mark_trade_attempts(n=1):
    ts = datetime.now(timezone.utc).timestamp()
    for _ in range(n):
        trade_timestamps.append(ts)


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
        )
        if consecutive_trade_errors >= MAX_CONSECUTIVE_ERRORS:
            paused_due_to_errors = True
            logging.warning("Paused due to consecutive trade errors=%s", consecutive_trade_errors)


async def heartbeat_loop(client: ClobClient | None):
    global paused_due_to_max_trades, trade_triggers, rotating, last_paper_skip_ts

    while True:
        now_ts = int(time())
        settings = read_bot_settings()
        is_enabled = settings["is_enabled"]
        mode = settings["mode"]
        edge_threshold = settings["edge_threshold"]
        trade_size_usd = settings["trade_size_usd"]
        max_trades_per_hour = settings["max_trades_per_hour"]

        ya = best_quotes["yes"]["ask"]
        na = best_quotes["no"]["ask"]
        yb = best_quotes["yes"]["bid"]
        nb = best_quotes["no"]["bid"]

        total_ask = (ya + na) if (ya is not None and na is not None) else None
        edge = (1.0 - total_ask) if (total_ask is not None) else None

        status = "ENABLED" if is_enabled else "DISABLED"
        msg = f"ya={fmt(ya)} na={fmt(na)} total={fmt(total_ask)} edge={fmt(edge)}"

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
            and is_enabled
            and edge is not None
            and edge >= edge_threshold
        ):
            status = "PAUSED_NO_PRIVATE_KEY"

        slug_field = current_slug or "none"
        msg_with_slug = f"{msg} slug={slug_field}"
        logging.info("%s %s", status, msg_with_slug)
        record_heartbeat(status, msg_with_slug)

        if is_enabled and edge is not None and edge >= edge_threshold:
            record_opportunity(total_ask, edge, ya, yb, na, nb)

        paper_mode_active = is_enabled and (mode == "PAPER" or KILL_SWITCH)
        if (
            paper_mode_active
            and edge is not None
            and edge < edge_threshold
            and now_ts - last_paper_skip_ts >= INTERVAL_SECONDS
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
            is_enabled
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
            if has_trade_capacity(2, max_trades_per_hour):
                if mode == "PAPER" or KILL_SWITCH:
                    paper_payload = {
                        "bot_id": BOT_ID,
                        "market": "FASTLOOP",
                        "side": "BUY_BOTH",
                        "price": total_ask,
                        "size": trade_size_usd,
                        "status": "PAPER_DECISION",
                        "meta": {
                            **meta_template(edge, ya, na),
                            "slug": current_slug,
                            "action": "BUY_BOTH",
                            "mode": mode,
                        },
                    }
                    try:
                        supabase.table("bot_trades").insert(paper_payload).execute()
                    except Exception:
                        logging.exception("Failed inserting PAPER_DECISION")
                elif client:
                    trade_triggers += 1
                    mark_trade_attempts(2)
                    submit_order(
                        client,
                        current_yes_token,
                        "yes",
                        ya,
                        edge,
                        ya,
                        na,
                        trade_size_usd,
                    )
                    submit_order(
                        client,
                        current_no_token,
                        "no",
                        na,
                        edge,
                        ya,
                        na,
                        trade_size_usd,
                    )
            else:
                logging.info("Rate limit reached; skipping")

        await asyncio.sleep(5)
        if rotating:
            rotating = False


async def main():
    trading_client = build_trading_client()
    rotator = asyncio.create_task(rotate_loop())
    restart_ws_task()
    try:
        await heartbeat_loop(trading_client)
    finally:
        rotator.cancel()
        if ws_task:
            ws_task.cancel()
        with suppress(asyncio.CancelledError):
            await rotator
        with suppress(asyncio.CancelledError):
            if ws_task:
                await ws_task


if __name__ == "__main__":
    asyncio.run(main())
