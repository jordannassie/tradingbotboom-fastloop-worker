import asyncio
import json
import logging
import os
from collections import deque
from contextlib import suppress
from datetime import datetime, timezone

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

def parse_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default

def parse_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default

EDGE_THRESHOLD = parse_float("EDGE_THRESHOLD", 0.004)
TRADE_SIZE = parse_float("TRADE_SIZE", 5.0)
MAX_TRADES_PER_HOUR = max(1, parse_int("MAX_TRADES_PER_HOUR", 30))
MAX_RUNTIME_TRADES = max(1, parse_int("MAX_RUNTIME_TRADES", 200))
MAX_CONSECUTIVE_ERRORS = 5

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

if not YES_TOKEN_ID or not NO_TOKEN_ID:
    raise SystemExit("Missing YES_TOKEN_ID or NO_TOKEN_ID")

if not PRIVATE_KEY:
    raise SystemExit("Missing PRIVATE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

ASSET_TO_SIDE = {
    YES_TOKEN_ID: "yes",
    NO_TOKEN_ID: "no",
}

best_quotes = {
    "yes": {"bid": None, "ask": None},
    "no": {"bid": None, "ask": None},
}

trade_timestamps = deque()
consecutive_trade_errors = 0
last_trade_error = None
paused_due_to_errors = False
paused_due_to_max_trades = False
trade_triggers = 0  # counts 2-leg attempts (YES+NO)

def mask(value: str | None) -> str:
    if not value:
        return "None"
    return value[:6] + "..."

logging.info(
    "Worker start BOT_ID=%s EDGE=%s SIZE=%s SIG=%s FUNDER=%s (Supabase connected)",
    BOT_ID, EDGE_THRESHOLD, TRADE_SIZE, SIGNATURE_TYPE, mask(FUNDER)
)

def float_or_none(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None

def read_is_enabled() -> bool:
    try:
        resp = (
            supabase.table("bot_settings")
            .select("is_enabled")
            .eq("bot_id", BOT_ID)
            .limit(1)
            .execute()
        )
        data = resp.data or []
        if data:
            return bool(data[0].get("is_enabled"))
    except Exception:
        logging.exception("Failed reading bot_settings")
    return False

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
    try:
        supabase.table("bot_trades").insert(payload).execute()
    except Exception:
        logging.exception("Failed inserting OPPORTUNITY")

def record_trade(token_id, side_label, status, price, edge, ya, na, response=None, error=None):
    meta = {**meta_template(edge, ya, na), "token_id": token_id, "side_label": side_label}
    if response is not None:
        meta["response"] = response
    if error is not None:
        meta["error"] = error

    payload = {
        "bot_id": BOT_ID,
        "market": "FASTLOOP",
        "side": side_label,
        "price": price,
        "size": TRADE_SIZE,
        "status": status,
        "meta": meta,
    }
    try:
        supabase.table("bot_trades").insert(payload).execute()
    except Exception:
        logging.exception("Failed inserting bot_trades row")

def update_best_quotes(asset_id, bid, ask):
    side = ASSET_TO_SIDE.get(asset_id)
    if not side:
        return
    if bid is not None:
        best_quotes[side]["bid"] = bid
    if ask is not None:
        best_quotes[side]["ask"] = ask

async def market_listener():
    subscribe_payload = {
        "type": "market",
        "assets_ids": [YES_TOKEN_ID, NO_TOKEN_ID],
        "custom_feature_enabled": True,
    }

    while True:
        try:
            async with websockets.connect(WS_MARKET, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps(subscribe_payload))
                logging.info("WS connected")
                async for raw in ws:
                    msg = raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
                    if not msg:
                        continue
                    try:
                        payload = json.loads(msg)
                    except json.JSONDecodeError:
                        continue

                    if payload.get("event_type") != "best_bid_ask":
                        continue

                    data = payload.get("data") or payload
                    asset_id = data.get("asset_id") or data.get("assetId") or data.get("token_id")
                    bid = float_or_none(data.get("best_bid") or data.get("bestBid"))
                    ask = float_or_none(data.get("best_ask") or data.get("bestAsk"))
                    if asset_id:
                        update_best_quotes(asset_id, bid, ask)
        except asyncio.CancelledError:
            break
        except Exception:
            logging.exception("WS error, reconnecting")
            await asyncio.sleep(2)

def prune_trade_history():
    cutoff = datetime.now(timezone.utc).timestamp() - 3600
    while trade_timestamps and trade_timestamps[0] < cutoff:
        trade_timestamps.popleft()

def has_trade_capacity(required=2) -> bool:
    prune_trade_history()
    return (len(trade_timestamps) + required) <= MAX_TRADES_PER_HOUR

def mark_trade_attempts(n=1):
    ts = datetime.now(timezone.utc).timestamp()
    for _ in range(n):
        trade_timestamps.append(ts)

def build_trading_client() -> ClobClient:
    sig = int(SIGNATURE_TYPE)
    funder = FUNDER if FUNDER else None
    client = ClobClient(HOST, key=PRIVATE_KEY, chain_id=CHAIN_ID, signature_type=sig, funder=funder)
    client.set_api_creds(client.create_or_derive_api_creds())
    return client

def fmt(v):
    return f"{v:.6f}" if v is not None else "n/a"

def submit_order(client: ClobClient, token_id: str, side_label: str, price: float, edge: float, ya: float, na: float):
    global consecutive_trade_errors, last_trade_error, paused_due_to_errors

    try:
        order = OrderArgs(token_id=token_id, price=price, size=TRADE_SIZE, side=BUY)
        signed = client.create_order(order)
        # IMPORTANT: match official usage: positional OrderType argument
        resp = client.post_order(signed, OrderType.FAK)
        record_trade(token_id, side_label, "SUBMITTED", price, edge, ya, na, response=resp)
        consecutive_trade_errors = 0
        last_trade_error = None
    except Exception as exc:
        consecutive_trade_errors += 1
        last_trade_error = str(exc)[:512]
        record_trade(token_id, side_label, "ERROR", price, edge, ya, na, error=str(exc))
        if consecutive_trade_errors >= MAX_CONSECUTIVE_ERRORS:
            paused_due_to_errors = True
            logging.warning("Paused due to consecutive trade errors=%s", consecutive_trade_errors)

async def heartbeat_loop(client: ClobClient):
    global paused_due_to_max_trades, trade_triggers

    while True:
        is_enabled = read_is_enabled()

        ya = best_quotes["yes"]["ask"]
        na = best_quotes["no"]["ask"]
        yb = best_quotes["yes"]["bid"]
        nb = best_quotes["no"]["bid"]

        total_ask = (ya + na) if (ya is not None and na is not None) else None
        edge = (1.0 - total_ask) if (total_ask is not None) else None

        status = "ENABLED" if is_enabled else "DISABLED"
        msg = f"ya={fmt(ya)} na={fmt(na)} total={fmt(total_ask)} edge={fmt(edge)}"

        if paused_due_to_errors:
            status = "PAUSED_ERRORS"
            msg = msg + f" last_error={last_trade_error or 'unknown'}"

        if trade_triggers >= MAX_RUNTIME_TRADES:
            paused_due_to_max_trades = True

        if paused_due_to_max_trades:
            status = "PAUSED_MAX_TRADES"
            msg = msg + f" trade_triggers={trade_triggers}"

        logging.info("%s %s", status, msg)
        record_heartbeat(status, msg)

        # opportunity logging
        if is_enabled and edge is not None and edge >= EDGE_THRESHOLD:
            record_opportunity(total_ask, edge, ya, yb, na, nb)

        trading_allowed = (
            is_enabled
            and edge is not None
            and ya is not None
            and na is not None
            and edge >= EDGE_THRESHOLD
            and not paused_due_to_errors
            and not paused_due_to_max_trades
        )

        if trading_allowed:
            if has_trade_capacity(2):
                # count a 2-leg attempt
                trade_triggers += 1
                mark_trade_attempts(2)
                submit_order(client, YES_TOKEN_ID, "yes", ya, edge, ya, na)
                submit_order(client, NO_TOKEN_ID, "no", na, edge, ya, na)
            else:
                logging.info("Rate limit reached; skipping")

        await asyncio.sleep(5)

async def main():
    trading_client = build_trading_client()
    listener = asyncio.create_task(market_listener())
    try:
        await heartbeat_loop(trading_client)
    finally:
        listener.cancel()
        with suppress(asyncio.CancelledError):
            await listener

if __name__ == "__main__":
    asyncio.run(main())
