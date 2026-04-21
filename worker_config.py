"""worker_config.py — Single source of truth for all worker constants and environment config.

Imported by worker.py via `from worker_config import *`.

Sections:
   1.  Network / API endpoints
   2.  Strategy identifiers       (BTC-SPECIFIC)
   3.  Strategy Supabase bot IDs  (BTC-SPECIFIC)
   4.  Auth / signing
   5.  Supabase / identity
   6.  Market tokens              (BTC-SPECIFIC defaults)
   7.  BTC-SPECIFIC: Market rotation config
   8.  PM account API
   9.  Core trading parameters
  10.  Order sizing and safety
  11.  BTC-SPECIFIC: Timing / TP-SL parameters
  12.  Exit ladder
  13.  Bankroll guards
  14.  Live wallet config
  15.  Monitoring
  16.  BTC-SPECIFIC: Candle engine constants
  17.  Operational / internal constants

COPY-TRADE PREP NOTES
---------------------
- Sections marked BTC-SPECIFIC can be removed or replaced when the worker
  is evolved into a copy-trading engine.
- Reusable core sections (auth, Supabase, trading params, order sizing,
  exit ladder, bankroll guards, live wallet, monitoring, internal constants)
  remain relevant for copy-trading.
- COPY-TRADE HOOK comments mark where future copy-trading additions plug in.
"""

import os
from decimal import Decimal

from dotenv import load_dotenv

# load_dotenv() is called here so ALL os.getenv() calls below pick up .env values.
# On Railway, env vars come from the process environment (Railway dashboard) and .env is not used.
load_dotenv()


# ── 1. Network / API endpoints ────────────────────────────────────────────────

HOST = "https://clob.polymarket.com"
CHAIN_ID = 137
WS_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"

# BTC-SPECIFIC: Coinbase spot price endpoint. Used only for BTC reference data.
# COPY-TRADE HOOK: Remove this when the BTC strategy engine is replaced.
COINBASE_SPOT_URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"


# ── 2. Strategy identifiers ───────────────────────────────────────────────────
#
# BTC-SPECIFIC: All strategy IDs below are BTC 5-min prediction market strategies.
# COPY-TRADE HOOK: Add STRATEGY_COPY_TRADE = "COPY_TRADE" here when building
#                  the copy-trading engine. The BTC strategy IDs can then be
#                  removed or kept as inactive legacy entries.

STRATEGY_FASTLOOP = "FASTLOOP"
STRATEGY_SNIPER = "SNIPER"
STRATEGY_CANDLE_BIAS = "CANDLE_BIAS"
STRATEGY_SWEEP_RECLAIM = "SWEEP_RECLAIM"
STRATEGY_BREAKOUT_CLOSE = "BREAKOUT_CLOSE"
STRATEGY_ENGULFING_LEVEL = "ENGULFING_LEVEL"
STRATEGY_REJECTION_WICK = "REJECTION_WICK"
STRATEGY_FOLLOW_THROUGH = "FOLLOW_THROUGH"


# ── 3. Strategy Supabase bot IDs ──────────────────────────────────────────────
#
# BTC-SPECIFIC: One bot_settings row per BTC strategy.
# COPY-TRADE HOOK: Add STRATEGY_COPY_TRADE_BOT_ID = "copy_trade" here.
#                  The copy engine reads its settings from that row.

STRATEGY_FASTLOOP_BOT_ID = "paper_fastloop"
STRATEGY_SNIPER_BOT_ID = "paper_sniper"
STRATEGY_CANDLE_BIAS_BOT_ID = "paper_candle_bias"
STRATEGY_SWEEP_RECLAIM_BOT_ID = "paper_sweep_reclaim"
STRATEGY_BREAKOUT_CLOSE_BOT_ID = "paper_breakout_close"
STRATEGY_ENGULFING_LEVEL_BOT_ID = "paper_engulfing_level"
STRATEGY_REJECTION_WICK_BOT_ID = "paper_rejection_wick"
STRATEGY_FOLLOW_THROUGH_BOT_ID = "paper_follow_through"

DEFAULT_PAPER_START_BALANCE = 1000.0
LIVE_MASTER_BOT_ID = "live"

# BTC-SPECIFIC: Maps strategy ID → bot_settings bot_id.
# COPY-TRADE HOOK: Add the copy-trade entry to this map when ready.
STRATEGY_TO_BOT_ID = {
    STRATEGY_FASTLOOP: STRATEGY_FASTLOOP_BOT_ID,
    STRATEGY_SNIPER: STRATEGY_SNIPER_BOT_ID,
    STRATEGY_CANDLE_BIAS: STRATEGY_CANDLE_BIAS_BOT_ID,
    STRATEGY_SWEEP_RECLAIM: STRATEGY_SWEEP_RECLAIM_BOT_ID,
    STRATEGY_BREAKOUT_CLOSE: STRATEGY_BREAKOUT_CLOSE_BOT_ID,
    STRATEGY_ENGULFING_LEVEL: STRATEGY_ENGULFING_LEVEL_BOT_ID,
    STRATEGY_REJECTION_WICK: STRATEGY_REJECTION_WICK_BOT_ID,
    STRATEGY_FOLLOW_THROUGH: STRATEGY_FOLLOW_THROUGH_BOT_ID,
}

# BTC-SPECIFIC: Ordered list of candle-pattern strategy IDs.
# COPY-TRADE HOOK: Remove or replace with copy-strategy IDs.
CANDLE_STRATEGY_IDS = [
    STRATEGY_SWEEP_RECLAIM,
    STRATEGY_BREAKOUT_CLOSE,
    STRATEGY_ENGULFING_LEVEL,
    STRATEGY_REJECTION_WICK,
    STRATEGY_FOLLOW_THROUGH,
]


# ── 4. Auth / signing ─────────────────────────────────────────────────────────

PRIVATE_KEY = os.getenv("PRIVATE_KEY")
SIGNATURE_TYPE = os.getenv("SIGNATURE_TYPE", "0")
FUNDER = os.getenv("FUNDER")  # optional; required for proxy wallets
KILL_SWITCH = os.getenv("KILL_SWITCH", "false").lower() == "true"
HAVE_PRIVATE_KEY = bool(PRIVATE_KEY)


# ── 5. Supabase / identity ────────────────────────────────────────────────────

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
BOT_ID = os.getenv("BOT_ID", "default")


# ── 6. Market tokens (BTC-SPECIFIC defaults) ──────────────────────────────────
#
# BTC-SPECIFIC: Default YES/NO token IDs for the BTC 5-min UP/DOWN market.
# At runtime, rotate_loop overwrites current_yes_token / current_no_token
# as markets rotate. These env vars provide the startup fallback.
#
# COPY-TRADE HOOK: For copy-trading, token IDs come from the target market
#                  config table (e.g. copy_markets in Supabase), not from
#                  env vars. YES_TOKEN_ID / NO_TOKEN_ID can be removed once
#                  the market config loader is in place.

YES_TOKEN_ID = os.getenv("YES_TOKEN_ID")
NO_TOKEN_ID = os.getenv("NO_TOKEN_ID")


# ── 7. BTC-SPECIFIC: Market rotation config ───────────────────────────────────
#
# ALL constants in this section are BTC 5-min market-specific.
# The rotate_loop builds slugs like "btc-updown-5m-{unix_timestamp}" and
# resolves "up"/"down" outcome names → YES/NO token IDs via the Gamma API.
#
# COPY-TRADE HOOK: When the copy-trading engine replaces rotate_loop, these
#                  constants can be removed entirely.  The copy engine reads
#                  target market slugs from a Supabase config table instead.

AUTO_ROTATE_ENV = os.getenv("AUTO_ROTATE", "true")
AUTO_ROTATE_ENABLED = AUTO_ROTATE_ENV.strip().lower() in ("1", "true", "yes", "y")

# BTC-SPECIFIC: Slug prefix for the BTC 5-min market family.
MARKET_SLUG_PREFIX = os.getenv("MARKET_SLUG_PREFIX", "btc-updown-5m")
MARKET_SLUG_PREFIXES_ENV = os.getenv("MARKET_SLUG_PREFIXES", "")
MARKET_SLUG_PREFIXES = [
    prefix.strip()
    for prefix in MARKET_SLUG_PREFIXES_ENV.split(",")
    if prefix.strip()
]
if not MARKET_SLUG_PREFIXES:
    MARKET_SLUG_PREFIXES = [MARKET_SLUG_PREFIX]

# BTC-SPECIFIC: Interval in seconds for BTC 5-min markets (default 300s).
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "300"))
ROTATE_POLL_SECONDS = int(os.getenv("ROTATE_POLL_SECONDS", "10"))
ROTATE_LOOKAHEAD_SECONDS = int(os.getenv("ROTATE_LOOKAHEAD_SECONDS", "20"))


# ── 8. PM account API ─────────────────────────────────────────────────────────

PM_ACCESS_KEY = os.getenv("PM_ACCESS_KEY")
PM_ED25519_PRIVATE_KEY_B64 = os.getenv("PM_ED25519_PRIVATE_KEY_B64")
PM_ACCOUNT_HOST = os.getenv("PM_ACCOUNT_HOST", "https://api.polymarket.us")


# ── 9. Core trading parameters ────────────────────────────────────────────────

EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "0.004"))
TRADE_SIZE = float(os.getenv("TRADE_SIZE", "5"))
MAX_TRADES_PER_HOUR = max(1, int(os.getenv("MAX_TRADES_PER_HOUR", "30")))
MAX_RUNTIME_TRADES = max(1, int(os.getenv("MAX_RUNTIME_TRADES", "200")))
MAX_CONSECUTIVE_ERRORS = 5

# BTC-SPECIFIC: Sniper size cap is specific to the SNIPER BTC strategy.
SNIPER_SIZE_CAP_USD = float(os.getenv("SNIPER_SIZE_CAP_USD", "500"))


# ── 10. Order sizing and safety ───────────────────────────────────────────────

MIN_ORDER_USD = float(os.getenv("MIN_ORDER_USD", "2.0"))
MIN_ORDER_SHARES = float(os.getenv("MIN_ORDER_SHARES", "5.0"))

# Decimal precision constants for order quantization.
SHARE_QUANT = Decimal("0.0001")
DEFAULT_TICK = Decimal("0.01")

# Ordered list of method names to try when querying minimum order size from client.
MIN_SIZE_METHODS = (
    "get_min_order_size",
    "get_minimum_order_size",
    "get_min_size",
    "get_minimum_size",
    "get_minimum_order_size",
)


# ── 11. BTC-SPECIFIC: Timing / TP-SL parameters ──────────────────────────────
#
# Entry cutoff and force-exit are based on market expiry time.
# BTC 5-min markets expire every 5 minutes; this timing logic is BTC-specific.
#
# COPY-TRADE HOOK: Remove ENTRY_CUTOFF_SECONDS and FORCE_EXIT_SECONDS for
#                  long-running copy-trading markets that do not expire on a
#                  fixed schedule.  Exit logic for copy trading is driven by
#                  wallet delta signals, not time-to-expiry.

ENTRY_CUTOFF_SECONDS = int(os.getenv("ENTRY_CUTOFF_SECONDS", "120"))
FORCE_EXIT_SECONDS = int(os.getenv("FORCE_EXIT_SECONDS", "150"))

# BTC-SPECIFIC: Per-strategy TP/SL in cents and max hold seconds.
# These values embed BTC 5-min price movement assumptions.
# COPY-TRADE HOOK: Replace with copy-trade exit logic driven by wallet delta.
FAST_TP_CENTS = float(os.getenv("FAST_TP_CENTS", "0.05"))
FAST_SL_CENTS = float(os.getenv("FAST_SL_CENTS", "0.05"))
FAST_MAX_HOLD_SECONDS = int(os.getenv("FAST_MAX_HOLD_SECONDS", "120"))

SNIPE_TP_CENTS = float(os.getenv("SNIPE_TP_CENTS", "0.1"))
SNIPE_SL_CENTS = float(os.getenv("SNIPE_SL_CENTS", "0.1"))
SNIPE_MAX_HOLD_SECONDS = int(os.getenv("SNIPE_MAX_HOLD_SECONDS", "240"))

CANDLE_BIAS_TP_CENTS = float(os.getenv("CANDLE_BIAS_TP_CENTS", "0.07"))
CANDLE_BIAS_SL_CENTS = float(os.getenv("CANDLE_BIAS_SL_CENTS", "0.07"))
CANDLE_BIAS_MAX_HOLD_SECONDS = int(os.getenv("CANDLE_BIAS_MAX_HOLD_SECONDS", "180"))


# ── 12. Exit ladder ───────────────────────────────────────────────────────────
#
# Staged exit: attempts up to EXIT_LADDER_MAX_STEPS sell orders, improving
# price by EXIT_LADDER_PRICE_IMPROVE_CENTS each step.
# REUSABLE: close_live_position_ladder() is market-agnostic and reusable for
#            copy-trading exits.

EXIT_LADDER_MAX_STEPS = int(os.getenv("EXIT_LADDER_MAX_STEPS", "8"))
EXIT_LADDER_STEP_SECONDS = int(os.getenv("EXIT_LADDER_STEP_SECONDS", "2"))
EXIT_LADDER_PRICE_IMPROVE_CENTS = float(os.getenv("EXIT_LADDER_PRICE_IMPROVE_CENTS", "1.0"))


# ── 13. Bankroll guards ───────────────────────────────────────────────────────

LOW_FUNDS_SKIP_USD = float(os.getenv("LOW_FUNDS_SKIP_USD", "4.0"))
LIVE_MIN_AVAILABLE_USD = float(os.getenv("LIVE_MIN_AVAILABLE_USD", "5.0"))
PAPER_MIN_AVAILABLE_USD = float(os.getenv("PAPER_MIN_AVAILABLE_USD", "5.0"))
PAPER_BANKROLL_SHARED_ENABLED = os.getenv("PAPER_BANKROLL_SHARED", "false").strip().lower() in (
    "1",
    "true",
    "yes",
)


# ── 14. Live wallet config ────────────────────────────────────────────────────

LIVE_WALLET_ADDRESS_EXPECTED = os.getenv("LIVE_WALLET_ADDRESS_EXPECTED")
LIVE_BANKROLL_REFRESH_SECONDS = int(os.getenv("LIVE_BANKROLL_REFRESH_SECONDS", "30"))
LIVE_TEST_ORDER_USD = (
    float(os.getenv("LIVE_TEST_ORDER_USD")) if os.getenv("LIVE_TEST_ORDER_USD") else None
)
LIVE_USDC_DECIMALS = int(os.getenv("LIVE_USDC_DECIMALS", "6"))


# ── 15. Monitoring ────────────────────────────────────────────────────────────

STUCK_LIVE_SECONDS = int(os.getenv("STUCK_LIVE_SECONDS", "600"))
STUCK_ANY_SECONDS = int(os.getenv("STUCK_ANY_SECONDS", "900"))


# ── 16. BTC-SPECIFIC: Candle engine constants ─────────────────────────────────
#
# BTC-SPECIFIC: The candle engine builds OHLC history from mid-price ticks and
# drives the candle-pattern strategies (SWEEP_RECLAIM, BREAKOUT_CLOSE, etc.).
# This is entirely BTC 5-min specific.
#
# COPY-TRADE HOOK: Remove MAX_CANDLE_HISTORY and CANDLE_HISTORY_MINIMUM when
#                  the BTC candle engine is replaced by a wallet watcher.

MAX_CANDLE_HISTORY = 32
CANDLE_HISTORY_MINIMUM = 1


# ── 17. Operational / internal constants ──────────────────────────────────────

LOG_THROTTLE_SECONDS = 60


# ── 18. Copy-trading engine config ────────────────────────────────────────────
#
# These constants control the new copy-trading worker path.
# The BTC strategy path is unaffected by these settings.
#
# COPY_TRADE_ENABLED:          Set to "false" to disable the copy_trade_loop entirely.
# COPY_TRADE_LOOP_INTERVAL:    Seconds between copy loop ticks (default 60s).
# COPY_WALLET_TRADE_FETCH_LIMIT: How many recent trades to fetch per wallet per tick.
# COPY_TRADE_LOOKBACK_HOURS:   How many hours back to look for unevaluated trades.
# COPY_DATA_API_BASE:          Polymarket data API base URL for wallet activity.

COPY_TRADE_ENABLED = os.getenv("COPY_TRADE_ENABLED", "true").strip().lower() in (
    "1", "true", "yes",
)
COPY_TRADE_LOOP_INTERVAL = int(os.getenv("COPY_TRADE_LOOP_INTERVAL", "60"))
# Raised 50 -> 200 so active wallets don't silently miss older SELL/BUY
# events between ticks. Override via env if needed.
COPY_WALLET_TRADE_FETCH_LIMIT = int(os.getenv("COPY_WALLET_TRADE_FETCH_LIMIT", "200"))
# COPY_WALLET_TRADE_DB_LIMIT: how many wallet_trades rows to scan per bot per tick
# from the local DB (separate from the API fetch limit). Higher values reduce the
# chance of SELL events being missed for high-activity wallets.
# Raised 200 → 500 → 1000. Active wallets with many trades need a large window
# so older SELL events remain visible. Set to 0 for unlimited (full lookback).
# IMPORTANT: if Railway has COPY_WALLET_TRADE_DB_LIMIT=200 set as an env var,
# remove it or update it — the old value blocks close detection.
COPY_WALLET_TRADE_DB_LIMIT = int(os.getenv("COPY_WALLET_TRADE_DB_LIMIT", "1000"))
COPY_TRADE_LOOKBACK_HOURS = int(os.getenv("COPY_TRADE_LOOKBACK_HOURS", "24"))
COPY_DATA_API_BASE = "https://data-api.polymarket.com"

# COPY_SETTLEMENT_LOOP_INTERVAL: seconds between settlement passes.
#   Default 90s — balances Gamma API politeness with position freshness.
# COPY_SETTLEMENT_BATCH_SIZE: max open positions checked per tick.
#   Prevents large position backlogs from stalling the loop.
COPY_SETTLEMENT_LOOP_INTERVAL = int(os.getenv("COPY_SETTLEMENT_LOOP_INTERVAL", "90"))
COPY_SETTLEMENT_BATCH_SIZE = int(os.getenv("COPY_SETTLEMENT_BATCH_SIZE", "100"))

# COPY_AUTO_EXIT_LOOP_INTERVAL: seconds between auto-profit / max-hold scans.
#   Default 60s — scans OPEN copied_positions for TP / max-hold triggers.
#   Completely independent from settlement; does not touch copy trading ingestion.
COPY_AUTO_EXIT_LOOP_INTERVAL = int(os.getenv("COPY_AUTO_EXIT_LOOP_INTERVAL", "60"))

# ── 19. Live copy-trading pilot config ────────────────────────────────────────
#
# ALL of these must be satisfied before any live copy order is submitted.
# The defaults are intentionally conservative for a first pilot.
#
# COPY_LIVE_ENABLED           Master env-var switch for the live path.
#                             Defaults to "false" — must be explicitly set to
#                             enable live copy execution.
# COPY_LIVE_MAX_TRADE_USD     Hard USD cap per live copy order.
#                             Computed size is clamped to this before submission.
#                             Default: $5.
# COPY_LIVE_MAX_OPEN_POSITIONS Max open live copied_positions across all live bots.
#                             0 = unlimited. Default: 3.
# COPY_LIVE_MAX_TRADES_PER_HOUR Global cap on live copy orders per hour (BUY only).
#                             SELL/close orders are exempt. 0 = unlimited. Default: 5.
#
# NOTE: COPY_LIVE_MAX_BOTS has been removed. Multiple ARM LIVE bots are
# allowed simultaneously with no bot-count gate. The old L4 gate is gone.

COPY_LIVE_ENABLED = os.getenv("COPY_LIVE_ENABLED", "false").strip().lower() in (
    "1", "true", "yes",
)
COPY_LIVE_MAX_TRADE_USD        = float(os.getenv("COPY_LIVE_MAX_TRADE_USD",       "5.0"))
COPY_LIVE_MAX_OPEN_POSITIONS   = int(os.getenv("COPY_LIVE_MAX_OPEN_POSITIONS",    "3"))
COPY_LIVE_MAX_TRADES_PER_HOUR  = int(os.getenv("COPY_LIVE_MAX_TRADES_PER_HOUR",   "5"))

# Seconds to suppress duplicate SELL exit submissions for the same CLOB token.
# Prevents "not enough balance" rejections caused by a pending exit order still
# reserving CLOB collateral from a previous loop tick.  Override via env var.
COPY_LIVE_EXIT_COOLDOWN_SEC    = int(os.getenv("COPY_LIVE_EXIT_COOLDOWN_SEC",     "90"))


# ── 20. Leaderboard wallet discovery config ────────────────────────────────────
#
# LEADERBOARD_INGEST_ENABLED   Toggle the leaderboard ingest loop on/off.
#                               Default: true.
# LEADERBOARD_INGEST_INTERVAL  Seconds between full leaderboard scans.
#                               Default: 3600 (1 hour). Polymarket leaderboard
#                               refreshes roughly hourly so polling faster
#                               gives little benefit.
# LEADERBOARD_MAX_PAGES        Max pages to fetch per scan.  Each page has
#                               LEADERBOARD_PAGE_SIZE rows. 10 pages × 100 = 1000.
#                               Default: 10.
# LEADERBOARD_PAGE_SIZE        Rows per API page. Default: 100.
# LEADERBOARD_CATEGORY         Polymarket category filter. Default: "crypto".
# LEADERBOARD_TIMEFRAME        Leaderboard timeframe. "1d" = today.
#                               Default: "1d".
# LEADERBOARD_MIN_COPY_SCORE   Minimum candidate copy_score to be surfaced
#                               as a Hot Wallet suggestion. Default: 50.
# LEADERBOARD_ENRICH_LIMIT     Recent activity rows fetched per candidate
#                               during enrichment. Default: 50.

LEADERBOARD_INGEST_ENABLED  = os.getenv("LEADERBOARD_INGEST_ENABLED", "true").strip().lower() in (
    "1", "true", "yes",
)
LEADERBOARD_INGEST_INTERVAL = int(os.getenv("LEADERBOARD_INGEST_INTERVAL", "3600"))
LEADERBOARD_MAX_PAGES       = int(os.getenv("LEADERBOARD_MAX_PAGES",       "10"))
LEADERBOARD_PAGE_SIZE       = int(os.getenv("LEADERBOARD_PAGE_SIZE",       "100"))
LEADERBOARD_CATEGORY        = os.getenv("LEADERBOARD_CATEGORY",            "crypto")
LEADERBOARD_TIMEFRAME       = os.getenv("LEADERBOARD_TIMEFRAME",           "1d")
LEADERBOARD_MIN_COPY_SCORE  = float(os.getenv("LEADERBOARD_MIN_COPY_SCORE", "50.0"))
LEADERBOARD_ENRICH_LIMIT    = int(os.getenv("LEADERBOARD_ENRICH_LIMIT",    "50"))

# ── 21. EMA_5M_BTC strategy config ────────────────────────────────────────────
#
# PAPER-only strategy.  Trades Polymarket btc-updown-5m-* markets using
# 9-period and 200-period EMAs computed from Binance 5-minute BTC/USDT candles.
#
# EMA_5M_ENABLED              Toggle the loop on/off. Default: true.
# EMA_5M_LOOP_INTERVAL        Seconds between each evaluation tick.
#                               Default: 60.  One check per minute is enough
#                               since a new signal can only arise when the
#                               latest closed 5m candle changes.
# EMA_5M_TRADE_SIZE_USD       Paper position size in USD. Default: 10.
# EMA_5M_SLUG_PREFIX          Polymarket market slug prefix to trade.
#                               Default: btc-updown-5m.
# EMA_5M_ENTRY_CUTOFF_SECONDS Do not enter a market within this many seconds
#                               of its close.  Default: 30.
# EMA_5M_BOT_ID               Identifier used in paper_positions.bot_id.
#                               Must be unique — do not reuse an existing BOT_ID.
#                               Default: ema5mbtc.

EMA_5M_ENABLED              = os.getenv("EMA_5M_ENABLED", "true").strip().lower() in (
    "1", "true", "yes",
)
EMA_5M_LOOP_INTERVAL        = int(os.getenv("EMA_5M_LOOP_INTERVAL",        "60"))
EMA_5M_TRADE_SIZE_USD       = float(os.getenv("EMA_5M_TRADE_SIZE_USD",     "10.0"))
EMA_5M_SLUG_PREFIX          = os.getenv("EMA_5M_SLUG_PREFIX",              "btc-updown-5m")
EMA_5M_ENTRY_CUTOFF_SECONDS = int(os.getenv("EMA_5M_ENTRY_CUTOFF_SECONDS", "30"))
EMA_5M_BOT_ID               = os.getenv("EMA_5M_BOT_ID",                   "btc_5m_ema")
EMA_5M_STRATEGY_ID          = "EMA_5M_BTC"  # constant — not configurable


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — FAST-TURNOVER COPY ENGINE CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════
#
# These constants define the RECOMMENDED paper rollout profile for fast-copy
# testing.  They are NOT enforced globally — they are reference values for
# operators to use when configuring individual copy_bots rows in Supabase.
#
# Apply to copy_bots rows via the Supabase table editor (or SQL UPDATE).
# Do NOT set these in Railway env vars — they are per-bot DB settings.
#
# ── PAPER FAST-COPY TEST PROFILE ─────────────────────────────────────────────
# Recommended per-bot settings for a safe paper fast-copy rollout:
#
#   max_wallets_per_bot      = 5–10   (track 5–10 source wallets per copy bot)
#   max_open_positions       = 1      (at most 1 open position per wallet)
#   default_position_size    = 5.0    (small fixed size — $5 paper per trade)
#   fast_markets_only        = true   (only copy FAST_MARKET trades)
#   require_fast_copy        = true   (only copy FAST_COPY wallets)
#   block_blocked_markets    = true   (always block sports/politics)
#   max_entry_age_minutes    = 4      (skip BUY if trade > 4 min old)
#   exit_mode                = auto_profit_max_hold
#   take_profit_pct          = 25     (close at +25% profit)
#   max_hold_minutes         = 60     (force-close after 60 minutes)
#
# These are named reference constants only — do not use them in code directly.
# ─────────────────────────────────────────────────────────────────────────────

PAPER_FAST_COPY_PROFILE_MAX_OPEN_POSITIONS  = 1
PAPER_FAST_COPY_PROFILE_POSITION_SIZE_USD   = 5.0
PAPER_FAST_COPY_PROFILE_MAX_ENTRY_AGE_MIN   = 4
PAPER_FAST_COPY_PROFILE_TAKE_PROFIT_PCT     = 25.0
PAPER_FAST_COPY_PROFILE_MAX_HOLD_MINUTES    = 60.0
PAPER_FAST_COPY_PROFILE_EXIT_MODE          = "auto_profit_max_hold"


# ── COPY CLOSE REASON CONSTANTS ───────────────────────────────────────────────
# Standard close_reason values written to copied_positions.raw_json["close_reason"]
# and (when the column exists) to copied_positions.close_reason.
# These are used by all three close paths (source-exit, auto-exit, settlement).
# Adding a new path? Use one of these constants — do not free-form the string.

CLOSE_REASON_SOURCE_WALLET_EXIT   = "source_wallet_exit"
CLOSE_REASON_AUTO_PROFIT          = "auto_profit"
CLOSE_REASON_MAX_HOLD             = "max_hold"
CLOSE_REASON_SETTLED_MARKET       = "settled_market"
CLOSE_REASON_MANUAL_RESET         = "manual_reset"
CLOSE_REASON_CLOSE_FAILED_RETRY   = "close_failed_retrying"
CLOSE_REASON_CLOSE_FAILED_FINAL   = "close_failed_final"
CLOSE_REASON_PRE_EXPIRY           = "pre_expiry_exit"

# Seconds before market close at which to attempt a LIVE pre-expiry SELL.
# Set to 0 to disable the feature entirely.
PRE_EXPIRY_EXIT_SECONDS           = int(os.getenv("PRE_EXPIRY_EXIT_SECONDS", "15"))


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — SUPABASE MIGRATION REFERENCE
# ══════════════════════════════════════════════════════════════════════════════
#
# Run these SQL statements in the Supabase SQL editor BEFORE enabling Phase 2
# features.  All statements use ADD COLUMN IF NOT EXISTS — safe to re-run.
#
# MIGRATION BLOCK (copy and paste into Supabase SQL editor):
# ─────────────────────────────────────────────────────────────────────────────
#
# -- market_cache: market classification
# ALTER TABLE public.market_cache
#   ADD COLUMN IF NOT EXISTS market_class text NOT NULL DEFAULT 'UNKNOWN';
# CREATE INDEX IF NOT EXISTS idx_market_cache_class
#   ON public.market_cache (market_class);
#
# -- wallet_metrics: fast-turnover scoring fields
# ALTER TABLE public.wallet_metrics
#   ADD COLUMN IF NOT EXISTS wallet_class          text    NOT NULL DEFAULT 'UNSCORABLE',
#   ADD COLUMN IF NOT EXISTS median_hold_minutes   numeric,
#   ADD COLUMN IF NOT EXISTS pct_under_15min       numeric,
#   ADD COLUMN IF NOT EXISTS pct_under_30min       numeric,
#   ADD COLUMN IF NOT EXISTS recent_closed_count   int     NOT NULL DEFAULT 0;
# CREATE INDEX IF NOT EXISTS idx_wallet_metrics_class
#   ON public.wallet_metrics (wallet_class);
#
# -- copy_bots: per-bot fast-copy feature flags (all optional, all safe defaults)
# ALTER TABLE public.copy_bots
#   ADD COLUMN IF NOT EXISTS block_blocked_markets  boolean NOT NULL DEFAULT true,
#   ADD COLUMN IF NOT EXISTS fast_markets_only      boolean NOT NULL DEFAULT false,
#   ADD COLUMN IF NOT EXISTS require_fast_copy      boolean NOT NULL DEFAULT false,
#   ADD COLUMN IF NOT EXISTS max_entry_age_minutes  int     NOT NULL DEFAULT 0;
#
# -- copied_positions: standardized close reason column
# ALTER TABLE public.copied_positions
#   ADD COLUMN IF NOT EXISTS close_reason text;
# CREATE INDEX IF NOT EXISTS idx_copied_positions_close_reason
#   ON public.copied_positions (close_reason)
#   WHERE close_reason IS NOT NULL;
#
# ── VERIFY MIGRATION ─────────────────────────────────────────────────────────
# SELECT column_name, data_type, column_default
# FROM information_schema.columns
# WHERE table_name IN ('market_cache','wallet_metrics','copy_bots','copied_positions')
#   AND column_name IN (
#     'market_class','wallet_class','median_hold_minutes','pct_under_15min',
#     'pct_under_30min','recent_closed_count','block_blocked_markets',
#     'fast_markets_only','require_fast_copy','max_entry_age_minutes','close_reason'
#   )
# ORDER BY table_name, column_name;
# ══════════════════════════════════════════════════════════════════════════════
