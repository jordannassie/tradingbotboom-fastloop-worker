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
COPY_WALLET_TRADE_FETCH_LIMIT = int(os.getenv("COPY_WALLET_TRADE_FETCH_LIMIT", "50"))
COPY_TRADE_LOOKBACK_HOURS = int(os.getenv("COPY_TRADE_LOOKBACK_HOURS", "24"))
COPY_DATA_API_BASE = "https://data-api.polymarket.com"

# COPY_SETTLEMENT_LOOP_INTERVAL: seconds between settlement passes.
#   Default 90s — balances Gamma API politeness with position freshness.
# COPY_SETTLEMENT_BATCH_SIZE: max open positions checked per tick.
#   Prevents large position backlogs from stalling the loop.
COPY_SETTLEMENT_LOOP_INTERVAL = int(os.getenv("COPY_SETTLEMENT_LOOP_INTERVAL", "90"))
COPY_SETTLEMENT_BATCH_SIZE = int(os.getenv("COPY_SETTLEMENT_BATCH_SIZE", "100"))

# ── 19. Live copy-trading pilot config ────────────────────────────────────────
#
# ALL of these must be satisfied before any live copy order is submitted.
# The defaults are intentionally conservative for a first pilot.
#
# COPY_LIVE_ENABLED           Master env-var switch for the live path.
#                             Defaults to "false" — must be explicitly set to
#                             enable live copy execution.
# COPY_LIVE_MAX_BOTS          Max enabled LIVE-mode copy bots allowed at once.
#                             Pilot default: 1.
# COPY_LIVE_MAX_TRADE_USD     Hard USD cap per live copy order.
#                             Computed size is clamped to this before submission.
#                             Pilot default: $5.
# COPY_LIVE_MAX_OPEN_POSITIONS Max open live copied_positions across all live bots.
#                             Pilot default: 3.
# COPY_LIVE_MAX_TRADES_PER_HOUR Global cap on live copy orders per hour (all bots).
#                             Pilot default: 5.

COPY_LIVE_ENABLED = os.getenv("COPY_LIVE_ENABLED", "false").strip().lower() in (
    "1", "true", "yes",
)
COPY_LIVE_MAX_BOTS             = int(os.getenv("COPY_LIVE_MAX_BOTS",              "1"))
COPY_LIVE_MAX_TRADE_USD        = float(os.getenv("COPY_LIVE_MAX_TRADE_USD",       "5.0"))
COPY_LIVE_MAX_OPEN_POSITIONS   = int(os.getenv("COPY_LIVE_MAX_OPEN_POSITIONS",    "3"))
COPY_LIVE_MAX_TRADES_PER_HOUR  = int(os.getenv("COPY_LIVE_MAX_TRADES_PER_HOUR",   "5"))
