create table if not exists bot_settings (
  bot_id text primary key,
  is_enabled boolean not null default false,
  updated_at timestamptz not null default now()
);

create table if not exists bot_heartbeat (
  id bigserial primary key,
  bot_id text not null,
  last_seen timestamptz not null,
  status text not null,
  message text
);

create table if not exists bot_trades (
  id bigserial primary key,
  bot_id text not null,
  market text,
  side text,
  price numeric,
  size numeric,
  status text,
  meta jsonb,
  created_at timestamptz not null default now()
);

insert into bot_settings (bot_id, is_enabled)
values ('default', false)
on conflict (bot_id) do update
set is_enabled = excluded.is_enabled;

-- ── Leaderboard wallet discovery ──────────────────────────────────────────────
-- Holds wallets discovered from the Polymarket leaderboard before they are
-- promoted to tracked_wallets.  BTCBOT reads this table for Hot Wallet
-- suggestions.  Rows are upserted on wallet_address so re-running the
-- ingest only refreshes the leaderboard snapshot, it does not duplicate rows.
--
-- Columns:
--   wallet_address              on-chain address (checksum or lower-case)
--   display_name                Polymarket display name (nullable)
--   rank                        leaderboard rank at time of fetch
--   daily_profit                USD profit on the leaderboard snapshot day
--   daily_volume                USD volume on the leaderboard snapshot day
--   source                      always 'leaderboard_crypto_today_profit'
--   fetched_at                  when this row was pulled from the API
--   -- enrichment (populated by enrich pass, may be null initially)
--   recent_trade_count          number of trades in recent activity window
--   trades_per_day              computed daily trade rate
--   avg_hold_minutes            average time between BUY and matching SELL
--   exit_before_resolution_rate fraction of trades exited before market resolved
--   recent_pnl                  net PnL from recent activity (notional basis)
--   copy_score                  composite suitability score 0–100
--   enriched_at                 when enrichment was last run
--   -- status
--   is_tracked                  true if wallet_address already in tracked_wallets
--   status                      'candidate' | 'tracked' | 'rejected'
--   updated_at                  auto-updated on any upsert

create table if not exists candidate_wallets (
  id                          bigserial primary key,
  wallet_address              text unique not null,
  display_name                text,
  rank                        int,
  daily_profit                numeric,
  daily_volume                numeric,
  source                      text not null default 'leaderboard_crypto_today_profit',
  fetched_at                  timestamptz not null,
  -- enrichment fields (null until enrich pass runs)
  recent_trade_count          int,
  trades_per_day              numeric,
  avg_hold_minutes            numeric,
  exit_before_resolution_rate numeric,
  recent_pnl                  numeric,
  copy_score                  numeric,
  enriched_at                 timestamptz,
  -- status
  is_tracked                  boolean not null default false,
  status                      text not null default 'candidate',
  updated_at                  timestamptz not null default now()
);

-- Index for BTCBOT Hot Wallet queries: good score, not yet tracked.
create index if not exists idx_candidate_wallets_score
  on candidate_wallets (copy_score desc)
  where is_tracked = false and status = 'candidate';

-- Index for quick address lookup (dedup check).
create index if not exists idx_candidate_wallets_address
  on candidate_wallets (wallet_address);
