# Worker (FastLoop)

## Environment variables (Railway / VPS only)
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY
- BOT_ID (optional; default: default)
- YES_TOKEN_ID
- NO_TOKEN_ID
- PRIVATE_KEY
- SIGNATURE_TYPE (optional; default: 0)
- FUNDER (optional; required for proxy wallets / some Polymarket.com setups)
- EDGE_THRESHOLD (optional; default: 0.004)
- TRADE_SIZE (optional; default: 5)
- MAX_TRADES_PER_HOUR (optional; default: 30)
- MAX_RUNTIME_TRADES (optional; default: 200)

## Run locally
python worker.py

## Railway deployment checklist
1) Deploy from GitHub
2) Start Command: python worker.py
3) Add env vars listed above
4) Run supabase.sql in Supabase (SQL editor)
5) Flip the kill switch: set bot_settings.is_enabled = true (for BOT_ID)

## Security reminder
Never put PRIVATE_KEY in Netlify, frontend code, or any client-side bundle.
Secrets only belong in Railway/VPS environment variables.
