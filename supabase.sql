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
