create table if not exists polymarket_trades (
  trade_id text primary key,
  proxy_wallet text not null,
  market_id text not null,
  slug text,
  title text,
  outcome text,
  side text,
  price numeric not null,
  size numeric not null,
  notional numeric generated always as (price * size) stored,
  ts timestamptz not null,
  raw jsonb not null
);

create index if not exists idx_polymarket_trades_wallet_ts
  on polymarket_trades(proxy_wallet, ts desc);

create index if not exists idx_polymarket_trades_market_ts
  on polymarket_trades(market_id, ts desc);

create table if not exists user_features (
  wallet text primary key,
  updated_at timestamptz not null default now(),
  total_trades int not null default 0,
  total_volume numeric not null default 0,
  win_rate numeric,
  realized_pnl numeric,
  active_weeks int,
  distinct_markets int,
  top_market_volume_share numeric,
  top3_market_volume_share numeric,
  avg_bet_size numeric,
  median_bet_size numeric,
  biggest_single_win numeric
);

create table if not exists user_tags (
  wallet text not null,
  tag text not null,
  score numeric,
  reason jsonb,
  updated_at timestamptz not null default now(),
  primary key (wallet, tag)
);

create index if not exists idx_user_tags_tag
  on user_tags(tag);

create index if not exists idx_user_tags_wallet
  on user_tags(wallet);

create table if not exists ingestion_state (
  key text primary key,
  value jsonb not null,
  updated_at timestamptz not null default now()
);

create or replace view v_user_summary as
select
  f.wallet,
  f.updated_at,
  f.total_trades,
  f.total_volume,
  f.win_rate,
  f.realized_pnl,
  f.active_weeks,
  f.distinct_markets,
  f.top_market_volume_share,
  f.top3_market_volume_share,
  f.avg_bet_size,
  f.median_bet_size,
  f.biggest_single_win,
  coalesce(
    (
      select json_agg(
        json_build_object(
          'tag', t.tag,
          'score', t.score,
          'reason', t.reason,
          'updated_at', t.updated_at
        )
        order by t.score desc nulls last, t.tag asc
      )
      from user_tags t
      where t.wallet = f.wallet
    ),
    '[]'::json
  ) as tags
from user_features f;
