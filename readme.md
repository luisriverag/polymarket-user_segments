# Polymarket User Segments

Analyze Polymarket wallets, classify user behavior, and explore segmented traders through a PostgreSQL-backed pipeline and Flask dashboard.

Repository: `https://github.com/luisriverag/polymarket-user_segments`

## Overview

This project is built for **user-level analysis** of Polymarket activity.

Instead of focusing only on raw trade ingestion, the core goal is to answer questions like:

- Which wallets are consistently active?
- Which traders concentrate on a small number of markets?
- Which wallets show unusually high win rates?
- Which users place larger bets than average?
- Which traders look worth monitoring over time?

The system continuously ingests recent Polymarket trades, aggregates them by wallet, computes behavioral features, and assigns rule-based tags that make exploration easier.

## Main use cases

This project is useful for:

- building trader watchlists
- identifying high-conviction or concentrated bettors
- finding wallets with strong recent performance
- exploring behavioral patterns across users
- segmenting wallets for dashboards, research, or alerting
- studying wallet specialization and trading consistency

## What the project does

The pipeline has four layers:

1. **Raw trade collection**
   - stores recent Polymarket trades in PostgreSQL

2. **Wallet feature generation**
   - computes wallet-level behavioral metrics from raw trades

3. **User segmentation**
   - assigns tags based on rules and thresholds

4. **Exploration UI**
   - provides a Flask dashboard for filtering, browsing, and inspecting wallets

## User analysis model

The project treats each wallet as a user profile with measurable traits.

### Examples of user-level features

Each wallet can be described with metrics such as:

- total trades
- total volume
- active weeks
- distinct markets traded
- concentration in top market
- concentration in top 3 markets
- average bet size
- median bet size
- realized PnL
- win rate
- biggest single realized win

These features are stored in `user_features`.

### Examples of tags

Wallets are segmented with heuristic tags such as:

- `big_single_win`
- `high_win_rate`
- `weekly_active`
- `single_market_specialist`
- `concentrated_bettor`
- `large_size_trader`
- `possible_informed`

These tags are stored in `user_tags`.

They are intended as **analytical labels**, not factual judgments or proof of misconduct.

## Project structure

```text
polymarket-user_segments/
├── app.py
├── init_db.py
├── ingest.py
├── schema.sql
├── requirements.txt
└── templates/
    ├── base.html
    ├── index.html
    ├── user.html
    └── tags.html
```

## Data model

### `polymarket_trades`

Stores raw trade-level events.

Fields include:

- `trade_id`
    
- `proxy_wallet`
    
- `market_id`
    
- `title`
    
- `slug`
    
- `outcome`
    
- `side`
    
- `price`
    
- `size`
    
- `ts`
    
- `raw`
    

### `user_features`

Stores wallet-level aggregate metrics.

### `user_tags`

Stores segmentation labels and scores for each wallet.

### `ingestion_state`

Stores pipeline cursor state for incremental ingestion.

### `closed_positions_cache`

Caches closed-position responses to reduce rate-limit pressure and avoid repeatedly querying the same wallet.

## Current segmentation philosophy

This repo is focused on **behavioral segmentation**, not prediction or accusation.

The emphasis is on questions like:

- Is this user highly active?
    
- Is this user concentrated in one market?
    
- Is this user a large-size trader?
    
- Does this wallet have strong realized outcomes?
    
- Does this trader appear consistently profitable over the available data?
    

The tags are meant to support:

- research
    
- ranking
    
- filtering
    
- watchlist creation
    
- dashboard exploration
    

## Tag definitions

### `big_single_win`

Assigned when a wallet has at least one large realized win.

This helps identify users who landed an outsized payout, even if they are not consistently profitable.

### `high_win_rate`

Assigned when a wallet has a high fraction of winning closed positions over a minimum sample size.

Useful for spotting potentially skilled or selective users.

### `weekly_active`

Assigned when a wallet is active across multiple recent weeks.

Useful for separating casual bettors from recurring traders.

### `single_market_specialist`

Assigned when one market dominates a wallet’s volume.

Useful for identifying users with narrow thematic focus or conviction.

### `concentrated_bettor`

Assigned when most volume sits in a very small number of markets.

Useful for identifying concentrated strategies.

### `large_size_trader`

Assigned when the wallet’s typical bet sizing is relatively large.

Useful for spotting higher-conviction or higher-capital participants.

### `possible_informed`

Assigned when a wallet combines positive realized PnL, decent win rate, and repeat activity.

This is only a heuristic research label and should be interpreted carefully.

## Dashboard focus

The dashboard is designed around **user exploration**, not just raw data inspection.

### Main dashboard

The main page helps answer:

- which users match certain tags
    
- which users meet minimum PnL / win-rate / activity thresholds
    
- which wallets are worth drilling into next
    

### User detail page

Each wallet page helps answer:

- what tags does this user have
    
- what are this trader’s key metrics
    
- which markets dominate their behavior
    
- what does their recent trading activity look like
    

### Tag exploration

The tag views help answer:

- how many users fall into each segment
    
- which tags are rare or common
    
- which wallets overlap across multiple tag groups
    

## Filtering capabilities

The Flask UI supports user-focused analysis through filters like:

- wallet search
    
- multiple included tags
    
- multiple excluded tags
    
- include mode:
    
    - `any`
        
    - `all`
        
    - `exact`
        
- minimum tag score
    
- minimum realized PnL
    
- minimum win rate
    
- minimum trade count
    
- sorting by key behavioral metrics
    

This makes the dashboard useful for workflows such as:

- "show me users who are both `high_win_rate` and `weekly_active`"
    
- "show concentrated bettors, but exclude large-size traders"
    
- "show users with `possible_informed` score above 0.7"
    
- "show wallets with high PnL and at least 20 trades"
    

## Installation

### Requirements

- Python 3.11+
    
- PostgreSQL
    
- `venv` or another virtual environment tool
    

### Install dependencies

```bash
pip install -r requirements.txt
```

## PostgreSQL setup

Set your PostgreSQL DSN:

```bash
export PG_DSN=postgresql://postgres:postgres@localhost:5432/postgres
```

Check that PostgreSQL is reachable:

```bash
pg_isready -h localhost -p 5432
```

## Initialize the database

```bash
python init_db.py
```

This creates the schema and supporting cache tables.

## Running the pipeline

### Start ingestion

```bash
python ingest.py
```

### Optional backfill reset

Example: start 7 days back

```bash
export RESET_CURSOR_MINUTES=10080
python ingest.py
```

Then return to normal mode:

```bash
unset RESET_CURSOR_MINUTES
python ingest.py
```

### Recommended environment variables

```bash
export PG_DSN=postgresql://postgres:postgres@localhost:5432/postgres
export POLL_SECONDS=30
export BACKFILL_MINUTES=10
export FEATURE_LOOKBACK_DAYS=90
export TRADE_PAGE_LIMIT=500
export CLOSED_POSITIONS_REFRESH_HOURS=24
export MAX_CLOSED_POSITIONS_REFRESH_PER_LOOP=5
export DEBUG_DB_COUNTS=1
```

## Running the dashboard

```bash
flask --app app run --debug
```

Open:

```text
http://127.0.0.1:5000
```

## Example user analysis workflows

### Find high-quality recurring traders

Use filters such as:

- include tags: `high_win_rate`, `weekly_active`
    
- tag mode: `all`
    
- min trades: `10`
    

### Find concentrated specialists

Use filters such as:

- include tags: `single_market_specialist`, `concentrated_bettor`
    
- tag mode: `all`
    

### Find large, profitable traders

Use filters such as:

- include tags: `large_size_trader`
    
- min realized PnL: `1000`
    
- min win rate: `0.60`
    

### Find candidate watchlist wallets

Use filters such as:

- include tags: `possible_informed`
    
- min tag score: `0.70`
    

## Useful SQL for analysis

### How many wallets have been ingested

```sql
select count(distinct proxy_wallet) from polymarket_trades;
```

### Total wallets with computed features

```sql
select count(*) from user_features;
```

### Most profitable wallets

```sql
select wallet, realized_pnl, win_rate, total_trades
from user_features
order by realized_pnl desc nulls last
limit 100;
```

### Wallets by tag

```sql
select tag, count(*) as wallets
from user_tags
group by tag
order by wallets desc, tag asc;
```

### Top tagged wallets with full context

```sql
select
  f.wallet,
  f.realized_pnl,
  f.win_rate,
  f.total_trades,
  f.active_weeks,
  array_agg(t.tag order by t.tag) as tags
from user_features f
left join user_tags t on t.wallet = f.wallet
group by f.wallet, f.realized_pnl, f.win_rate, f.total_trades, f.active_weeks
order by f.realized_pnl desc nulls last
limit 100;
```

## Rate limits and caching

The project uses the public Polymarket data endpoints, which can rate-limit aggressively when profile-like endpoints are queried too often.

To reduce rate-limit issues:

- closed positions are cached in `closed_positions_cache`
    
- cached wallet data is refreshed on a slower schedule
    
- only a limited number of wallets are refreshed per loop
    
- retry/backoff logic is used for throttled requests
    

This keeps the pipeline focused on user analysis without overwhelming the API.

## Known limitations

### Historical depth

The public `/trades` endpoint is best for:

- continuous collection
    
- recent data windows
    
- shallow backfills
    

It is not ideal for reconstructing deep full-history trade archives from scratch.

### Tag quality depends on available data

User tags are only as good as the observed trade window and available closed-position coverage.

### Tags are heuristics

Labels such as `possible_informed` are best treated as screening tools, not hard conclusions.

## Roadmap

Planned or useful next steps:

- add user cohorts and segment groups
    
- add user ranking pages
    
- add per-market performance pages
    
- add freshness indicators for cached PnL data
    
- add saved dashboard presets
    
- add charts for user behavior over time
    
- add exportable watchlists
    
- add deep historical enrichment if a better source is added
    

## Intended audience

This repo is for people who want to analyze Polymarket users, including:

- researchers
    
- traders
    
- data analysts
    
- market observers
    
- anyone building watchlists or behavioral dashboards
    

## Disclaimer

This repository is for analytics and research.  
Tags and scores are heuristic outputs based on observable public activity and cached derived features. They should not be interpreted as definitive proof of intent, identity, or misconduct.

## License

TBD

