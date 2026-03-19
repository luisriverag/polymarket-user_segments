```markdown
# Polymarket Stats

Track Polymarket trading activity, store it in PostgreSQL, compute per-wallet features, tag wallets into useful segments, and explore the results in a Flask dashboard.

## What this project does

This project has three main parts:

- `init_db.py` initializes the PostgreSQL schema
- `ingest.py` continuously ingests Polymarket trades and refreshes wallet features/tags
- `app.py` serves a Flask dashboard to inspect wallets, tags, and recent trades

The pipeline is built around:

- raw trade storage
- derived wallet features
- rule-based tagging
- a simple web UI for exploration

## Current feature set

### Raw ingestion

The ingestion process stores:

- wallet address
- market ID
- title / slug
- outcome
- side
- price
- size
- timestamp
- raw JSON payload

Trades are stored idempotently using a generated stable trade hash.

### Wallet features

For each wallet, the app computes metrics like:

- total trades
- total volume
- active weeks
- distinct markets
- top market concentration
- top 3 market concentration
- average bet size
- median bet size
- realized PnL
- win rate
- biggest single win

### Tags

Example wallet tags:

- `big_single_win`
- `high_win_rate`
- `weekly_active`
- `single_market_specialist`
- `concentrated_bettor`
- `large_size_trader`
- `possible_informed`

These are heuristic labels, not proof of anything illicit.

## Project structure

```text
polymarket-stats/
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

## Requirements

- Python 3.11+
    
- PostgreSQL
    
- virtualenv or venv recommended
    

## Python dependencies

Install dependencies with:

```bash
pip install -r requirements.txt
```

Example `requirements.txt`:

```txt
Flask>=3.1
psycopg2-binary>=2.9
requests>=2.32
```

## PostgreSQL setup

Make sure PostgreSQL is running and reachable.

Example local DSN:

```bash
export PG_DSN=postgresql://postgres:postgres@localhost:5432/postgres
```

To verify Postgres is listening:

```bash
pg_isready -h localhost -p 5432
```

If needed, start PostgreSQL.

On Ubuntu/Debian:

```bash
sudo systemctl start postgresql
sudo systemctl status postgresql
```

On macOS with Homebrew:

```bash
brew services start postgresql
```

## Initialize the database

Run:

```bash
python init_db.py
```

This creates:

- `polymarket_trades`
    
- `user_features`
    
- `user_tags`
    
- `ingestion_state`
    
- `v_user_summary`
    

## Run the ingestor

### Normal run

```bash
python ingest.py
```

### Run with a reset cursor

To backfill from a point in the past, set `RESET_CURSOR_MINUTES`.

Example: 7 days

```bash
export RESET_CURSOR_MINUTES=10080
python ingest.py
```

Then return to normal mode:

```bash
unset RESET_CURSOR_MINUTES
python ingest.py
```

### Helpful environment variables

```bash
export PG_DSN=postgresql://postgres:postgres@localhost:5432/postgres
export POLL_SECONDS=30
export BACKFILL_MINUTES=10
export FEATURE_LOOKBACK_DAYS=90
export TRADE_PAGE_LIMIT=500
export DEBUG_DB_COUNTS=1
```

### Notes about ingestion depth

The current Polymarket public `/trades` ingestion logic is best for:

- continuous collection
    
- shallow recent backfills
    

It is not ideal for deep historical reconstruction using the public trades feed alone, because pagination is limited.

## Run the Flask dashboard

```bash
flask --app app run --debug
```

Then open:

```text
http://127.0.0.1:5000
```

## Dashboard pages

### `/`

Main dashboard with:

- tracked user stats
    
- wallet search
    
- tag filter
    
- sortable user table
    
- tag leaderboard
    

### `/user/<wallet>`

Wallet detail page with:

- feature summary
    
- tags and reasons
    
- top markets
    
- recent trades
    

### `/tags`

Tag leaderboard page showing:

- tag name
    
- user count
    
- average score
    
- max score
    

## Common workflow

Start with:

```bash
export PG_DSN=postgresql://postgres:postgres@localhost:5432/postgres
python init_db.py
python ingest.py
```

In another terminal:

```bash
export PG_DSN=postgresql://postgres:postgres@localhost:5432/postgres
flask --app app run --debug
```

## Useful SQL checks

See how much data you actually have:

```sql
select count(*) from polymarket_trades;
select count(distinct proxy_wallet) from polymarket_trades;
select count(*) from user_features;
select min(ts), max(ts) from polymarket_trades;
```

See top wallets:

```sql
select
  f.wallet,
  f.win_rate,
  f.realized_pnl,
  f.active_weeks,
  f.top_market_volume_share,
  array_agg(t.tag order by t.tag) as tags
from user_features f
left join user_tags t on t.wallet = f.wallet
group by 1,2,3,4,5
order by f.realized_pnl desc nulls last
limit 100;
```

## Why the dashboard may show fewer users than expected

A few common reasons:

- ingestion only started from a recent cursor
    
- the public trades endpoint only covers a limited recent pagination window
    
- the Flask table itself may have a `LIMIT` in SQL
    
- features are only refreshed for wallets touched by recent inserts
    

Check the database counts directly before assuming the UI is the source of truth.

## Tag definitions

Current tags are rule-based.

### `big_single_win`

Assigned when a wallet has at least one large realized win.

### `high_win_rate`

Assigned when a wallet has a high win rate over enough samples.

### `weekly_active`

Assigned when a wallet appears consistently active across multiple weeks.

### `single_market_specialist`

Assigned when one market dominates the wallet’s volume.

### `concentrated_bettor`

Assigned when the top three markets dominate the wallet’s volume.

### `large_size_trader`

Assigned when the wallet’s median bet size is high.

### `possible_informed`

Assigned when a wallet combines positive realized PnL, decent win rate, and sustained activity.

## Limitations

Current limitations include:

- public trades pagination is shallow
    
- realized PnL depends on closed-positions endpoint coverage
    
- no deep historical market outcome warehouse yet
    
- tags are heuristics, not ground truth
    
- no async job queue yet
    
- no authentication on the Flask dashboard
    

## Recommended next upgrades

Good next improvements:

- add pagination to the Flask UI
    
- add charts for wallet activity and PnL
    
- add a `resolved_markets` table
    
- compute per-market and per-category accuracy
    
- build a leaderboard discovery strategy for better wallet coverage
    
- add Docker Compose
    
- add Alembic migrations
    
- add caching for closed-position fetches
    
- add background workers
    

## Troubleshooting

### PostgreSQL connection refused

Example error:

```text
connection to server at "localhost" (127.0.0.1), port 5432 failed: Connection refused
```

This means Postgres is not reachable. Check:

```bash
pg_isready -h localhost -p 5432
ss -ltnp | grep 5432
```

### Flask starts but the table looks too small

Possible causes:

- SQL `LIMIT` in `app.py`
    
- not enough ingested trades
    
- cursor started too recently
    

Check database counts directly.

### Ingestor keeps fetching but not growing

Possible causes:

- cursor is too close to now
    
- repeated overlap with already ingested trades
    
- public trades pagination depth reached
    

Try a one-time cursor reset:

```bash
export RESET_CURSOR_MINUTES=10080
python ingest.py
```

### Network 400 on `/trades`

Make sure `TRADE_PAGE_LIMIT` is not above 500.

Use:

```bash
export TRADE_PAGE_LIMIT=500
```

## Security note

This project is intended for analytics and watchlist-style segmentation. Labels such as `possible_informed` are heuristic and should not be treated as proof of insider trading or misconduct.

## License

Add your preferred license here.

