import os
import time
import json
import math
import hashlib
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras

DATA_API = "https://data-api.polymarket.com"
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
BACKFILL_MINUTES = int(os.getenv("BACKFILL_MINUTES", "10"))

PG_DSN = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5432/postgres")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_db():
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    return conn


def init_db(conn):
    ddl = """
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

    create table if not exists ingestion_state (
      key text primary key,
      value jsonb not null,
      updated_at timestamptz not null default now()
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)


def state_get(conn, key: str) -> Optional[dict]:
    with conn.cursor() as cur:
        cur.execute("select value from ingestion_state where key=%s", (key,))
        row = cur.fetchone()
        return row[0] if row else None


def state_set(conn, key: str, value: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into ingestion_state(key, value, updated_at)
            values (%s, %s, now())
            on conflict (key)
            do update set value=excluded.value, updated_at=now()
            """,
            (key, json.dumps(value)),
        )


def stable_trade_id(t: dict) -> str:
    """
    The trades endpoint docs show transactionHash but do not guarantee
    a unique trade id in the snippet, so build an idempotent hash from stable fields.
    """
    base = {
        "proxyWallet": t.get("proxyWallet"),
        "conditionId": t.get("conditionId"),
        "side": t.get("side"),
        "price": t.get("price"),
        "size": t.get("size"),
        "timestamp": t.get("timestamp"),
        "outcome": t.get("outcome"),
        "transactionHash": t.get("transactionHash"),
        "asset": t.get("asset"),
    }
    payload = json.dumps(base, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def fetch_trades_page(params: dict) -> List[dict]:
    r = requests.get(f"{DATA_API}/trades", params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError(f"Unexpected response: {type(data)}")
    return data


def fetch_recent_trades(since_ts: int) -> List[dict]:
    """
    Pull recent trades using timestamp lower bound if supported by the API instance.
    Falls back to repeated recent polling windows if needed.
    """
    # Depending on deployment/version, filters may differ. Keep the call small and robust.
    # You can extend with limit/offset or cursor if your observed endpoint supports them.
    params = {"limit": 500, "since": since_ts}
    try:
        return fetch_trades_page(params)
    except Exception:
        # Fallback: plain fetch, then local filter
        trades = fetch_trades_page({"limit": 500})
        return [t for t in trades if int(t.get("timestamp", 0)) >= since_ts]


def normalize_trade(t: dict) -> dict:
    ts = datetime.fromtimestamp(int(t["timestamp"]), tz=timezone.utc)
    return {
        "trade_id": stable_trade_id(t),
        "proxy_wallet": (t.get("proxyWallet") or "").lower(),
        "market_id": t.get("conditionId"),
        "slug": t.get("slug"),
        "title": t.get("title"),
        "outcome": t.get("outcome"),
        "side": t.get("side"),
        "price": float(t.get("price") or 0),
        "size": float(t.get("size") or 0),
        "ts": ts,
        "raw": json.dumps(t),
    }


def upsert_trades(conn, trades: Iterable[dict]) -> int:
    rows = [normalize_trade(t) for t in trades if t.get("proxyWallet") and t.get("conditionId")]
    if not rows:
        return 0

    sql = """
    insert into polymarket_trades (
      trade_id, proxy_wallet, market_id, slug, title, outcome, side,
      price, size, ts, raw
    )
    values %s
    on conflict (trade_id) do nothing
    """
    values = [
        (
            r["trade_id"], r["proxy_wallet"], r["market_id"], r["slug"], r["title"],
            r["outcome"], r["side"], r["price"], r["size"], r["ts"], r["raw"]
        )
        for r in rows
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
    return len(rows)


def get_candidate_wallets(conn, lookback_days: int = 90) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select distinct proxy_wallet
            from polymarket_trades
            where ts >= now() - (%s || ' days')::interval
            """,
            (lookback_days,),
        )
        return [r[0] for r in cur.fetchall()]


def fetch_closed_positions(wallet: str) -> List[dict]:
    """
    Public profile endpoints include closed positions according to Polymarket docs.
    Exact filter names can vary, so keep the function tolerant.
    """
    url = f"{DATA_API}/closed-positions"
    try:
        r = requests.get(url, params={"user": wallet}, timeout=30)
        if r.status_code == 200:
            data = r.json()
            return data if isinstance(data, list) else []
    except Exception:
        pass
    return []


def compute_features(conn, wallet: str) -> dict:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select market_id, ts, price, size
            from polymarket_trades
            where proxy_wallet = %s
              and ts >= now() - interval '90 days'
            order by ts desc
            """,
            (wallet,),
        )
        trades = cur.fetchall()

    total_trades = len(trades)
    notionals = [float(t["price"]) * float(t["size"]) for t in trades]
    total_volume = sum(notionals)
    avg_bet_size = (sum(notionals) / len(notionals)) if notionals else 0.0
    median_bet_size = statistics.median(notionals) if notionals else 0.0

    # Active ISO weeks
    active_weeks = len({
        f"{t['ts'].isocalendar().year}-{t['ts'].isocalendar().week}"
        for t in trades
    })

    # Market concentration
    by_market = {}
    for t, n in zip(trades, notionals):
        by_market[t["market_id"]] = by_market.get(t["market_id"], 0.0) + n

    sorted_market_vols = sorted(by_market.values(), reverse=True)
    distinct_markets = len(by_market)
    top_market_volume_share = (sorted_market_vols[0] / total_volume) if total_volume and sorted_market_vols else 0.0
    top3_market_volume_share = (sum(sorted_market_vols[:3]) / total_volume) if total_volume and sorted_market_vols else 0.0

    # Closed positions / realized metrics
    closed_positions = fetch_closed_positions(wallet)
    pnls = []
    wins = 0
    total_closed = 0

    for p in closed_positions:
        pnl = p.get("realizedPnl") or p.get("pnl") or 0
        try:
            pnl = float(pnl)
        except Exception:
            pnl = 0.0
        pnls.append(pnl)
        total_closed += 1
        if pnl > 0:
            wins += 1

    realized_pnl = sum(pnls)
    win_rate = (wins / total_closed) if total_closed else None
    biggest_single_win = max([x for x in pnls if x > 0], default=0.0)

    return {
        "wallet": wallet,
        "total_trades": total_trades,
        "total_volume": total_volume,
        "win_rate": win_rate,
        "realized_pnl": realized_pnl,
        "active_weeks": active_weeks,
        "distinct_markets": distinct_markets,
        "top_market_volume_share": top_market_volume_share,
        "top3_market_volume_share": top3_market_volume_share,
        "avg_bet_size": avg_bet_size,
        "median_bet_size": median_bet_size,
        "biggest_single_win": biggest_single_win,
    }


def upsert_features(conn, f: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into user_features (
              wallet, updated_at, total_trades, total_volume, win_rate, realized_pnl,
              active_weeks, distinct_markets, top_market_volume_share, top3_market_volume_share,
              avg_bet_size, median_bet_size, biggest_single_win
            ) values (
              %(wallet)s, now(), %(total_trades)s, %(total_volume)s, %(win_rate)s, %(realized_pnl)s,
              %(active_weeks)s, %(distinct_markets)s, %(top_market_volume_share)s, %(top3_market_volume_share)s,
              %(avg_bet_size)s, %(median_bet_size)s, %(biggest_single_win)s
            )
            on conflict (wallet)
            do update set
              updated_at = now(),
              total_trades = excluded.total_trades,
              total_volume = excluded.total_volume,
              win_rate = excluded.win_rate,
              realized_pnl = excluded.realized_pnl,
              active_weeks = excluded.active_weeks,
              distinct_markets = excluded.distinct_markets,
              top_market_volume_share = excluded.top_market_volume_share,
              top3_market_volume_share = excluded.top3_market_volume_share,
              avg_bet_size = excluded.avg_bet_size,
              median_bet_size = excluded.median_bet_size,
              biggest_single_win = excluded.biggest_single_win
            """,
            f,
        )


def replace_tags(conn, wallet: str, tags: List[Tuple[str, float, dict]]):
    with conn.cursor() as cur:
        cur.execute("delete from user_tags where wallet=%s", (wallet,))
        for tag, score, reason in tags:
            cur.execute(
                """
                insert into user_tags (wallet, tag, score, reason, updated_at)
                values (%s, %s, %s, %s, now())
                """,
                (wallet, tag, score, json.dumps(reason)),
            )


def tag_user(f: dict) -> List[Tuple[str, float, dict]]:
    tags: List[Tuple[str, float, dict]] = []

    # Your examples
    if f["biggest_single_win"] >= 5000:
        tags.append(("big_single_win", min(1.0, f["biggest_single_win"] / 20000), {
            "biggest_single_win": f["biggest_single_win"]
        }))

    if f["win_rate"] is not None and f["win_rate"] > 0.75 and f["total_trades"] >= 12:
        tags.append(("high_win_rate", f["win_rate"], {
            "win_rate": f["win_rate"],
            "total_trades": f["total_trades"]
        }))

    if f["active_weeks"] >= 4:
        tags.append(("weekly_active", min(1.0, f["active_weeks"] / 8), {
            "active_weeks": f["active_weeks"]
        }))

    if f["top_market_volume_share"] >= 0.60 and f["distinct_markets"] >= 1:
        tags.append(("single_market_specialist", f["top_market_volume_share"], {
            "top_market_volume_share": f["top_market_volume_share"],
            "distinct_markets": f["distinct_markets"]
        }))

    # Similar tags
    if f["top3_market_volume_share"] >= 0.80:
        tags.append(("concentrated_bettor", f["top3_market_volume_share"], {
            "top3_market_volume_share": f["top3_market_volume_share"]
        }))

    if f["median_bet_size"] >= 250:
        tags.append(("large_size_trader", min(1.0, f["median_bet_size"] / 2000), {
            "median_bet_size": f["median_bet_size"]
        }))

    if (f["realized_pnl"] or 0) > 0 and (f["win_rate"] or 0) >= 0.65 and f["active_weeks"] >= 4:
        score = min(1.0, ((f["win_rate"] or 0) * 0.5) + min((f["realized_pnl"] or 0) / 10000, 0.5))
        tags.append(("possible_informed", score, {
            "realized_pnl": f["realized_pnl"],
            "win_rate": f["win_rate"],
            "active_weeks": f["active_weeks"]
        }))

    return tags


def refresh_features_and_tags(conn):
    wallets = get_candidate_wallets(conn, lookback_days=90)
    for wallet in wallets:
        try:
            f = compute_features(conn, wallet)
            upsert_features(conn, f)
            replace_tags(conn, wallet, tag_user(f))
        except Exception as e:
            print(f"[feature_error] wallet={wallet} err={e}")


def main():
    conn = get_db()
    init_db(conn)

    state = state_get(conn, "trades_cursor")
    if state and "since_ts" in state:
        since_ts = int(state["since_ts"])
    else:
        since_ts = int((utc_now() - timedelta(minutes=BACKFILL_MINUTES)).timestamp())

    while True:
        try:
            trades = fetch_recent_trades(since_ts)
            inserted = upsert_trades(conn, trades)

            max_seen = since_ts
            for t in trades:
                try:
                    max_seen = max(max_seen, int(t.get("timestamp", 0)))
                except Exception:
                    pass

            # overlap by 30s to avoid missing edge trades
            since_ts = max(0, max_seen - 30)
            state_set(conn, "trades_cursor", {"since_ts": since_ts})

            refresh_features_and_tags(conn)

            print(
                f"{datetime.utcnow().isoformat()} "
                f"fetched={len(trades)} inserted={inserted} next_since={since_ts}"
            )
        except Exception as e:
            print(f"[loop_error] {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
