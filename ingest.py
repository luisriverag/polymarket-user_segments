import os
import sys
import time
import json
import random
import hashlib
import statistics
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Tuple, Optional, Set, Dict

import requests
import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError

DATA_API = os.getenv("DATA_API", "https://data-api.polymarket.com")
PG_DSN = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5432/postgres")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
BACKFILL_MINUTES = int(os.getenv("BACKFILL_MINUTES", "10"))
FEATURE_LOOKBACK_DAYS = int(os.getenv("FEATURE_LOOKBACK_DAYS", "90"))

TRADE_PAGE_LIMIT = min(int(os.getenv("TRADE_PAGE_LIMIT", "500")), 500)
TRADE_OFFSETS = (0, 500, 1000)

CLOSED_POSITIONS_LIMIT = min(int(os.getenv("CLOSED_POSITIONS_LIMIT", "50")), 50)
CLOSED_POSITIONS_MAX_OFFSET = int(os.getenv("CLOSED_POSITIONS_MAX_OFFSET", "500"))

CLOSED_POSITIONS_REFRESH_HOURS = int(os.getenv("CLOSED_POSITIONS_REFRESH_HOURS", "12"))
MAX_CLOSED_POSITIONS_REFRESH_PER_LOOP = int(os.getenv("MAX_CLOSED_POSITIONS_REFRESH_PER_LOOP", "10"))

HTTP_MAX_RETRIES = int(os.getenv("HTTP_MAX_RETRIES", "5"))
HTTP_BACKOFF_BASE_SECONDS = float(os.getenv("HTTP_BACKOFF_BASE_SECONDS", "1.0"))
HTTP_BACKOFF_MAX_SECONDS = float(os.getenv("HTTP_BACKOFF_MAX_SECONDS", "30.0"))

MULTI_TIME_MIN_SPREAD_SECONDS = int(os.getenv("MULTI_TIME_MIN_SPREAD_SECONDS", "1800"))  # 30 min
SINGLE_TIME_MAX_SPREAD_SECONDS = int(os.getenv("SINGLE_TIME_MAX_SPREAD_SECONDS", "300"))  # 5 min

DEBUG_DB_COUNTS = os.getenv("DEBUG_DB_COUNTS", "0") == "1"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"

session = requests.Session()
session.headers.update({"User-Agent": "polymarket-user-segments/5.0"})


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_db():
    try:
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = True
        return conn
    except OperationalError as e:
        print("Could not connect to PostgreSQL.")
        print(f"PG_DSN={PG_DSN}")
        print(f"Original error: {e}")
        sys.exit(1)


def init_db(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
        cur.execute(
            """
            create table if not exists closed_positions_cache (
              wallet text primary key,
              fetched_at timestamptz not null,
              positions jsonb not null
            )
            """
        )
        cur.execute(
            """
            create index if not exists idx_closed_positions_cache_fetched_at
              on closed_positions_cache(fetched_at)
            """
        )


def state_get(conn, key: str) -> Optional[dict]:
    with conn.cursor() as cur:
        cur.execute("select value from ingestion_state where key = %s", (key,))
        row = cur.fetchone()
        return row[0] if row else None


def state_set(conn, key: str, value: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into ingestion_state(key, value, updated_at)
            values (%s, %s::jsonb, now())
            on conflict (key)
            do update set value = excluded.value, updated_at = now()
            """,
            (key, json.dumps(value)),
        )


def stable_trade_id(trade: dict) -> str:
    base = {
        "proxyWallet": trade.get("proxyWallet"),
        "conditionId": trade.get("conditionId"),
        "side": trade.get("side"),
        "price": trade.get("price"),
        "size": trade.get("size"),
        "timestamp": trade.get("timestamp"),
        "outcome": trade.get("outcome"),
        "transactionHash": trade.get("transactionHash"),
        "asset": trade.get("asset"),
    }
    payload = json.dumps(base, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_ts(value) -> datetime:
    return datetime.fromtimestamp(int(value), tz=timezone.utc)


def normalize_trade(trade: dict) -> Optional[dict]:
    if not trade.get("proxyWallet"):
        return None
    if not trade.get("conditionId"):
        return None
    if trade.get("timestamp") is None:
        return None

    try:
        price = float(trade.get("price") or 0)
        size = float(trade.get("size") or 0)
        ts = parse_ts(trade["timestamp"])
    except Exception:
        return None

    return {
        "trade_id": stable_trade_id(trade),
        "proxy_wallet": str(trade.get("proxyWallet")).lower(),
        "market_id": str(trade.get("conditionId")),
        "slug": trade.get("slug"),
        "title": trade.get("title"),
        "outcome": trade.get("outcome"),
        "side": trade.get("side"),
        "price": price,
        "size": size,
        "ts": ts,
        "raw": json.dumps(trade),
    }


def sleep_backoff(attempt: int):
    delay = min(HTTP_BACKOFF_MAX_SECONDS, HTTP_BACKOFF_BASE_SECONDS * (2 ** attempt))
    delay = delay * (0.8 + random.random() * 0.4)
    time.sleep(delay)


def fetch_json(url: str, params: dict) -> object:
    last_exc = None

    for attempt in range(HTTP_MAX_RETRIES):
        try:
            resp = session.get(url, params=params, timeout=30)

            if resp.status_code == 429:
                last_exc = requests.HTTPError(
                    f"429 Too Many Requests for url: {resp.url}",
                    response=resp,
                )
                if attempt < HTTP_MAX_RETRIES - 1:
                    sleep_backoff(attempt)
                    continue
                resp.raise_for_status()

            resp.raise_for_status()
            return resp.json()

        except requests.RequestException as e:
            last_exc = e
            if attempt < HTTP_MAX_RETRIES - 1:
                sleep_backoff(attempt)
                continue
            raise

    if last_exc:
        raise last_exc

    raise RuntimeError("fetch_json failed unexpectedly")


def extract_trade_list(data: object) -> List[dict]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if isinstance(data.get("data"), list):
            return data["data"]
        if isinstance(data.get("trades"), list):
            return data["trades"]
    return []


def extract_positions_list(data: object) -> List[dict]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if isinstance(data.get("data"), list):
            return data["data"]
        if isinstance(data.get("positions"), list):
            return data["positions"]
    return []


def fetch_recent_trades_paginated(since_ts: int) -> List[dict]:
    url = f"{DATA_API}/trades"
    all_trades: List[dict] = []
    seen_ids: Set[str] = set()

    for offset in TRADE_OFFSETS:
        params = {"limit": TRADE_PAGE_LIMIT, "offset": offset}
        data = fetch_json(url, params)
        page = extract_trade_list(data)

        if not page:
            break

        oldest_ts_in_page = None
        newer_in_page = 0

        for trade in page:
            try:
                ts = int(trade.get("timestamp", 0))
            except Exception:
                continue

            if oldest_ts_in_page is None or ts < oldest_ts_in_page:
                oldest_ts_in_page = ts

            if ts >= since_ts:
                trade_id = stable_trade_id(trade)
                if trade_id not in seen_ids:
                    seen_ids.add(trade_id)
                    all_trades.append(trade)
                    newer_in_page += 1

        if len(page) < TRADE_PAGE_LIMIT:
            break

        if oldest_ts_in_page is not None and oldest_ts_in_page < since_ts:
            break

        if newer_in_page == 0:
            break

    return all_trades


def upsert_trades(conn, trades: Iterable[dict]) -> Tuple[int, Set[str], int]:
    normalized = [normalize_trade(t) for t in trades]
    rows = [r for r in normalized if r is not None]

    if not rows:
        return 0, set(), 0

    sql = """
    insert into polymarket_trades (
      trade_id, proxy_wallet, market_id, slug, title, outcome, side, price, size, ts, raw
    ) values %s
    on conflict (trade_id) do nothing
    returning proxy_wallet
    """

    values = [
        (
            r["trade_id"],
            r["proxy_wallet"],
            r["market_id"],
            r["slug"],
            r["title"],
            r["outcome"],
            r["side"],
            r["price"],
            r["size"],
            r["ts"],
            r["raw"],
        )
        for r in rows
    ]

    inserted_wallets: Set[str] = set()

    with conn.cursor() as cur:
        returned = psycopg2.extras.execute_values(cur, sql, values, page_size=1000, fetch=True) or []
        for row in returned:
            inserted_wallets.add(row[0])

    return len(returned), inserted_wallets, len(rows)


def fetch_closed_positions_from_api(wallet: str) -> List[dict]:
    url = f"{DATA_API}/closed-positions"
    results: List[dict] = []
    offset = 0

    while True:
        params = {
            "user": wallet,
            "limit": CLOSED_POSITIONS_LIMIT,
            "offset": offset,
            "sortBy": "REALIZEDPNL",
            "sortDirection": "DESC",
        }

        data = fetch_json(url, params)
        page = extract_positions_list(data)

        if not page:
            break

        results.extend(page)

        if len(page) < CLOSED_POSITIONS_LIMIT:
            break

        offset += CLOSED_POSITIONS_LIMIT
        if offset > CLOSED_POSITIONS_MAX_OFFSET:
            break

    return results


def get_cached_closed_positions(conn, wallet: str) -> Tuple[Optional[List[dict]], Optional[datetime]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select fetched_at, positions
            from closed_positions_cache
            where wallet = %s
            """,
            (wallet,),
        )
        row = cur.fetchone()
        if not row:
            return None, None
        return row["positions"], row["fetched_at"]


def set_cached_closed_positions(conn, wallet: str, positions: List[dict]):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into closed_positions_cache(wallet, fetched_at, positions)
            values (%s, now(), %s::jsonb)
            on conflict (wallet)
            do update set fetched_at = now(), positions = excluded.positions
            """,
            (wallet, json.dumps(positions)),
        )


def should_refresh_closed_positions(fetched_at: Optional[datetime]) -> bool:
    if fetched_at is None:
        return True
    return (utc_now() - fetched_at) >= timedelta(hours=CLOSED_POSITIONS_REFRESH_HOURS)


def get_closed_positions_cached(conn, wallet: str, allow_refresh: bool) -> Tuple[List[dict], bool]:
    cached_positions, fetched_at = get_cached_closed_positions(conn, wallet)

    if cached_positions is not None and not should_refresh_closed_positions(fetched_at):
        return cached_positions, False

    if cached_positions is not None and not allow_refresh:
        return cached_positions, False

    try:
        fresh = fetch_closed_positions_from_api(wallet)
        set_cached_closed_positions(conn, wallet, fresh)
        return fresh, True
    except requests.RequestException:
        if cached_positions is not None:
            return cached_positions, False
        raise


def build_market_timing_features(trades: List[dict]) -> Dict[str, float]:
    by_market: Dict[str, List[datetime]] = {}
    for t in trades:
        by_market.setdefault(t["market_id"], []).append(t["ts"])

    repeated_entry_markets = 0
    single_time_markets = 0
    total_markets = len(by_market)

    for timestamps in by_market.values():
        timestamps = sorted(timestamps)
        count = len(timestamps)
        spread_seconds = (timestamps[-1] - timestamps[0]).total_seconds() if count > 1 else 0

        if count >= 2 and spread_seconds >= MULTI_TIME_MIN_SPREAD_SECONDS:
            repeated_entry_markets += 1

        if count == 1 or spread_seconds <= SINGLE_TIME_MAX_SPREAD_SECONDS:
            single_time_markets += 1

    repeated_entry_market_ratio = (
        repeated_entry_markets / total_markets if total_markets > 0 else 0.0
    )
    single_time_market_ratio = (
        single_time_markets / total_markets if total_markets > 0 else 0.0
    )

    return {
        "repeated_entry_markets": repeated_entry_markets,
        "repeated_entry_market_ratio": repeated_entry_market_ratio,
        "single_time_markets": single_time_markets,
        "single_time_market_ratio": single_time_market_ratio,
    }


def compute_features(conn, wallet: str, allow_closed_positions_refresh: bool) -> dict:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select market_id, ts, price, size
            from polymarket_trades
            where proxy_wallet = %s
              and ts >= now() - (%s || ' days')::interval
            order by ts desc
            """,
            (wallet, FEATURE_LOOKBACK_DAYS),
        )
        trades = cur.fetchall()

    total_trades = len(trades)
    notionals = [float(t["price"]) * float(t["size"]) for t in trades]
    total_volume = sum(notionals)
    avg_bet_size = (sum(notionals) / len(notionals)) if notionals else 0.0
    median_bet_size = statistics.median(notionals) if notionals else 0.0

    active_weeks = len(
        {f"{t['ts'].isocalendar().year}-{t['ts'].isocalendar().week}" for t in trades}
    )

    market_volume: Dict[str, float] = {}
    for t in trades:
        notional = float(t["price"]) * float(t["size"])
        market_volume[t["market_id"]] = market_volume.get(t["market_id"], 0.0) + notional

    sorted_market_vols = sorted(market_volume.values(), reverse=True)
    distinct_markets = len(market_volume)

    top_market_volume_share = (
        sorted_market_vols[0] / total_volume if total_volume > 0 and sorted_market_vols else 0.0
    )
    top3_market_volume_share = (
        sum(sorted_market_vols[:3]) / total_volume if total_volume > 0 and sorted_market_vols else 0.0
    )

    timing_features = build_market_timing_features(trades)

    closed_positions = []
    realized_pnl = None
    win_rate = None
    biggest_single_win = None

    try:
        closed_positions, _ = get_closed_positions_cached(
            conn,
            wallet,
            allow_refresh=allow_closed_positions_refresh,
        )
    except requests.RequestException as e:
        print(f"[closed_positions_warn] wallet={wallet} err={e}")

    if closed_positions:
        pnls = []
        wins = 0
        total_closed = 0

        for pos in closed_positions:
            pnl = pos.get("realizedPnl", pos.get("pnl", 0))
            try:
                pnl = float(pnl or 0)
            except Exception:
                pnl = 0.0

            pnls.append(pnl)
            total_closed += 1
            if pnl > 0:
                wins += 1

        realized_pnl = sum(pnls)
        win_rate = (wins / total_closed) if total_closed > 0 else None
        biggest_single_win = max([p for p in pnls if p > 0], default=0.0)

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
        "repeated_entry_markets": timing_features["repeated_entry_markets"],
        "repeated_entry_market_ratio": timing_features["repeated_entry_market_ratio"],
        "single_time_markets": timing_features["single_time_markets"],
        "single_time_market_ratio": timing_features["single_time_market_ratio"],
    }


def upsert_features(conn, features: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into user_features (
              wallet, updated_at, total_trades, total_volume, win_rate, realized_pnl,
              active_weeks, distinct_markets, top_market_volume_share, top3_market_volume_share,
              avg_bet_size, median_bet_size, biggest_single_win,
              repeated_entry_markets, repeated_entry_market_ratio,
              single_time_markets, single_time_market_ratio
            ) values (
              %(wallet)s, now(), %(total_trades)s, %(total_volume)s, %(win_rate)s, %(realized_pnl)s,
              %(active_weeks)s, %(distinct_markets)s, %(top_market_volume_share)s, %(top3_market_volume_share)s,
              %(avg_bet_size)s, %(median_bet_size)s, %(biggest_single_win)s,
              %(repeated_entry_markets)s, %(repeated_entry_market_ratio)s,
              %(single_time_markets)s, %(single_time_market_ratio)s
            )
            on conflict (wallet) do update set
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
              biggest_single_win = excluded.biggest_single_win,
              repeated_entry_markets = excluded.repeated_entry_markets,
              repeated_entry_market_ratio = excluded.repeated_entry_market_ratio,
              single_time_markets = excluded.single_time_markets,
              single_time_market_ratio = excluded.single_time_market_ratio
            """,
            features,
        )


def replace_tags(conn, wallet: str, tags: List[Tuple[str, float, dict]]):
    with conn.cursor() as cur:
        cur.execute("delete from user_tags where wallet = %s", (wallet,))
        for tag, score, reason in tags:
            cur.execute(
                """
                insert into user_tags (wallet, tag, score, reason, updated_at)
                values (%s, %s, %s, %s::jsonb, now())
                """,
                (wallet, tag, score, json.dumps(reason)),
            )


def tag_user(features: dict) -> List[Tuple[str, float, dict]]:
    tags: List[Tuple[str, float, dict]] = []

    biggest_single_win = features["biggest_single_win"] or 0.0
    win_rate = features["win_rate"]
    total_trades = features["total_trades"] or 0
    active_weeks = features["active_weeks"] or 0
    top_market_share = features["top_market_volume_share"] or 0.0
    top3_market_share = features["top3_market_volume_share"] or 0.0
    median_bet_size = features["median_bet_size"] or 0.0
    realized_pnl = features["realized_pnl"] or 0.0
    distinct_markets = features["distinct_markets"] or 0
    repeated_entry_markets = features["repeated_entry_markets"] or 0
    repeated_entry_market_ratio = features["repeated_entry_market_ratio"] or 0.0
    single_time_markets = features["single_time_markets"] or 0
    single_time_market_ratio = features["single_time_market_ratio"] or 0.0

    if biggest_single_win >= 5000:
        tags.append(("big_single_win", min(1.0, biggest_single_win / 20000.0), {
            "biggest_single_win": biggest_single_win
        }))

    if win_rate is not None and win_rate > 0.75 and total_trades >= 12:
        tags.append(("high_win_rate", float(win_rate), {
            "win_rate": win_rate,
            "total_trades": total_trades
        }))

    if active_weeks >= 4:
        tags.append(("weekly_active", min(1.0, active_weeks / 8.0), {
            "active_weeks": active_weeks
        }))

    if top_market_share >= 0.60 and distinct_markets >= 1:
        tags.append(("single_market_specialist", float(top_market_share), {
            "top_market_volume_share": top_market_share,
            "distinct_markets": distinct_markets
        }))

    if top3_market_share >= 0.80:
        tags.append(("concentrated_bettor", float(top3_market_share), {
            "top3_market_volume_share": top3_market_share
        }))

    if median_bet_size >= 250:
        tags.append(("large_size_trader", min(1.0, median_bet_size / 2000.0), {
            "median_bet_size": median_bet_size
        }))

    if realized_pnl > 0 and (win_rate or 0) >= 0.65 and active_weeks >= 4:
        score = min(1.0, ((win_rate or 0) * 0.5) + min(realized_pnl / 10000.0, 0.5))
        tags.append(("possible_informed", score, {
            "realized_pnl": realized_pnl,
            "win_rate": win_rate,
            "active_weeks": active_weeks
        }))

    if repeated_entry_markets >= 2 and repeated_entry_market_ratio >= 0.40:
        tags.append(("multi_time_event_scaler", min(1.0, repeated_entry_market_ratio), {
            "repeated_entry_markets": repeated_entry_markets,
            "repeated_entry_market_ratio": repeated_entry_market_ratio
        }))

    if single_time_markets >= 3 and single_time_market_ratio >= 0.70:
        tags.append(("single_time_event_entry", min(1.0, single_time_market_ratio), {
            "single_time_markets": single_time_markets,
            "single_time_market_ratio": single_time_market_ratio
        }))

    return tags


def split_wallets_for_refresh(conn, wallets: Iterable[str]) -> Tuple[List[str], List[str]]:
    wallets = list(dict.fromkeys(wallets))
    if not wallets:
        return [], []

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select wallet, fetched_at
            from closed_positions_cache
            where wallet = any(%s)
            """,
            (wallets,),
        )
        rows = cur.fetchall()

    fetched_map = {r["wallet"]: r["fetched_at"] for r in rows}

    stale_wallets = []
    fresh_wallets = []

    for wallet in wallets:
        fetched_at = fetched_map.get(wallet)
        if should_refresh_closed_positions(fetched_at):
            stale_wallets.append(wallet)
        else:
            fresh_wallets.append(wallet)

    stale_wallets.sort(key=lambda w: fetched_map.get(w) or datetime(1970, 1, 1, tzinfo=timezone.utc))
    refresh_now = stale_wallets[:MAX_CLOSED_POSITIONS_REFRESH_PER_LOOP]
    no_refresh = fresh_wallets + stale_wallets[MAX_CLOSED_POSITIONS_REFRESH_PER_LOOP:]

    return refresh_now, no_refresh


def refresh_features_and_tags(conn, wallets: Iterable[str]):
    refresh_now, no_refresh = split_wallets_for_refresh(conn, wallets)

    for wallet in refresh_now:
        try:
            features = compute_features(conn, wallet, allow_closed_positions_refresh=True)
            upsert_features(conn, features)
            replace_tags(conn, wallet, tag_user(features))
        except Exception as e:
            print(f"[feature_error] wallet={wallet} err={e}")

    for wallet in no_refresh:
        try:
            features = compute_features(conn, wallet, allow_closed_positions_refresh=False)
            upsert_features(conn, features)
            replace_tags(conn, wallet, tag_user(features))
        except Exception as e:
            print(f"[feature_error] wallet={wallet} err={e}")


def ensure_cursor(conn) -> int:
    state = state_get(conn, "trades_cursor")
    if state and state.get("since_ts") is not None:
        return int(state["since_ts"])
    since_ts = int((utc_now() - timedelta(minutes=BACKFILL_MINUTES)).timestamp())
    state_set(conn, "trades_cursor", {"since_ts": since_ts})
    return since_ts


def reset_cursor(conn, minutes_back: int):
    since_ts = int((utc_now() - timedelta(minutes=minutes_back)).timestamp())
    state_set(conn, "trades_cursor", {"since_ts": since_ts})
    print(f"Cursor reset to since_ts={since_ts} ({minutes_back} minutes back)")


def debug_db_counts(conn):
    with conn.cursor() as cur:
        cur.execute("select count(*) from polymarket_trades")
        trades_count = cur.fetchone()[0]

        cur.execute("select count(distinct proxy_wallet) from polymarket_trades")
        wallet_count = cur.fetchone()[0]

        cur.execute("select count(*) from user_features")
        features_count = cur.fetchone()[0]

        cur.execute("select count(*) from closed_positions_cache")
        closed_cache_count = cur.fetchone()[0]

    print(
        f"[db] trades={trades_count} distinct_wallets={wallet_count} "
        f"user_features={features_count} closed_positions_cache={closed_cache_count}"
    )


def main():
    conn = get_db()
    init_db(conn)

    if os.getenv("RESET_CURSOR_MINUTES"):
        reset_cursor(conn, int(os.getenv("RESET_CURSOR_MINUTES")))

    since_ts = ensure_cursor(conn)
    print(f"Starting ingestion from since_ts={since_ts}")

    while True:
        try:
            trades = fetch_recent_trades_paginated(since_ts)
            inserted_count, touched_wallets, attempted_count = upsert_trades(conn, trades)

            max_seen = since_ts
            for trade in trades:
                try:
                    max_seen = max(max_seen, int(trade.get("timestamp", 0)))
                except Exception:
                    continue

            since_ts = max(0, max_seen - 30)
            state_set(conn, "trades_cursor", {"since_ts": since_ts})

            if touched_wallets:
                refresh_features_and_tags(conn, touched_wallets)

            print(
                f"{datetime.now(timezone.utc).isoformat()} "
                f"fetched={len(trades)} attempted={attempted_count} inserted={inserted_count} "
                f"wallets_touched={len(touched_wallets)} next_since={since_ts}"
            )

            if DEBUG_DB_COUNTS:
                debug_db_counts(conn)

        except requests.RequestException as e:
            print(f"[network_error] {e}")
        except Exception as e:
            print(f"[loop_error] {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
