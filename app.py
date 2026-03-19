import math
import os
from urllib.parse import urlencode

import psycopg2
import psycopg2.extras
from flask import Flask, abort, render_template, request

PG_DSN = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5432/postgres")

app = Flask(__name__)


def get_db():
    return psycopg2.connect(PG_DSN)


@app.template_filter("pct")
def pct_filter(v):
    if v is None:
        return "—"
    return f"{float(v) * 100:.1f}%"


@app.template_filter("num")
def num_filter(v):
    if v is None:
        return "—"
    return f"{float(v):,.2f}"


@app.template_filter("intcomma")
def intcomma_filter(v):
    if v is None:
        return "—"
    return f"{int(v):,}"


def parse_tag_list_param(name: str):
    values = request.args.getlist(name)
    if not values:
        raw = request.args.get(name, "").strip()
        if raw:
            values = [x.strip() for x in raw.split(",") if x.strip()]
    return sorted(set(v for v in values if v))


def parse_float_arg(name: str):
    raw = request.args.get(name, "").strip()
    if not raw:
        return "", None
    try:
        return raw, float(raw)
    except ValueError:
        return "", None


def parse_int_arg(name: str):
    raw = request.args.get(name, "").strip()
    if not raw:
        return "", None
    try:
        return raw, int(raw)
    except ValueError:
        return "", None


def build_query_params(**overrides):
    args = request.args.to_dict(flat=False)

    for key, value in overrides.items():
        if value is None:
            args.pop(key, None)
            continue

        if isinstance(value, list):
            args[key] = [str(v) for v in value]
        else:
            args[key] = [str(value)]

    return urlencode(args, doseq=True)


@app.context_processor
def utility_processor():
    return {"build_query_params": build_query_params}


def build_sort_clause(sort: str, direction: str):
    allowed_sorts = {
        "wallet": "f.wallet",
        "realized_pnl": "f.realized_pnl",
        "win_rate": "f.win_rate",
        "total_volume": "f.total_volume",
        "total_trades": "f.total_trades",
        "active_weeks": "f.active_weeks",
        "biggest_single_win": "f.biggest_single_win",
        "distinct_markets": "f.distinct_markets",
        "top_market_volume_share": "f.top_market_volume_share",
        "top3_market_volume_share": "f.top3_market_volume_share",
        "avg_bet_size": "f.avg_bet_size",
        "median_bet_size": "f.median_bet_size",
        "repeated_entry_market_ratio": "f.repeated_entry_market_ratio",
        "single_time_market_ratio": "f.single_time_market_ratio",
    }
    sort_sql = allowed_sorts.get(sort, "f.realized_pnl")
    dir_sql = "asc" if direction == "asc" else "desc"
    return sort_sql, dir_sql


def apply_profile_preset(where_parts, where_params, profile_preset: str):
    if profile_preset == "specialist":
        where_parts.append("coalesce(f.top_market_volume_share, 0) >= %s")
        where_params.append(0.60)
        where_parts.append("coalesce(f.distinct_markets, 0) <= %s")
        where_params.append(5)

    elif profile_preset == "diversified":
        where_parts.append("coalesce(f.distinct_markets, 0) >= %s")
        where_params.append(10)
        where_parts.append("coalesce(f.top_market_volume_share, 0) <= %s")
        where_params.append(0.35)

    elif profile_preset == "whale":
        where_parts.append("coalesce(f.median_bet_size, 0) >= %s")
        where_params.append(500)
        where_parts.append("coalesce(f.total_volume, 0) >= %s")
        where_params.append(5000)

    elif profile_preset == "consistent_active":
        where_parts.append("coalesce(f.active_weeks, 0) >= %s")
        where_params.append(4)
        where_parts.append("coalesce(f.total_trades, 0) >= %s")
        where_params.append(15)

    elif profile_preset == "concentrated_sharp":
        where_parts.append("coalesce(f.top3_market_volume_share, 0) >= %s")
        where_params.append(0.80)
        where_parts.append("coalesce(f.win_rate, 0) >= %s")
        where_params.append(0.65)

    elif profile_preset == "casual":
        where_parts.append("coalesce(f.total_trades, 0) <= %s")
        where_params.append(10)
        where_parts.append("coalesce(f.active_weeks, 0) <= %s")
        where_params.append(2)

    elif profile_preset == "multi_time_scaler":
        where_parts.append("coalesce(f.repeated_entry_market_ratio, 0) >= %s")
        where_params.append(0.40)

    elif profile_preset == "single_time_entry":
        where_parts.append("coalesce(f.single_time_market_ratio, 0) >= %s")
        where_params.append(0.70)


@app.route("/")
def index():
    sort = request.args.get("sort", "realized_pnl")
    direction = request.args.get("dir", "desc")

    include_tags = parse_tag_list_param("tag")
    exclude_tags = parse_tag_list_param("exclude_tag")

    tag_mode = request.args.get("tag_mode", "any").strip().lower()
    if tag_mode not in {"any", "all", "exact"}:
        tag_mode = "any"

    tag_search = request.args.get("tag_search", "").strip().lower()
    profile_preset = request.args.get("profile_preset", "").strip().lower()

    min_tag_score_raw, min_tag_score = parse_float_arg("min_tag_score")
    q = request.args.get("q", "").strip().lower()

    min_pnl_raw, min_pnl = parse_float_arg("min_pnl")
    min_win_rate_raw, min_win_rate = parse_float_arg("min_win_rate")
    min_trades_raw, min_trades = parse_int_arg("min_trades")

    min_active_weeks_raw, min_active_weeks = parse_int_arg("min_active_weeks")
    max_active_weeks_raw, max_active_weeks = parse_int_arg("max_active_weeks")

    min_distinct_markets_raw, min_distinct_markets = parse_int_arg("min_distinct_markets")
    max_distinct_markets_raw, max_distinct_markets = parse_int_arg("max_distinct_markets")

    min_top_market_share_raw, min_top_market_share = parse_float_arg("min_top_market_share")
    max_top_market_share_raw, max_top_market_share = parse_float_arg("max_top_market_share")

    min_top3_market_share_raw, min_top3_market_share = parse_float_arg("min_top3_market_share")
    max_top3_market_share_raw, max_top3_market_share = parse_float_arg("max_top3_market_share")

    min_avg_bet_size_raw, min_avg_bet_size = parse_float_arg("min_avg_bet_size")
    min_median_bet_size_raw, min_median_bet_size = parse_float_arg("min_median_bet_size")
    min_biggest_single_win_raw, min_biggest_single_win = parse_float_arg("min_biggest_single_win")

    min_repeated_entry_ratio_raw, min_repeated_entry_ratio = parse_float_arg("min_repeated_entry_ratio")
    min_single_time_ratio_raw, min_single_time_ratio = parse_float_arg("min_single_time_ratio")

    if min_win_rate is not None and min_win_rate > 1:
        min_win_rate = min_win_rate / 100.0
        min_win_rate_raw = str(min_win_rate)

    if min_top_market_share is not None and min_top_market_share > 1:
        min_top_market_share = min_top_market_share / 100.0
        min_top_market_share_raw = str(min_top_market_share)
    if max_top_market_share is not None and max_top_market_share > 1:
        max_top_market_share = max_top_market_share / 100.0
        max_top_market_share_raw = str(max_top_market_share)
    if min_top3_market_share is not None and min_top3_market_share > 1:
        min_top3_market_share = min_top3_market_share / 100.0
        min_top3_market_share_raw = str(min_top3_market_share)
    if max_top3_market_share is not None and max_top3_market_share > 1:
        max_top3_market_share = max_top3_market_share / 100.0
        max_top3_market_share_raw = str(max_top3_market_share)

    if min_repeated_entry_ratio is not None and min_repeated_entry_ratio > 1:
        min_repeated_entry_ratio = min_repeated_entry_ratio / 100.0
        min_repeated_entry_ratio_raw = str(min_repeated_entry_ratio)
    if min_single_time_ratio is not None and min_single_time_ratio > 1:
        min_single_time_ratio = min_single_time_ratio / 100.0
        min_single_time_ratio_raw = str(min_single_time_ratio)

    page = max(int(request.args.get("page", 1) or 1), 1)
    per_page = min(max(int(request.args.get("per_page", 50) or 50), 1), 200)

    sort_sql, dir_sql = build_sort_clause(sort, direction)

    where_parts = ["1=1"]
    where_params = []

    if q:
        where_parts.append("f.wallet like %s")
        where_params.append(f"%{q}%")

    if min_pnl is not None:
        where_parts.append("coalesce(f.realized_pnl, 0) >= %s")
        where_params.append(min_pnl)

    if min_win_rate is not None:
        where_parts.append("coalesce(f.win_rate, 0) >= %s")
        where_params.append(min_win_rate)

    if min_trades is not None:
        where_parts.append("coalesce(f.total_trades, 0) >= %s")
        where_params.append(min_trades)

    if min_active_weeks is not None:
        where_parts.append("coalesce(f.active_weeks, 0) >= %s")
        where_params.append(min_active_weeks)

    if max_active_weeks is not None:
        where_parts.append("coalesce(f.active_weeks, 0) <= %s")
        where_params.append(max_active_weeks)

    if min_distinct_markets is not None:
        where_parts.append("coalesce(f.distinct_markets, 0) >= %s")
        where_params.append(min_distinct_markets)

    if max_distinct_markets is not None:
        where_parts.append("coalesce(f.distinct_markets, 0) <= %s")
        where_params.append(max_distinct_markets)

    if min_top_market_share is not None:
        where_parts.append("coalesce(f.top_market_volume_share, 0) >= %s")
        where_params.append(min_top_market_share)

    if max_top_market_share is not None:
        where_parts.append("coalesce(f.top_market_volume_share, 0) <= %s")
        where_params.append(max_top_market_share)

    if min_top3_market_share is not None:
        where_parts.append("coalesce(f.top3_market_volume_share, 0) >= %s")
        where_params.append(min_top3_market_share)

    if max_top3_market_share is not None:
        where_parts.append("coalesce(f.top3_market_volume_share, 0) <= %s")
        where_params.append(max_top3_market_share)

    if min_avg_bet_size is not None:
        where_parts.append("coalesce(f.avg_bet_size, 0) >= %s")
        where_params.append(min_avg_bet_size)

    if min_median_bet_size is not None:
        where_parts.append("coalesce(f.median_bet_size, 0) >= %s")
        where_params.append(min_median_bet_size)

    if min_biggest_single_win is not None:
        where_parts.append("coalesce(f.biggest_single_win, 0) >= %s")
        where_params.append(min_biggest_single_win)

    if min_repeated_entry_ratio is not None:
        where_parts.append("coalesce(f.repeated_entry_market_ratio, 0) >= %s")
        where_params.append(min_repeated_entry_ratio)

    if min_single_time_ratio is not None:
        where_parts.append("coalesce(f.single_time_market_ratio, 0) >= %s")
        where_params.append(min_single_time_ratio)

    if profile_preset:
        apply_profile_preset(where_parts, where_params, profile_preset)

    join_parts = []
    join_params = []

    if include_tags:
        if tag_mode == "all":
            join_parts.append(
                """
                join (
                    select wallet
                    from user_tags
                    where tag = any(%s)
                    {score_filter}
                    group by wallet
                    having count(distinct tag) = %s
                ) include_tf on include_tf.wallet = f.wallet
                """.replace(
                    "{score_filter}",
                    "and coalesce(score, 0) >= %s" if min_tag_score is not None else "",
                )
            )
            join_params.append(include_tags)
            if min_tag_score is not None:
                join_params.append(min_tag_score)
            join_params.append(len(include_tags))

        elif tag_mode == "exact":
            join_parts.append(
                """
                join (
                    select wallet
                    from user_tags
                    group by wallet
                    having array_agg(distinct tag order by tag) = %s
                ) include_tf on include_tf.wallet = f.wallet
                """
            )
            join_params.append(include_tags)

            if min_tag_score is not None:
                join_parts.append(
                    """
                    join (
                        select wallet
                        from user_tags
                        where tag = any(%s) and coalesce(score, 0) >= %s
                        group by wallet
                        having count(distinct tag) = %s
                    ) include_score_tf on include_score_tf.wallet = f.wallet
                    """
                )
                join_params.append(include_tags)
                join_params.append(min_tag_score)
                join_params.append(len(include_tags))

        else:
            tag_mode = "any"
            join_parts.append(
                """
                join (
                    select distinct wallet
                    from user_tags
                    where tag = any(%s)
                    {score_filter}
                ) include_tf on include_tf.wallet = f.wallet
                """.replace(
                    "{score_filter}",
                    "and coalesce(score, 0) >= %s" if min_tag_score is not None else "",
                )
            )
            join_params.append(include_tags)
            if min_tag_score is not None:
                join_params.append(min_tag_score)

    if exclude_tags:
        join_parts.append(
            """
            left join (
                select distinct wallet
                from user_tags
                where tag = any(%s)
            ) exclude_tf on exclude_tf.wallet = f.wallet
            """
        )
        join_params.append(exclude_tags)
        where_parts.append("exclude_tf.wallet is null")

    where_sql = " and ".join(where_parts)
    join_sql = "\n".join(join_parts)
    base_params = join_params + where_params
    offset = (page - 1) * per_page

    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                select
                  count(*) as total_users,
                  coalesce(sum(realized_pnl), 0) as total_realized_pnl,
                  coalesce(sum(total_volume), 0) as total_volume,
                  coalesce(sum(total_trades), 0) as total_trades
                from user_features
                """
            )
            global_stats = cur.fetchone()

            tag_where = []
            tag_params = []

            if tag_search:
                tag_where.append("lower(tag) like %s")
                tag_params.append(f"%{tag_search}%")

            tag_where_sql = ""
            if tag_where:
                tag_where_sql = "where " + " and ".join(tag_where)

            cur.execute(
                f"""
                select
                  tag,
                  count(*) as cnt,
                  avg(score) as avg_score,
                  max(score) as max_score
                from user_tags
                {tag_where_sql}
                group by tag
                order by cnt desc, avg_score desc nulls last, tag asc
                """,
                tag_params,
            )
            available_tags = cur.fetchall()

            count_sql = f"""
                select count(*) as total
                from user_features f
                {join_sql}
                where {where_sql}
            """
            cur.execute(count_sql, base_params)
            total_rows = cur.fetchone()["total"]

            users_sql = f"""
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
                  f.repeated_entry_markets,
                  f.repeated_entry_market_ratio,
                  f.single_time_markets,
                  f.single_time_market_ratio,
                  coalesce(
                    json_agg(
                      distinct jsonb_build_object(
                        'tag', ut.tag,
                        'score', ut.score
                      )
                    ) filter (where ut.tag is not null),
                    '[]'::json
                  ) as tags
                from user_features f
                {join_sql}
                left join user_tags ut on ut.wallet = f.wallet
                where {where_sql}
                group by
                  f.wallet, f.updated_at, f.total_trades, f.total_volume, f.win_rate,
                  f.realized_pnl, f.active_weeks, f.distinct_markets, f.top_market_volume_share,
                  f.top3_market_volume_share, f.avg_bet_size, f.median_bet_size, f.biggest_single_win,
                  f.repeated_entry_markets, f.repeated_entry_market_ratio,
                  f.single_time_markets, f.single_time_market_ratio
                order by {sort_sql} {dir_sql} nulls last, f.wallet asc
                limit %s offset %s
            """
            cur.execute(users_sql, base_params + [per_page, offset])
            users = cur.fetchall()

            filtered_stats_sql = f"""
                select
                  count(*) as total_users,
                  coalesce(sum(f.realized_pnl), 0) as total_realized_pnl,
                  coalesce(sum(f.total_volume), 0) as total_volume,
                  coalesce(sum(f.total_trades), 0) as total_trades
                from user_features f
                {join_sql}
                where {where_sql}
            """
            cur.execute(filtered_stats_sql, base_params)
            filtered_stats = cur.fetchone()

            tag_breakdown_sql = f"""
                select
                  ut.tag,
                  count(distinct f.wallet) as wallet_count
                from user_features f
                {join_sql}
                join user_tags ut on ut.wallet = f.wallet
                where {where_sql}
                group by ut.tag
                order by wallet_count desc, ut.tag asc
            """
            cur.execute(tag_breakdown_sql, base_params)
            tag_breakdown = cur.fetchall()

            bubble_sql = f"""
                select
                  f.wallet,
                  coalesce(f.distinct_markets, 0) as x,
                  coalesce(f.win_rate, 0) * 100.0 as y,
                  coalesce(f.total_volume, 0) as volume,
                  coalesce(f.realized_pnl, 0) as realized_pnl,
                  coalesce(f.active_weeks, 0) as active_weeks
                from user_features f
                {join_sql}
                where {where_sql}
                order by coalesce(f.total_volume, 0) desc, f.wallet asc
                limit 250
            """
            cur.execute(bubble_sql, base_params)
            bubble_rows = cur.fetchall()

        total_pages = max(1, math.ceil(total_rows / per_page))
        has_prev = page > 1
        has_next = page < total_pages

        chart_labels = [row["tag"] for row in tag_breakdown[:15]]
        chart_values = [int(row["wallet_count"]) for row in tag_breakdown[:15]]

        bubble_points = []
        for row in bubble_rows:
            volume = float(row["volume"] or 0)
            radius = max(4, min(22, 4 + math.log10(volume + 1) * 3))
            bubble_points.append({
                "x": int(row["x"] or 0),
                "y": float(row["y"] or 0),
                "r": radius,
                "wallet": row["wallet"],
                "volume": float(volume),
                "realized_pnl": float(row["realized_pnl"] or 0),
                "active_weeks": int(row["active_weeks"] or 0),
            })

        return render_template(
            "index.html",
            users=users,
            global_stats=global_stats,
            filtered_stats=filtered_stats,
            available_tags=available_tags,
            include_tags=include_tags,
            exclude_tags=exclude_tags,
            tag_mode=tag_mode,
            tag_search=tag_search,
            profile_preset=profile_preset,
            min_tag_score=min_tag_score_raw,
            q=q,
            min_pnl=min_pnl_raw,
            min_win_rate=min_win_rate_raw,
            min_trades=min_trades_raw,
            min_active_weeks=min_active_weeks_raw,
            max_active_weeks=max_active_weeks_raw,
            min_distinct_markets=min_distinct_markets_raw,
            max_distinct_markets=max_distinct_markets_raw,
            min_top_market_share=min_top_market_share_raw,
            max_top_market_share=max_top_market_share_raw,
            min_top3_market_share=min_top3_market_share_raw,
            max_top3_market_share=max_top3_market_share_raw,
            min_avg_bet_size=min_avg_bet_size_raw,
            min_median_bet_size=min_median_bet_size_raw,
            min_biggest_single_win=min_biggest_single_win_raw,
            min_repeated_entry_ratio=min_repeated_entry_ratio_raw,
            min_single_time_ratio=min_single_time_ratio_raw,
            current_sort=sort,
            current_dir=direction,
            page=page,
            per_page=per_page,
            total_rows=total_rows,
            total_pages=total_pages,
            has_prev=has_prev,
            has_next=has_next,
            tag_breakdown=tag_breakdown,
            chart_labels=chart_labels,
            chart_values=chart_values,
            bubble_points=bubble_points,
        )
    finally:
        conn.close()


@app.route("/tags")
def tags_page():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                select
                  tag,
                  count(*) as user_count,
                  avg(score) as avg_score,
                  max(score) as max_score
                from user_tags
                group by tag
                order by user_count desc, avg_score desc nulls last, tag asc
                """
            )
            rows = cur.fetchall()

        return render_template("tags.html", rows=rows)
    finally:
        conn.close()


@app.route("/user/<wallet>")
def user_detail(wallet):
    wallet = wallet.lower()

    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                select *
                from user_features
                where wallet = %s
                """,
                (wallet,),
            )
            user = cur.fetchone()

            if not user:
                abort(404)

            cur.execute(
                """
                select tag, score, reason, updated_at
                from user_tags
                where wallet = %s
                order by score desc nulls last, tag asc
                """,
                (wallet,),
            )
            tags = cur.fetchall()

            cur.execute(
                """
                select
                  trade_id,
                  market_id,
                  slug,
                  title,
                  outcome,
                  side,
                  price,
                  size,
                  notional,
                  ts
                from polymarket_trades
                where proxy_wallet = %s
                order by ts desc
                limit 150
                """,
                (wallet,),
            )
            trades = cur.fetchall()

            cur.execute(
                """
                select
                  market_id,
                  max(title) as title,
                  max(slug) as slug,
                  sum(notional) as volume,
                  count(*) as trades,
                  min(ts) as first_trade_at,
                  max(ts) as last_trade_at
                from polymarket_trades
                where proxy_wallet = %s
                group by market_id
                order by volume desc nulls last
                limit 20
                """,
                (wallet,),
            )
            top_markets = cur.fetchall()

        return render_template(
            "user.html",
            user=user,
            tags=tags,
            trades=trades,
            top_markets=top_markets,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
