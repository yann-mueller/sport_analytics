"""
13_extend_odds_1x2_sm.py

Enhance public.odds_1x2 with ONE Sportmonks standard 1X2 snapshot per fixture.

This script processes fixtures that:
  A) do NOT yet have a row in public.odds_1x2 with
       provider='sportmonks' AND timeline_identifier='sm_odds'
  OR
  B) already have such a row, but that row has home IS NULL OR away IS NULL OR draw IS NULL
     (i.e., previous parsing/coverage issues; re-fetch to try to fill)

Row written:
  fixture_id, timestamp, timeline_identifier='sm_odds', provider='sportmonks', home, draw, away

Timestamp:
  - latest_bookmaker_update (latest across outcomes) preferred
  - fallback: created_at (latest across outcomes)
  - fallback: fixture kickoff (ensures NOT NULL timestamp PK)

Filters (Sportmonks):
  - market_id = 1 (Fulltime Result)
  - bookmaker_id = 9 (Betfair; legacy_id=15)

Label handling:
  - supports both "1/X/2" and "Home/Draw/Away" (and falls back to `name`)

Usage:
  python -m database.13_extend_odds_1x2_sm
  python -m database.13_extend_odds_1x2_sm --limit 500
  python -m database.13_extend_odds_1x2_sm --sleep 0.05
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

import requests
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    Float,
    Text,
    DateTime,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

from database.connection.engine import get_engine
from api_calls.auth.auth import get_access_params


# ----------------------------
# Time helpers
# ----------------------------
def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _dt_from_any(x: Any) -> datetime:
    """
    Accepts:
      - datetime
      - ISO strings like '...T...Z'
      - Sportmonks strings 'YYYY-MM-DD HH:MM:SS' (assumed UTC)
    """
    if isinstance(x, datetime):
        return x

    s = str(x).strip()
    if not s:
        raise ValueError("Empty datetime value")

    if "T" in s:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))

    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass

    return datetime.fromisoformat(s.replace(" ", "T").replace("Z", "+00:00"))


def _parse_ts(x: Any) -> Optional[datetime]:
    """
    Parse Sportmonks timestamps robustly into tz-aware UTC datetimes.
    Returns None if parsing fails.
    """
    if not x:
        return None
    try:
        return _to_utc(_dt_from_any(x))
    except Exception:
        return None


# ----------------------------
# DB schema: odds_1x2 (same structure as your existing table)
# ----------------------------
def make_odds_1x2_table(metadata: MetaData) -> Table:
    return Table(
        "odds_1x2",
        metadata,
        Column("fixture_id", Integer, primary_key=True, nullable=False),
        Column("timestamp", DateTime(timezone=True), primary_key=True, nullable=False),
        Column("timeline_identifier", Text, primary_key=True, nullable=False),
        Column("provider", Text, primary_key=True, nullable=False),
        Column("home", Float, nullable=True),
        Column("draw", Float, nullable=True),
        Column("away", Float, nullable=True),
        Column("computed_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        schema="public",
    )


def upsert_odds_1x2(engine, rows: Sequence[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    md = MetaData()
    tbl = make_odds_1x2_table(md)
    md.create_all(engine)

    stmt = pg_insert(tbl).values(list(rows))
    excluded = stmt.excluded

    changed = (
        tbl.c.home.is_distinct_from(excluded.home)
        | tbl.c.draw.is_distinct_from(excluded.draw)
        | tbl.c.away.is_distinct_from(excluded.away)
    )

    stmt = stmt.on_conflict_do_update(
        index_elements=[tbl.c.fixture_id, tbl.c.timestamp, tbl.c.timeline_identifier, tbl.c.provider],
        set_={
            "home": excluded.home,
            "draw": excluded.draw,
            "away": excluded.away,
            "computed_at": func.now(),
        },
        where=changed,
    )

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return int(res.rowcount or 0)


# ----------------------------
# DB counters / diagnostics
# ----------------------------
def count_sm_odds_rows(engine) -> Dict[str, int]:
    """
    Counts current sportmonks sm_odds rows in public.odds_1x2.
    - total rows
    - complete rows (home/draw/away all non-null)
    """
    sql = text(
        """
        SELECT
            COUNT(*) AS n_total,
            SUM(
                CASE WHEN home IS NOT NULL AND draw IS NOT NULL AND away IS NOT NULL THEN 1 ELSE 0 END
            ) AS n_complete
        FROM public.odds_1x2
        WHERE provider = 'sportmonks'
          AND timeline_identifier = 'sm_odds'
        """
    )
    with engine.begin() as conn:
        r = conn.execute(sql).fetchone()
    n_total = int(r.n_total or 0) if r is not None else 0
    n_complete = int(r.n_complete or 0) if r is not None else 0
    return {"n_total": n_total, "n_complete": n_complete}


# ----------------------------
# Fetch fixtures missing sm_odds OR having NULL odds in sm_odds
# ----------------------------
def fetch_fixtures_to_update_sm_odds(engine, limit: Optional[int]) -> List[Dict[str, Any]]:
    """
    Returns fixtures in public.fixtures that either:
      - do not have sm_odds row yet, OR
      - have sm_odds row but incomplete odds (any of home/draw/away is NULL)

    We also return kickoff (fallback timestamp).
    """
    limit_sql = "" if limit is None else "LIMIT :limit"

    sql = text(
        f"""
        SELECT
            f.fixture_id,
            f.date AS kickoff
        FROM public.fixtures f
        WHERE f.fixture_id IS NOT NULL
          AND f.date IS NOT NULL
          AND (
              -- Case A: no sm_odds row at all
              NOT EXISTS (
                  SELECT 1
                  FROM public.odds_1x2 o
                  WHERE o.fixture_id = f.fixture_id
                    AND o.provider = 'sportmonks'
                    AND o.timeline_identifier = 'sm_odds'
              )
              OR
              -- Case B: sm_odds exists but incomplete odds
              EXISTS (
                  SELECT 1
                  FROM public.odds_1x2 o
                  WHERE o.fixture_id = f.fixture_id
                    AND o.provider = 'sportmonks'
                    AND o.timeline_identifier = 'sm_odds'
                    AND (o.home IS NULL OR o.draw IS NULL OR o.away IS NULL)
              )
          )
        ORDER BY f.date, f.fixture_id
        {limit_sql}
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(sql, {"limit": limit} if limit is not None else {}).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({"fixture_id": int(r.fixture_id), "kickoff": _dt_from_any(r.kickoff)})
    return out


# ----------------------------
# Sportmonks fetch: one 1X2 snapshot (robust labels + robust timestamp comparison)
# ----------------------------
def fetch_sportmonks_1x2_snapshot(
    fixture_id: int,
    market_id: int = 1,      # Fulltime Result
    bookmaker_id: int = 9,   # Betfair (Sportmonks bookmaker.id)
) -> Dict[str, Any]:
    """
    Fetch Sportmonks pre-match odds for 1X2 (market_id=1) from Betfair (bookmaker_id=9).

    Handles label variants:
      - "1" / "X" / "2"
      - "Home" / "Draw" / "Away"
    (falls back to `name` if `label` is missing)

    Returns:
      timestamp: latest update timestamp (latest_bookmaker_update preferred; latest across outcomes)
      home/draw/away: latest odds per outcome
    """
    params = get_access_params("sportmonks")
    api_token = params["api_token"]

    url = f"https://api.sportmonks.com/v3/football/odds/pre-match/fixtures/{fixture_id}"

    resp = requests.get(
        url,
        params={
            "api_token": api_token,
            "filters": f"markets:{market_id};bookmakers:{bookmaker_id}",
        },
        timeout=45,
    )
    resp.raise_for_status()
    js = resp.json()

    def _norm(x: Any) -> str:
        return str(x or "").strip().lower()

    label_map = {
        "1": "home",
        "x": "draw",
        "2": "away",
        "home": "home",
        "draw": "draw",
        "away": "away",
    }

    latest: Dict[str, Dict[str, Any]] = {}
    ts_all: List[datetime] = []

    for o in js.get("data", []) or []:
        raw = o.get("label") or o.get("name")
        side = label_map.get(_norm(raw))
        if side is None:
            continue

        ts_raw = o.get("latest_bookmaker_update") or o.get("created_at")
        ts_dt = _parse_ts(ts_raw)
        if ts_dt is None:
            continue

        val = o.get("value")
        if val is None:
            continue
        try:
            odds_val = float(val)
        except Exception:
            continue

        if (side not in latest) or (ts_dt > latest[side]["ts_dt"]):
            latest[side] = {"odds": odds_val, "ts_dt": ts_dt}

    for v in latest.values():
        if "ts_dt" in v and isinstance(v["ts_dt"], datetime):
            ts_all.append(v["ts_dt"])

    snap_ts: Optional[datetime] = max(ts_all) if ts_all else None

    return {
        "timestamp": snap_ts,
        "home": latest.get("home", {}).get("odds"),
        "draw": latest.get("draw", {}).get("odds"),
        "away": latest.get("away", {}).get("odds"),
    }


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="Process only first N fixtures (default: all qualifying)")
    ap.add_argument("--sleep", type=float, default=0.03, help="Sleep between requests (seconds), default 0.03")
    args = ap.parse_args()

    engine = get_engine()

    md = MetaData()
    make_odds_1x2_table(md)
    md.create_all(engine)

    before = count_sm_odds_rows(engine)

    fixtures = fetch_fixtures_to_update_sm_odds(engine, limit=args.limit)
    print(
        f"[SM_ODDS EXTEND] fixtures to add/fix sm_odds: {len(fixtures)} (limit={args.limit}) | "
        f"existing sm_odds rows: total={before['n_total']} complete={before['n_complete']}"
    )

    ok = failed = 0
    total_upserted = 0

    # counts based on what we received/wrote in this run
    received_complete = 0  # API snapshot had all 3 outcomes non-null
    wrote_complete = 0     # row we attempted to upsert had all 3 non-null

    for i, fx in enumerate(fixtures, start=1):
        fixture_id = fx["fixture_id"]
        kickoff = fx["kickoff"]

        try:
            sm = fetch_sportmonks_1x2_snapshot(fixture_id=fixture_id)

            # timestamp must be NOT NULL due to PK; fallback to kickoff if missing
            ts = sm["timestamp"] if sm["timestamp"] is not None else _to_utc(kickoff)

            row = {
                "fixture_id": fixture_id,
                "timestamp": ts,
                "timeline_identifier": "sm_odds",
                "provider": "sportmonks",
                "home": sm["home"],
                "draw": sm["draw"],
                "away": sm["away"],
            }

            if (sm["home"] is not None) and (sm["draw"] is not None) and (sm["away"] is not None):
                received_complete += 1
            if (row["home"] is not None) and (row["draw"] is not None) and (row["away"] is not None):
                wrote_complete += 1

            up = upsert_odds_1x2(engine, [row])
            total_upserted += up
            ok += 1

            if ok % 200 == 0 or i == len(fixtures):
                current = count_sm_odds_rows(engine)
                print(
                    f"[PROGRESS] {i}/{len(fixtures)} ok={ok} failed={failed} "
                    f"upserted={total_upserted} "
                    f"run_complete(api)={received_complete} run_complete(wrote)={wrote_complete} "
                    f"db_total={current['n_total']} db_complete={current['n_complete']}"
                )

            time.sleep(max(0.0, float(args.sleep)))

        except KeyboardInterrupt:
            print("\n[INTERRUPT] Stopping early (CTRL+C). Progress saved row-by-row.")
            break
        except Exception as e:
            failed += 1
            print(f"[ERROR] fixture_id={fixture_id} failed: {e}")
            time.sleep(0.2)
            continue

    after = count_sm_odds_rows(engine)

    print("\nDone.")
    print(
        f"[SM_ODDS EXTEND] ok={ok} failed={failed} total_upserted={total_upserted} | "
        f"run_complete(api)={received_complete} run_complete(wrote)={wrote_complete} | "
        f"db_total={after['n_total']} db_complete={after['n_complete']}"
    )
    print("Table: public.odds_1x2")


if __name__ == "__main__":
    main()
