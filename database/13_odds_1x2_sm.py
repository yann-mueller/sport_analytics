"""
Enhance public.odds_1x2 with ONE Sportmonks standard 1X2 snapshot per fixture.

Writes rows:
  fixture_id, timestamp, timeline_identifier="sm_odds", provider="sportmonks", home, draw, away

Timestamp:
  - Uses Sportmonks latest_bookmaker_update (latest across outcomes 1/X/2)
  - If missing, falls back to created_at (latest across outcomes)
  - If still missing, falls back to fixture kickoff (to satisfy NOT NULL timestamp PK)

Filters:
  - market_id = 1 (Fulltime Result)
  - bookmaker_id = 9 (Betfair; legacy_id=15)

Usage:
  python -m database.12b_odds_1x2_sportmonks
  python -m database.12b_odds_1x2_sportmonks --limit 50
  python -m database.12b_odds_1x2_sportmonks --skip-existing
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
    select,
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

    # ISO with Z / offset
    if "T" in s:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))

    # 'YYYY-MM-DD HH:MM:SS' (Sportmonks)
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass

    # last resort
    return datetime.fromisoformat(s.replace(" ", "T").replace("Z", "+00:00"))


# ----------------------------
# DB schema: odds_1x2 (same as main file)
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
# Fixture selection
# ----------------------------
def fetch_candidate_fixtures(engine, limit: Optional[int]) -> List[Dict[str, Any]]:
    """
    Fixtures that exist in your DB. We only need fixture_id + kickoff.
    (We don't need oa_event_id for Sportmonks.)
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
        ORDER BY f.date, f.fixture_id
        {limit_sql}
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(sql, {"limit": limit} if limit is not None else {}).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "fixture_id": int(r.fixture_id),
                "kickoff": _dt_from_any(r.kickoff),
            }
        )
    return out


def sm_row_already_done(engine, fixture_id: int) -> bool:
    """
    Checks if we already have the Sportmonks enhancement row:
      provider='sportmonks' AND timeline_identifier='sm_odds'
    """
    md = MetaData()
    tbl = make_odds_1x2_table(md)
    md.create_all(engine)

    q = (
        select(func.count())
        .select_from(tbl)
        .where(
            (tbl.c.fixture_id == fixture_id)
            & (tbl.c.provider == "sportmonks")
            & (tbl.c.timeline_identifier == "sm_odds")
        )
    )
    with engine.begin() as conn:
        n = conn.execute(q).scalar_one()
    return int(n) > 0


# ----------------------------
# Sportmonks fetch: one 1X2 snapshot
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
    (and falls back to `name` if `label` is missing)

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

    # Accept both old and new labeling conventions
    label_map = {
        "1": "home",
        "x": "draw",
        "2": "away",
        "home": "home",
        "draw": "draw",
        "away": "away",
    }

    latest: Dict[str, Dict[str, Any]] = {}
    for o in js.get("data", []) or []:
        raw_label = o.get("label") or o.get("name")
        side = label_map.get(_norm(raw_label))
        if side is None:
            continue

        # Prefer latest_bookmaker_update, fallback created_at
        ts = o.get("latest_bookmaker_update") or o.get("created_at") or ""
        ts_s = str(ts)

        val = o.get("value")
        if val is None:
            continue

        try:
            odds_val = float(val)
        except Exception:
            odds_val = None

        if side not in latest or ts_s > str(latest[side].get("ts", "")):
            latest[side] = {"odds": odds_val, "ts": ts_s}

    # Snapshot timestamp = latest across outcomes (if any)
    ts_candidates = [v.get("ts") for v in latest.values() if v.get("ts")]
    snap_ts: Optional[datetime] = None
    if ts_candidates:
        snap_ts = _to_utc(_dt_from_any(max(ts_candidates)))

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
    ap.add_argument("--limit", type=int, default=None, help="Process only first N fixtures (default: all)")
    ap.add_argument("--skip-existing", action="store_true", help="Skip fixtures that already have sm_odds row")
    ap.add_argument("--sleep", type=float, default=0.03, help="Sleep between requests (seconds), default 0.03")
    args = ap.parse_args()

    engine = get_engine()

    md = MetaData()
    make_odds_1x2_table(md)
    md.create_all(engine)

    fixtures = fetch_candidate_fixtures(engine, limit=args.limit)
    print(f"[SM_ODDS] candidate fixtures: {len(fixtures)} (limit={args.limit})")

    ok = skipped = failed = 0
    total_upserted = 0

    for i, fx in enumerate(fixtures, start=1):
        fixture_id = fx["fixture_id"]
        kickoff = fx["kickoff"]

        try:
            if args.skip_existing and sm_row_already_done(engine, fixture_id):
                skipped += 1
                if skipped % 500 == 0:
                    print(f"[PROGRESS] {i}/{len(fixtures)} ok={ok} skipped={skipped} failed={failed}")
                continue

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

            up = upsert_odds_1x2(engine, [row])
            total_upserted += up

            ok += 1
            if ok % 200 == 0 or i == len(fixtures):
                print(
                    f"[PROGRESS] {i}/{len(fixtures)} ok={ok} skipped={skipped} failed={failed} "
                    f"upserted={total_upserted}"
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

    print("\nDone.")
    print(f"[SM_ODDS] ok={ok} skipped={skipped} failed={failed} total_upserted={total_upserted}")
    print("Table: public.odds_1x2")


if __name__ == "__main__":
    main()
