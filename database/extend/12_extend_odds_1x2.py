"""
database/12_extend_odds_1x2.py

Extend public.odds_1x2 (1X2 odds snapshots) by fetching odds ONLY for fixtures that
do not yet have any odds rows (for the given provider label).

Source:
- OddsAPI historical event odds endpoint
- restricted to BETFAIR bookmaker and EU region by default

Requires:
- public.fixtures_matching provides oa_event_id for fixtures
- public.previous_matches for prev_1 context (optional; only for home team)
- public.fixtures provides kickoff + league_id + teams

Adds filters:
- --season-id (repeatable): only process fixtures in these season_ids
- (optional) --league-id (repeatable)

Usage:
  python -m database.12_extend_odds_1x2
  python -m database.12_extend_odds_1x2 --limit 100
  python -m database.12_extend_odds_1x2 --season-id 21608
  python -m database.12_extend_odds_1x2 --season-id 21608 --season-id 21609 --limit 200
  python -m database.12_extend_odds_1x2 --league-id 8 --limit 100
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone
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
from api_calls.helpers.providers.general import get_url


# ----------------------------
# Time helpers
# ----------------------------
def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso(dt: datetime) -> str:
    return _to_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")


def _dt_from_any(x: Any) -> datetime:
    if isinstance(x, datetime):
        return x
    return datetime.fromisoformat(str(x).replace("Z", "+00:00"))


# ----------------------------
# DB schema: odds_1x2
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
# Candidate fixtures: ONLY those missing odds rows
# ----------------------------
def fetch_candidate_fixtures_missing_odds(
    engine,
    *,
    provider_label: str,
    limit: Optional[int],
    season_ids: Optional[List[int]],
    league_ids: Optional[List[int]],
) -> List[Dict[str, Any]]:
    """
    Fixtures eligible for odds fetching:
    - fixtures has required fields + kickoff
    - fixtures_matching has oa_event_id
    - odds_1x2 has NO row yet for (fixture_id, provider_label)
    Optional filters: season_ids, league_ids
    """
    limit_sql = "" if limit is None else "LIMIT :limit"

    season_filter = ""
    league_filter = ""

    params: Dict[str, Any] = {"provider": provider_label}
    if limit is not None:
        params["limit"] = limit

    if season_ids:
        season_filter = "AND f.season_id = ANY(:season_ids)"
        params["season_ids"] = season_ids

    if league_ids:
        league_filter = "AND f.league_id = ANY(:league_ids)"
        params["league_ids"] = league_ids

    sql = text(
        f"""
        SELECT
            f.fixture_id,
            f.league_id,
            f.season_id,
            f.date AS kickoff,
            f.home_team_id,
            f.away_team_id,
            fm.oa_event_id
        FROM public.fixtures f
        JOIN public.fixtures_matching fm
          ON fm.fixture_id = f.fixture_id
        LEFT JOIN public.odds_1x2 o
          ON o.fixture_id = f.fixture_id
         AND o.provider = :provider
        WHERE f.fixture_id IS NOT NULL
          AND f.date IS NOT NULL
          AND f.league_id IS NOT NULL
          AND f.season_id IS NOT NULL
          AND f.home_team_id IS NOT NULL
          AND f.away_team_id IS NOT NULL
          AND fm.oa_event_id IS NOT NULL
          AND o.fixture_id IS NULL
          {season_filter}
          {league_filter}
        ORDER BY f.date, f.fixture_id
        {limit_sql}
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(sql, params).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "fixture_id": int(r.fixture_id),
                "league_id": int(r.league_id),
                "season_id": int(r.season_id),
                "kickoff": _dt_from_any(r.kickoff),
                "home_team_id": int(r.home_team_id),
                "away_team_id": int(r.away_team_id),
                "oa_event_id": str(r.oa_event_id),
            }
        )
    return out


def fetch_prev1_for_home_team(engine, fixture_id: int, home_team_id: int) -> Optional[int]:
    sql = text(
        """
        SELECT prev_1
        FROM public.previous_matches
        WHERE fixture_id = :fixture_id
          AND team_id = :team_id
        """
    )
    with engine.begin() as conn:
        r = conn.execute(sql, {"fixture_id": fixture_id, "team_id": home_team_id}).fetchone()
    if not r or r.prev_1 is None:
        return None
    return int(r.prev_1)


def fetch_fixture_kickoff(engine, fixture_id: int) -> Optional[datetime]:
    sql = text("SELECT date AS kickoff FROM public.fixtures WHERE fixture_id = :fixture_id")
    with engine.begin() as conn:
        r = conn.execute(sql, {"fixture_id": fixture_id}).fetchone()
    if not r or r.kickoff is None:
        return None
    return _dt_from_any(r.kickoff)


def resolve_sport_key_from_league(engine, league_id: int) -> str:
    """
    Resolve OddsAPI sport_key for a league_id.

    Prefers DB table public.league_mapping (league_id -> oa_league_name).
    Falls back to a small hardcoded mapping if table isn't present.
    """
    try:
        sql = text("SELECT oa_league_name FROM public.league_mapping WHERE league_id = :league_id")
        with engine.begin() as conn:
            r = conn.execute(sql, {"league_id": league_id}).fetchone()
        if r and r.oa_league_name:
            return str(r.oa_league_name).strip()
    except Exception:
        pass

    fallback = {82: "soccer_germany_bundesliga", 8: "soccer_epl"}
    if league_id in fallback:
        return fallback[league_id]

    raise RuntimeError(
        f"Could not resolve OddsAPI sport_key for league_id={league_id}. "
        f"Create public.league_mapping (league_id, oa_league_name) or extend fallback."
    )


# ----------------------------
# Snapshot schedule
# ----------------------------
def build_snapshot_times(kickoff: datetime, prev_kickoff: Optional[datetime]) -> List[datetime]:
    k = _to_utc(kickoff)
    out: List[datetime] = []

    # 2h before kickoff: 10-min interval (includes last snapshot at kickoff-10min)
    t = k - timedelta(hours=2)
    while t <= k - timedelta(minutes=10):
        out.append(t)
        t += timedelta(minutes=10)

    # 24h to 2h before kickoff: hourly (stop at kickoff-3h to avoid overlap)
    t = k - timedelta(hours=24)
    while t <= k - timedelta(hours=3):
        out.append(t)
        t += timedelta(hours=1)

    if prev_kickoff is not None:
        p = _to_utc(prev_kickoff)
        for h in (1, 2, 3):
            out.append(p + timedelta(hours=h))
        for h in (1, 2, 3):
            out.append(p - timedelta(hours=h))
        out.append(p - timedelta(days=3))

    return sorted(set(out), reverse=True)


# ----------------------------
# OddsAPI fetch with retry/backoff
# ----------------------------
def _is_rate_limit(e: Exception) -> bool:
    if isinstance(e, requests.HTTPError):
        resp = getattr(e, "response", None)
        if resp is not None and resp.status_code == 429:
            return True
    return False


def _sleep_with_feedback(seconds: float) -> None:
    print(f"[RATE LIMIT] Sleeping {seconds:.1f}s then retrying...")
    time.sleep(seconds)


def fetch_h2h_odds_snapshot(
    sport_key: str,
    event_id: str,
    snapshot_dt: datetime,
    bookmaker_key: str,
    region: str,
) -> Dict[str, Any]:
    params = get_access_params("oddsapi")
    api_key = params["api_token"]

    url_tmpl = get_url("oddsapi", "historical_event_odds")
    url = url_tmpl.format(sport=sport_key, event_id=event_id)

    r = requests.get(
        url,
        params={
            "apiKey": api_key,
            "date": _iso(snapshot_dt),
            "dateFormat": "iso",
            "markets": "h2h",
            "oddsFormat": "decimal",
            "regions": region,
            "bookmakers": bookmaker_key,
        },
        timeout=45,
    )
    r.raise_for_status()
    payload = r.json()

    data = payload.get("data") or {}
    bookmakers = data.get("bookmakers") or []

    chosen = None
    for b in bookmakers:
        if str(b.get("key", "")).lower() == bookmaker_key.lower():
            chosen = b
            break
    if chosen is None and bookmakers:
        chosen = bookmakers[0]

    home = draw = away = None

    if chosen:
        markets = chosen.get("markets") or []
        h2h = next((m for m in markets if m.get("key") == "h2h"), None)
        if h2h:
            outcomes = h2h.get("outcomes") or []

            ht = str(data.get("home_team", "")).strip().lower()
            at = str(data.get("away_team", "")).strip().lower()

            for o in outcomes:
                name = str(o.get("name", "")).strip().lower()
                price = o.get("price")
                if price is None:
                    continue

                if name == "draw":
                    draw = float(price)
                elif name == ht:
                    home = float(price)
                elif name == at:
                    away = float(price)

    return {
        "snapshot_dt": _to_utc(snapshot_dt),
        "bookmaker_used": (chosen.get("key") if chosen else None),
        "home": home,
        "draw": draw,
        "away": away,
        "raw_snapshot_timestamp": payload.get("timestamp"),
    }


def fetch_h2h_with_retry(
    sport_key: str,
    event_id: str,
    snapshot_dt: datetime,
    bookmaker_key: str,
    region: str,
    max_retries: int = 10,
    base_sleep_s: float = 2.0,
    max_sleep_s: float = 60.0,
) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            return fetch_h2h_odds_snapshot(
                sport_key=sport_key,
                event_id=event_id,
                snapshot_dt=snapshot_dt,
                bookmaker_key=bookmaker_key,
                region=region,
            )
        except Exception as e:
            last_err = e
            if _is_rate_limit(e):
                sleep_s = min(max_sleep_s, base_sleep_s * (1.6 ** (attempt - 1)))
                print(
                    f"[RATE LIMIT] sport={sport_key} event_id={event_id} snapshot={_to_utc(snapshot_dt).isoformat()} "
                    f"attempt={attempt}/{max_retries}"
                )
                _sleep_with_feedback(sleep_s)
                continue
            raise
    raise RuntimeError(f"Failed odds fetch after {max_retries} retries") from last_err


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="Process only first N missing fixtures (default: all)")
    ap.add_argument("--provider", type=str, default="betfair", help="Stored provider label (default: betfair)")
    ap.add_argument("--region", type=str, default="eu", help="OddsAPI region (default: eu)")
    ap.add_argument("--bookmaker", type=str, default="betfair", help="OddsAPI bookmaker key (default: betfair)")
    ap.add_argument(
        "--season-id",
        type=int,
        action="append",
        default=None,
        help="Restrict to this season_id (repeatable: --season-id 1 --season-id 2)",
    )
    ap.add_argument(
        "--league-id",
        type=int,
        action="append",
        default=None,
        help="Restrict to this league_id (repeatable: --league-id 8 --league-id 82)",
    )
    args = ap.parse_args()

    provider_label = str(args.provider).strip().lower()
    region = str(args.region).strip().lower()
    bookmaker = str(args.bookmaker).strip().lower()

    season_ids = [int(x) for x in (args.season_id or [])] or None
    league_ids = [int(x) for x in (args.league_id or [])] or None

    engine = get_engine()

    # Ensure table exists
    md = MetaData()
    make_odds_1x2_table(md)
    md.create_all(engine)

    fixtures = fetch_candidate_fixtures_missing_odds(
        engine,
        provider_label=provider_label,
        limit=args.limit,
        season_ids=season_ids,
        league_ids=league_ids,
    )

    print(
        f"[ODDS_1X2 EXTEND] missing fixtures (provider={provider_label}): {len(fixtures)} "
        f"(limit={args.limit}, season_ids={season_ids}, league_ids={league_ids})"
    )

    if not fixtures:
        print("[ODDS_1X2 EXTEND] Nothing to do.")
        return

    ok = failed = 0
    total_rows = 0
    total_upserted = 0

    for i, fx in enumerate(fixtures, start=1):
        fixture_id = fx["fixture_id"]

        try:
            sport_key = resolve_sport_key_from_league(engine, fx["league_id"])
            kickoff = fx["kickoff"]
            oa_event_id = fx["oa_event_id"]

            prev1 = fetch_prev1_for_home_team(engine, fixture_id=fixture_id, home_team_id=fx["home_team_id"])
            prev_kickoff: Optional[datetime] = None
            if prev1 is not None:
                prev_kickoff = fetch_fixture_kickoff(engine, prev1)

            times = build_snapshot_times(kickoff=kickoff, prev_kickoff=prev_kickoff)

            rows: List[Dict[str, Any]] = []
            for idx, t in enumerate(times, start=1):
                timeline_id = f"odd_{idx}"
                snap_ts = _to_utc(t)
                try:
                    snap = fetch_h2h_with_retry(
                        sport_key=sport_key,
                        event_id=oa_event_id,
                        snapshot_dt=snap_ts,
                        bookmaker_key=bookmaker,
                        region=region,
                    )
                    rows.append(
                        {
                            "fixture_id": fixture_id,
                            "timestamp": snap_ts,
                            "timeline_identifier": timeline_id,
                            "provider": provider_label,
                            "home": snap["home"],
                            "draw": snap["draw"],
                            "away": snap["away"],
                        }
                    )
                except Exception as e:
                    # keep timeline complete
                    print(
                        f"[WARN] fixture_id={fixture_id} snapshot={snap_ts.isoformat()} "
                        f"sport={sport_key} event={oa_event_id} error={e}"
                    )
                    rows.append(
                        {
                            "fixture_id": fixture_id,
                            "timestamp": snap_ts,
                            "timeline_identifier": timeline_id,
                            "provider": provider_label,
                            "home": None,
                            "draw": None,
                            "away": None,
                        }
                    )
                    time.sleep(0.2)

                time.sleep(0.05)

            up = upsert_odds_1x2(engine, rows)

            print(
                f"[FIXTURE DONE] {i}/{len(fixtures)} fixture_id={fixture_id} season_id={fx['season_id']} "
                f"snapshots={len(rows)} upserted={up}"
            )

            ok += 1
            total_rows += len(rows)
            total_upserted += up

            if ok % 5 == 0 or i == len(fixtures):
                print(
                    f"[PROGRESS] {i}/{len(fixtures)} ok={ok} failed={failed} "
                    f"rows_prepared={total_rows} upserted={total_upserted}"
                )

        except KeyboardInterrupt:
            print("\n[INTERRUPT] Stopping early (CTRL+C). Progress saved fixture-by-fixture.")
            break
        except Exception as e:
            failed += 1
            print(f"[ERROR] {i}/{len(fixtures)} fixture_id={fixture_id} failed: {e}")
            time.sleep(0.5)
            continue

    print("\nDone.")
    print(f"[ODDS_1X2 EXTEND] ok={ok} failed={failed}")
    print(f"[ODDS_1X2 EXTEND] rows_prepared={total_rows} total_upserted={total_upserted}")
    print("Table: public.odds_1x2")


if __name__ == "__main__":
    main()
