"""
Create + maintain public.odds_1x2 (1X2 odds snapshots) using OddsAPI historical event odds,
restricted to BETFAIR bookmaker and EU region.

Table columns:
  fixture_id, timestamp, timeline_identifier, provider, home, draw, away

Timeline per fixture:
- 2 hours before kickoff (10-minute interval)
- 24 hours to 2 hours before kickoff (hourly interval)
- around previous match (HOME team's prev_1), only if prev_1 exists:
  * 3 hourly snapshots after prev kickoff
  * 3 hourly snapshots before prev kickoff
  * 1 snapshot 3 days before prev kickoff

timeline_identifier:
  odd_1, odd_2, ... assigned by sorting timestamps DESC (closest to kickoff => odd_1)

Usage:
  python -m database.12_odds_1x2
  python -m database.12_odds_1x2 --limit 50
  python -m database.12_odds_1x2 --limit 50 --provider betfair --region eu --bookmaker betfair
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

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
# Fetch fixtures + matching
# ----------------------------
def fetch_candidate_fixtures(engine, limit: Optional[int]) -> List[Dict[str, Any]]:
    """
    Read fixtures (with kickoff + league + home/away) and join oa_event_id from fixtures_matching.
    We only consider fixtures that have:
      - date
      - league_id
      - home_team_id, away_team_id
      - oa_event_id present in fixtures_matching
    """
    limit_sql = "" if limit is None else "LIMIT :limit"

    sql = text(
        f"""
        SELECT
            f.fixture_id,
            f.league_id,
            f.date AS kickoff,
            f.home_team_id,
            f.away_team_id,
            fm.oa_event_id
        FROM public.fixtures f
        JOIN public.fixtures_matching fm
          ON fm.fixture_id = f.fixture_id
        WHERE f.fixture_id IS NOT NULL
          AND f.date IS NOT NULL
          AND f.league_id IS NOT NULL
          AND f.home_team_id IS NOT NULL
          AND f.away_team_id IS NOT NULL
          AND fm.oa_event_id IS NOT NULL
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
                "league_id": int(r.league_id),
                "kickoff": _dt_from_any(r.kickoff),
                "home_team_id": int(r.home_team_id),
                "away_team_id": int(r.away_team_id),
                "oa_event_id": str(r.oa_event_id),
            }
        )
    return out


def fixture_already_done(engine, fixture_id: int, provider: str) -> bool:
    """
    If any odds rows exist for (fixture_id, provider), we consider it done.
    """
    md = MetaData()
    tbl = make_odds_1x2_table(md)
    md.create_all(engine)

    q = select(func.count()).select_from(tbl).where((tbl.c.fixture_id == fixture_id) & (tbl.c.provider == provider))
    with engine.begin() as conn:
        n = conn.execute(q).scalar_one()
    return int(n) > 0


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
    # Prefer DB table public.league_mapping if you created it.
    try:
        sql = text("SELECT oa_league_name FROM public.league_mapping WHERE league_id = :league_id")
        with engine.begin() as conn:
            r = conn.execute(sql, {"league_id": league_id}).fetchone()
        if r and r.oa_league_name:
            return str(r.oa_league_name).strip()
    except Exception:
        pass

    # Fallback for your common leagues (extend if needed)
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
            "regions": region,         # required by OddsAPI
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
    ap.add_argument("--limit", type=int, default=None, help="Process only first N fixtures (default: all)")
    ap.add_argument("--provider", type=str, default="betfair", help="Stored provider label (default: betfair)")
    ap.add_argument("--region", type=str, default="eu", help="OddsAPI region (default: eu)")
    ap.add_argument("--bookmaker", type=str, default="betfair", help="OddsAPI bookmaker key (default: betfair)")
    ap.add_argument("--skip-existing", action="store_true", help="Skip fixtures already having odds for provider")
    args = ap.parse_args()

    provider_label = str(args.provider).strip().lower()
    region = str(args.region).strip().lower()
    bookmaker = str(args.bookmaker).strip().lower()

    engine = get_engine()

    # Ensure table exists
    md = MetaData()
    make_odds_1x2_table(md)
    md.create_all(engine)

    fixtures = fetch_candidate_fixtures(engine, limit=args.limit)
    print(f"[ODDS_1X2] candidate fixtures with oa_event_id: {len(fixtures)} (limit={args.limit})")

    ok = skipped = failed = 0
    total_rows = 0
    total_upserted = 0

    for i, fx in enumerate(fixtures, start=1):
        fixture_id = fx["fixture_id"]

        try:
            if args.skip_existing and fixture_already_done(engine, fixture_id, provider_label):
                skipped += 1
                if skipped % 200 == 0:
                    print(f"[PROGRESS] {i}/{len(fixtures)} skipped={skipped} ok={ok} failed={failed}")
                continue

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
                    # store NULL odds so the timeline remains complete
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
                    # small delay to avoid hammering on repeated errors
                    time.sleep(0.2)

                # be nice to API
                time.sleep(0.05)

            up = upsert_odds_1x2(engine, rows)

            print(
                f"[FIXTURE DONE] fixture_id={fixture_id} "
                f"snapshots={len(rows)} upserted={up}"
            )

            ok += 1
            total_rows += len(rows)
            total_upserted += up

            if ok % 5 == 0 or i == len(fixtures):
                print(
                    f"[PROGRESS] {i}/{len(fixtures)} ok={ok} skipped={skipped} failed={failed} "
                    f"rows_prepared={total_rows} upserted={total_upserted}"
                )

        except KeyboardInterrupt:
            print("\n[INTERRUPT] Stopping early (CTRL+C). Progress saved fixture-by-fixture.")
            break
        except Exception as e:
            failed += 1
            print(f"[ERROR] fixture_id={fixture_id} failed: {e}")
            time.sleep(0.5)
            continue

    print("\nDone.")
    print(f"[ODDS_1X2] ok={ok} skipped={skipped} failed={failed}")
    print(f"[ODDS_1X2] rows_prepared={total_rows} total_upserted={total_upserted}")
    print("Table: public.odds_1x2")


if __name__ == "__main__":
    main()