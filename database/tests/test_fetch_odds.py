"""
TEST + WRITE: Fetch 1X2 (h2h) odds snapshots (BETFAIR via OddsAPI) and STORE into public.odds_1x2.

Can be called with:
  - SportMonks fixture_id, OR
  - OddsAPI event_id that already exists in public.fixtures_matching

Timestamps captured:
- 2 hours before kickoff (10-minute interval)
- 24 hours to 2 hours before kickoff (hourly interval)
- around previous match (HOME team's prev_1), only if prev_1 exists:
  * 3 hourly snapshots after previous kickoff
  * 3 hourly snapshots before previous kickoff
  * 1 snapshot 3 days before previous kickoff

timeline_identifier:
- odd_1, odd_2, ... assigned by sorting all requested timestamps DESC
  (closest to kickoff => odd_1)

Writes into:
  public.odds_1x2(
      fixture_id, timestamp, timeline_identifier, provider, home, draw, away, computed_at
  )

Usage:
  python -m database.tests.test_fetch_odds --fixture-id 19154824
  python -m database.tests.test_fetch_odds --oa-event-id 7b33ac3dc1ab6e9e0734da0f9c0f3e7f
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from sqlalchemy import MetaData, Table, Column, Integer, Float, Text, DateTime, func, text
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


def upsert_odds_1x2(engine, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    md = MetaData()
    tbl = make_odds_1x2_table(md)
    md.create_all(engine)

    stmt = pg_insert(tbl).values(rows)
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
# DB lookups
# ----------------------------
def fetch_fixture(engine, fixture_id: int) -> Dict[str, Any]:
    sql = text(
        """
        SELECT fixture_id, league_id, date AS kickoff, home_team_id, away_team_id
        FROM public.fixtures
        WHERE fixture_id = :fixture_id
        """
    )
    with engine.begin() as conn:
        r = conn.execute(sql, {"fixture_id": fixture_id}).fetchone()
    if not r:
        raise RuntimeError(f"Fixture not found in public.fixtures: fixture_id={fixture_id}")

    return {
        "fixture_id": int(r.fixture_id),
        "league_id": int(r.league_id) if r.league_id is not None else None,
        "kickoff": _dt_from_any(r.kickoff),
        "home_team_id": int(r.home_team_id),
        "away_team_id": int(r.away_team_id),
    }


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


def resolve_sport_key_from_league(engine, league_id: int) -> str:
    sport_key: Optional[str] = None
    try:
        sql = text(
            """
            SELECT oa_league_name
            FROM public.league_mapping
            WHERE league_id = :league_id
            """
        )
        with engine.begin() as conn:
            r = conn.execute(sql, {"league_id": league_id}).fetchone()
        if r and r.oa_league_name:
            sport_key = str(r.oa_league_name).strip()
    except Exception:
        sport_key = None

    if sport_key:
        return sport_key

    fallback = {82: "soccer_germany_bundesliga", 8: "soccer_epl"}
    if league_id in fallback:
        return fallback[league_id]

    raise RuntimeError(
        f"Could not resolve OddsAPI sport_key for league_id={league_id}. "
        f"Create public.league_mapping (league_id, oa_league_name) or extend fallback."
    )


def resolve_from_fixture_id(engine, fixture_id: int) -> Tuple[str, str, datetime, Optional[datetime], Optional[int]]:
    fx = fetch_fixture(engine, fixture_id)
    kickoff = fx["kickoff"]

    sql = text(
        """
        SELECT oa_event_id
        FROM public.fixtures_matching
        WHERE fixture_id = :fixture_id
        """
    )
    with engine.begin() as conn:
        r = conn.execute(sql, {"fixture_id": fixture_id}).fetchone()

    if not r or not r.oa_event_id:
        raise RuntimeError(
            f"No oa_event_id found for fixture_id={fixture_id}. "
            f"Run database.11_fixtures_matching first."
        )

    oa_event_id = str(r.oa_event_id)
    sport_key = resolve_sport_key_from_league(engine, fx["league_id"])

    prev1 = fetch_prev1_for_home_team(engine, fixture_id=fixture_id, home_team_id=fx["home_team_id"])
    prev_kickoff: Optional[datetime] = None
    if prev1 is not None:
        try:
            prev_kickoff = fetch_fixture(engine, prev1)["kickoff"]
        except Exception:
            prev_kickoff = None

    return sport_key, oa_event_id, kickoff, prev_kickoff, prev1


def resolve_from_oa_event_id(engine, oa_event_id: str) -> Tuple[int, str, datetime, Optional[datetime], Optional[int]]:
    sql = text(
        """
        SELECT fixture_id
        FROM public.fixtures_matching
        WHERE oa_event_id = :oa_event_id
        ORDER BY fixture_id
        LIMIT 1
        """
    )
    with engine.begin() as conn:
        r = conn.execute(sql, {"oa_event_id": oa_event_id}).fetchone()

    if not r or r.fixture_id is None:
        raise RuntimeError(
            f"oa_event_id={oa_event_id} not found in public.fixtures_matching. "
            f"Add it via database.11_fixtures_matching first."
        )

    fixture_id = int(r.fixture_id)
    sport_key, _, kickoff, prev_kickoff, prev1 = resolve_from_fixture_id(engine, fixture_id)
    return fixture_id, sport_key, kickoff, prev_kickoff, prev1


# ----------------------------
# Snapshot schedule
# ----------------------------
def build_snapshot_times(kickoff: datetime, prev_kickoff: Optional[datetime]) -> List[datetime]:
    k = _to_utc(kickoff)
    out: List[datetime] = []

    # 2h before kickoff: 10-min interval
    t = k - timedelta(hours=2)
    while t <= k - timedelta(minutes=10):
        out.append(t)
        t += timedelta(minutes=10)

    # 24h to 2h before kickoff: hourly (avoid overlap by stopping at -3h)
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
# OddsAPI fetch (historical event odds) with BETFAIR
# ----------------------------
def fetch_h2h_odds_snapshot(
    sport_key: str,
    event_id: str,
    snapshot_dt: datetime,
    provider: str = "oddsapi",
    bookmaker_key: str = "betfair",
    region: str = "eu",
) -> Dict[str, Any]:
    """
    Requires providers_config.yaml to contain:
      historical_event_odds: "/v4/historical/sports/{sport}/events/{event_id}/odds"

    IMPORTANT:
      OddsAPI requires regions or bookmakers; we send BOTH:
        regions=eu, bookmakers=betfair
    """
    params = get_access_params(provider)
    api_key = params["api_token"]

    url_tmpl = get_url(provider, "historical_event_odds")
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


def main() -> None:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--fixture-id", type=int, help="SportMonks fixture_id")
    g.add_argument("--oa-event-id", type=str, help="OddsAPI event_id (must exist in public.fixtures_matching)")

    ap.add_argument("--provider", type=str, default="betfair", help="Stored provider label (default: betfair)")
    ap.add_argument("--region", type=str, default="eu", help="OddsAPI region (default: eu)")
    ap.add_argument("--bookmaker", type=str, default="betfair", help="OddsAPI bookmaker key (default: betfair)")

    args = ap.parse_args()
    engine = get_engine()

    if args.fixture_id is not None:
        fixture_id = int(args.fixture_id)
        sport_key, oa_event_id, kickoff, prev_kickoff, prev1 = resolve_from_fixture_id(engine, fixture_id)
        title = f"SportMonks fixture_id={fixture_id}"
    else:
        oa_event_id = str(args.oa_event_id).strip()
        fixture_id, sport_key, kickoff, prev_kickoff, prev1 = resolve_from_oa_event_id(engine, oa_event_id)
        title = f"OddsAPI event_id={oa_event_id} (resolved fixture_id={fixture_id})"

    times = build_snapshot_times(kickoff=kickoff, prev_kickoff=prev_kickoff)

    print("\n" + "=" * 140)
    print(f"Odds snapshot TEST+WRITE (OddsAPI {args.bookmaker} / region={args.region}) for {title}")
    print(f"  kickoff:        {kickoff.isoformat()}")
    print(f"  prev_1 (home):  {prev1}  kickoff={prev_kickoff.isoformat() if prev_kickoff else 'N/A'}")
    print(f"  OddsAPI sport:  {sport_key}")
    print(f"  OddsAPI event:  {oa_event_id}")
    print(f"  snapshots:      {len(times)} timestamps")
    print("=" * 140)

    rows_to_store: List[Dict[str, Any]] = []

    for idx, t in enumerate(times, start=1):
        timeline_id = f"odd_{idx}"  # times sorted DESC: closest to kickoff -> odd_1
        snap_ts = _to_utc(t)

        try:
            snap = fetch_h2h_odds_snapshot(
                sport_key=sport_key,
                event_id=oa_event_id,
                snapshot_dt=snap_ts,
                provider="oddsapi",
                bookmaker_key=args.bookmaker,
                region=args.region,
            )

            home = snap["home"]
            draw = snap["draw"]
            away = snap["away"]

            print(
                f"{timeline_id:>6} | snapshot={snap_ts.isoformat()} | "
                f"book={snap['bookmaker_used']} | home={home} draw={draw} away={away}"
            )

            rows_to_store.append(
                {
                    "fixture_id": fixture_id,
                    "timestamp": snap_ts,
                    "timeline_identifier": timeline_id,
                    "provider": str(args.provider).strip().lower(),
                    "home": home,
                    "draw": draw,
                    "away": away,
                }
            )

        except Exception as e:
            print(f"{timeline_id:>6} | snapshot={snap_ts.isoformat()} | ERROR: {e}")
            # still store a row with NULL odds so your timeline is complete
            rows_to_store.append(
                {
                    "fixture_id": fixture_id,
                    "timestamp": snap_ts,
                    "timeline_identifier": timeline_id,
                    "provider": str(args.provider).strip().lower(),
                    "home": None,
                    "draw": None,
                    "away": None,
                }
            )

    up = upsert_odds_1x2(engine, rows_to_store)
    print("\n" + "-" * 140)
    print(f"[WRITE] rows_prepared={len(rows_to_store)} upserted={up}")
    print("Table: public.odds_1x2")
    print("Done.")


if __name__ == "__main__":
    main()
