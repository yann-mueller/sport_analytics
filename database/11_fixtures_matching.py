"""
Create + maintain `public.fixtures_matching` which maps:
  fixture_id (your DB) + league_id  ->  oa_event_id (OddsAPI)
and stores the matched OddsAPI home/away team names + commence_time (OddsAPI date).

Table columns:
  fixture_id, league_id, oa_event_id, oa_home_team, oa_away_team, oa_commence_time, matched_at

Matching logic:
- Uses CSV mappings:
    database/output/league_mapping.csv        (league_id -> oa_league_name == OddsAPI sport_key)
    database/output/team_name_matching.csv    (team_id -> oa_name == OddsAPI team name)
- For each fixture:
    - map home/away team_ids to oa_name
    - call OddsAPI historical events endpoint for that sport_key and a time window around fixture time
    - find matching event by (home, away) (allow swapped) and closest commence_time
- UPSERT: updates if values changed
- Safety: default processes ONLY first 10 fixtures (set --limit to larger when ready)

Usage:
  python -m database.11_fixtures_matching
  python -m database.11_fixtures_matching --limit 10
  python -m database.11_fixtures_matching --limit 1000 --window-hours 12
"""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from sqlalchemy import MetaData, Table, Column, Integer, Text, DateTime, func, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from database.connection.engine import get_engine
from api_calls.auth.auth import get_access_params
from api_calls.helpers.providers.general import get_url


DEFAULT_LEAGUE_CSV = Path("database/output/league_mapping.csv")
DEFAULT_TEAM_CSV = Path("database/output/team_name_matching.csv")
DEFAULT_WINDOW_HOURS = 12

ISO_Z_RE = re.compile(r"Z$")


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso_utc(dt: datetime) -> str:
    return _to_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso_z(s: str) -> datetime:
    s2 = ISO_Z_RE.sub("+00:00", s)
    return datetime.fromisoformat(s2).astimezone(timezone.utc)


def _norm_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("&", "and")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\s\-\.]", "", s)
    return s


@dataclass
class FixtureRow:
    fixture_id: int
    league_id: int
    kickoff: datetime
    home_team_id: int
    away_team_id: int


# ----------------------------
# DB: fixtures_matching table
# ----------------------------
def make_fixtures_matching_table(metadata: MetaData) -> Table:
    return Table(
        "fixtures_matching",
        metadata,
        Column("fixture_id", Integer, primary_key=True, nullable=False),
        Column("league_id", Integer, nullable=False),
        Column("oa_event_id", Text, nullable=True),
        Column("oa_home_team", Text, nullable=True),
        Column("oa_away_team", Text, nullable=True),
        Column("oa_commence_time", DateTime(timezone=True), nullable=True),
        Column("matched_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    )


def ensure_columns_exist(engine) -> None:
    """
    If the table exists from an earlier run, add new columns if missing.
    """
    sql_cols = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name='fixtures_matching'
        """
    )
    with engine.begin() as conn:
        existing = {r[0] for r in conn.execute(sql_cols).fetchall()}

        if "oa_home_team" not in existing:
            conn.execute(text("ALTER TABLE public.fixtures_matching ADD COLUMN oa_home_team text"))
        if "oa_away_team" not in existing:
            conn.execute(text("ALTER TABLE public.fixtures_matching ADD COLUMN oa_away_team text"))
        if "oa_commence_time" not in existing:
            conn.execute(text("ALTER TABLE public.fixtures_matching ADD COLUMN oa_commence_time timestamptz"))


def upsert_fixtures_matching(engine, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    metadata = MetaData()
    tbl = make_fixtures_matching_table(metadata)
    metadata.create_all(engine)
    ensure_columns_exist(engine)

    stmt = pg_insert(tbl).values(rows)
    excluded = stmt.excluded

    stmt = stmt.on_conflict_do_update(
        index_elements=[tbl.c.fixture_id],
        set_={
            "league_id": excluded.league_id,
            "oa_event_id": excluded.oa_event_id,
            "oa_home_team": excluded.oa_home_team,
            "oa_away_team": excluded.oa_away_team,
            "oa_commence_time": excluded.oa_commence_time,
            "matched_at": func.now(),
        },
        where=(
            tbl.c.oa_event_id.is_distinct_from(excluded.oa_event_id)
            | tbl.c.oa_home_team.is_distinct_from(excluded.oa_home_team)
            | tbl.c.oa_away_team.is_distinct_from(excluded.oa_away_team)
            | tbl.c.oa_commence_time.is_distinct_from(excluded.oa_commence_time)
            | tbl.c.league_id.is_distinct_from(excluded.league_id)
        ),
    )

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return int(res.rowcount or 0)


def table_exists(engine, schema: str, table: str) -> bool:
    sql = text(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = :schema
          AND table_name = :table
        LIMIT 1
        """
    )
    with engine.begin() as conn:
        r = conn.execute(sql, {"schema": schema, "table": table}).fetchone()
    return r is not None


# ----------------------------
# CSV loaders
# ----------------------------
def load_league_mapping(path: Path) -> Dict[int, str]:
    if not path.exists():
        raise FileNotFoundError(f"League mapping CSV not found: {path}")

    out: Dict[int, str] = {}
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            if "league_id" not in row or "oa_league_name" not in row:
                raise RuntimeError(
                    f"League mapping CSV must have columns league_id, oa_league_name. Got: {reader.fieldnames}"
                )
            try:
                lid = int(str(row["league_id"]).strip())
            except Exception:
                continue
            sport_key = (row.get("oa_league_name") or "").strip()
            if sport_key:
                out[lid] = sport_key
    return out


def load_team_mapping(path: Path) -> Dict[int, str]:
    if not path.exists():
        raise FileNotFoundError(f"Team mapping CSV not found: {path}")

    out: Dict[int, str] = {}
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            if "team_id" not in row or "oa_name" not in row:
                raise RuntimeError(f"Team mapping CSV must have columns team_id, oa_name. Got: {reader.fieldnames}")
            try:
                tid = int(str(row["team_id"]).strip())
            except Exception:
                continue
            oa_name = (row.get("oa_name") or "").strip()
            if oa_name:
                out[tid] = oa_name
    return out


# ----------------------------
# DB reading
# ----------------------------
def fetch_candidate_fixtures(engine, league_ids: List[int], limit: int) -> List[FixtureRow]:
    if table_exists(engine, "public", "fixtures_matching"):
        sql = text(
            """
            SELECT
                f.fixture_id,
                f.league_id,
                f.date AS kickoff,
                f.home_team_id,
                f.away_team_id
            FROM public.fixtures f
            LEFT JOIN public.fixtures_matching fm
              ON fm.fixture_id = f.fixture_id
            WHERE f.league_id = ANY(:league_ids)
              AND f.date IS NOT NULL
              AND f.home_team_id IS NOT NULL
              AND f.away_team_id IS NOT NULL
              AND (fm.oa_event_id IS NULL)
            ORDER BY f.date, f.fixture_id
            LIMIT :limit
            """
        )
    else:
        sql = text(
            """
            SELECT
                f.fixture_id,
                f.league_id,
                f.date AS kickoff,
                f.home_team_id,
                f.away_team_id
            FROM public.fixtures f
            WHERE f.league_id = ANY(:league_ids)
              AND f.date IS NOT NULL
              AND f.home_team_id IS NOT NULL
              AND f.away_team_id IS NOT NULL
            ORDER BY f.date, f.fixture_id
            LIMIT :limit
            """
        )

    with engine.begin() as conn:
        rows = conn.execute(sql, {"league_ids": league_ids, "limit": limit}).fetchall()

    out: List[FixtureRow] = []
    for r in rows:
        out.append(
            FixtureRow(
                fixture_id=int(r.fixture_id),
                league_id=int(r.league_id),
                kickoff=r.kickoff,
                home_team_id=int(r.home_team_id),
                away_team_id=int(r.away_team_id),
            )
        )
    return out


# ----------------------------
# OddsAPI call
# ----------------------------
def fetch_oddsapi_historical_events(
    sport_key: str,
    snapshot_date_iso: str,
    commence_from_iso: str,
    commence_to_iso: str,
    provider: str = "oddsapi",
) -> Dict[str, Any]:
    params = get_access_params(provider)
    api_key = params["api_token"]
    url_tmpl = get_url(provider, "historical_events")
    url = url_tmpl.format(sport=sport_key)

    r = requests.get(
        url,
        params={
            "apiKey": api_key,
            "date": snapshot_date_iso,
            "dateFormat": "iso",
            "commenceTimeFrom": commence_from_iso,
            "commenceTimeTo": commence_to_iso,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def choose_best_event(
    events: List[Dict[str, Any]],
    home_name: str,
    away_name: str,
    kickoff_utc: datetime,
) -> Tuple[Optional[str], Optional[str], Optional[datetime], Optional[str], Optional[str]]:
    """
    Returns (event_id, match_type, commence_time_utc, oa_home_team, oa_away_team)
    """
    hn = _norm_name(home_name)
    an = _norm_name(away_name)

    best: Tuple[float, str, str, datetime, str, str] | None = None
    for e in events:
        eid = e.get("id")
        ht = e.get("home_team") or ""
        at = e.get("away_team") or ""
        ct = e.get("commence_time")
        if not eid or not ct:
            continue

        ht_s = str(ht)
        at_s = str(at)

        ht_n = _norm_name(ht_s)
        at_n = _norm_name(at_s)

        match_type: Optional[str] = None
        if ht_n == hn and at_n == an:
            match_type = "exact"
        elif ht_n == an and at_n == hn:
            match_type = "swapped"
        else:
            continue

        commence_dt = _parse_iso_z(str(ct))
        abs_s = abs((commence_dt - kickoff_utc).total_seconds())

        if best is None or abs_s < best[0]:
            best = (abs_s, match_type, str(eid), commence_dt, ht_s, at_s)

    if best is None:
        return None, None, None, None, None
    return best[2], best[1], best[3], best[4], best[5]


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league-csv", type=str, default=str(DEFAULT_LEAGUE_CSV))
    ap.add_argument("--team-csv", type=str, default=str(DEFAULT_TEAM_CSV))
    ap.add_argument("--limit", type=int, help="Only process this many fixtures (safety).")
    ap.add_argument("--window-hours", type=int, default=DEFAULT_WINDOW_HOURS)
    ap.add_argument(
        "--snapshot-mode",
        type=str,
        default="kickoff_plus_1h",
        choices=["kickoff_plus_1h", "now"],
        help="Which OddsAPI snapshot date to query.",
    )
    args = ap.parse_args()

    engine = get_engine()

    # ensure table exists early
    metadata = MetaData()
    make_fixtures_matching_table(metadata)
    metadata.create_all(engine)
    ensure_columns_exist(engine)

    league_map = load_league_mapping(Path(args.league_csv))
    team_map = load_team_mapping(Path(args.team_csv))

    if not league_map:
        raise RuntimeError("No league mappings with oa_league_name found. Fill database/output/league_mapping.csv first.")
    if not team_map:
        raise RuntimeError("No team mappings with oa_name found. Fill database/output/team_name_matching.csv first.")

    league_ids = sorted(list(league_map.keys()))
    fixtures = fetch_candidate_fixtures(engine, league_ids=league_ids, limit=args.limit)

    print(f"[FIXTURE MATCH] candidate fixtures (unmatched): {len(fixtures)} (limit={args.limit})")
    if not fixtures:
        print("[FIXTURE MATCH] Nothing to do.")
        return

    to_upsert: List[Dict[str, Any]] = []
    matched = 0
    skipped_missing_mapping = 0
    no_event_found = 0

    for fx in fixtures:
        sport_key = league_map.get(fx.league_id)
        if not sport_key:
            skipped_missing_mapping += 1
            print(f"[SKIP] fixture_id={fx.fixture_id} league_id={fx.league_id}: no oa_league_name (sport_key) mapping")
            continue

        home_oa = team_map.get(fx.home_team_id)
        away_oa = team_map.get(fx.away_team_id)
        if not home_oa or not away_oa:
            skipped_missing_mapping += 1
            print(
                f"[SKIP] fixture_id={fx.fixture_id}: missing team mapping "
                f"home_team_id={fx.home_team_id} mapped={bool(home_oa)} "
                f"away_team_id={fx.away_team_id} mapped={bool(away_oa)}"
            )
            continue

        kickoff_utc = _to_utc(fx.kickoff)
        window = timedelta(hours=int(args.window_hours))
        commence_from = _iso_utc(kickoff_utc - window)
        commence_to = _iso_utc(kickoff_utc + window)

        if args.snapshot_mode == "kickoff_plus_1h":
            snapshot_date = _iso_utc(kickoff_utc + timedelta(hours=1))
        else:
            snapshot_date = _iso_utc(datetime.now(timezone.utc))

        try:
            payload = fetch_oddsapi_historical_events(
                sport_key=sport_key,
                snapshot_date_iso=snapshot_date,
                commence_from_iso=commence_from,
                commence_to_iso=commence_to,
            )
        except Exception as e:
            print(f"[ERROR] OddsAPI call failed fixture_id={fx.fixture_id} sport_key={sport_key}: {e}")
            continue

        events = payload.get("data", []) or []
        event_id, match_type, commence_dt, oa_home_team, oa_away_team = choose_best_event(
            events=events,
            home_name=home_oa,
            away_name=away_oa,
            kickoff_utc=kickoff_utc,
        )

        if not event_id:
            no_event_found += 1
            print(
                f"[NO MATCH] fixture_id={fx.fixture_id} league_id={fx.league_id} sport_key={sport_key} "
                f"kickoff_utc={kickoff_utc.isoformat()} home='{home_oa}' away='{away_oa}' "
                f"events_returned={len(events)} window_h={args.window_hours}"
            )
            continue

        matched += 1
        print(
            f"[MATCH] fixture_id={fx.fixture_id} -> oa_event_id={event_id} ({match_type}) "
            f"kickoff_utc={kickoff_utc.isoformat()} oa_commence_utc={commence_dt.isoformat() if commence_dt else 'N/A'} "
            f"sport_key={sport_key} oa_home='{oa_home_team}' oa_away='{oa_away_team}'"
        )

        to_upsert.append(
            {
                "fixture_id": fx.fixture_id,
                "league_id": fx.league_id,
                "oa_event_id": event_id,
                "oa_home_team": oa_home_team,
                "oa_away_team": oa_away_team,
                "oa_commence_time": commence_dt,  # timestamptz
            }
        )

    up = upsert_fixtures_matching(engine, to_upsert)

    print("\nDone.")
    print(f"[FIXTURE MATCH] processed={len(fixtures)} matched={matched} to_upsert={len(to_upsert)} upserted={up}")
    print(f"[FIXTURE MATCH] skipped_missing_mapping={skipped_missing_mapping} no_event_found={no_event_found}")
    print("Table: public.fixtures_matching")


if __name__ == "__main__":
    main()
