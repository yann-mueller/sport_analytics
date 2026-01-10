"""
Update missing fixtures_matching rows by re-trying OddsAPI matching with a relaxed rule:
ONLY ONE team name has to match within kickoff ± window-days (league must match via sport_key).

This is useful if one team name changed over time in OddsAPI.

CHANGES vs your previous version:
1) Uses `matched_at` (NOT `updated_at`) to match your fixtures_matching schema.
2) Adds optional `--season-id` filter: only retry missing matchings for fixtures in that season_id.

Usage:
  python -m database.12_update_missing_fixtures --limit 10
  python -m database.12_update_missing_fixtures --limit 200
  python -m database.12_update_missing_fixtures --league-id 8 --limit 50
  python -m database.12_update_missing_fixtures --season-id 1234 --limit 200
  python -m database.12_update_missing_fixtures --dry-run --limit 20
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from sqlalchemy import MetaData, Table, Column, Integer, Text, DateTime, func, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from database.connection.engine import get_engine

# OddsAPI helpers (your project)
from api_calls.auth.auth import get_access_params
from api_calls.helpers.providers.general import get_url


# ----------------------------
# Helpers
# ----------------------------
def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso(dt: datetime) -> str:
    return _to_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")


def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _find_mapping_csv(kind: str) -> Optional[Path]:
    """
    kind: 'league' or 'team'
    Always look in database/output/ (one level above this file's folder).
    """
    db_dir = Path(__file__).resolve().parents[1]
    out_dir = db_dir / "output"

    if not out_dir.exists():
        return None

    candidates = sorted(out_dir.glob("*.csv"))
    preferred = [p for p in candidates if (kind in p.name.lower() and "mapping" in p.name.lower())]
    if preferred:
        return preferred[0]

    fallback = [p for p in candidates if kind in p.name.lower()]
    if fallback:
        return fallback[0]

    return None


def load_league_id_to_sport_key() -> Dict[int, str]:
    """
    Expects CSV with at least columns: league_id, oa_league_name
    where oa_league_name is the OddsAPI sport_key.
    """
    path = _find_mapping_csv("league")
    if path is None:
        return {}

    df = pd.read_csv(path)
    cols = {c.lower(): c for c in df.columns}
    if "league_id" not in cols or "oa_league_name" not in cols:
        return {}

    out: Dict[int, str] = {}
    for _, row in df.iterrows():
        try:
            lid = int(row[cols["league_id"]])
        except Exception:
            continue
        sk = str(row[cols["oa_league_name"]]).strip()
        if sk and sk.lower() != "nan":
            out[lid] = sk
    return out


def load_team_id_to_oa_name() -> Dict[int, str]:
    """
    Expects CSV with at least columns: team_id, oa_name
    """
    path = _find_mapping_csv("team")
    if path is None:
        return {}

    df = pd.read_csv(path)
    cols = {c.lower(): c for c in df.columns}
    if "team_id" not in cols or "oa_name" not in cols:
        return {}

    out: Dict[int, str] = {}
    for _, row in df.iterrows():
        try:
            tid = int(row[cols["team_id"]])
        except Exception:
            continue
        nm = str(row[cols["oa_name"]]).strip()
        if nm and nm.lower() != "nan":
            out[tid] = nm
    return out


def oddsapi_historical_events(
    sport_key: str,
    commence_from: datetime,
    commence_to: datetime,
    provider: str = "oddsapi",
) -> List[Dict[str, Any]]:
    """
    OddsAPI GET /v4/historical/sports/{sport}/events?apiKey=...&date=...
    filtered by commenceTimeFrom/To.

    We set snapshot 'date' to end of window so returned snapshot is <= that time.
    """
    params = get_access_params(provider)
    api_key = params["api_token"]

    url_tmpl = get_url(provider, "historical_events")
    url = url_tmpl.format(sport=sport_key)

    r = requests.get(
        url,
        params={
            "apiKey": api_key,
            "date": _iso(commence_to),
            "dateFormat": "iso",
            "commenceTimeFrom": _iso(commence_from),
            "commenceTimeTo": _iso(commence_to),
        },
        timeout=45,
    )
    r.raise_for_status()
    payload = r.json()
    return payload.get("data", []) or []


# ----------------------------
# DB table (use matched_at)
# ----------------------------
def make_fixtures_matching_table(metadata: MetaData) -> Table:
    return Table(
        "fixtures_matching",
        metadata,
        Column("fixture_id", Integer, primary_key=True, nullable=False),
        Column("league_id", Integer, nullable=True),
        Column("oa_event_id", Text, nullable=True),
        Column("oa_commence_time", DateTime(timezone=True), nullable=True),
        Column("oa_home_team", Text, nullable=True),
        Column("oa_away_team", Text, nullable=True),
        Column("matched_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        schema="public",
    )


def ensure_columns_exist(engine) -> None:
    """
    If table exists from earlier run, add missing columns if necessary.
    (Keeps this script resilient to schema drift.)
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
        if "matched_at" not in existing:
            conn.execute(
                text("ALTER TABLE public.fixtures_matching ADD COLUMN matched_at timestamptz NOT NULL DEFAULT now()")
            )


def upsert_match(engine, row: Dict[str, Any], dry_run: bool = False) -> int:
    if dry_run:
        return 0

    metadata = MetaData()
    tbl = make_fixtures_matching_table(metadata)
    metadata.create_all(engine)
    ensure_columns_exist(engine)

    stmt = pg_insert(tbl).values([row])
    excluded = stmt.excluded

    stmt = stmt.on_conflict_do_update(
        index_elements=[tbl.c.fixture_id],
        set_={
            "league_id": excluded.league_id,
            "oa_event_id": excluded.oa_event_id,
            "oa_commence_time": excluded.oa_commence_time,
            "oa_home_team": excluded.oa_home_team,
            "oa_away_team": excluded.oa_away_team,
            "matched_at": func.now(),
        },
        where=(
            tbl.c.oa_event_id.is_distinct_from(excluded.oa_event_id)
            | tbl.c.oa_commence_time.is_distinct_from(excluded.oa_commence_time)
            | tbl.c.oa_home_team.is_distinct_from(excluded.oa_home_team)
            | tbl.c.oa_away_team.is_distinct_from(excluded.oa_away_team)
            | tbl.c.league_id.is_distinct_from(excluded.league_id)
        ),
    )

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return int(res.rowcount or 0)


# ----------------------------
# Fetch missing fixtures (with optional league_id + season_id)
# ----------------------------
def fetch_missing_fixtures(
    engine,
    limit: int,
    league_id: Optional[int] = None,
    season_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    filters = []
    params: Dict[str, Any] = {"limit": limit}

    if league_id is not None:
        filters.append("f.league_id = :league_id")
        params["league_id"] = league_id

    if season_id is not None:
        filters.append("f.season_id = :season_id")
        params["season_id"] = season_id

    extra_where = ""
    if filters:
        extra_where = "AND " + " AND ".join(filters)

    sql = text(
        f"""
        SELECT
            f.fixture_id,
            f.league_id,
            f.season_id,
            f.date AS kickoff,
            f.home_team_id,
            f.away_team_id,
            th.team_name AS home_team_name,
            ta.team_name AS away_team_name
        FROM public.fixtures f
        LEFT JOIN public.fixtures_matching fm
          ON fm.fixture_id = f.fixture_id
        LEFT JOIN public.teams th
          ON th.team_id = f.home_team_id
        LEFT JOIN public.teams ta
          ON ta.team_id = f.away_team_id
        WHERE f.date IS NOT NULL
          AND f.home_team_id IS NOT NULL
          AND f.away_team_id IS NOT NULL
          {extra_where}
          AND fm.oa_event_id IS NULL
        ORDER BY f.date, f.fixture_id
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(sql, params).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "fixture_id": int(r.fixture_id),
                "league_id": int(r.league_id) if r.league_id is not None else None,
                "season_id": int(r.season_id) if r.season_id is not None else None,
                "kickoff": r.kickoff,
                "home_team_id": int(r.home_team_id),
                "away_team_id": int(r.away_team_id),
                "home_team_name": r.home_team_name,
                "away_team_name": r.away_team_name,
            }
        )
    return out


# ----------------------------
# Matching logic (relaxed)
# ----------------------------
@dataclass
class Candidate:
    event_id: str
    commence_time: datetime
    home_team: str
    away_team: str
    score: int
    time_diff_seconds: float


def choose_best_event(
    events: List[Dict[str, Any]],
    kickoff: datetime,
    oa_home_expected: Optional[str],
    oa_away_expected: Optional[str],
) -> Optional[Candidate]:
    """
    Score:
      2 = both teams match (either orientation)
      1 = only one team matches (either orientation)
      0 = none (ignored)

    Tie-break: smallest abs time diff to kickoff.
    """
    k_utc = _to_utc(kickoff)

    eh = _norm(oa_home_expected)
    ea = _norm(oa_away_expected)

    best: Optional[Candidate] = None

    for e in events:
        eid = e.get("id")
        ct = e.get("commence_time")
        ht = e.get("home_team") or ""
        at = e.get("away_team") or ""
        if not eid or not ct:
            continue

        try:
            ct_dt = datetime.fromisoformat(str(ct).replace("Z", "+00:00"))
        except Exception:
            continue

        ht_n = _norm(str(ht))
        at_n = _norm(str(at))

        direct_home = (eh != "" and eh == ht_n)
        direct_away = (ea != "" and ea == at_n)
        direct_score = (1 if direct_home else 0) + (1 if direct_away else 0)

        swap_home = (eh != "" and eh == at_n)
        swap_away = (ea != "" and ea == ht_n)
        swap_score = (1 if swap_home else 0) + (1 if swap_away else 0)

        score = max(direct_score, swap_score)

        # RELAXED RULE: accept score >= 1
        if score < 1:
            continue

        diff = abs((_to_utc(ct_dt) - k_utc).total_seconds())

        cand = Candidate(
            event_id=str(eid),
            commence_time=ct_dt,
            home_team=str(ht),
            away_team=str(at),
            score=score,
            time_diff_seconds=diff,
        )

        if best is None:
            best = cand
            continue

        if cand.score > best.score:
            best = cand
        elif cand.score == best.score and cand.time_diff_seconds < best.time_diff_seconds:
            best = cand

    return best


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=200, help="how many missing fixtures to try (start small)")
    ap.add_argument("--league-id", type=int, default=None, help="optional: only fix one league_id")
    ap.add_argument("--season-id", type=int, default=None, help="optional: only fix one season_id")
    ap.add_argument("--dry-run", action="store_true", help="do not write to DB, only print what would happen")
    ap.add_argument("--window-days", type=int, default=1, help="kickoff ± window-days for OddsAPI query")
    args = ap.parse_args()

    engine = get_engine()

    # Ensure table exists + has expected columns
    md = MetaData()
    make_fixtures_matching_table(md)
    md.create_all(engine)
    ensure_columns_exist(engine)

    league_to_sport = load_league_id_to_sport_key()
    team_to_oa = load_team_id_to_oa_name()

    if not league_to_sport:
        raise RuntimeError(
            "League mapping not found/loaded. Put league_mapping.csv into database/output/ with league_id + oa_league_name."
        )
    if not team_to_oa:
        raise RuntimeError(
            "Team mapping not found/loaded. Put team_name_matching.csv into database/output/ with team_id + oa_name."
        )

    missing = fetch_missing_fixtures(engine, limit=args.limit, league_id=args.league_id, season_id=args.season_id)
    if not missing:
        print("[UPDATE_MISSING] No missing fixtures found (oa_event_id IS NULL) for given filters.")
        return

    print(
        f"[UPDATE_MISSING] Missing fixtures to try: {len(missing)} "
        f"(dry_run={args.dry_run}, league_id={args.league_id}, season_id={args.season_id})"
    )

    total_upserted = 0
    total_matched = 0
    total_failed = 0
    total_skipped = 0

    for i, f in enumerate(missing, start=1):
        fixture_id = f["fixture_id"]
        league_id = f["league_id"]
        kickoff = f["kickoff"]
        home_id = f["home_team_id"]
        away_id = f["away_team_id"]

        if league_id is None or league_id not in league_to_sport:
            print(f"[{i}/{len(missing)}] fixture_id={fixture_id}: no league mapping for league_id={league_id} -> skip")
            total_skipped += 1
            continue

        sport_key = league_to_sport[league_id]

        oa_home = team_to_oa.get(home_id, "")
        oa_away = team_to_oa.get(away_id, "")

        if not oa_home and not oa_away:
            print(f"[{i}/{len(missing)}] fixture_id={fixture_id}: no team mapping for both teams -> skip")
            total_skipped += 1
            continue

        k_utc = _to_utc(kickoff)
        window_from = k_utc - timedelta(days=args.window_days)
        window_to = k_utc + timedelta(days=args.window_days)

        try:
            events = oddsapi_historical_events(
                sport_key=sport_key,
                commence_from=window_from,
                commence_to=window_to,
            )
        except Exception as e:
            print(f"[{i}/{len(missing)}] fixture_id={fixture_id}: OddsAPI call failed: {e}")
            total_failed += 1
            continue

        best = choose_best_event(events, kickoff=kickoff, oa_home_expected=oa_home, oa_away_expected=oa_away)
        if best is None:
            print(
                f"[{i}/{len(missing)}] fixture_id={fixture_id}: no candidate "
                f"(sport_key={sport_key}, oa_home='{oa_home}', oa_away='{oa_away}', events={len(events)})"
            )
            total_failed += 1
            continue

        row = {
            "fixture_id": fixture_id,
            "league_id": league_id,
            "oa_event_id": best.event_id,
            "oa_commence_time": best.commence_time,
            "oa_home_team": best.home_team,
            "oa_away_team": best.away_team,
        }

        up = upsert_match(engine, row, dry_run=args.dry_run)
        total_upserted += up
        total_matched += 1

        sm_home = f["home_team_name"] or f"team_id={home_id}"
        sm_away = f["away_team_name"] or f"team_id={away_id}"
        print(
            f"[{i}/{len(missing)}] fixture_id={fixture_id} | season_id={f.get('season_id')} | {kickoff.isoformat()} | {sm_home} vs {sm_away}\n"
            f"   -> MATCH score={best.score} time_diff_s={best.time_diff_seconds:.0f} "
            f"oa_event_id={best.event_id} | {best.commence_time.isoformat()} | {best.home_team} vs {best.away_team}\n"
        )

    print("\nDone.")
    print(
        f"[UPDATE_MISSING] matched={total_matched} failed={total_failed} skipped={total_skipped} upserted={total_upserted}"
    )
    print("Table: public.fixtures_matching")


if __name__ == "__main__":
    main()
