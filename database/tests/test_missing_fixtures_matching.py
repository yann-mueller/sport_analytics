"""
List fixtures that are NOT matched to OddsAPI yet (i.e., no row in fixtures_matching OR oa_event_id is NULL).

Then for the *last fixture in the printed list*, call OddsAPI historical events
for its league (sport_key) within kickoff ± 1 day and print all returned events.

Usage:
  python -m database.tests.test_missing_fixtures_matching
  python -m database.tests.test_missing_fixtures_matching --limit 200
  python -m database.tests.test_missing_fixtures_matching --league-id 8
  python -m database.tests.test_missing_fixtures_matching --season-id 17420
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from sqlalchemy import text

from database.connection.engine import get_engine

# OddsAPI helpers (your project)
from api_calls.auth.auth import get_access_params
from api_calls.helpers.providers.general import get_url


def _pad(s: str, width: int) -> str:
    return (s or "").ljust(width)


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        # If somehow naive, assume UTC
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso(dt: datetime) -> str:
    return _to_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")


def _find_league_mapping_csv() -> Optional[Path]:
    """
    Best-effort: find a league mapping csv in database/output/.
    Looks for filenames containing both 'league' and 'mapping' and ending with .csv
    """
    base = Path(__file__).resolve().parents[1]  # .../database
    out_dir = base / "output"
    if not out_dir.exists():
        return None

    candidates = sorted(out_dir.glob("*.csv"))
    # prefer those that look like league mapping
    preferred = [p for p in candidates if ("league" in p.name.lower() and "mapping" in p.name.lower())]
    if preferred:
        return preferred[0]

    # fallback: any csv with "league" in name
    fallback = [p for p in candidates if "league" in p.name.lower()]
    if fallback:
        return fallback[0]

    return None


def load_league_id_to_sport_key() -> Dict[int, str]:
    """
    Load mapping league_id -> oa_league_name (which is actually OddsAPI sport_key).
    Expects columns: league_id, league_name, oa_league_name
    """
    path = _find_league_mapping_csv()
    if path is None:
        return {}

    df = pd.read_csv(path)
    cols = {c.lower(): c for c in df.columns}

    if "league_id" not in cols or "oa_league_name" not in cols:
        return {}

    league_id_col = cols["league_id"]
    oa_col = cols["oa_league_name"]

    out: Dict[int, str] = {}
    for _, row in df.iterrows():
        try:
            lid = int(row[league_id_col])
        except Exception:
            continue
        sk = str(row[oa_col]).strip()
        if sk and sk.lower() != "nan":
            out[lid] = sk
    return out


def oddsapi_historical_events_in_window(
    sport_key: str,
    commence_from: datetime,
    commence_to: datetime,
    provider: str = "oddsapi",
) -> List[Dict[str, Any]]:
    """
    Calls OddsAPI historical events endpoint for a sport_key, filtered by commenceTimeFrom/To.
    We set 'date' to the end of the window (UTC) to get a snapshot >= events.
    """
    params = get_access_params(provider)
    api_key = params["api_token"]

    url_tmpl = get_url(provider, "historical_events")  # should be like: https://api.the-odds-api.com/v4/historical/sports/{sport}/events
    url = url_tmpl.format(sport=sport_key)

    # Choose snapshot date as window end (UTC) so OddsAPI returns the closest snapshot <= that time.
    date_iso = _iso(commence_to)
    from_iso = _iso(commence_from)
    to_iso = _iso(commence_to)

    r = requests.get(
        url,
        params={
            "apiKey": api_key,
            "date": date_iso,
            "dateFormat": "iso",
            "commenceTimeFrom": from_iso,
            "commenceTimeTo": to_iso,
        },
        timeout=45,
    )
    r.raise_for_status()
    payload = r.json()
    return payload.get("data", []) or []


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--league-id", type=int, default=None, help="optional filter: only this league_id")
    ap.add_argument("--season-id", type=int, default=None, help="optional filter: only this season_id (from fixtures)")
    args = ap.parse_args()

    engine = get_engine()

    league_filter = ""
    season_filter = ""
    params: Dict[str, Any] = {"limit": args.limit}

    if args.league_id is not None:
        league_filter = "AND f.league_id = :league_id"
        params["league_id"] = args.league_id

    if args.season_id is not None:
        season_filter = "AND f.season_id = :season_id"
        params["season_id"] = args.season_id

    sql = text(
        f"""
        SELECT
            f.fixture_id,
            f.date AS sm_date,
            f.league_id,
            f.home_team_id,
            f.away_team_id,
            th.team_name AS home_team_name,
            ta.team_name AS away_team_name,
            fm.oa_event_id
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
          {league_filter}
          {season_filter}
          AND (fm.oa_event_id IS NULL)
        ORDER BY f.date, f.fixture_id
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(sql, params).fetchall()

    if not rows:
        print("No missing matches found 🎉 (or filters excluded everything).")
        return

    # widths
    home_names = [(r.home_team_name or f"team_id={r.home_team_id}") for r in rows]
    away_names = [(r.away_team_name or f"team_id={r.away_team_id}") for r in rows]
    w_home = max(10, min(40, max(len(x) for x in home_names)))
    w_away = max(10, min(40, max(len(x) for x in away_names)))

    print("\n" + "=" * 120)
    print(f"Fixtures missing OddsAPI match (rows={len(rows)})")
    if args.league_id is not None:
        print(f"Filter: league_id={args.league_id}")
    if args.season_id is not None:
        print(f"Filter: season_id={args.season_id}")
    print("=" * 120)

    for r in rows:
        dt = r.sm_date.isoformat() if r.sm_date is not None else "—"
        home = r.home_team_name or f"team_id={r.home_team_id}"
        away = r.away_team_name or f"team_id={r.away_team_id}"

        print(
            f"fixture_id={r.fixture_id} | {dt} | "
            f"{_pad(home, w_home)} vs {_pad(away, w_away)} | league_id={r.league_id}"
        )

    print("\nNote: These are Sportmonks fixtures where fixtures_matching.oa_event_id is NULL (unmatched).")

    # ------------------------------------------------------------------
    # EXTRA: OddsAPI call for the last fixture in the list (last element)
    # ------------------------------------------------------------------
    last = rows[-1]
    if last.sm_date is None:
        print("\n[OddsAPI preview] Last fixture has no date -> skipping OddsAPI call.")
        return

    league_id = int(last.league_id)
    kickoff = last.sm_date  # timezone-aware timestamp (as stored)
    kickoff_utc = _to_utc(kickoff)

    mapping = load_league_id_to_sport_key()
    sport_key = mapping.get(league_id)

    print("\n" + "=" * 120)
    print("[OddsAPI preview for last missing fixture]")
    print("=" * 120)
    print(f"Last missing fixture: fixture_id={int(last.fixture_id)} | league_id={league_id} | kickoff={kickoff.isoformat()}")

    if not sport_key:
        print(
            "Could not find OddsAPI sport_key for this league_id in your league mapping CSV.\n"
            "Make sure your league mapping CSV exists in database/output/ and has columns:\n"
            "  league_id, league_name, oa_league_name\n"
            f"and that league_id={league_id} has a non-empty oa_league_name."
        )
        return

    window_from = kickoff_utc - timedelta(days=1)
    window_to = kickoff_utc + timedelta(days=1)

    print(f"Using sport_key={sport_key}")
    print(f"Query window: {window_from.isoformat()}  ->  {window_to.isoformat()} (kickoff ± 1 day, UTC)\n")

    try:
        events = oddsapi_historical_events_in_window(
            sport_key=sport_key,
            commence_from=window_from,
            commence_to=window_to,
        )
    except Exception as e:
        print(f"[OddsAPI preview] API call failed: {e}")
        return

    if not events:
        print("[OddsAPI preview] No events returned in this timeframe.")
        return

    # Pretty print events
    # (limit printed events just in case there are too many)
    print(f"[OddsAPI preview] events returned: {len(events)}")
    print("-" * 120)
    for e in events[:200]:
        eid = e.get("id", "—")
        ct = e.get("commence_time", "—")
        ht = e.get("home_team", "—")
        at = e.get("away_team", "—")
        print(f"{ct} | event_id={eid} | {ht} vs {at}")

    if len(events) > 200:
        print(f"... truncated, showing first 200 of {len(events)} events.")


if __name__ == "__main__":
    main()
