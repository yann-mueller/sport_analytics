"""
database/fixtures/03_extend_fixtures.py

EXTEND-ONLY version of fixtures loading.

Goal:
- Check which season_ids (for this provider) exist in public.seasons but have
  no fixtures in public.fixtures yet.
- For those season_ids, fetch all fixtures and upsert them into public.fixtures.
- Do NOT delete anything.

Stores fixture dates as TIMESTAMP WITH TIME ZONE.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple, Set
from datetime import datetime, timezone

import requests

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    DateTime,
    Text,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

from api_calls.helpers.general import get_current_provider
from api_calls.helpers.providers.general import get_url
from api_calls.auth.auth import get_access_params

from database.connection.engine import get_engine


# -------------------------------------------------
# Helpers
# -------------------------------------------------
def _parse_datetime_utc(dt_str: Optional[str]) -> Optional[datetime]:
    """
    Parse SportMonks datetime string -> timezone-aware UTC datetime.
    Expected format: 'YYYY-MM-DD HH:MM:SS'
    """
    if not dt_str:
        return None
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _teams_from_participants(participants: List[Dict[str, Any]]) -> Tuple[Optional[int], Optional[int]]:
    home_id = away_id = None
    for p in participants or []:
        loc = (p.get("meta") or {}).get("location")
        if loc == "home":
            home_id = p.get("id")
        elif loc == "away":
            away_id = p.get("id")
    return (
        int(home_id) if home_id is not None else None,
        int(away_id) if away_id is not None else None,
    )


def _goals_from_scores(scores: List[Dict[str, Any]]) -> Tuple[Optional[int], Optional[int]]:
    home_goals = away_goals = None

    # Prefer CURRENT
    for s in scores or []:
        if s.get("description") == "CURRENT":
            sc = s.get("score", {}) or {}
            if sc.get("participant") == "home":
                home_goals = sc.get("goals")
            elif sc.get("participant") == "away":
                away_goals = sc.get("goals")
            if home_goals is not None and away_goals is not None:
                return _to_int(home_goals), _to_int(away_goals)

    # Fallback
    for s in scores or []:
        sc = s.get("score", {}) or {}
        if sc.get("participant") == "home":
            home_goals = sc.get("goals")
        elif sc.get("participant") == "away":
            away_goals = sc.get("goals")

    return _to_int(home_goals), _to_int(away_goals)


def _to_int(x: Any) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None


# -------------------------------------------------
# Parsing
# -------------------------------------------------
def _parse_season_schedule(
    schedule_json: Dict[str, Any],
    *,
    league_id: int,
    season_id: int,
    provider: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for stage in schedule_json.get("data", []) or []:
        for rnd in stage.get("rounds", []) or []:
            for fx in rnd.get("fixtures", []) or []:
                if int(fx.get("season_id", -1)) != season_id:
                    continue

                fixture_id = fx.get("id")
                if fixture_id is None:
                    continue

                home_team_id, away_team_id = _teams_from_participants(fx.get("participants", []))
                home_goals, away_goals = _goals_from_scores(fx.get("scores", []))

                rows.append(
                    {
                        "fixture_id": int(fixture_id),
                        "date": _parse_datetime_utc(fx.get("starting_at")),
                        "league_id": league_id,
                        "season_id": season_id,
                        "home_team_id": home_team_id,
                        "away_team_id": away_team_id,
                        "home_goals": home_goals,
                        "away_goals": away_goals,
                        "provider": provider,
                    }
                )

    return rows


# -------------------------------------------------
# API
# -------------------------------------------------
def fetch_fixtures_for_season(provider: str, league_id: int, season_id: int) -> List[Dict[str, Any]]:
    params = get_access_params(provider)
    url = get_url(provider, "schedules_seasons").format(season_id=season_id)

    r = requests.get(url, params={"api_token": params["api_token"]}, timeout=60)
    r.raise_for_status()

    return _parse_season_schedule(
        r.json(),
        league_id=league_id,
        season_id=season_id,
        provider=provider,
    )


# -------------------------------------------------
# Tables
# -------------------------------------------------
def make_seasons_table(metadata: MetaData) -> Table:
    return Table(
        "seasons",
        metadata,
        Column("season_id", Integer, primary_key=True),
        Column("league_id", Integer, nullable=False),
        Column("provider", Text, nullable=False),
        schema="public",
    )


def make_fixtures_table(metadata: MetaData) -> Table:
    return Table(
        "fixtures",
        metadata,
        Column("fixture_id", Integer, primary_key=True),
        Column("date", DateTime(timezone=True), nullable=True),
        Column("league_id", Integer, nullable=False),
        Column("season_id", Integer, nullable=False),
        Column("home_team_id", Integer, nullable=True),
        Column("away_team_id", Integer, nullable=True),
        Column("home_goals", Integer, nullable=True),
        Column("away_goals", Integer, nullable=True),
        Column("provider", Text, nullable=False),
        Column("updated_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
        schema="public",
    )


# -------------------------------------------------
# Upsert logic (same as baseline)
# -------------------------------------------------
def upsert_fixtures(engine, rows: Sequence[Dict[str, Any]]) -> int:
    md = MetaData()
    fixtures = make_fixtures_table(md)
    md.create_all(engine)

    if not rows:
        return 0

    stmt = pg_insert(fixtures).values(list(rows))

    changed = (
        fixtures.c.date.is_distinct_from(stmt.excluded.date)
        | fixtures.c.home_team_id.is_distinct_from(stmt.excluded.home_team_id)
        | fixtures.c.away_team_id.is_distinct_from(stmt.excluded.away_team_id)
        | fixtures.c.home_goals.is_distinct_from(stmt.excluded.home_goals)
        | fixtures.c.away_goals.is_distinct_from(stmt.excluded.away_goals)
    )

    stmt = stmt.on_conflict_do_update(
        index_elements=[fixtures.c.fixture_id],
        set_={
            "date": stmt.excluded.date,
            "league_id": stmt.excluded.league_id,
            "season_id": stmt.excluded.season_id,
            "home_team_id": stmt.excluded.home_team_id,
            "away_team_id": stmt.excluded.away_team_id,
            "home_goals": stmt.excluded.home_goals,
            "away_goals": stmt.excluded.away_goals,
            "provider": stmt.excluded.provider,
            "updated_at": func.now(),
        },
        where=changed,
    )

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return int(res.rowcount or 0)


# -------------------------------------------------
# Extend logic: find seasons missing in fixtures
# -------------------------------------------------
def seasons_without_fixtures(engine, *, provider: str) -> List[Tuple[int, int]]:
    """
    Returns list of (season_id, league_id) for seasons that have no fixtures yet
    in public.fixtures for this provider.
    """
    md = MetaData()
    seasons = make_seasons_table(md)
    fixtures = make_fixtures_table(md)

    # Ensure fixtures table exists (so the query doesn't break on fresh DBs)
    md.create_all(engine)

    # season_ids already present in fixtures (provider-scoped)
    existing_stmt = select(fixtures.c.season_id).where(fixtures.c.provider == provider).distinct()

    # seasons for provider that are NOT in existing fixtures seasons
    stmt = (
        select(seasons.c.season_id, seasons.c.league_id)
        .where(seasons.c.provider == provider)
        .where(~seasons.c.season_id.in_(existing_stmt))
    )

    with engine.begin() as conn:
        rows = conn.execute(stmt).fetchall()

    return [(int(r[0]), int(r[1])) for r in rows]


# -------------------------------------------------
# Main
# -------------------------------------------------
def main() -> None:
    provider = get_current_provider(default="sportmonks").strip().lower()
    engine = get_engine()

    missing = seasons_without_fixtures(engine, provider=provider)

    if not missing:
        print("No new seasons without fixtures found. Nothing to extend.")
        print("Table: public.fixtures")
        return

    all_rows: List[Dict[str, Any]] = []

    for season_id, league_id in missing:
        print(f"Fetching fixtures for NEW season {season_id} | league {league_id}...")
        all_rows.extend(fetch_fixtures_for_season(provider, league_id, season_id))

    changed = upsert_fixtures(engine, all_rows)

    print(f"Extend complete. Seasons processed: {len(missing)}")
    print(f"Upserted rows: {changed}")
    print("Table: public.fixtures")


if __name__ == "__main__":
    main()
