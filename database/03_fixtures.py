"""
database/fixtures/01_fixtures.py

Builds/updates public.fixtures for all seasons stored in public.seasons.

Table columns:
- fixture_id (PK)
- date
- league_id
- season_id
- home_team_id
- away_team_id
- home_goals
- away_goals
- provider
- updated_at

Behavior:
- Insert new fixtures
- Update existing fixtures ONLY if values changed (updated_at only then)
- Delete fixtures for this provider that are not part of the currently selected seasons set
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple, Set

import requests

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    Text,
    DateTime,
    func,
    select,
    delete,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

from api_calls.helpers.general import get_current_provider
from api_calls.helpers.providers.general import get_url
from api_calls.auth.auth import get_access_params

from database.connection.engine import get_engine


# -------------------------
# Parsing helpers
# -------------------------
def _teams_from_participants(participants: List[Dict[str, Any]]) -> Tuple[Optional[int], Optional[int]]:
    """
    Returns (home_team_id, away_team_id) based on participants[].meta.location.
    """
    home_id = away_id = None
    for p in participants or []:
        meta = p.get("meta") or {}
        loc = meta.get("location")
        if loc == "home":
            home_id = p.get("id")
        elif loc == "away":
            away_id = p.get("id")
    return (int(home_id) if home_id is not None else None,
            int(away_id) if away_id is not None else None)


def _goals_from_scores(scores: List[Dict[str, Any]]) -> Tuple[Optional[int], Optional[int]]:
    """
    Prefer description == 'CURRENT'. Otherwise last available.
    Returns (home_goals, away_goals).
    """
    home_goals = away_goals = None

    # Prefer CURRENT
    for s in scores or []:
        if s.get("description") == "CURRENT":
            sc = s.get("score", {}) or {}
            part = sc.get("participant")
            goals = sc.get("goals")
            if part == "home":
                home_goals = goals
            elif part == "away":
                away_goals = goals
            if home_goals is not None and away_goals is not None:
                return _to_int(home_goals), _to_int(away_goals)

    # Fallback: last available
    for s in scores or []:
        sc = s.get("score", {}) or {}
        part = sc.get("participant")
        goals = sc.get("goals")
        if part == "home":
            home_goals = goals
        elif part == "away":
            away_goals = goals

    return _to_int(home_goals), _to_int(away_goals)


def _to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


def _parse_season_schedule(
    schedule_json: Dict[str, Any],
    *,
    league_id: int,
    season_id: int,
    provider: str,
) -> List[Dict[str, Any]]:
    """
    Parses schedule JSON into rows for DB insert/upsert.
    """
    out: List[Dict[str, Any]] = []

    for stage in schedule_json.get("data", []) or []:
        for rnd in (stage.get("rounds", []) or []):
            for fx in (rnd.get("fixtures", []) or []):
                # make sure it's the right season
                if int(fx.get("season_id", -1)) != int(season_id):
                    continue

                fixture_id = fx.get("id")
                if fixture_id is None:
                    continue

                home_team_id, away_team_id = _teams_from_participants(fx.get("participants", []) or [])
                home_goals, away_goals = _goals_from_scores(fx.get("scores", []) or [])

                out.append(
                    {
                        "fixture_id": int(fixture_id),
                        "date": str(fx.get("starting_at") or "").strip() or None,
                        "league_id": int(league_id),
                        "season_id": int(season_id),
                        "home_team_id": home_team_id,
                        "away_team_id": away_team_id,
                        "home_goals": home_goals,
                        "away_goals": away_goals,
                        "provider": provider,
                    }
                )

    return out


# -------------------------
# API fetch
# -------------------------
def fetch_fixtures_for_season(provider: str, league_id: int, season_id: int) -> List[Dict[str, Any]]:
    """
    SportMonks schedules endpoint -> parse into fixture rows.
    """
    params = get_access_params(provider)
    api_token = params["api_token"]

    url = get_url(provider, "schedules_seasons").format(season_id=season_id)

    r = requests.get(url, params={"api_token": api_token}, timeout=60)
    r.raise_for_status()

    return _parse_season_schedule(r.json(), league_id=league_id, season_id=season_id, provider=provider)


# -------------------------
# DB tables
# -------------------------
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
        Column("date", Text, nullable=True),  # keep as text (SportMonks gives string); you can change to DateTime later
        Column("league_id", Integer, nullable=False),
        Column("season_id", Integer, nullable=False),
        Column("home_team_id", Integer, nullable=True),
        Column("away_team_id", Integer, nullable=True),
        Column("home_goals", Integer, nullable=True),
        Column("away_goals", Integer, nullable=True),
        Column("provider", Text, nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        schema="public",
    )


def upsert_fixtures(engine, fixture_rows: Sequence[Dict[str, Any]]) -> int:
    md = MetaData()
    fixtures = make_fixtures_table(md)

    md.create_all(engine)

    if not fixture_rows:
        return 0

    stmt = pg_insert(fixtures).values(list(fixture_rows))

    # Only update when something changed (null-safe)
    changed_cond = (
        fixtures.c.date.is_distinct_from(stmt.excluded.date)
        | fixtures.c.league_id.is_distinct_from(stmt.excluded.league_id)
        | fixtures.c.season_id.is_distinct_from(stmt.excluded.season_id)
        | fixtures.c.home_team_id.is_distinct_from(stmt.excluded.home_team_id)
        | fixtures.c.away_team_id.is_distinct_from(stmt.excluded.away_team_id)
        | fixtures.c.home_goals.is_distinct_from(stmt.excluded.home_goals)
        | fixtures.c.away_goals.is_distinct_from(stmt.excluded.away_goals)
        | fixtures.c.provider.is_distinct_from(stmt.excluded.provider)
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
        where=changed_cond,
    )

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return int(res.rowcount or 0)


def delete_fixtures_not_in_selected_seasons(engine, *, provider: str, keep_season_ids: Set[int]) -> int:
    """
    Delete fixtures for provider whose season_id is not in keep_season_ids.
    This is the natural analogue of your seasons/leagues scripts.
    """
    md = MetaData()
    fixtures = make_fixtures_table(md)
    md.create_all(engine)

    with engine.begin() as conn:
        if keep_season_ids:
            stmt = delete(fixtures).where(
                fixtures.c.provider == provider,
                ~fixtures.c.season_id.in_(keep_season_ids),
            )
        else:
            stmt = delete(fixtures).where(fixtures.c.provider == provider)

        res = conn.execute(stmt)
        return int(res.rowcount or 0)


# -------------------------
# Main
# -------------------------
def main() -> None:
    provider = get_current_provider(default="sportmonks").strip().lower()
    if provider != "sportmonks":
        raise ValueError("01_fixtures.py currently implements SportMonks schedule parsing only.")

    engine = get_engine()

    # Read all seasons from DB for this provider
    md = MetaData()
    seasons = make_seasons_table(md)

    with engine.begin() as conn:
        rows = conn.execute(
            select(seasons.c.season_id, seasons.c.league_id).where(seasons.c.provider == provider)
        ).fetchall()

    if not rows:
        print("No seasons found in public.seasons for this provider. Run 02_seasons first.")
        return

    keep_season_ids = {int(r[0]) for r in rows}

    all_fixture_rows: List[Dict[str, Any]] = []

    # Fetch fixtures for each season
    for season_id, league_id in rows:
        season_id_i = int(season_id)
        league_id_i = int(league_id)

        print(f"Fetching fixtures: league_id={league_id_i}, season_id={season_id_i}")
        fx_rows = fetch_fixtures_for_season(provider, league_id_i, season_id_i)
        all_fixture_rows.extend(fx_rows)

    changed = upsert_fixtures(engine, all_fixture_rows)
    deleted = delete_fixtures_not_in_selected_seasons(engine, provider=provider, keep_season_ids=keep_season_ids)

    print(f"Upsert complete. Rows inserted/updated: {changed}")
    print(f"Deleted fixtures not in selected seasons (provider={provider}): {deleted}")
    print("Table: public.fixtures")


if __name__ == "__main__":
    main()