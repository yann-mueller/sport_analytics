"""
database/previous_matches/06_extend_previous_matches.py

EXTEND-ONLY builder for `public.previous_matches`.

Goal:
- Extend `public.previous_matches` for newly added fixtures (per provider),
  using the same logic as the full builder:
    - one row per team per fixture (home + away)
    - previous match ids computed via LAG within (season_id, team_id) ordered by (date, fixture_id)

Behavior:
- Creates `public.previous_matches` if missing
- Detects fixtures (for this provider) that are NOT yet present in `public.previous_matches`
  (i.e., fixture_ids missing entirely from the table)
- Recomputes the LAG structure for the relevant seasons/teams and inserts only the
  missing (fixture_id, team_id) rows (ON CONFLICT DO NOTHING for safety)
- Does NOT update existing rows
- Does NOT delete anything

Why it recomputes within-season:
- Adding new fixtures changes the "previous match" chain. However, this extend-only script
  intentionally only fills in missing rows. If you need full consistency after backfilling
  older fixtures or adding fixtures in-between dates, run the full builder instead.
"""

from __future__ import annotations

from typing import Optional, Set, Tuple, List

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    text,
    select,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

from database.connection.engine import get_engine
from api_calls.helpers.general import get_current_provider


def make_previous_matches_table(metadata: MetaData) -> Table:
    return Table(
        "previous_matches",
        metadata,
        Column("fixture_id", Integer, primary_key=True, nullable=False),
        Column("team_id", Integer, primary_key=True, nullable=False),
        Column("season_id", Integer, nullable=False),
        Column("prev_1", Integer, nullable=True),
        Column("prev_2", Integer, nullable=True),
        Column("prev_3", Integer, nullable=True),
        Column("prev_4", Integer, nullable=True),
        Column("prev_5", Integer, nullable=True),
        schema="public",
    )


def get_fixture_ids_in_fixtures(engine, *, provider: str) -> Set[int]:
    stmt = text(
        """
        SELECT DISTINCT fixture_id
        FROM public.fixtures
        WHERE provider = :provider
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(stmt, {"provider": provider}).fetchall()
    return {int(r[0]) for r in rows}


def get_fixture_ids_in_previous_matches(engine) -> Set[int]:
    stmt = text("SELECT DISTINCT fixture_id FROM public.previous_matches")
    with engine.begin() as conn:
        rows = conn.execute(stmt).fetchall()
    return {int(r[0]) for r in rows}


def get_missing_fixture_ids(engine, *, provider: str) -> Set[int]:
    fixture_ids = get_fixture_ids_in_fixtures(engine, provider=provider)
    if not fixture_ids:
        return set()

    # if previous_matches doesn't exist yet, treat as empty
    with engine.begin() as conn:
        exists = conn.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema='public' AND table_name='previous_matches'
                )
                """
            )
        ).scalar_one()
    if not bool(exists):
        return fixture_ids

    prev_fixture_ids = get_fixture_ids_in_previous_matches(engine)
    return fixture_ids - prev_fixture_ids


def seasons_to_recompute(engine, *, provider: str, missing_fixture_ids: Set[int]) -> Set[int]:
    """
    Which seasons contain missing fixtures? We'll recompute LAG for those seasons
    (across all teams in that season) but only INSERT missing fixture rows.
    """
    if not missing_fixture_ids:
        return set()

    stmt = text(
        """
        SELECT DISTINCT season_id
        FROM public.fixtures
        WHERE provider = :provider
          AND fixture_id = ANY(:fixture_ids)
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(
            stmt,
            {"provider": provider, "fixture_ids": list(missing_fixture_ids)},
        ).fetchall()
    return {int(r[0]) for r in rows}


def main(provider: Optional[str] = None) -> None:
    # provider scope (so you don’t accidentally mix providers in one table)
    if provider is None or not str(provider).strip():
        provider = get_current_provider(default="sportmonks")
    provider = str(provider).strip().lower()

    engine = get_engine()

    metadata = MetaData()
    prev_tbl = make_previous_matches_table(metadata)
    metadata.create_all(engine)

    # 1) Find missing fixtures (provider-scoped)
    missing_fixture_ids = get_missing_fixture_ids(engine, provider=provider)

    if not missing_fixture_ids:
        print(f"No missing fixture_ids for provider={provider}. Nothing to extend.")
        print("Table: public.previous_matches")
        return

    # 2) Recompute LAG structure for seasons that contain missing fixtures
    season_ids = seasons_to_recompute(engine, provider=provider, missing_fixture_ids=missing_fixture_ids)

    if not season_ids:
        print(f"Missing fixtures found ({len(missing_fixture_ids)}), but could not resolve seasons. Nothing to do.")
        return

    print(f"Provider: {provider}")
    print(f"Missing fixture_ids: {len(missing_fixture_ids)}")
    print(f"Recomputing previous-match chains for seasons: {len(season_ids)}")

    # Build dataset in SQL for only these seasons (fast in Postgres)
    build_sql = text(
        """
        WITH team_fixtures AS (
            SELECT
                fixture_id,
                season_id,
                date,
                home_team_id AS team_id
            FROM public.fixtures
            WHERE provider = :provider
              AND season_id = ANY(:season_ids)

            UNION ALL

            SELECT
                fixture_id,
                season_id,
                date,
                away_team_id AS team_id
            FROM public.fixtures
            WHERE provider = :provider
              AND season_id = ANY(:season_ids)
        ),
        lagged AS (
            SELECT
                fixture_id,
                team_id,
                season_id,
                LAG(fixture_id, 1) OVER (PARTITION BY season_id, team_id ORDER BY date, fixture_id) AS prev_1,
                LAG(fixture_id, 2) OVER (PARTITION BY season_id, team_id ORDER BY date, fixture_id) AS prev_2,
                LAG(fixture_id, 3) OVER (PARTITION BY season_id, team_id ORDER BY date, fixture_id) AS prev_3,
                LAG(fixture_id, 4) OVER (PARTITION BY season_id, team_id ORDER BY date, fixture_id) AS prev_4,
                LAG(fixture_id, 5) OVER (PARTITION BY season_id, team_id ORDER BY date, fixture_id) AS prev_5
            FROM team_fixtures
        )
        SELECT fixture_id, team_id, season_id, prev_1, prev_2, prev_3, prev_4, prev_5
        FROM lagged
        WHERE fixture_id = ANY(:missing_fixture_ids)
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(
            build_sql,
            {
                "provider": provider,
                "season_ids": list(season_ids),
                "missing_fixture_ids": list(missing_fixture_ids),
            },
        ).mappings().all()

    if not rows:
        print("No rows computed for missing fixtures. Nothing to insert.")
        return

    # 3) Insert only missing rows (extend-only)
    stmt = pg_insert(prev_tbl).values([dict(r) for r in rows])
    stmt = stmt.on_conflict_do_nothing(index_elements=[prev_tbl.c.fixture_id, prev_tbl.c.team_id])

    with engine.begin() as conn:
        res = conn.execute(stmt)
        inserted = int(res.rowcount or 0)

    print("Done.")
    print(f"Provider: {provider}")
    print(f"Rows computed (missing fixtures only): {len(rows)}")
    print(f"Rows inserted: {inserted}")
    print("Table: public.previous_matches")


if __name__ == "__main__":
    main()
