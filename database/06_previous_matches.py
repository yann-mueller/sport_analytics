from __future__ import annotations

from typing import Optional

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    create_engine,
    text,
    func,
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
    )


def main(provider: Optional[str] = None) -> None:
    # provider scope (so you don’t accidentally mix providers in one table)
    if provider is None or not str(provider).strip():
        provider = get_current_provider(default="sportmonks")
    provider = str(provider).strip().lower()

    engine = get_engine()

    metadata = MetaData()
    prev_tbl = make_previous_matches_table(metadata)
    metadata.create_all(engine)

    # Build the dataset in SQL using window functions (fast + correct in Postgres)
    # We create one row per team per fixture (home + away), then LAG within (season_id, team_id)
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

            UNION ALL

            SELECT
                fixture_id,
                season_id,
                date,
                away_team_id AS team_id
            FROM public.fixtures
            WHERE provider = :provider
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
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(build_sql, {"provider": provider}).mappings().all()

    if not rows:
        print(f"No fixtures found for provider={provider}. Nothing to do.")
        return

    # Upsert only when changed (so no pointless rewrites)
    stmt = pg_insert(prev_tbl).values([dict(r) for r in rows])
    excluded = stmt.excluded

    changed_condition = (
        prev_tbl.c.season_id.is_distinct_from(excluded.season_id)
        | prev_tbl.c.prev_1.is_distinct_from(excluded.prev_1)
        | prev_tbl.c.prev_2.is_distinct_from(excluded.prev_2)
        | prev_tbl.c.prev_3.is_distinct_from(excluded.prev_3)
        | prev_tbl.c.prev_4.is_distinct_from(excluded.prev_4)
        | prev_tbl.c.prev_5.is_distinct_from(excluded.prev_5)
    )

    stmt = stmt.on_conflict_do_update(
        index_elements=[prev_tbl.c.fixture_id, prev_tbl.c.team_id],
        set_={
            "season_id": excluded.season_id,
            "prev_1": excluded.prev_1,
            "prev_2": excluded.prev_2,
            "prev_3": excluded.prev_3,
            "prev_4": excluded.prev_4,
            "prev_5": excluded.prev_5,
        },
        where=changed_condition,
    )

    with engine.begin() as conn:
        res = conn.execute(stmt)
        upserted = int(res.rowcount or 0)

    # Delete rows that are no longer supported by fixtures (for THIS provider)
    # We compute current valid keys from fixtures, then delete everything else.
    delete_sql = text(
        """
        WITH valid AS (
            SELECT fixture_id, home_team_id AS team_id
            FROM public.fixtures
            WHERE provider = :provider

            UNION ALL

            SELECT fixture_id, away_team_id AS team_id
            FROM public.fixtures
            WHERE provider = :provider
        )
        DELETE FROM public.previous_matches pm
        WHERE NOT EXISTS (
            SELECT 1 FROM valid v
            WHERE v.fixture_id = pm.fixture_id
              AND v.team_id = pm.team_id
        )
        """
    )

    with engine.begin() as conn:
        del_res = conn.execute(delete_sql, {"provider": provider})
        deleted = int(del_res.rowcount or 0)

    print("Done.")
    print(f"Provider: {provider}")
    print(f"Rows inserted/updated: {upserted}")
    print(f"Rows deleted (no longer in fixtures): {deleted}")
    print("Table: public.previous_matches")


if __name__ == "__main__":
    main()