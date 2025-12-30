"""
Create + maintain `public.team_ratings`:
For every (fixture_id, team_id), compute avg_rating = mean(rating_player) over lineup players
where rating_player IS NOT NULL.

Columns:
- fixture_id (PK)
- team_id (PK)
- avg_rating
- updated_at
"""

from __future__ import annotations

from typing import List, Dict, Any

from sqlalchemy import MetaData, Table, Column, Integer, Float, DateTime, func, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from database.connection.engine import get_engine


def make_team_ratings_table(metadata: MetaData) -> Table:
    return Table(
        "team_ratings",
        metadata,
        Column("fixture_id", Integer, primary_key=True, nullable=False),
        Column("team_id", Integer, primary_key=True, nullable=False),
        Column("avg_rating", Float, nullable=True),
        Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    )


def compute_team_ratings(engine) -> List[Dict[str, Any]]:
    """
    Compute avg team ratings from public.lineups.
    AVG() ignores NULLs automatically.
    If a team has no non-null ratings in a fixture, AVG returns NULL (we keep it).
    """
    sql = text(
        """
        SELECT
            fixture_id,
            team_id,
            AVG(rating_player)::float AS avg_rating
        FROM public.lineups
        GROUP BY fixture_id, team_id
        ORDER BY fixture_id, team_id
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(sql).fetchall()

    return [
        {
            "fixture_id": int(r.fixture_id),
            "team_id": int(r.team_id),
            "avg_rating": float(r.avg_rating) if r.avg_rating is not None else None,
        }
        for r in rows
    ]


def upsert_team_ratings(engine, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    metadata = MetaData()
    team_ratings = make_team_ratings_table(metadata)
    metadata.create_all(engine)

    stmt = pg_insert(team_ratings).values(rows)
    excluded = stmt.excluded

    stmt = stmt.on_conflict_do_update(
        index_elements=[team_ratings.c.fixture_id, team_ratings.c.team_id],
        set_={
            "avg_rating": excluded.avg_rating,
            "updated_at": func.now(),
        },
        where=team_ratings.c.avg_rating.is_distinct_from(excluded.avg_rating),
    )

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return int(res.rowcount or 0)


def main() -> None:
    engine = get_engine()

    # Ensure table exists
    metadata = MetaData()
    make_team_ratings_table(metadata)
    metadata.create_all(engine)

    rows = compute_team_ratings(engine)
    print(f"[TEAM_RATINGS] computed rows: {len(rows)}")

    upserted = upsert_team_ratings(engine, rows)
    print(f"[TEAM_RATINGS] upserted rows (insert/update): {upserted}")
    print("Done. Table: public.team_ratings")


if __name__ == "__main__":
    main()