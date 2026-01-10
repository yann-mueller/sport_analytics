"""
database/team_ratings/08_extend_team_ratings.py

EXTEND-ONLY builder for `public.team_ratings`.

Goal:
- Compute and insert team average lineup ratings ONLY for newly added fixtures
  (i.e., fixtures that appear in public.lineups but not yet in public.team_ratings).

Definition:
- For every (fixture_id, team_id), avg_rating = mean(rating_player) over lineup players
  (AVG ignores NULLs; if all ratings are NULL, avg_rating becomes NULL).

Behavior:
- Creates `public.team_ratings` if missing
- Detects missing fixture_ids by comparing:
    DISTINCT fixture_id in public.lineups  vs.  DISTINCT fixture_id in public.team_ratings
- Computes avg_rating for those missing fixture_ids only
- Inserts rows (ON CONFLICT DO NOTHING for safety)
- Does NOT update existing rows and does NOT delete anything
"""

from __future__ import annotations

from typing import List, Dict, Any, Set

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
        schema="public",
    )


def get_fixture_ids_in_lineups(engine) -> Set[int]:
    sql = text("SELECT DISTINCT fixture_id FROM public.lineups WHERE fixture_id IS NOT NULL")
    with engine.begin() as conn:
        rows = conn.execute(sql).fetchall()
    return {int(r[0]) for r in rows}


def get_fixture_ids_in_team_ratings(engine) -> Set[int]:
    # if table doesn't exist yet, treat as empty
    exists_sql = text(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name='team_ratings'
        )
        """
    )
    with engine.begin() as conn:
        exists = bool(conn.execute(exists_sql).scalar_one())

    if not exists:
        return set()

    sql = text("SELECT DISTINCT fixture_id FROM public.team_ratings WHERE fixture_id IS NOT NULL")
    with engine.begin() as conn:
        rows = conn.execute(sql).fetchall()
    return {int(r[0]) for r in rows}


def compute_team_ratings_for_fixtures(engine, fixture_ids: List[int]) -> List[Dict[str, Any]]:
    """
    Compute avg team ratings from public.lineups for the given fixture_ids.
    AVG() ignores NULLs automatically.
    If a team has no non-null ratings in a fixture, AVG returns NULL (we keep it).
    """
    if not fixture_ids:
        return []

    sql = text(
        """
        SELECT
            fixture_id,
            team_id,
            AVG(rating_player)::float AS avg_rating
        FROM public.lineups
        WHERE fixture_id = ANY(:fixture_ids)
        GROUP BY fixture_id, team_id
        ORDER BY fixture_id, team_id
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(sql, {"fixture_ids": fixture_ids}).fetchall()

    return [
        {
            "fixture_id": int(r.fixture_id),
            "team_id": int(r.team_id),
            "avg_rating": float(r.avg_rating) if r.avg_rating is not None else None,
        }
        for r in rows
    ]


def insert_new_team_ratings(engine, rows: List[Dict[str, Any]]) -> int:
    """
    Insert-only (extend). No updates.
    """
    if not rows:
        return 0

    metadata = MetaData()
    team_ratings = make_team_ratings_table(metadata)
    metadata.create_all(engine)

    stmt = pg_insert(team_ratings).values(rows)
    stmt = stmt.on_conflict_do_nothing(index_elements=[team_ratings.c.fixture_id, team_ratings.c.team_id])

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return int(res.rowcount or 0)


def main() -> None:
    engine = get_engine()

    # Ensure table exists
    metadata = MetaData()
    make_team_ratings_table(metadata)
    metadata.create_all(engine)

    lineup_fixture_ids = get_fixture_ids_in_lineups(engine)
    existing_fixture_ids = get_fixture_ids_in_team_ratings(engine)

    missing_fixture_ids = sorted(list(lineup_fixture_ids - existing_fixture_ids))

    print(f"[TEAM_RATINGS-EXTEND] fixtures in lineups: {len(lineup_fixture_ids)}")
    print(f"[TEAM_RATINGS-EXTEND] fixtures already in team_ratings: {len(existing_fixture_ids)}")
    print(f"[TEAM_RATINGS-EXTEND] missing fixtures to compute: {len(missing_fixture_ids)}")

    if not missing_fixture_ids:
        print("[TEAM_RATINGS-EXTEND] Nothing to extend. public.team_ratings is up to date.")
        print("Done. Table: public.team_ratings")
        return

    rows = compute_team_ratings_for_fixtures(engine, missing_fixture_ids)
    print(f"[TEAM_RATINGS-EXTEND] computed rows: {len(rows)}")

    inserted = insert_new_team_ratings(engine, rows)
    print(f"[TEAM_RATINGS-EXTEND] inserted rows: {inserted}")
    print("Done. Table: public.team_ratings")


if __name__ == "__main__":
    main()
