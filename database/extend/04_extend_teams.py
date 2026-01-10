"""
database/teams/04_extend_teams.py

EXTEND-ONLY teams loader.

Goal:
- Keep public.teams in sync with new teams that appear in public.fixtures,
  without touching existing rows unnecessarily.

Behavior:
- Reads distinct team_ids referenced in public.fixtures (home_team_id and away_team_id)
- Reads existing team_ids already present in public.teams for the active provider
- Computes the set difference (new team_ids = in fixtures but not in teams)
- Fetches team metadata (name) from the provider API for each new team_id
- Inserts those new teams into public.teams (no deletes)
- Uses ON CONFLICT DO NOTHING for safety (idempotent if re-run)

Run:
  python -m database.teams.04_extend_teams
"""

from __future__ import annotations

from typing import Any, Dict, List, Sequence, Set

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    Text,
    DateTime,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

from database.connection.engine import get_engine
from api_calls.helpers.general import get_current_provider
from api_calls.teams import get_team


def make_teams_table(metadata: MetaData) -> Table:
    return Table(
        "teams",
        metadata,
        Column("team_id", Integer, primary_key=True),
        Column("team_name", Text, nullable=False),
        Column("provider", Text, nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        schema="public",
    )


def get_distinct_team_ids_from_fixtures(engine) -> Set[int]:
    """
    Reads distinct team_ids from public.fixtures.home_team_id and away_team_id.
    """
    md = MetaData(schema="public")
    fixtures = Table("fixtures", md, autoload_with=engine)

    q = (
        select(fixtures.c.home_team_id.label("team_id"))
        .where(fixtures.c.home_team_id.isnot(None))
        .union(
            select(fixtures.c.away_team_id.label("team_id"))
            .where(fixtures.c.away_team_id.isnot(None))
        )
    )

    with engine.begin() as conn:
        rows = conn.execute(q).fetchall()

    out: Set[int] = set()
    for (tid,) in rows:
        try:
            out.add(int(tid))
        except Exception:
            pass
    return out


def get_existing_team_ids(engine, provider: str) -> Set[int]:
    """
    Reads existing team_ids from public.teams for the given provider.
    """
    md = MetaData()
    teams = make_teams_table(md)
    md.create_all(engine)

    stmt = select(teams.c.team_id).where(teams.c.provider == provider)

    with engine.begin() as conn:
        rows = conn.execute(stmt).fetchall()

    out: Set[int] = set()
    for (tid,) in rows:
        try:
            out.add(int(tid))
        except Exception:
            pass
    return out


def insert_new_teams(engine, rows: Sequence[Dict[str, Any]]) -> int:
    """
    Insert rows only (extend-only). No updates, no deletes.
    """
    md = MetaData()
    teams = make_teams_table(md)
    md.create_all(engine)

    if not rows:
        return 0

    stmt = pg_insert(teams).values(list(rows))
    stmt = stmt.on_conflict_do_nothing(index_elements=[teams.c.team_id])

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return int(res.rowcount or 0)


def main() -> None:
    provider = get_current_provider(default="sportmonks").strip().lower()
    engine = get_engine()

    fixture_team_ids = get_distinct_team_ids_from_fixtures(engine)
    existing_team_ids = get_existing_team_ids(engine, provider=provider)

    new_team_ids = sorted(fixture_team_ids - existing_team_ids)

    print(f"Found {len(fixture_team_ids)} distinct team_ids in fixtures.")
    print(f"Existing in public.teams (provider={provider}): {len(existing_team_ids)}")
    print(f"New team_ids to add: {len(new_team_ids)}")

    if not new_team_ids:
        print("Nothing to extend. public.teams is up to date for this provider.")
        print("Table: public.teams")
        return

    rows: List[Dict[str, Any]] = []
    for tid in new_team_ids:
        try:
            team = get_team(int(tid), provider=provider)  # parsed mode
            name = (team.get("team_name") or "").strip()
            if not name:
                continue

            rows.append(
                {
                    "team_id": int(tid),
                    "team_name": name,
                    "provider": provider,
                }
            )
        except Exception as e:
            print(f"Warning: failed to fetch team_id={tid}: {e}")

    inserted = insert_new_teams(engine, rows)

    missing_names = len(new_team_ids) - len(rows)
    if missing_names > 0:
        print(f"Warning: {missing_names} new team_ids had no usable team_name and were skipped.")

    print(f"Extend complete. Rows inserted: {inserted}")
    print("Table: public.teams")


if __name__ == "__main__":
    main()
