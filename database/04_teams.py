from __future__ import annotations

from typing import Any, Dict, List, Sequence, Set

from sqlalchemy import (
    MetaData, Table, Column,
    Integer, Text, DateTime,
    func, select, delete
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


def upsert_teams(engine, rows: Sequence[Dict[str, Any]]) -> int:
    """
    Upsert rows and only bump updated_at when something actually changed.
    """
    md = MetaData()
    teams = make_teams_table(md)
    md.create_all(engine)

    if not rows:
        return 0

    ins = pg_insert(teams).values(list(rows))

    # Only update if team_name/provider changed
    update_where = (
        (teams.c.team_name.is_distinct_from(ins.excluded.team_name)) |
        (teams.c.provider.is_distinct_from(ins.excluded.provider))
    )

    stmt = ins.on_conflict_do_update(
        index_elements=[teams.c.team_id],
        set_={
            "team_name": ins.excluded.team_name,
            "provider": ins.excluded.provider,
            "updated_at": func.now(),
        },
        where=update_where,
    )

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return int(res.rowcount or 0)


def delete_teams_not_in_fixtures(engine, provider: str, keep_ids: Set[int]) -> int:
    """
    Delete teams for this provider that are no longer referenced by fixtures.
    """
    md = MetaData()
    teams = make_teams_table(md)
    md.create_all(engine)

    if not keep_ids:
        # if fixtures empty, you can choose to delete none or all; here: delete all provider teams
        stmt = delete(teams).where(teams.c.provider == provider)
    else:
        stmt = delete(teams).where(
            (teams.c.provider == provider) &
            (~teams.c.team_id.in_(list(keep_ids)))
        )

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return int(res.rowcount or 0)


def main() -> None:
    provider = get_current_provider(default="sportmonks").strip().lower()
    engine = get_engine()

    team_ids = sorted(get_distinct_team_ids_from_fixtures(engine))
    print(f"Found {len(team_ids)} distinct team_ids in fixtures.")

    rows: List[Dict[str, Any]] = []
    for tid in team_ids:
        try:
            team = get_team(tid, provider=provider)  # parsed mode
            name = (team.get("team_name") or "").strip()
            if not name:
                # skip empty names
                continue

            rows.append({
                "team_id": int(tid),
                "team_name": name,
                "provider": provider,
            })
        except Exception as e:
            print(f"Warning: failed to fetch team_id={tid}: {e}")

    changed = upsert_teams(engine, rows)
    deleted = delete_teams_not_in_fixtures(engine, provider, set(team_ids))

    print(f"Upsert complete. Rows inserted/updated: {changed}")
    print(f"Deleted rows not in fixtures (provider={provider}): {deleted}")
    print("Table: public.teams")


if __name__ == "__main__":
    main()