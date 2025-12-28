"""
Create / update the leagues table from SportMonks
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Sequence, Union

import yaml
import requests

from sqlalchemy import (
    MetaData, Table, Column,
    Integer, Text, DateTime, func, delete
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

from api_calls.helpers.general import get_current_provider
from api_calls.helpers.providers.general import get_url
from api_calls.auth.auth import get_access_params
from database.connection.engine import get_engine


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def load_league_ids(yaml_path: Union[str, Path]) -> List[int]:
    yaml_path = Path(yaml_path)
    with yaml_path.open("r", encoding="utf-8") as f:
        cfg: Any = yaml.safe_load(f)

    if not isinstance(cfg, dict) or "leagues" not in cfg:
        raise ValueError(
            f"Invalid leagues YAML: {yaml_path}. Expected key 'leagues: [...]'"
        )

    ids: List[int] = []
    for x in cfg["leagues"]:
        ids.append(int(x))
    return ids


def fetch_all_leagues(provider: str) -> List[Dict[str, Any]]:
    params = get_access_params(provider)
    api_token = params["api_token"]

    url = get_url(provider, "leagues")
    r = requests.get(url, params={"api_token": api_token}, timeout=30)
    r.raise_for_status()

    return r.json().get("data", [])


# ---------------------------------------------------------------------
# Table definition
# ---------------------------------------------------------------------

def make_leagues_table(metadata: MetaData) -> Table:
    return Table(
        "leagues",
        metadata,
        Column("league_id", Integer, primary_key=True),
        Column("league_name", Text, nullable=False),
        Column("provider", Text, nullable=False),
        Column(
            "updated_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
        ),
    )


# ---------------------------------------------------------------------
# Upsert logic
# ---------------------------------------------------------------------

def upsert_leagues(
    engine,
    league_rows: Sequence[Dict[str, Any]],
) -> int:
    metadata = MetaData()
    leagues = make_leagues_table(metadata)

    # Create table if it does not exist
    metadata.create_all(engine)

    if not league_rows:
        return 0

    stmt = pg_insert(leagues).values(list(league_rows))

    stmt = stmt.on_conflict_do_update(
        index_elements=[leagues.c.league_id],
        set_={
            "league_name": stmt.excluded.league_name,
            "provider": stmt.excluded.provider,
            "updated_at": func.now(),
        },
        where=(
            (leagues.c.league_name.is_distinct_from(stmt.excluded.league_name))
            | (leagues.c.provider.is_distinct_from(stmt.excluded.provider))
        ),
    )

    with engine.begin() as conn:
        result = conn.execute(stmt)
        return int(result.rowcount or 0)


def delete_missing_leagues(engine, keep_ids: set[int], provider: str) -> int:
    """
    Delete rows from public.leagues that are NOT in keep_ids (for this provider).
    """
    metadata = MetaData()
    leagues = make_leagues_table(metadata)

    # If keep_ids is empty -> delete everything for this provider
    cond = (leagues.c.provider == provider)
    if keep_ids:
        stmt = delete(leagues).where(cond & (~leagues.c.league_id.in_(keep_ids)))
    else:
        stmt = delete(leagues).where(cond)

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return int(res.rowcount or 0)
    
# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main(
    league_ids_yaml: Union[str, Path] = "database/leagues/leagues.yaml",
) -> None:
    provider = get_current_provider().strip().lower()
    engine = get_engine()

    league_ids = set(load_league_ids(league_ids_yaml))
    all_leagues = fetch_all_leagues(provider)

    rows: List[Dict[str, Any]] = []
    for x in all_leagues:
        try:
            lid = int(x.get("id"))
        except Exception:
            continue

        if lid in league_ids:
            rows.append(
                {
                    "league_id": lid,
                    "league_name": str(x.get("name", "")).strip(),
                    "provider": provider,
                }
            )

    found = {r["league_id"] for r in rows}
    missing = sorted(league_ids - found)
    if missing:
        print(
            f"Warning: {len(missing)} league_ids not found: "
            f"{missing[:10]}{'...' if len(missing) > 10 else ''}"
        )

    changed = upsert_leagues(engine, rows)
    deleted = delete_missing_leagues(engine, keep_ids=league_ids, provider=provider)
    print(f"Upsert complete. Rows inserted/updated: {changed}")
    print(f"Deleted rows not in YAML: {deleted}")
    print("Table: public.leagues")


if __name__ == "__main__":
    main()
