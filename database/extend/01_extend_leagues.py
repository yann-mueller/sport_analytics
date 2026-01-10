"""
Create / update the leagues table from SportMonks

NEW BEHAVIOR:
- Only inserts leagues that are NEW in the YAML (i.e., league_id not yet present in DB for this provider).
- Does NOT update existing rows.
- Does NOT delete anything.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Sequence, Union

import yaml
import requests

from sqlalchemy import (
    MetaData, Table, Column,
    Integer, Text, DateTime, func, select
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
# DB helpers (NEW)
# ---------------------------------------------------------------------
def get_existing_league_ids(engine, provider: str) -> set[int]:
    """
    Fetch existing league_ids in public.leagues for a provider.
    """
    metadata = MetaData()
    leagues = make_leagues_table(metadata)
    metadata.create_all(engine)

    stmt = select(leagues.c.league_id).where(leagues.c.provider == provider)

    with engine.begin() as conn:
        rows = conn.execute(stmt).fetchall()

    return {int(r[0]) for r in rows}


def insert_new_leagues(engine, league_rows: Sequence[Dict[str, Any]]) -> int:
    """
    Insert new leagues only. Does not update existing.
    Uses ON CONFLICT DO NOTHING to be safe.
    """
    metadata = MetaData()
    leagues = make_leagues_table(metadata)
    metadata.create_all(engine)

    if not league_rows:
        return 0

    stmt = pg_insert(leagues).values(list(league_rows))
    stmt = stmt.on_conflict_do_nothing(index_elements=[leagues.c.league_id])

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return int(res.rowcount or 0)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main(
    league_ids_yaml: Union[str, Path] = "database/input/leagues.yaml",
) -> None:
    provider = get_current_provider().strip().lower()
    engine = get_engine()

    yaml_ids = set(load_league_ids(league_ids_yaml))
    existing_ids = get_existing_league_ids(engine, provider=provider)

    new_ids = set(sorted(yaml_ids - existing_ids))
    if not new_ids:
        print("No new league_ids found in YAML (relative to DB). Nothing to insert.")
        print("Table: public.leagues")
        return

    all_leagues = fetch_all_leagues(provider)

    rows: List[Dict[str, Any]] = []
    for x in all_leagues:
        try:
            lid = int(x.get("id"))
        except Exception:
            continue

        if lid in new_ids:
            rows.append(
                {
                    "league_id": lid,
                    "league_name": str(x.get("name", "")).strip(),
                    "provider": provider,
                }
            )

    found = {r["league_id"] for r in rows}
    missing = sorted(new_ids - found)
    if missing:
        print(
            f"Warning: {len(missing)} NEW league_ids not found in API response: "
            f"{missing[:10]}{'...' if len(missing) > 10 else ''}"
        )

    inserted = insert_new_leagues(engine, rows)

    print(f"Insert complete. New IDs in YAML: {len(new_ids)}")
    print(f"Rows inserted: {inserted}")
    print("Table: public.leagues")


if __name__ == "__main__":
    main()
