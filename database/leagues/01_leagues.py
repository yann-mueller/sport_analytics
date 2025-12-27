'''
This module contains the code to create the 
'''
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Sequence, Union

import yaml
import requests

from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, Text, DateTime, func
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

from api_calls.helpers.general import get_current_provider
from api_calls.helpers.providers.general import get_url
from api_calls.auth.auth import get_access_params


def load_league_ids(yaml_path: Union[str, Path]) -> List[int]:
    yaml_path = Path(yaml_path)
    with yaml_path.open("r", encoding="utf-8") as f:
        cfg: Any = yaml.safe_load(f)

    if not isinstance(cfg, dict) or "leagues" not in cfg or not isinstance(cfg["leagues"], list):
        raise ValueError(f"Invalid leagues YAML: {yaml_path}. Expected top-level key 'leagues: [..]'")

    ids: List[int] = []
    for x in cfg["leagues"]:
        try:
            ids.append(int(x))
        except Exception:
            raise ValueError(f"Invalid league id '{x}' in {yaml_path} (must be int-like)")
    return ids


def fetch_all_leagues(provider: str) -> List[Dict[str, Any]]:
    params = get_access_params(provider)
    api_token = params["api_token"]

    url = get_url(provider, "leagues")
    r = requests.get(url, params={"api_token": api_token}, timeout=30)
    r.raise_for_status()

    data = r.json().get("data", [])
    if not isinstance(data, list):
        return []
    return data


def make_leagues_table(metadata: MetaData) -> Table:
    # You can add more columns later if you want (country_id, logo, etc.)
    return Table(
        "leagues",
        metadata,
        Column("league_id", Integer, primary_key=True),
        Column("league_name", Text, nullable=False),
        Column("provider", Text, nullable=False),
        Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False),
    )


def upsert_leagues(
    engine,
    league_rows: Sequence[Dict[str, Any]],
) -> int:
    metadata = MetaData()
    leagues = make_leagues_table(metadata)

    # create table if not exists
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
    )

    with engine.begin() as conn:
        result = conn.execute(stmt)
        # For PostgreSQL, rowcount is usually meaningful for insert/update here.
        return int(result.rowcount or 0)


def main(
    league_ids_yaml: Union[str, Path] = "input/leagues.yaml",
    # Use an env var or a hardcoded DSN. Example:
    # postgresql+psycopg2://USER:PASSWORD@HOST:5432/sport_analytics
    db_url: str | None = None,
) -> None:
    provider = get_current_provider(default="sportmonks")  # uses config.yaml if set, else sportmonks
    provider = str(provider).strip().lower()

    # DB URL resolution
    if db_url is None:
        # Prefer env var (recommended)
        import os
        db_url = os.getenv("SPORT_ANALYTICS_DB_URL")

    if not db_url:
        raise ValueError(
            "No DB URL provided. Set SPORT_ANALYTICS_DB_URL or pass db_url=...\n"
            "Example: postgresql+psycopg2://postgres:YOURPASS@127.0.0.1:5432/sport_analytics"
        )

    engine = create_engine(db_url, future=True)

    league_ids = set(load_league_ids(league_ids_yaml))
    all_leagues = fetch_all_leagues(provider)

    # Filter API payload to the league_ids you want
    wanted = []
    for x in all_leagues:
        try:
            lid = int(x.get("id"))
        except Exception:
            continue
        if lid in league_ids:
            wanted.append(
                {
                    "league_id": lid,
                    "league_name": str(x.get("name", "")).strip(),
                    "provider": provider,
                }
            )

    # If some IDs weren’t found, you might want to know
    found_ids = {r["league_id"] for r in wanted}
    missing = sorted(list(league_ids - found_ids))
    if missing:
        print(f"Warning: {len(missing)} league_ids not found in API response: {missing[:20]}{'...' if len(missing) > 20 else ''}")

    changed = upsert_leagues(engine, wanted)
    print(f"Upsert complete. Rows inserted/updated: {changed}")
    print(f"Table: public.leagues")


if __name__ == "__main__":
    main()