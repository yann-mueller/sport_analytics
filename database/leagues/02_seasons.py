"""
database/leagues/02_seasons.py

Creates/updates public.seasons based on:
- leagues already present in public.leagues
- season names listed in database/leagues/seasons.yaml

Behavior:
- Insert new seasons
- Update existing seasons ONLY if values changed (and then updated_at changes)
- Delete seasons for this provider that are NOT in the YAML selection anymore
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Sequence, Set, Tuple, Union

import re
import yaml
import requests

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    Text,
    Boolean,
    DateTime,
    select,
    delete,
    func,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

from api_calls.helpers.general import get_current_provider
from api_calls.helpers.providers.general import get_url
from api_calls.auth.auth import get_access_params

from database.connection.engine import get_engine


# -------------------------
# YAML helpers
# -------------------------
def load_season_names(yaml_path: Union[str, Path]) -> List[str]:
    yaml_path = Path(yaml_path)
    with yaml_path.open("r", encoding="utf-8") as f:
        cfg: Any = yaml.safe_load(f)

    if not isinstance(cfg, dict) or "seasons" not in cfg or not isinstance(cfg["seasons"], list):
        raise ValueError(f"Invalid seasons YAML: {yaml_path}. Expected top-level key 'seasons: [..]'")

    out: List[str] = []
    for x in cfg["seasons"]:
        if x is None:
            continue
        s = str(x).strip()
        if not s:
            continue
        out.append(s)
    return out


def _season_start_year(name: str) -> str | None:
    """
    Extract a "start year" key from season label.
    Examples:
      "2024/2025" -> "2024"
      "2024"      -> "2024"
      " 2024/25 " -> "2024" (best effort if it begins with 4-digit year)
    """
    name = str(name).strip()
    m = re.match(r"^(\d{4})", name)
    return m.group(1) if m else None


def _build_allowed_keys(yaml_seasons: Sequence[str]) -> Tuple[Set[str], Set[str]]:
    """
    Returns:
      allowed_exact: exact strings, stripped
      allowed_start_years: start-year keys (e.g. {"2024","2020"})
    """
    allowed_exact = {str(s).strip() for s in yaml_seasons if str(s).strip()}
    allowed_years: Set[str] = set()
    for s in allowed_exact:
        y = _season_start_year(s)
        if y:
            allowed_years.add(y)
    return allowed_exact, allowed_years


# -------------------------
# SportMonks fetch
# -------------------------
def _sportmonks_fetch_seasons_for_league(provider: str, league_id: int) -> List[Dict[str, Any]]:
    """
    SportMonks: GET /seasons?filters=seasonLeagues:{league_id}
    Handles paging (best-effort).
    """
    params = get_access_params(provider)
    api_token = params["api_token"]

    url = get_url(provider, "seasons")

    out: List[Dict[str, Any]] = []
    page = 1

    while True:
        r = requests.get(
            url,
            params={
                "api_token": api_token,
                "filters": f"seasonLeagues:{league_id}",
                "per_page": 50,
                "page": page,
            },
            timeout=30,
        )
        r.raise_for_status()
        payload = r.json()

        data = payload.get("data", [])
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list):
            data = []

        out.extend([x for x in data if isinstance(x, dict)])

        # Pagination is sometimes in payload["pagination"], sometimes in meta.
        pagination = payload.get("pagination") or payload.get("meta", {}).get("pagination") or {}
        has_more = False

        if isinstance(pagination, dict):
            current_page = pagination.get("current_page")
            last_page = pagination.get("last_page")
            if isinstance(current_page, int) and isinstance(last_page, int) and current_page < last_page:
                has_more = True

        # Fallback: if we got < per_page, likely done
        if not has_more and len(data) < 50:
            break

        page += 1

        # avoid infinite loops
        if page > 200:
            break

    return out


# -------------------------
# DB tables
# -------------------------
def make_leagues_table(metadata: MetaData) -> Table:
    # reflect minimal shape for reading league ids
    return Table(
        "leagues",
        metadata,
        Column("league_id", Integer, primary_key=True),
        schema="public",
    )


def make_seasons_table(metadata: MetaData) -> Table:
    return Table(
        "seasons",
        metadata,
        Column("season_id", Integer, primary_key=True),
        Column("season_name", Text, nullable=False),
        Column("league_id", Integer, nullable=False),
        Column("is_current", Boolean, nullable=True),
        Column("provider", Text, nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        schema="public",
    )


def upsert_seasons(engine, season_rows: Sequence[Dict[str, Any]]) -> int:
    metadata = MetaData()
    seasons = make_seasons_table(metadata)

    # create if not exists
    metadata.create_all(engine)

    if not season_rows:
        return 0

    stmt = pg_insert(seasons).values(list(season_rows))

    # Only update when something actually changed (null-safe)
    changed_cond = (
        seasons.c.season_name.is_distinct_from(stmt.excluded.season_name)
        | seasons.c.league_id.is_distinct_from(stmt.excluded.league_id)
        | seasons.c.is_current.is_distinct_from(stmt.excluded.is_current)
        | seasons.c.provider.is_distinct_from(stmt.excluded.provider)
    )

    stmt = stmt.on_conflict_do_update(
        index_elements=[seasons.c.season_id],
        set_={
            "season_name": stmt.excluded.season_name,
            "league_id": stmt.excluded.league_id,
            "is_current": stmt.excluded.is_current,
            "provider": stmt.excluded.provider,
            "updated_at": func.now(),
        },
        where=changed_cond,  # <-- critical: updated_at only when changed
    )

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return int(res.rowcount or 0)


def delete_seasons_not_in_yaml(engine, *, provider: str, keep_season_ids: Set[int]) -> int:
    """
    Delete seasons for this provider that are not in the YAML-selected set.
    """
    metadata = MetaData()
    seasons = make_seasons_table(metadata)

    # Ensure table exists
    metadata.create_all(engine)

    with engine.begin() as conn:
        if keep_season_ids:
            stmt = delete(seasons).where(
                seasons.c.provider == provider,
                ~seasons.c.season_id.in_(keep_season_ids),
            )
        else:
            # If YAML selection yields nothing, delete all seasons for this provider
            stmt = delete(seasons).where(seasons.c.provider == provider)

        res = conn.execute(stmt)
        return int(res.rowcount or 0)


# -------------------------
# Main
# -------------------------
def main(
    seasons_yaml: Union[str, Path] = "database/leagues/seasons.yaml",
) -> None:
    provider = get_current_provider(default="sportmonks").strip().lower()
    engine = get_engine()

    # 1) Load YAML season names
    yaml_seasons = load_season_names(seasons_yaml)
    allowed_exact, allowed_years = _build_allowed_keys(yaml_seasons)

    # 2) Get league_ids from public.leagues
    md = MetaData()
    leagues = make_leagues_table(md)

    with engine.begin() as conn:
        league_ids = [int(r[0]) for r in conn.execute(select(leagues.c.league_id)).fetchall()]

    if not league_ids:
        print("No leagues found in public.leagues. Add leagues first, then re-run.")
        return

    # 3) Fetch seasons for each league and match
    wanted_rows: List[Dict[str, Any]] = []
    matched_yaml: Set[str] = set()
    keep_ids: Set[int] = set()

    for league_id in league_ids:
        if provider == "sportmonks":
            api_rows = _sportmonks_fetch_seasons_for_league(provider, league_id)
        else:
            raise ValueError(f"Unsupported provider for seasons: {provider}")

        for s in api_rows:
            sid = s.get("id")
            name = str(s.get("name", "")).strip()
            if sid is None or not name:
                continue

            start_year = _season_start_year(name)

            # MATCH RULE:
            # - exact match OR start-year match
            is_match = (name in allowed_exact) or (start_year is not None and start_year in allowed_years)

            if not is_match:
                continue

            try:
                sid_i = int(sid)
            except Exception:
                continue

            # record "which YAML entry matched" (for warning)
            # If exact matched, use that exact; else use start year as proxy
            if name in allowed_exact:
                matched_yaml.add(name)
            elif start_year in allowed_years:
                # mark all YAML entries sharing that start year as "matched"
                for y in allowed_exact:
                    if _season_start_year(y) == start_year:
                        matched_yaml.add(y)

            row = {
                "season_id": sid_i,
                "season_name": name,
                "league_id": int(league_id),
                "is_current": bool(s.get("is_current")) if s.get("is_current") is not None else None,
                "provider": provider,
            }
            wanted_rows.append(row)
            keep_ids.add(sid_i)

    # 4) Warn if YAML entries didn’t match anything
    missing_yaml = sorted(list(allowed_exact - matched_yaml))
    if missing_yaml:
        print(f"Warning: Some YAML season entries did not match any API season name: {missing_yaml}")

    # 5) Upsert + delete removed
    changed = upsert_seasons(engine, wanted_rows)
    deleted = delete_seasons_not_in_yaml(engine, provider=provider, keep_season_ids=keep_ids)

    print(f"Upsert complete. Rows inserted/updated: {changed}")
    print(f"Deleted rows not in YAML (provider={provider}): {deleted}")
    print("Table: public.seasons")


if __name__ == "__main__":
    main()