"""
database/leagues/extend_seasons.py

Extends public.seasons based on:
- leagues already present in public.leagues
- season names listed in database/leagues/seasons.yaml

NEW BEHAVIOR (extend-only):
- Insert NEW seasons implied by the YAML selection that are not yet present in public.seasons for this provider
- Do NOT update existing seasons
- Do NOT delete anything
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

        pagination = payload.get("pagination") or payload.get("meta", {}).get("pagination") or {}
        has_more = False

        if isinstance(pagination, dict):
            current_page = pagination.get("current_page")
            last_page = pagination.get("last_page")
            if isinstance(current_page, int) and isinstance(last_page, int) and current_page < last_page:
                has_more = True

        if not has_more and len(data) < 50:
            break

        page += 1
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


# -------------------------
# Extend-only insert logic
# -------------------------
def get_existing_season_ids(engine, *, provider: str) -> Set[int]:
    """
    Return set of season_ids already present in public.seasons for provider.
    """
    md = MetaData()
    seasons = make_seasons_table(md)

    # Ensure table exists
    md.create_all(engine)

    stmt = select(seasons.c.season_id).where(seasons.c.provider == provider)

    with engine.begin() as conn:
        rows = conn.execute(stmt).fetchall()

    return {int(r[0]) for r in rows}


def insert_new_seasons(engine, season_rows: Sequence[Dict[str, Any]]) -> int:
    """
    Insert only; do not update existing rows.
    Uses ON CONFLICT DO NOTHING to be safe.
    """
    md = MetaData()
    seasons = make_seasons_table(md)
    md.create_all(engine)

    if not season_rows:
        return 0

    stmt = pg_insert(seasons).values(list(season_rows))
    stmt = stmt.on_conflict_do_nothing(index_elements=[seasons.c.season_id])

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return int(res.rowcount or 0)


# -------------------------
# Main
# -------------------------
def main(
    seasons_yaml: Union[str, Path] = "database/input/seasons.yaml",
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

    # 3) Existing seasons in DB for this provider
    existing_season_ids = get_existing_season_ids(engine, provider=provider)

    # 4) Fetch seasons for each league and match YAML selection
    candidate_rows: List[Dict[str, Any]] = []
    matched_yaml: Set[str] = set()

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

            # mark YAML matched (for warning)
            if name in allowed_exact:
                matched_yaml.add(name)
            elif start_year in allowed_years:
                for y in allowed_exact:
                    if _season_start_year(y) == start_year:
                        matched_yaml.add(y)

            # collect row (even if already existing; we'll filter later)
            candidate_rows.append(
                {
                    "season_id": sid_i,
                    "season_name": name,
                    "league_id": int(league_id),
                    "is_current": bool(s.get("is_current")) if s.get("is_current") is not None else None,
                    "provider": provider,
                }
            )

    # 5) Warn if YAML entries didn’t match anything in API response
    missing_yaml = sorted(list(allowed_exact - matched_yaml))
    if missing_yaml:
        print(f"Warning: Some YAML season entries did not match any API season name: {missing_yaml}")

    if not candidate_rows:
        print("No seasons matched the YAML selection. Nothing to insert.")
        print("Table: public.seasons")
        return

    # 6) Extend-only: keep only truly new season_ids for this provider
    new_rows = [r for r in candidate_rows if int(r["season_id"]) not in existing_season_ids]

    # Dedupe in case multiple leagues return the same season_id
    seen: Set[int] = set()
    deduped_new_rows: List[Dict[str, Any]] = []
    for r in new_rows:
        sid = int(r["season_id"])
        if sid in seen:
            continue
        seen.add(sid)
        deduped_new_rows.append(r)

    if not deduped_new_rows:
        print("No new seasons to insert (all matched seasons already exist in public.seasons).")
        print("Table: public.seasons")
        return

    inserted = insert_new_seasons(engine, deduped_new_rows)

    print(f"Extend complete. Candidate matched seasons: {len(candidate_rows)}")
    print(f"New seasons (not in DB for provider={provider}): {len(deduped_new_rows)}")
    print(f"Rows inserted: {inserted}")
    print("Table: public.seasons")


if __name__ == "__main__":
    main()
