"""
database/09_extend_team_mapping.py

EXTEND-ONLY updater for the team name mapping CSV used to match teams across providers.

CSV columns:
  team_id,team_name,oa_name

Behavior:
- If the CSV doesn't exist: create it with all teams (oa_name empty).
- If it exists: append ONLY new team_ids (do NOT modify existing rows at all).
  This preserves manual `oa_name` edits and any manual changes to `team_name`.
- Output ordering (optional but implemented):
    1) by league_id (ascending; NULL league_id last)
    2) within league_id: rows with empty oa_name first (unmapped), then mapped
    3) then by team_name, then by team_id

Notes:
- We fetch league_id from public.fixtures by taking the most frequent league_id
  for each team_id (across home/away appearances). If a team never appears in fixtures,
  league_id will be NULL and it will be placed last.
- We do not add extra columns to the CSV; league_id is only used for sorting.

Usage:
  python -m database.09_extend_team_mapping
  python -m database.09_extend_team_mapping --out /root/sport_analytics/database/output/team_name_matching.csv
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, Tuple, Optional, List, Any

from sqlalchemy import text

from database.connection.engine import get_engine

HEADERS = ["team_id", "team_name", "oa_name"]


# ----------------------------
# DB helpers
# ----------------------------
def detect_team_name_column(engine) -> Tuple[str, str]:
    with engine.begin() as conn:
        cols = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public'
                  AND table_name='teams'
                """
            )
        ).fetchall()

    available = {str(r[0]) for r in cols}

    id_candidates = ["team_id", "id"]
    id_col = next((c for c in id_candidates if c in available), None)
    if not id_col:
        raise RuntimeError(f"Could not find a team id column in public.teams. Available: {sorted(available)}")

    name_candidates = ["team_name", "name", "display_name", "common_name", "short_name", "official_name"]
    name_col = next((c for c in name_candidates if c in available), None)
    if not name_col:
        raise RuntimeError(f"Could not find a team name column in public.teams. Available: {sorted(available)}")

    return id_col, name_col


def fetch_teams(engine) -> Dict[int, str]:
    id_col, name_col = detect_team_name_column(engine)

    sql = text(
        f"""
        SELECT {id_col} AS team_id, {name_col} AS team_name
        FROM public.teams
        WHERE {id_col} IS NOT NULL
        ORDER BY {id_col}
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(sql).fetchall()

    return {int(r.team_id): (str(r.team_name) if r.team_name is not None else "") for r in rows}


def fetch_team_primary_league(engine) -> Dict[int, Optional[int]]:
    """
    Best-effort league_id for each team_id based on fixtures:
    choose the most frequent league_id where team appears (home or away).
    """
    sql = text(
        """
        WITH appearances AS (
            SELECT home_team_id AS team_id, league_id
            FROM public.fixtures
            WHERE home_team_id IS NOT NULL AND league_id IS NOT NULL

            UNION ALL

            SELECT away_team_id AS team_id, league_id
            FROM public.fixtures
            WHERE away_team_id IS NOT NULL AND league_id IS NOT NULL
        ),
        counted AS (
            SELECT team_id, league_id, COUNT(*) AS n
            FROM appearances
            GROUP BY team_id, league_id
        ),
        ranked AS (
            SELECT
                team_id,
                league_id,
                n,
                ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY n DESC, league_id ASC) AS rn
            FROM counted
        )
        SELECT team_id, league_id
        FROM ranked
        WHERE rn = 1
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(sql).fetchall()

    out: Dict[int, Optional[int]] = {}
    for r in rows:
        try:
            out[int(r.team_id)] = int(r.league_id) if r.league_id is not None else None
        except Exception:
            continue
    return out


# ----------------------------
# CSV helpers
# ----------------------------
def load_existing_rows(path: Path) -> Dict[int, Dict[str, str]]:
    """
    Returns {team_id: row_dict}. Keeps whatever is in the file (esp. oa_name and team_name).
    """
    if not path.exists():
        return {}

    existing: Dict[int, Dict[str, str]] = {}
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            if "team_id" not in row:
                raise RuntimeError(f"CSV at {path} is missing 'team_id' column. Found columns: {reader.fieldnames}")
            try:
                tid = int(str(row["team_id"]).strip())
            except Exception:
                continue

            # Preserve exactly what is in the file (don’t normalize team_name / oa_name).
            existing[tid] = {
                "team_id": str(tid),
                "team_name": row.get("team_name", "") or "",
                "oa_name": row.get("oa_name", "") or "",
            }
    return existing


def write_rows_ordered(
    path: Path,
    rows: Dict[int, Dict[str, str]],
    team_league: Dict[int, Optional[int]],
) -> None:
    """
    Writes CSV sorted by:
      league_id asc (None last),
      unmapped first (oa_name empty),
      then team_name asc,
      then team_id asc.
    """
    def sort_key(item: tuple[int, Dict[str, str]]):
        tid, row = item
        league_id = team_league.get(tid)
        league_sort = (1, 0) if league_id is None else (0, int(league_id))  # None last
        unmapped = 1 if (row.get("oa_name") or "").strip() else 0  # 0=unmapped first
        team_name = (row.get("team_name") or "").strip().lower()
        return (league_sort[0], league_sort[1], unmapped, team_name, tid)

    items = sorted(rows.items(), key=sort_key)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        for _, row in items:
            writer.writerow(row)


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--out",
        type=str,
        default=str(Path("database") / "output" / "team_name_matching.csv"),
        help="Output CSV path",
    )
    args = ap.parse_args()

    out_path = Path(args.out).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    engine = get_engine()
    teams = fetch_teams(engine)  # team_id -> current team_name from DB

    existing = load_existing_rows(out_path)  # preserves manual edits
    merged = dict(existing)

    added = 0
    skipped = 0

    # Extend-only: only add new IDs; never modify existing rows.
    for team_id, team_name in teams.items():
        if team_id in merged:
            skipped += 1
            continue
        merged[team_id] = {"team_id": str(team_id), "team_name": team_name, "oa_name": ""}
        added += 1

    # Sorting metadata (does not affect content)
    team_league = fetch_team_primary_league(engine)

    # Write out (ordered)
    write_rows_ordered(out_path, merged, team_league)

    print(f"[TEAM MATCHING EXTEND] teams in DB: {len(teams)}")
    print(f"[TEAM MATCHING EXTEND] existing in file: {len(existing)}")
    print(f"[TEAM MATCHING EXTEND] added: {added} | skipped(existing): {skipped}")
    print(f"[TEAM MATCHING EXTEND] wrote: {out_path}")


if __name__ == "__main__":
    main()
