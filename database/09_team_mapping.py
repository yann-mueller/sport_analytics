"""
Create/maintain a CSV mapping file for matching teams across providers.

CSV columns:
  team_id,team_name,oa_name

Behavior:
- If the file doesn't exist: create it with all teams.
- If it exists: append ONLY new team_ids.
- Never overwrite existing rows (so manual oa_name entries remain intact).

Usage:
  python -m database.09_team_mapping
  python -m database.09_team_mapping --out /root/sport_analytics/database/output/team_name_matching.csv
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, Tuple

from sqlalchemy import text

from database.connection.engine import get_engine

HEADERS = ["team_id", "team_name", "oa_name"]


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


def load_existing_rows(path: Path) -> Dict[int, Dict[str, str]]:
    """
    Returns {team_id: row_dict}. Keeps whatever is in the file (esp. oa_name).
    """
    if not path.exists():
        return {}

    existing: Dict[int, Dict[str, str]] = {}
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # tolerate slightly different header capitalization
        for row in reader:
            if not row:
                continue
            if "team_id" not in row:
                # unexpected header; bail early
                raise RuntimeError(f"CSV at {path} is missing 'team_id' column. Found columns: {reader.fieldnames}")
            try:
                tid = int(str(row["team_id"]).strip())
            except Exception:
                continue
            existing[tid] = {
                "team_id": str(tid),
                "team_name": row.get("team_name", "") or "",
                "oa_name": row.get("oa_name", "") or "",
            }
    return existing


def write_rows(path: Path, rows: Dict[int, Dict[str, str]]) -> None:
    """
    Writes CSV deterministically sorted by team_id.
    """
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        for tid in sorted(rows.keys()):
            writer.writerow(rows[tid])


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
    teams = fetch_teams(engine)

    existing = load_existing_rows(out_path)

    added = 0
    skipped = 0

    # Start from existing (preserve oa_name), then append new IDs
    merged = dict(existing)

    for team_id, team_name in teams.items():
        if team_id in merged:
            skipped += 1
            # Do NOT overwrite old row (keeps manual oa_name, and also preserves old team_name if you edited it)
            continue
        merged[team_id] = {"team_id": str(team_id), "team_name": team_name, "oa_name": ""}
        added += 1

    write_rows(out_path, merged)

    print(f"[TEAM MATCHING] teams in DB: {len(teams)}")
    print(f"[TEAM MATCHING] existing in file: {len(existing)}")
    print(f"[TEAM MATCHING] added: {added} | skipped(existing): {skipped}")
    print(f"[TEAM MATCHING] wrote: {out_path}")


if __name__ == "__main__":
    main()