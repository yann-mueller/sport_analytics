"""
database/10_extend_league_mapping.py

EXTEND-ONLY updater for the league mapping CSV used to match leagues across providers.

CSV columns:
  league_id,league_name,oa_league_name

Behavior:
- If the CSV doesn't exist: create it with all leagues (oa_league_name empty).
- If it exists: append ONLY new league_ids (do NOT modify existing rows at all).
  This preserves manual `oa_league_name` edits and any manual changes to `league_name`.
- Writes the file deterministically sorted by league_id.

Usage:
  python -m database.10_extend_league_mapping
  python -m database.10_extend_league_mapping --out /root/sport_analytics/database/output/league_mapping.csv
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, Tuple

from sqlalchemy import text

from database.connection.engine import get_engine

HEADERS = ["league_id", "league_name", "oa_league_name"]


# ----------------------------
# Detect columns in public.leagues
# ----------------------------
def detect_league_name_column(engine) -> Tuple[str, str]:
    """
    Detect (id_col, name_col) for public.leagues.
    """
    with engine.begin() as conn:
        cols = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public'
                  AND table_name='leagues'
                """
            )
        ).fetchall()

    available = {str(r[0]) for r in cols}

    id_candidates = ["league_id", "id"]
    id_col = next((c for c in id_candidates if c in available), None)
    if not id_col:
        raise RuntimeError(f"Could not find a league id column in public.leagues. Available: {sorted(available)}")

    name_candidates = [
        "league_name",
        "name",
        "display_name",
        "common_name",
        "short_name",
        "official_name",
    ]
    name_col = next((c for c in name_candidates if c in available), None)
    if not name_col:
        raise RuntimeError(f"Could not find a league name column in public.leagues. Available: {sorted(available)}")

    return id_col, name_col


def fetch_leagues(engine) -> Dict[int, str]:
    """
    Returns {league_id: league_name} from public.leagues using detected columns.
    """
    id_col, name_col = detect_league_name_column(engine)

    sql = text(
        f"""
        SELECT {id_col} AS league_id, {name_col} AS league_name
        FROM public.leagues
        WHERE {id_col} IS NOT NULL
        ORDER BY {id_col}
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(sql).fetchall()

    return {
        int(r.league_id): (str(r.league_name) if r.league_name is not None else "")
        for r in rows
    }


# ----------------------------
# CSV helpers
# ----------------------------
def load_existing_rows(path: Path) -> Dict[int, Dict[str, str]]:
    """
    Returns {league_id: row_dict}. Keeps whatever is in the file (esp. oa_league_name).
    Never normalizes/overwrites values.
    """
    if not path.exists():
        return {}

    existing: Dict[int, Dict[str, str]] = {}
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            if "league_id" not in row:
                raise RuntimeError(
                    f"CSV at {path} is missing 'league_id' column. Found columns: {reader.fieldnames}"
                )
            try:
                lid = int(str(row["league_id"]).strip())
            except Exception:
                continue

            existing[lid] = {
                "league_id": str(lid),
                "league_name": row.get("league_name", "") or "",
                "oa_league_name": row.get("oa_league_name", "") or "",
            }
    return existing


def write_rows(path: Path, rows: Dict[int, Dict[str, str]]) -> None:
    """
    Writes CSV deterministically sorted by league_id.
    """
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        for lid in sorted(rows.keys()):
            writer.writerow(rows[lid])


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--out",
        type=str,
        default=str(Path("database") / "output" / "league_mapping.csv"),
        help="Output CSV path",
    )
    args = ap.parse_args()

    out_path = Path(args.out).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    engine = get_engine()
    leagues = fetch_leagues(engine)

    existing = load_existing_rows(out_path)

    added = 0
    skipped = 0

    merged = dict(existing)

    # Extend-only: only add new IDs; never modify existing rows.
    for league_id, league_name in leagues.items():
        if league_id in merged:
            skipped += 1
            continue
        merged[league_id] = {
            "league_id": str(league_id),
            "league_name": league_name,
            "oa_league_name": "",
        }
        added += 1

    write_rows(out_path, merged)

    print(f"[LEAGUE MAPPING EXTEND] leagues in DB: {len(leagues)}")
    print(f"[LEAGUE MAPPING EXTEND] existing in file: {len(existing)}")
    print(f"[LEAGUE MAPPING EXTEND] added: {added} | skipped(existing): {skipped}")
    print(f"[LEAGUE MAPPING EXTEND] wrote: {out_path}")


if __name__ == "__main__":
    main()
