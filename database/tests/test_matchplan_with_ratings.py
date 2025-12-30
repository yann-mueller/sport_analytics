"""
Match plan for a team in a season:
- opponent (name + id)
- result (score + W/D/L from selected team's perspective)
- team rating for both teams (avg_rating from public.team_ratings)

Usage:
  python -m database.tests.test_team_matchplan_with_ratings --team-id 503 --season-id 23744
"""

from __future__ import annotations

import argparse
from typing import Optional, Tuple, Dict, List

from sqlalchemy import text

from database.connection.engine import get_engine


# ----------------------------
# Detect helper tables/cols
# ----------------------------
def _get_public_columns(engine, table_name: str) -> set[str]:
    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=:t
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(sql, {"t": table_name}).fetchall()
    return {r[0] for r in rows}


def detect_teams_source(engine) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    candidates_tables = [
        ("public", "teams"),
        ("public", "team"),
        ("public", "teams_meta"),
        ("public", "teams_info"),
    ]
    candidate_id_cols = ["team_id", "id"]
    candidate_name_cols = ["name", "team_name", "display_name"]

    with engine.begin() as conn:
        cols = conn.execute(
            text(
                """
                SELECT table_schema, table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                """
            )
        ).fetchall()

    table_to_cols: Dict[Tuple[str, str], set[str]] = {}
    for schema, table, col in cols:
        table_to_cols.setdefault((schema, table), set()).add(col)

    for schema, table in candidates_tables:
        available = table_to_cols.get((schema, table))
        if not available:
            continue

        id_col = next((c for c in candidate_id_cols if c in available), None)
        name_col = next((c for c in candidate_name_cols if c in available), None)

        if id_col and name_col:
            return f"{schema}.{table}", id_col, name_col

    return None, None, None


def detect_score_cols(engine) -> Tuple[Optional[str], Optional[str]]:
    """
    Try to find home/away score columns in public.fixtures.
    Returns (home_score_col, away_score_col) or (None, None).
    """
    cols = _get_public_columns(engine, "fixtures")

    candidates = [
        ("home_score", "away_score"),
        ("home_goals", "away_goals"),
        ("home_team_score", "away_team_score"),
        ("scores_home", "scores_away"),
        ("home", "away"),  # sometimes nested-ish schemas flatten to home/away
    ]
    for h, a in candidates:
        if h in cols and a in cols:
            return h, a
    return None, None


# ----------------------------
# Formatting helpers
# ----------------------------
def _w_d_l(team_goals: Optional[int], opp_goals: Optional[int]) -> str:
    if team_goals is None or opp_goals is None:
        return "NA"
    if team_goals > opp_goals:
        return "W"
    if team_goals < opp_goals:
        return "L"
    return "D"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
    "--team-id",
    type=int,
    default=3319,
    help="Team ID (default: 3319)",
    )
    ap.add_argument(
        "--season-id",
        type=int,
        default=21795,
        help="Season ID (default: 21795)",
    )
    ap.add_argument("--limit", type=int, default=1000)
    args = ap.parse_args()

    engine = get_engine()

    teams_table, team_id_col, team_name_col = detect_teams_source(engine)
    if not teams_table:
        raise RuntimeError("Could not find a teams table with names (expected public.teams(team_id, name) or similar).")

    home_score_col, away_score_col = detect_score_cols(engine)

    # Build SELECT parts for score columns (or NULLs if not found)
    if home_score_col and away_score_col:
        score_select = f"f.{home_score_col} AS home_score, f.{away_score_col} AS away_score"
    else:
        score_select = "NULL::int AS home_score, NULL::int AS away_score"

    sql = text(
        f"""
        SELECT
            f.fixture_id,
            f.date AS fixture_date,
            f.home_team_id,
            f.away_team_id,
            {score_select},

            th.{team_name_col} AS home_team_name,
            ta.{team_name_col} AS away_team_name,

            trh.avg_rating AS home_avg_rating,
            tra.avg_rating AS away_avg_rating
        FROM public.fixtures f
        JOIN {teams_table} th ON th.{team_id_col} = f.home_team_id
        JOIN {teams_table} ta ON ta.{team_id_col} = f.away_team_id
        LEFT JOIN public.team_ratings trh
            ON trh.fixture_id = f.fixture_id AND trh.team_id = f.home_team_id
        LEFT JOIN public.team_ratings tra
            ON tra.fixture_id = f.fixture_id AND tra.team_id = f.away_team_id
        WHERE f.season_id = :season_id
          AND (:team_id IN (f.home_team_id, f.away_team_id))
        ORDER BY f.date, f.fixture_id
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        matches = conn.execute(sql, {"team_id": args.team_id, "season_id": args.season_id, "limit": args.limit}).fetchall()

    if not matches:
        print(f"No fixtures found for team_id={args.team_id} in season_id={args.season_id}.")
        return

    # Precompute opponent labels for alignment
    opponent_labels: List[str] = []
    for m in matches:
        if int(m.home_team_id) == args.team_id:
            opponent_labels.append(f"{m.away_team_name} ({int(m.away_team_id)})")
        else:
            opponent_labels.append(f"{m.home_team_name} ({int(m.home_team_id)})")
    max_opp = max(len(s) for s in opponent_labels)

    print("\n" + "=" * 110)
    print(f"Match plan (team_id={args.team_id}, season_id={args.season_id})")
    if home_score_col and away_score_col:
        print(f"Scores from fixtures: {home_score_col}/{away_score_col}")
    else:
        print("Scores from fixtures: NOT FOUND (printing NA)")
    print("=" * 110)

    for idx, m in enumerate(matches):
        fixture_id = int(m.fixture_id)
        d = m.fixture_date

        home_id = int(m.home_team_id)
        away_id = int(m.away_team_id)

        home_name = m.home_team_name
        away_name = m.away_team_name

        home_score = int(m.home_score) if m.home_score is not None else None
        away_score = int(m.away_score) if m.away_score is not None else None

        home_r = float(m.home_avg_rating) if m.home_avg_rating is not None else None
        away_r = float(m.away_avg_rating) if m.away_avg_rating is not None else None

        # Perspective: selected team
        if home_id == args.team_id:
            opponent = opponent_labels[idx].ljust(max_opp)
            team_goals, opp_goals = home_score, away_score
            team_rating, opp_rating = home_r, away_r
            scoreline = f"{home_score}-{away_score}" if home_score is not None and away_score is not None else "NA"
        else:
            opponent = opponent_labels[idx].ljust(max_opp)
            team_goals, opp_goals = away_score, home_score
            team_rating, opp_rating = away_r, home_r
            scoreline = f"{home_score}-{away_score}" if home_score is not None and away_score is not None else "NA"

        outcome = _w_d_l(team_goals, opp_goals)

        tr = f"{team_rating:.2f}" if team_rating is not None else "NA"
        or_ = f"{opp_rating:.2f}" if opp_rating is not None else "NA"

        print(
            f"{d} | fixture_id={fixture_id} | opponent={opponent} | "
            f"score={scoreline:>7} ({outcome}) | rating_team={tr:>5} | rating_opp={or_:>5}"
        )


if __name__ == "__main__":
    main()
