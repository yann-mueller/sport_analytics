"""
analytics/base/01_robustness_lineups.py

Purpose:
- Check how many fixtures exist in the database (optionally filtered by season_ids/provider)
- Check for how many fixtures there exists "lineups" (player rows per fixture)
- Also show for how many fixtures there exists minutes played and ratings (player-level)
- Provide overall stats and stats grouped by league_name

Notes (from your DB overview):
- public.fixtures: fixture-level outcomes + league_id/season_id/teams/goals
- leagues: league_id, league_name, provider
- There is a player-by-fixture table (your overview labels it as "fixtures" again) with:
  fixture_id, player_id, team_id, minutes_player, rating_player, formation_position, ...
  The script auto-detects this table via information_schema, but you can override it via --lineups-table.

Usage examples:
    python analytics/base/01_robustness_lineups.py
    python analytics/base/01_robustness_lineups.py --season-ids 21640,21641
    python analytics/base/01_robustness_lineups.py --provider sportmonks
    python analytics/base/01_robustness_lineups.py --lineups-table public.fixture_players
"""

from __future__ import annotations

import argparse
from typing import Optional

import pandas as pd
from sqlalchemy import text, bindparam

from database.connection.engine import get_engine


DEFAULT_PROVIDER = "sportmonks"

FIXTURES_TABLE = "public.fixtures"
LEAGUES_TABLE = "public.leagues"


# -------------------------
# Pretty terminal printing
# -------------------------
def print_section(title: str) -> None:
    line = "═" * 86
    print("\n" + line)
    print(f" {title}")
    print(line)


def print_subsection(title: str) -> None:
    line = "─" * 86
    print("\n" + line)
    print(f" {title}")
    print(line)


# ------------------
# Args
# ------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season-ids", default=None, help="Optional comma-separated season_ids filter.")
    ap.add_argument("--provider", default=DEFAULT_PROVIDER, help="League provider filter (default sportmonks).")
    ap.add_argument(
        "--lineups-table",
        default=None,
        help="Optional fully-qualified table name for player-by-fixture stats (e.g. public.fixture_players). "
        "If omitted, auto-detect via information_schema.",
    )
    ap.add_argument("--schema", default="public", help="Schema to search for the lineups table (default public).")
    ap.add_argument("--min-player-rows", type=int, default=1, help="Minimum player rows to count as 'has lineups'.")
    return ap.parse_args()


# ------------------
# Helpers
# ------------------
def parse_season_ids(season_ids_str: Optional[str]) -> Optional[list[int]]:
    if not season_ids_str:
        return None
    out: list[int] = []
    for x in season_ids_str.split(","):
        x = x.strip()
        if x:
            out.append(int(x))
    return out or None


def autodetect_lineups_table(engine, schema: str = "public") -> str:
    """
    Try to find the player-by-fixture table based on column signatures.
    We look for tables with:
      - fixture_id
      - player_id
    and prefer those that also contain:
      - minutes_player
      - rating_player
    """
    q = """
    WITH cols AS (
        SELECT
            table_schema,
            table_name,
            SUM(CASE WHEN column_name = 'fixture_id' THEN 1 ELSE 0 END) AS has_fixture_id,
            SUM(CASE WHEN column_name = 'player_id' THEN 1 ELSE 0 END) AS has_player_id,
            SUM(CASE WHEN column_name = 'minutes_player' THEN 1 ELSE 0 END) AS has_minutes_player,
            SUM(CASE WHEN column_name = 'rating_player' THEN 1 ELSE 0 END) AS has_rating_player
        FROM information_schema.columns
        WHERE table_schema = :schema
        GROUP BY table_schema, table_name
    )
    SELECT
        table_schema,
        table_name,
        has_fixture_id,
        has_player_id,
        has_minutes_player,
        has_rating_player,
        (has_minutes_player + has_rating_player) AS score
    FROM cols
    WHERE has_fixture_id = 1 AND has_player_id = 1
    ORDER BY score DESC, table_name ASC
    LIMIT 10
    """
    df = pd.read_sql(text(q), engine, params={"schema": schema})

    if df.empty:
        raise RuntimeError(
            f"Could not auto-detect a lineups/player table in schema='{schema}'. "
            f"Please pass --lineups-table explicitly."
        )

    # Prefer a table that has BOTH minutes and rating, else take best score.
    best = df.iloc[0]
    return f"{best['table_schema']}.{best['table_name']}"


# ------------------
# Main query logic
# ------------------
def compute_coverage(engine, provider: str, season_ids: Optional[list[int]], lineups_table: str, min_player_rows: int):
    """
    Compute overall and league-level counts:
      - total fixtures
      - fixtures with >= min_player_rows in lineups_table
      - fixtures with any minutes_player not null (at least one row)
      - fixtures with any rating_player not null
      - fixtures with both minutes+rating not null (at least one row with both non-null)
    """
    # Build WHERE filters for fixtures universe
    where = []
    params: dict = {"provider": provider, "min_player_rows": int(min_player_rows)}

    where.append("l.provider = :provider")
    if season_ids:
        where.append("f.season_id IN :season_ids")
        params["season_ids"] = [int(x) for x in season_ids]

    where_sql = " AND ".join(where) if where else "TRUE"

    # Use a CTE fixtures_universe then left-join aggregated flags per fixture_id from lineup table
    q = f"""
    WITH fixtures_universe AS (
        SELECT
            f.fixture_id,
            f.league_id
        FROM {FIXTURES_TABLE} f
        JOIN {LEAGUES_TABLE} l
          ON l.league_id = f.league_id
        WHERE {where_sql}
          AND f.fixture_id IS NOT NULL
          AND f.league_id IS NOT NULL
    ),
    lineup_flags AS (
        SELECT
            lp.fixture_id,
            COUNT(*) AS n_player_rows,
            MAX(CASE WHEN lp.minutes_player IS NOT NULL THEN 1 ELSE 0 END) AS has_minutes,
            MAX(CASE WHEN lp.rating_player  IS NOT NULL THEN 1 ELSE 0 END) AS has_rating,
            MAX(CASE WHEN lp.minutes_player IS NOT NULL AND lp.rating_player IS NOT NULL THEN 1 ELSE 0 END) AS has_both
        FROM {lineups_table} lp
        GROUP BY lp.fixture_id
    ),
    joined AS (
        SELECT
            fu.fixture_id,
            fu.league_id,
            COALESCE(lf.n_player_rows, 0) AS n_player_rows,
            COALESCE(lf.has_minutes, 0) AS has_minutes,
            COALESCE(lf.has_rating, 0) AS has_rating,
            COALESCE(lf.has_both, 0) AS has_both
        FROM fixtures_universe fu
        LEFT JOIN lineup_flags lf
          ON lf.fixture_id = fu.fixture_id
    )
    SELECT
        l.league_name,
        COUNT(*) AS n_fixtures,
        SUM(CASE WHEN j.n_player_rows >= :min_player_rows THEN 1 ELSE 0 END) AS n_with_lineups,
        SUM(CASE WHEN j.has_minutes = 1 THEN 1 ELSE 0 END) AS n_with_minutes,
        SUM(CASE WHEN j.has_rating  = 1 THEN 1 ELSE 0 END) AS n_with_ratings,
        SUM(CASE WHEN j.has_both    = 1 THEN 1 ELSE 0 END) AS n_with_minutes_and_ratings
    FROM joined j
    JOIN {LEAGUES_TABLE} l
      ON l.league_id = j.league_id
    WHERE l.provider = :provider
    GROUP BY l.league_name
    ORDER BY n_fixtures DESC, l.league_name ASC
    """

    if season_ids:
        sql = text(q).bindparams(bindparam("season_ids", expanding=True))
    else:
        sql = text(q)

    league_tbl = pd.read_sql(sql, engine, params=params)

    # Overall totals from the league table
    overall = pd.DataFrame(
        {
            "n_fixtures": [int(league_tbl["n_fixtures"].sum()) if not league_tbl.empty else 0],
            "n_with_lineups": [int(league_tbl["n_with_lineups"].sum()) if not league_tbl.empty else 0],
            "n_with_minutes": [int(league_tbl["n_with_minutes"].sum()) if not league_tbl.empty else 0],
            "n_with_ratings": [int(league_tbl["n_with_ratings"].sum()) if not league_tbl.empty else 0],
            "n_with_minutes_and_ratings": [
                int(league_tbl["n_with_minutes_and_ratings"].sum()) if not league_tbl.empty else 0
            ],
        }
    )

    # Add shares for readability
    def add_shares(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        denom = df["n_fixtures"].replace(0, pd.NA)
        df = df.copy()
        df["share_with_lineups"] = (df["n_with_lineups"] / denom).astype(float).round(4)
        df["share_with_minutes"] = (df["n_with_minutes"] / denom).astype(float).round(4)
        df["share_with_ratings"] = (df["n_with_ratings"] / denom).astype(float).round(4)
        df["share_with_minutes_and_ratings"] = (df["n_with_minutes_and_ratings"] / denom).astype(float).round(4)
        return df

    overall = add_shares(overall)
    league_tbl = add_shares(league_tbl)

    return overall, league_tbl


# ------------------
# Entry point
# ------------------
def main() -> int:
    args = parse_args()
    season_ids = parse_season_ids(args.season_ids)

    engine = get_engine()

    # Resolve lineups table
    if args.lineups_table:
        lineups_table = args.lineups_table.strip()
    else:
        lineups_table = autodetect_lineups_table(engine, schema=args.schema)

    print_section("01) Robustness: Lineups / Minutes / Ratings coverage")
    print(f"provider={args.provider}, season_ids={season_ids}, min_player_rows={args.min_player_rows}")
    print(f"lineups_table={lineups_table}")

    overall, by_league = compute_coverage(
        engine=engine,
        provider=args.provider,
        season_ids=season_ids,
        lineups_table=lineups_table,
        min_player_rows=args.min_player_rows,
    )

    print_subsection("Overall coverage")
    pd.set_option("display.width", 220)
    pd.set_option("display.max_rows", 2000)
    pd.set_option("display.max_columns", 2000)
    print(overall.to_string(index=False))

    print_subsection("Coverage by league_name")
    if by_league.empty:
        print("No fixtures found for the given filters.")
    else:
        print(by_league.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
