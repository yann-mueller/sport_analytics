"""
Test fixture matching results by printing Sportmonks fixture info alongside OddsAPI matched info.

Default: prints up to 20 rows.

Usage:
  python -m database.tests.fixtures_matching
  python -m database.tests.fixtures_matching --limit 50
  python -m database.tests.fixtures_matching --only-matched
  python -m database.tests.fixtures_matching --only-unmatched
"""

from __future__ import annotations

import argparse

from sqlalchemy import text

from database.connection.engine import get_engine


def _pad(s: str, width: int) -> str:
    return (s or "").ljust(width)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--only-matched", action="store_true", help="Only show fixtures with oa_event_id not null")
    ap.add_argument("--only-unmatched", action="store_true", help="Only show fixtures with oa_event_id is null")
    args = ap.parse_args()

    if args.only_matched and args.only_unmatched:
        raise SystemExit("Choose only one of --only-matched or --only-unmatched")

    engine = get_engine()

    where_extra = ""
    if args.only_matched:
        where_extra = "AND fm.oa_event_id IS NOT NULL"
    elif args.only_unmatched:
        where_extra = "AND fm.oa_event_id IS NULL"

    sql = text(
        f"""
        SELECT
            f.fixture_id        AS fixture_id,
            f.date              AS sm_date,
            f.home_team_id      AS sm_home_team_id,
            f.away_team_id      AS sm_away_team_id,
            th.team_name        AS sm_home_team_name,
            ta.team_name        AS sm_away_team_name,

            fm.oa_event_id      AS oa_event_id,
            fm.oa_commence_time AS oa_date,
            fm.oa_home_team     AS oa_home_team_name,
            fm.oa_away_team     AS oa_away_team_name
        FROM public.fixtures f
        LEFT JOIN public.fixtures_matching fm
          ON fm.fixture_id = f.fixture_id
        LEFT JOIN public.teams th
          ON th.team_id = f.home_team_id
        LEFT JOIN public.teams ta
          ON ta.team_id = f.away_team_id
        WHERE f.date IS NOT NULL
        {where_extra}
        ORDER BY f.date, f.fixture_id
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(sql, {"limit": args.limit}).fetchall()

    if not rows:
        print("No rows found (maybe fixtures_matching empty, or you filtered everything out).")
        return

    # Compute widths for nice alignment
    sm_home_labels = [(r.sm_home_team_name or f"team_id={r.sm_home_team_id}") for r in rows]
    sm_away_labels = [(r.sm_away_team_name or f"team_id={r.sm_away_team_id}") for r in rows]
    oa_home_labels = [(r.oa_home_team_name or "—") for r in rows]
    oa_away_labels = [(r.oa_away_team_name or "—") for r in rows]

    w_sm_home = max(10, min(40, max(len(x) for x in sm_home_labels)))
    w_sm_away = max(10, min(40, max(len(x) for x in sm_away_labels)))
    w_oa_home = max(10, min(40, max(len(x) for x in oa_home_labels)))
    w_oa_away = max(10, min(40, max(len(x) for x in oa_away_labels)))

    print("\n" + "=" * 120)
    print(f"Fixture matching preview (rows={len(rows)})")
    print("Sportmonks names from: public.teams.team_name")
    print("=" * 120)

    for r in rows:
        sm_date = r.sm_date.isoformat() if r.sm_date is not None else "—"
        sm_home = r.sm_home_team_name or f"team_id={r.sm_home_team_id}"
        sm_away = r.sm_away_team_name or f"team_id={r.sm_away_team_id}"

        oa_event_id = r.oa_event_id or "—"
        oa_date = r.oa_date.isoformat() if r.oa_date is not None else "—"
        oa_home = r.oa_home_team_name or "—"
        oa_away = r.oa_away_team_name or "—"

        print(
            f"SM  fixture_id={r.fixture_id} | {sm_date} | "
            f"{_pad(sm_home, w_sm_home)} vs {_pad(sm_away, w_sm_away)}"
        )
        print(
            f"OA  event_id={oa_event_id} | {oa_date} | "
            f"{_pad(oa_home, w_oa_home)} vs {_pad(oa_away, w_oa_away)}"
        )
        print("-" * 120)


if __name__ == "__main__":
    main()
