"""
Odds availability overview for public.odds_1x2.

What it does:
- Defines the "universe" as fixtures that have an OddsAPI mapping (public.fixtures_matching.oa_event_id IS NOT NULL).
- Checks how many of those fixtures have odds stored in public.odds_1x2 for a provider (default: betfair).
- Provides breakdown by timeline_identifier (odd_1, odd_2, ...) of:
    * complete quotes: home/draw/away all present
    * partial quotes: some present, some missing
    * missing quotes: all missing (row exists but all null) OR row missing entirely (depends on table content)
- Provides per-fixture completeness distribution (#complete snapshots per fixture).

Usage:
  python -m database.tests.test_odds_availability_1x2
  python -m database.tests.test_odds_availability_1x2 --provider betfair
  python -m database.tests.test_odds_availability_1x2 --limit-fixtures 500
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from typing import Dict, List, Tuple

from sqlalchemy import text

from database.connection.engine import get_engine


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--provider", type=str, default="betfair", help="provider label stored in odds_1x2.provider")
    ap.add_argument(
        "--limit-fixtures",
        type=int,
        default=None,
        help="Optional cap on number of fixtures from universe (useful for quick checks)",
    )
    ap.add_argument(
        "--examples",
        type=int,
        default=20,
        help="How many example fixture_ids to print for missing / low-coverage cases",
    )
    args = ap.parse_args()

    provider = str(args.provider).strip().lower()
    engine = get_engine()

    # ----------------------------
    # Universe: fixtures that have an OddsAPI mapping
    # ----------------------------
    limit_sql = "" if args.limit_fixtures is None else "LIMIT :limit"

    sql_universe = text(
        f"""
        SELECT fm.fixture_id
        FROM public.fixtures_matching fm
        WHERE fm.oa_event_id IS NOT NULL
        ORDER BY fm.fixture_id
        {limit_sql}
        """
    )

    with engine.begin() as conn:
        uni_rows = conn.execute(sql_universe, {"limit": args.limit_fixtures} if args.limit_fixtures else {}).fetchall()

    universe_fixture_ids = [int(r.fixture_id) for r in uni_rows]
    universe_n = len(universe_fixture_ids)

    if universe_n == 0:
        print("No universe fixtures found (public.fixtures_matching.oa_event_id IS NOT NULL).")
        return

    # ----------------------------
    # Load odds rows for those fixtures
    # ----------------------------
    sql_odds = text(
        """
        SELECT
            o.fixture_id,
            o.timeline_identifier,
            o.home,
            o.draw,
            o.away
        FROM public.odds_1x2 o
        WHERE o.provider = :provider
          AND o.fixture_id = ANY(:fixture_ids)
        """
    )

    with engine.begin() as conn:
        odds_rows = conn.execute(sql_odds, {"provider": provider, "fixture_ids": universe_fixture_ids}).fetchall()

    # ----------------------------
    # Fixture-level coverage
    # ----------------------------
    fixtures_with_any_rows = set()
    fixtures_with_any_complete = set()

    # timeline_identifier -> counts
    # We measure:
    #  - complete: home/draw/away all not null
    #  - partial: at least 1 not null but not all 3
    #  - all_null: row exists but all are null (often from error rows)
    per_tl_complete = Counter()
    per_tl_partial = Counter()
    per_tl_all_null = Counter()

    # per fixture, number of complete snapshots
    complete_count_by_fixture: Dict[int, int] = defaultdict(int)
    row_count_by_fixture: Dict[int, int] = defaultdict(int)

    all_timeline_ids = set()

    for r in odds_rows:
        fid = int(r.fixture_id)
        tl = str(r.timeline_identifier)
        all_timeline_ids.add(tl)

        fixtures_with_any_rows.add(fid)
        row_count_by_fixture[fid] += 1

        home = r.home
        draw = r.draw
        away = r.away

        non_null = sum(x is not None for x in (home, draw, away))

        if non_null == 3:
            per_tl_complete[tl] += 1
            fixtures_with_any_complete.add(fid)
            complete_count_by_fixture[fid] += 1
        elif non_null == 0:
            per_tl_all_null[tl] += 1
        else:
            per_tl_partial[tl] += 1

    fixtures_missing_entirely = [fid for fid in universe_fixture_ids if fid not in fixtures_with_any_rows]

    # ----------------------------
    # Print headline coverage
    # ----------------------------
    print("\n" + "=" * 110)
    print(f"Odds availability overview (public.odds_1x2) | provider='{provider}'")
    print("=" * 110)

    print(f"Universe fixtures (have oa_event_id in fixtures_matching): {universe_n}")
    print(f"Fixtures with ANY odds rows stored:                  {len(fixtures_with_any_rows)} ({len(fixtures_with_any_rows)/universe_n:.1%})")
    print(f"Fixtures with ANY COMPLETE 1X2 quote stored:         {len(fixtures_with_any_complete)} ({len(fixtures_with_any_complete)/universe_n:.1%})")
    print(f"Fixtures with NO odds rows stored at all:            {len(fixtures_missing_entirely)} ({len(fixtures_missing_entirely)/universe_n:.1%})")

    if fixtures_missing_entirely:
        ex = fixtures_missing_entirely[: args.examples]
        print(f"\nExample missing fixture_ids (no rows): {ex}")

    # ----------------------------
    # Per timeline_identifier breakdown
    # ----------------------------
    # Note: "missing" per odd_x is tricky because odds_1x2 only contains rows you wrote.
    # If you always insert NULL rows on error, then "all_null" approximates "missing".
    # If you skip storing on error, missing rows won't appear and we cannot count them without reconstructing the expected timeline.
    # Here we report within-existing-rows completeness.
    print("\n" + "-" * 110)
    print("Breakdown by timeline_identifier (within stored rows)")
    print("-" * 110)
    tls_sorted = sorted(all_timeline_ids, key=lambda s: int(s.split("_")[1]) if "_" in s and s.split("_")[1].isdigit() else 10**9)

    header = f"{'timeline':<10} {'rows':>8} {'complete':>9} {'partial':>9} {'all_null':>9} {'complete%':>10}"
    print(header)
    print("-" * len(header))

    for tl in tls_sorted:
        c = per_tl_complete.get(tl, 0)
        p = per_tl_partial.get(tl, 0)
        z = per_tl_all_null.get(tl, 0)
        rows = c + p + z
        pct = (c / rows) if rows else 0.0
        print(f"{tl:<10} {rows:>8} {c:>9} {p:>9} {z:>9} {pct:>9.1%}")

    # ----------------------------
    # Fixture-level completeness distribution
    # ----------------------------
    print("\n" + "-" * 110)
    print("Fixture-level completeness distribution (complete snapshots per fixture)")
    print("-" * 110)

    # Build distribution for fixtures that have any rows
    dist = Counter(complete_count_by_fixture.get(fid, 0) for fid in fixtures_with_any_rows)

    # Print sorted by #complete
    for k in sorted(dist.keys()):
        print(f"complete_snapshots={k:>3}  fixtures={dist[k]:>6}  share_of_fixtures_with_rows={dist[k]/max(1,len(fixtures_with_any_rows)):.1%}")

    # Show a few fixtures with lowest/highest complete counts (among those with rows)
    def _pick_extremes(n: int = 10) -> Tuple[List[int], List[int]]:
        items = [(fid, complete_count_by_fixture.get(fid, 0), row_count_by_fixture.get(fid, 0)) for fid in fixtures_with_any_rows]
        items.sort(key=lambda x: (x[1], x[2], x[0]))  # low complete first
        low = [fid for fid, _, _ in items[:n]]
        high = [fid for fid, _, _ in items[-n:]]
        return low, high

    low, high = _pick_extremes(n=min(args.examples, 20))
    print(f"\nLowest complete-count fixtures (example): {low}")
    print(f"Highest complete-count fixtures (example): {high}")

    # Optional: show a quick line for each of the "low" fixtures
    print("\nDetails for lowest complete-count fixtures:")
    for fid in low:
        cc = complete_count_by_fixture.get(fid, 0)
        rc = row_count_by_fixture.get(fid, 0)
        print(f"  fixture_id={fid} | rows={rc} | complete_rows={cc} | complete_share={(cc/rc if rc else 0):.1%}")

    print("\nDone.")


if __name__ == "__main__":
    main()