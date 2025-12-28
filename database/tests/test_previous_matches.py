from __future__ import annotations

from sqlalchemy import text
from database.connection.engine import get_engine
from api_calls.helpers.general import get_current_provider


def main() -> None:
    provider = get_current_provider(default="sportmonks")
    provider = provider.strip().lower()

    engine = get_engine()

    with engine.begin() as conn:
        # 1) Pick a random fixture
        fixture = conn.execute(
            text(
                """
                SELECT
                    fixture_id,
                    season_id,
                    date,
                    home_team_id,
                    away_team_id
                FROM public.fixtures
                WHERE provider = :provider
                ORDER BY random()
                LIMIT 1
                """
            ),
            {"provider": provider},
        ).mappings().one()

        fixture_id = fixture["fixture_id"]
        season_id = fixture["season_id"]
        date = fixture["date"]
        home_team_id = fixture["home_team_id"]
        away_team_id = fixture["away_team_id"]

        def team_name(conn, team_id: int) -> str:
            res = conn.execute(
                text(
                    """
                    SELECT team_name
                    FROM public.teams
                    WHERE team_id = :team_id
                    """
                ),
                {"team_id": team_id},
            ).scalar_one_or_none()

            return res or f"[unknown:{team_id}]"

        home_name = team_name(conn, home_team_id)
        away_name = team_name(conn, away_team_id)

        print("\n=== RANDOM FIXTURE ===")
        print(f"Fixture ID : {fixture_id}")
        print(f"Season ID  : {season_id}")
        print(f"Date       : {date}")
        print(f"Home Team  : {home_name} ({home_team_id})")
        print(f"Away Team  : {away_name} ({away_team_id})")

        # 2) Fetch previous_matches entries
        prev_matches = conn.execute(
            text(
                """
                SELECT *
                FROM public.previous_matches
                WHERE fixture_id = :fixture_id
                  AND team_id IN (:home, :away)
                ORDER BY team_id
                """
            ),
            {
                "fixture_id": fixture_id,
                "home": home_team_id,
                "away": away_team_id,
            },
        ).mappings().all()

        print("\n=== PREVIOUS_MATCHES ENTRIES ===")
        for row in prev_matches:
            print(dict(row))

        # Helper function to fetch last 5 games
        def last_games(team_id: int):
            return conn.execute(
                text(
                    """
                    SELECT *
                    FROM public.fixtures
                    WHERE provider = :provider
                      AND season_id = :season_id
                      AND date < :date
                      AND (home_team_id = :team_id OR away_team_id = :team_id)
                    ORDER BY date DESC
                    LIMIT 5
                    """
                ),
                {
                    "provider": provider,
                    "season_id": season_id,
                    "date": date,
                    "team_id": team_id,
                },
            ).mappings().all()

        # 3) Fetch last 5 fixtures for both teams
        home_last = last_games(home_team_id)
        away_last = last_games(away_team_id)

        print("\n=== LAST 5 FIXTURES (HOME TEAM) ===")
        for r in home_last:
            h_name = team_name(conn, r["home_team_id"])
            a_name = team_name(conn, r["away_team_id"])

            print(
                f"{r['fixture_id']} | {r['date']} | "
                f"{h_name} vs {a_name} "
                f"({r['home_goals']}:{r['away_goals']})"
            )

        print("\n=== LAST 5 FIXTURES (AWAY TEAM) ===")
        for r in away_last:
            h_name = team_name(conn, r["home_team_id"])
            a_name = team_name(conn, r["away_team_id"])

            print(
                f"{r['fixture_id']} | {r['date']} | "
                f"{h_name} vs {a_name} "
                f"({r['home_goals']}:{r['away_goals']})"
            )

    print("\n✓ Test completed successfully.")


if __name__ == "__main__":
    main()