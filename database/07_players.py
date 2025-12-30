"""
Create + maintain the `public.players` table with:
- player_id (PK)
- player_name (from Sportmonks player 'name')

Source of IDs:
- DISTINCT player_id from public.lineups

Behavior:
- Only fetches players missing in public.players
- UPSERTs player_name
"""

from __future__ import annotations

import time
from typing import List, Set, Dict, Any, Optional

from sqlalchemy import MetaData, Table, Column, Integer, Text, DateTime, func, text, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from database.connection.engine import get_engine
from api_calls.players import get_player


# ----------------------------
# DB schema
# ----------------------------
def make_players_table(metadata: MetaData) -> Table:
    return Table(
        "players",
        metadata,
        Column("player_id", Integer, primary_key=True, nullable=False),
        Column("player_name", Text, nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    )


# ----------------------------
# Helpers
# ----------------------------
def _get_distinct_lineup_player_ids(engine) -> List[int]:
    sql = text("SELECT DISTINCT player_id FROM public.lineups WHERE player_id IS NOT NULL ORDER BY player_id")
    with engine.begin() as conn:
        rows = conn.execute(sql).fetchall()
    return [int(r[0]) for r in rows]


def _get_existing_player_ids(engine, players: Table) -> Set[int]:
    with engine.begin() as conn:
        rows = conn.execute(select(players.c.player_id)).fetchall()
    return {int(r[0]) for r in rows}


def _clean_name(name: Optional[str]) -> Optional[str]:
    if name is None:
        return None
    # normalize whitespace (including non-breaking spaces)
    return " ".join(str(name).replace("\u00a0", " ").split()).strip()


def upsert_players(engine, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    metadata = MetaData()
    players = make_players_table(metadata)
    metadata.create_all(engine)

    stmt = pg_insert(players).values(rows)
    excluded = stmt.excluded

    stmt = stmt.on_conflict_do_update(
        index_elements=[players.c.player_id],
        set_={
            "player_name": excluded.player_name,
            "updated_at": func.now(),
        },
        where=players.c.player_name.is_distinct_from(excluded.player_name),
    )

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return int(res.rowcount or 0)


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    engine = get_engine()

    metadata = MetaData()
    players_tbl = make_players_table(metadata)
    metadata.create_all(engine)

    lineup_ids = _get_distinct_lineup_player_ids(engine)
    existing_ids = _get_existing_player_ids(engine, players_tbl)

    missing = [pid for pid in lineup_ids if pid not in existing_ids]

    print(f"[PLAYERS] unique player_ids in lineups: {len(lineup_ids)}")
    print(f"[PLAYERS] already in public.players: {len(existing_ids)}")
    print(f"[PLAYERS] missing to fetch: {len(missing)}")

    fetched = 0
    upserted_total = 0
    failed = 0

    batch: List[Dict[str, Any]] = []
    BATCH_SIZE = 250

    for i, player_id in enumerate(missing, start=1):
        try:
            p = get_player(player_id=player_id, provider="sportmonks", return_mode="parsed")
            name = _clean_name(p.get("name"))

            if not name:
                # If API returns no name (rare), skip but count as failed
                failed += 1
                print(f"[PLAYERS] Warning: player_id={player_id} returned empty name, skipping")
                continue

            batch.append({"player_id": int(player_id), "player_name": name})
            fetched += 1

            if len(batch) >= BATCH_SIZE:
                upserted_total += upsert_players(engine, batch)
                batch.clear()

            if i % 200 == 0 or i == len(missing):
                print(f"[PLAYERS] progress {i}/{len(missing)} fetched={fetched} failed={failed} upserted={upserted_total}")

            time.sleep(0.10)  # be nice to API

        except KeyboardInterrupt:
            print("\n[INTERRUPT] Stopping early (CTRL+C). Progress is saved batch-by-batch.")
            break
        except Exception as e:
            failed += 1
            print(f"[PLAYERS] Warning: failed player_id={player_id}: {e}")
            time.sleep(0.3)
            continue

    # flush remaining
    if batch:
        upserted_total += upsert_players(engine, batch)

    print("\nDone.")
    print(f"[PLAYERS] fetched={fetched} failed={failed} upserted_rows={upserted_total}")
    print("Table: public.players")


if __name__ == "__main__":
    main()