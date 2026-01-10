# database/05_extend_lineups.py
"""
EXTEND-ONLY: Add missing lineups to `public.lineups` (one row per player per fixture).

Behavior:
- Creates table if missing
- Ensures NEW card columns exist (ALTER TABLE IF NOT EXISTS...)
- Finds fixture_ids in public.fixtures that do NOT yet appear in public.lineups
- Fetches lineups fixture-by-fixture (progress saved even if rate-limited)
- Retries SportMonks 429 (rate limit) with backoff + clear terminal feedback
- UPSERT: only updates rows if something actually changed (updated_at is meaningful)
- (Optional/safe) Deletes lineups for fixtures that are no longer in fixtures table

NOTE:
- This version stores lineups WITHOUT a `provider` column.
  Provider is only used for the API call (not persisted in lineups).
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Sequence, Set

import requests
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    Float,
    DateTime,
    func,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

from database.connection.engine import get_engine
from api_calls.helpers.general import get_current_provider
from api_calls.lineups import get_lineup  # your API call wrapper


# ----------------------------
# Helpers: rate-limit retry
# ----------------------------
def _is_rate_limit(resp: Optional[requests.Response], err: Exception) -> bool:
    return isinstance(err, requests.HTTPError) and resp is not None and resp.status_code == 429


def _sleep_with_feedback(seconds: float) -> None:
    print(f"[RATE LIMIT] Sleeping {seconds:.1f}s then retrying...")
    time.sleep(seconds)


def _call_get_lineup_with_retry(
    fixture_id: int,
    provider: str,
    max_retries: int = 12,
    base_sleep_s: float = 2.0,
    max_sleep_s: float = 60.0,
) -> Dict[str, Any]:
    """
    Calls get_lineup(fixture_id, provider=...) and retries on rate limit (429).
    """
    last_err: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            # IMPORTANT: this relies on your updated API call wrapper
            # returning parsed lineups including:
            # yellowcards_player, redcards_player, yellowred_player
            return get_lineup(fixture_id=fixture_id, provider=provider, return_mode="parsed")
        except Exception as e:
            last_err = e

            resp: Optional[requests.Response] = getattr(e, "response", None)
            if _is_rate_limit(resp, e):
                try:
                    payload = resp.json() if resp is not None else {}
                except Exception:
                    payload = {}

                msg = payload.get("message") or "Rate limit reached"
                reset_code = payload.get("reset_code")
                print(
                    f"[RATE LIMIT] fixture_id={fixture_id} attempt={attempt}/{max_retries} "
                    f"message='{msg}' reset_code={reset_code}"
                )

                sleep_s = min(max_sleep_s, base_sleep_s * (1.6 ** (attempt - 1)))
                _sleep_with_feedback(sleep_s)
                continue

            raise

    raise RuntimeError(
        f"Failed to fetch lineup for fixture_id={fixture_id} after {max_retries} retries"
    ) from last_err


# ----------------------------
# DB schema (NO provider column)
# ----------------------------
def make_lineups_table(metadata: MetaData) -> Table:
    return Table(
        "lineups",
        metadata,
        Column("fixture_id", Integer, primary_key=True, nullable=False),
        Column("player_id", Integer, primary_key=True, nullable=False),
        Column("team_id", Integer, nullable=True),
        Column("type_id", Integer, nullable=True),
        Column("minutes_player", Integer, nullable=True),
        Column("rating_player", Float, nullable=True),
        Column("formation_position", Integer, nullable=True),

        # NEW: cards
        Column("yellowcards_player", Integer, nullable=True),
        Column("redcards_player", Integer, nullable=True),
        Column("yellowred_player", Integer, nullable=True),

        Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        schema="public",
    )


def ensure_lineups_schema(engine) -> None:
    """
    Create table if missing AND add new columns if the table already existed.
    Safe to run every time.
    """
    metadata = MetaData()
    make_lineups_table(metadata)
    metadata.create_all(engine)

    # create_all won't add columns to existing tables -> ALTER TABLE for safety
    ddl = text(
        """
        ALTER TABLE public.lineups
          ADD COLUMN IF NOT EXISTS yellowcards_player integer,
          ADD COLUMN IF NOT EXISTS redcards_player integer,
          ADD COLUMN IF NOT EXISTS yellowred_player integer;
        """
    )
    with engine.begin() as conn:
        conn.execute(ddl)


# ----------------------------
# Parse lineup payload → rows
# ----------------------------
def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def flatten_lineup(parsed_lineup: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Input: parsed dict from your get_lineup() call.
    Output: rows for DB insert (one player per row).
    """
    rows: List[Dict[str, Any]] = []

    fixture_id = _safe_int(parsed_lineup.get("fixture_id"))

    for side in ("home_lineup", "away_lineup"):
        for p in parsed_lineup.get(side, []) or []:
            rows.append(
                {
                    "fixture_id": fixture_id,
                    "player_id": _safe_int(p.get("player_id")),
                    "team_id": _safe_int(p.get("team_id")),
                    "type_id": _safe_int(p.get("type_id")),
                    "minutes_player": _safe_int(p.get("minutes_player")),
                    "rating_player": _safe_float(p.get("rating_player")),
                    "formation_position": _safe_int(p.get("formation_position")),

                    # NEW: cards
                    "yellowcards_player": _safe_int(p.get("yellowcards_player")),
                    "redcards_player": _safe_int(p.get("redcards_player")),
                    "yellowred_player": _safe_int(p.get("yellowred_player")),
                }
            )

    # Drop rows with missing keys (player_id/fixture_id must exist)
    rows = [r for r in rows if r.get("fixture_id") is not None and r.get("player_id") is not None]
    return rows


# ----------------------------
# Upsert logic (updated_at only if changed)
# ----------------------------
def upsert_lineups(engine, rows: Sequence[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    metadata = MetaData()
    lineups = make_lineups_table(metadata)
    metadata.create_all(engine)

    stmt = pg_insert(lineups).values(list(rows))
    excluded = stmt.excluded

    changed_condition = (
        (lineups.c.team_id.is_distinct_from(excluded.team_id))
        | (lineups.c.type_id.is_distinct_from(excluded.type_id))
        | (lineups.c.minutes_player.is_distinct_from(excluded.minutes_player))
        | (lineups.c.rating_player.is_distinct_from(excluded.rating_player))
        | (lineups.c.formation_position.is_distinct_from(excluded.formation_position))
        # NEW: cards
        | (lineups.c.yellowcards_player.is_distinct_from(excluded.yellowcards_player))
        | (lineups.c.redcards_player.is_distinct_from(excluded.redcards_player))
        | (lineups.c.yellowred_player.is_distinct_from(excluded.yellowred_player))
    )

    stmt = stmt.on_conflict_do_update(
        index_elements=[lineups.c.fixture_id, lineups.c.player_id],
        set_={
            "team_id": excluded.team_id,
            "type_id": excluded.type_id,
            "minutes_player": excluded.minutes_player,
            "rating_player": excluded.rating_player,
            "formation_position": excluded.formation_position,
            # NEW: cards
            "yellowcards_player": excluded.yellowcards_player,
            "redcards_player": excluded.redcards_player,
            "yellowred_player": excluded.yellowred_player,
            "updated_at": func.now(),
        },
        where=changed_condition,
    )

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return int(res.rowcount or 0)


# ----------------------------
# Extend pipeline helpers
# ----------------------------
def get_fixture_ids(engine) -> List[int]:
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT DISTINCT fixture_id FROM public.fixtures ORDER BY fixture_id")
        ).fetchall()
    return [int(r[0]) for r in rows]


def get_fixture_ids_with_lineups(engine) -> Set[int]:
    """
    Returns distinct fixture_ids already present in public.lineups.
    """
    metadata = MetaData()
    lineups = make_lineups_table(metadata)
    metadata.create_all(engine)

    stmt = select(lineups.c.fixture_id).distinct()

    with engine.begin() as conn:
        rows = conn.execute(stmt).fetchall()

    return {int(r[0]) for r in rows}


def delete_lineups_not_in_fixtures(engine) -> int:
    """
    Delete lineups rows for fixtures no longer present in fixtures table.
    Safe to run; optional for extend-only, but keeps DB tidy.
    """
    sql = text(
        """
        DELETE FROM public.lineups l
        WHERE NOT EXISTS (
            SELECT 1 FROM public.fixtures f
            WHERE f.fixture_id = l.fixture_id
        )
        """
    )
    with engine.begin() as conn:
        res = conn.execute(sql)
        return int(res.rowcount or 0)


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    provider = get_current_provider(default="sportmonks").strip().lower()
    engine = get_engine()

    # Ensure table exists + new card columns exist
    ensure_lineups_schema(engine)

    fixture_ids = get_fixture_ids(engine)
    print(f"Found {len(fixture_ids)} fixtures in DB (public.fixtures).")

    existing_lineup_fixture_ids = get_fixture_ids_with_lineups(engine)
    missing_fixture_ids = [fid for fid in fixture_ids if fid not in existing_lineup_fixture_ids]

    print(f"Fixtures with any lineup rows already: {len(existing_lineup_fixture_ids)}")
    print(f"Missing lineups for fixtures: {len(missing_fixture_ids)}")

    if not missing_fixture_ids:
        deleted = delete_lineups_not_in_fixtures(engine)
        print("Nothing to extend. public.lineups is up to date.")
        print(f"Deleted rows not in fixtures: {deleted}")
        print("Table: public.lineups")
        return

    total_upserted = 0
    ok = 0
    failed = 0

    for i, fixture_id in enumerate(missing_fixture_ids, start=1):
        try:
            parsed = _call_get_lineup_with_retry(fixture_id=fixture_id, provider=provider)
            rows = flatten_lineup(parsed)
            changed = upsert_lineups(engine, rows)

            ok += 1
            total_upserted += changed

            if ok % 25 == 0 or i == len(missing_fixture_ids):
                print(
                    f"[PROGRESS] {i}/{len(missing_fixture_ids)} ok={ok} failed={failed} "
                    f"rows_inserted_or_updated={total_upserted}"
                )

            time.sleep(0.15)

        except KeyboardInterrupt:
            print("\n[INTERRUPT] Stopping early (CTRL+C). Progress is saved fixture-by-fixture.")
            break
        except Exception as e:
            failed += 1
            print(f"Warning: failed to fetch/save lineup for fixture_id={fixture_id}: {e}")
            time.sleep(0.5)
            continue

    deleted = delete_lineups_not_in_fixtures(engine)

    print("\nDone.")
    print(f"Fixtures processed: {len(missing_fixture_ids)} | ok: {ok} | failed: {failed}")
    print(f"Upserted rows (insert/update): {total_upserted}")
    print(f"Deleted rows not in fixtures: {deleted}")
    print("Table: public.lineups")


if __name__ == "__main__":
    main()
