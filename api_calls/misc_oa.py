#%%###########################
### Fetch Sports (OddsAPI) ###
##############################
from helpers.general import get_current_provider
from helpers.providers.general import get_url
from auth.auth import get_access_params

import pandas as pd
import requests
import json

# Credentials
provider = "oddsapi"
params = get_access_params(provider)
api_key = params["api_token"]   # stored as api_token in your YAML (even though OddsAPI calls it apiKey)

# API Call
url = get_url(provider, "sports")
response = requests.get(url, params={"apiKey": api_key})
response.raise_for_status()

data = response.json()  # OddsAPI returns a LIST (not {"data": ...})

# Prepare Output
df_sports = pd.DataFrame([
    {
        "sport_key": x.get("key"),
        "group": x.get("group"),
        "title": x.get("title"),
        "description": x.get("description"),
        "active": x.get("active"),
        "has_outrights": x.get("has_outrights"),
    }
    for x in data
])

print(df_sports)


#%%###############################
### Fetch Seasons for a League ###
##################################
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import requests
import pandas as pd

from auth.auth import get_access_params
from helpers.providers.general import get_url


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_historical_events_oddsapi(
    sport_key: str,
    commence_from_iso: str,
    commence_to_iso: str,
    snapshot_every_days: int = 7,
    provider: str = "oddsapi",
) -> List[Dict[str, Any]]:
    """
    Collect historical events by iterating OddsAPI snapshot dates and de-duplicating event ids.
    Uses your config-driven auth + url helpers.

    Notes:
      - historical endpoint is paid-plan only
      - weekly snapshots reduce API cost; use snapshot_every_days=1 for max coverage
    """
    params = get_access_params(provider)
    api_key = params["api_token"]

    # URL template from providers_config.yaml
    url_tmpl = get_url(provider, "historical_events")

    # Bundesliga 2020/21: iterate snapshots across season (no need to be exact to the hour)
    start_dt = datetime(2020, 9, 18, 12, 0, 0, tzinfo=timezone.utc)
    end_dt   = datetime(2021, 5, 22, 12, 0, 0, tzinfo=timezone.utc)

    seen: Dict[str, Dict[str, Any]] = {}
    cur = start_dt

    while cur <= end_dt:
        snapshot_iso = _iso(cur)
        url = url_tmpl.format(sport=sport_key)

        r = requests.get(
            url,
            params={
                "apiKey": api_key,
                "date": snapshot_iso,
                "commenceTimeFrom": commence_from_iso,
                "commenceTimeTo": commence_to_iso,
                "dateFormat": "iso",
            },
            timeout=30,
        )
        r.raise_for_status()
        payload = r.json()

        events = payload.get("data", []) or []
        for e in events:
            eid = e.get("id")
            if not eid:
                continue
            if eid not in seen:
                seen[eid] = {
                    "event_id": eid,
                    "sport_key": e.get("sport_key"),
                    "sport_title": e.get("sport_title"),
                    "commence_time": e.get("commence_time"),
                    "home_team": e.get("home_team"),
                    "away_team": e.get("away_team"),
                    "found_in_snapshot": payload.get("timestamp"),
                }

        cur += timedelta(days=snapshot_every_days)

    return list(seen.values())


# --------- Example: Bundesliga 2020/21 ----------
SPORT_KEY = "soccer_germany_bundesliga"
SEASON_START = "2020-09-18T00:00:00Z"
SEASON_END   = "2021-05-22T23:59:59Z"

events = get_historical_events_oddsapi(
    sport_key=SPORT_KEY,
    commence_from_iso=SEASON_START,
    commence_to_iso=SEASON_END,
    snapshot_every_days=7,   # set to 1 for max coverage (more calls)
)

df = pd.DataFrame(events).sort_values("commence_time")
print(df.head(20))
print("Total unique events:", len(df))


#%%####################################
### Fetch Historical Odds for Event  ###
### (OddsAPI - 1x2 over time, Betfair) #
#######################################
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
import time

import requests
import pandas as pd

from auth.auth import get_access_params
from helpers.providers.general import get_url


def oddsapi_historical_event_odds_snapshot(
    sport_key: str,
    event_id: str,
    *,
    date_iso: str,
    regions: str = "eu",
    markets: str = "h2h",
    odds_format: str = "decimal",
    date_format: str = "iso",
    provider: str = "oddsapi",
) -> Dict[str, Any]:
    """
    Fetch ONE historical odds snapshot for a specific event at (or before) date_iso.
    """
    params = get_access_params(provider)
    api_key = params["api_token"]

    url = get_url(provider, "historical_event_odds").format(
        sport=sport_key,
        event_id=event_id,
    )

    r = requests.get(
        url,
        params={
            "apiKey": api_key,
            "date": date_iso,
            "regions": regions,
            "markets": markets,
            "oddsFormat": odds_format,
            "dateFormat": date_format,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def oddsapi_h2h_timeseries_betfair_wide(
    sport_key: str,
    event_id: str,
    *,
    start_date_iso: str,
    end_date_iso: Optional[str] = None,
    regions: str = "eu",
    odds_format: str = "decimal",
    bookmaker_key: str = "betfair",
    provider: str = "oddsapi",
    sleep_s: float = 0.2,
    max_steps: int = 2000,
) -> pd.DataFrame:
    """
    Build a historical 1x2 (h2h) odds series for ONE bookmaker (default: betfair),
    by walking the snapshot chain using previous_timestamp.

    Returns a WIDE DataFrame:
      snapshot_timestamp | event info... | home_odds | draw_odds | away_odds
    """
    bookmaker_key_lc = bookmaker_key.strip().lower()

    cur_date = start_date_iso
    end_dt = None
    if end_date_iso:
        end_dt = datetime.fromisoformat(end_date_iso.replace("Z", "+00:00")).astimezone(timezone.utc)

    seen_snapshots = set()
    wide_rows: List[Dict[str, Any]] = []

    steps = 0
    while cur_date and steps < max_steps:
        steps += 1

        payload = oddsapi_historical_event_odds_snapshot(
            sport_key=sport_key,
            event_id=event_id,
            date_iso=cur_date,
            regions=regions,
            markets="h2h",            # 1x2
            odds_format=odds_format,
            provider=provider,
        )

        snap_ts = payload.get("timestamp")
        prev_ts = payload.get("previous_timestamp")

        # avoid loops
        if snap_ts in seen_snapshots:
            break
        seen_snapshots.add(snap_ts)

        data = payload.get("data") or {}
        bookmakers = data.get("bookmakers") or []

        home_team = data.get("home_team")
        away_team = data.get("away_team")
        home_team_lc = str(home_team or "").strip().lower()
        away_team_lc = str(away_team or "").strip().lower()

        # find betfair bookmaker block (if present in this snapshot)
        bm = next(
            (b for b in bookmakers if str(b.get("key", "")).strip().lower() == bookmaker_key_lc),
            None
        )

        if bm is not None:
            bm_last = bm.get("last_update")
            # find h2h market
            m = next((mm for mm in (bm.get("markets") or []) if mm.get("key") == "h2h"), None)

            home_odds = draw_odds = away_odds = None
            market_last = None

            if m is not None:
                market_last = m.get("last_update")
                for out in (m.get("outcomes") or []):
                    nm = str(out.get("name", "")).strip().lower()
                    price = out.get("price")

                    if nm == home_team_lc:
                        home_odds = price
                    elif nm == away_team_lc:
                        away_odds = price
                    elif nm in {"draw", "tie"}:
                        draw_odds = price

            wide_rows.append({
                "snapshot_timestamp": snap_ts,
                "event_id": data.get("id"),
                "sport_key": data.get("sport_key"),
                "commence_time": data.get("commence_time"),
                "home_team": home_team,
                "away_team": away_team,

                "bookmaker_key": bm.get("key"),
                "bookmaker_title": bm.get("title"),
                "bookmaker_last_update": bm_last,

                "market": "h2h",
                "market_last_update": market_last,

                "home_odds": home_odds,
                "draw_odds": draw_odds,
                "away_odds": away_odds,
            })

        # stopping condition (optional): stop once we cross end_dt going backwards
        if end_dt and prev_ts:
            prev_dt = datetime.fromisoformat(prev_ts.replace("Z", "+00:00")).astimezone(timezone.utc)
            if prev_dt < end_dt:
                break

        cur_date = prev_ts
        time.sleep(sleep_s)

    df_wide = pd.DataFrame(wide_rows)

    # sort chronological (oldest -> newest) for plotting/inspection
    if not df_wide.empty and "snapshot_timestamp" in df_wide.columns:
        df_wide = df_wide.sort_values("snapshot_timestamp").reset_index(drop=True)

    return df_wide


# -----------------------
# Example usage
# -----------------------
sport_key = "soccer_germany_bundesliga"
event_id = "7b33ac3dc1ab6e9e0734da0f9c0f3e7f"

df_betfair = oddsapi_h2h_timeseries_betfair_wide(
    sport_key=sport_key,
    event_id=event_id,
    start_date_iso="2020-11-07T14:30:00Z",
    end_date_iso="2020-11-06T10:40:00Z",
    regions="eu",
    bookmaker_key="betfair",
)

print(df_betfair.head(30))
print("rows:", len(df_betfair))
# %%
