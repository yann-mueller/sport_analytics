#%%##################
### Fetch Leagues ###
#####################
from helpers.general import get_current_provider
from helpers.providers.general import get_url
from auth.auth import get_access_params
import pandas as pd
import requests
import json

# Credentials
provider = get_current_provider()
api_token = get_access_params(provider)["api_token"]

# API Call
url = get_url(provider, "leagues") 
response = requests.get(url, params={"api_token": api_token})

# Extract Data
data = response.json()["data"]

# Prepare Output
df_leagues = pd.DataFrame([
    {"league_id": x["id"], "league_name": x["name"]}
    for x in data
])

print(df_leagues)


#%%###############################
### Fetch Seasons for a League ###
##################################
from helpers.general import get_current_provider
from helpers.providers.general import get_url
from auth.auth import get_access_params

import requests
import pandas as pd

# Inputs
league_id = 82

# Credentials
provider = get_current_provider()
api_token = get_access_params(provider)["api_token"]

# API Call
url = get_url(provider, "seasons")
response = requests.get(
    url,
    params={
        "api_token": api_token,
        "filters": f"seasonLeagues:{league_id}",
        "per_page": 50,
    },
)

json_data = response.json()
print(response.status_code, json_data.get("message", ""))

# Extract Data
data = json_data["data"]

# Prepare Output
df_seasons = pd.DataFrame([{
    "season_id": s["id"],
    "season_name": s["name"],
    "league_id": s.get("league_id"),
    "is_current": s.get("is_current"),
} for s in data])

print(df_seasons)


#%%################################
### Fetch Schedule for a Season ###
###################################
from helpers.general import get_current_provider
from helpers.providers.general import get_url
from auth.auth import get_access_params

import requests
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple

# Inputs
season_id = 25646

def _teams_from_participants(participants: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    home = away = None
    for p in participants or []:
        loc = (p.get("meta") or {}).get("location")
        if loc == "home":
            home = p.get("name")
        elif loc == "away":
            away = p.get("name")
    return home, away


def _goals_from_scores(scores: List[Dict[str, Any]]) -> Tuple[Optional[int], Optional[int]]:
    # Prefer CURRENT
    home_goals = away_goals = None
    for s in scores or []:
        if s.get("description") == "CURRENT":
            sc = s.get("score", {}) or {}
            if sc.get("participant") == "home":
                home_goals = sc.get("goals")
            elif sc.get("participant") == "away":
                away_goals = sc.get("goals")
            # if both filled we can stop
            if home_goals is not None and away_goals is not None:
                return home_goals, away_goals

    # Fallback: last available
    for s in scores or []:
        sc = s.get("score", {}) or {}
        if sc.get("participant") == "home":
            home_goals = sc.get("goals")
        elif sc.get("participant") == "away":
            away_goals = sc.get("goals")

    return home_goals, away_goals


def _parse_season_schedule(schedule_json: Dict[str, Any], season_id: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for stage in schedule_json.get("data", []) or []:
        for rnd in (stage.get("rounds", []) or []):
            for fx in (rnd.get("fixtures", []) or []):
                if fx.get("season_id") != season_id:
                    continue

                home_team, away_team = _teams_from_participants(fx.get("participants", []) or [])
                home_goals, away_goals = _goals_from_scores(fx.get("scores", []) or [])

                out.append({
                    "fixture_id": fx.get("id"),
                    "date": fx.get("starting_at"),
                    "home_team_name": home_team,
                    "away_team_name": away_team,
                    "home_goals": home_goals,
                    "away_goals": away_goals,
                })

    return out


def fixtures_from_season(season_id: int) -> List[Dict[str, Any]]:
    # Credentials (same structure as your other scripts)
    provider = get_current_provider()
    access_params = get_access_params(provider)
    api_token = access_params["api_token"]

    # URL (from providers_url_structure.yaml)
    # Make sure your YAML has something like:
    # schedules_seasons: "/schedules/seasons/{season_id}"
    url = get_url(provider, "schedules_seasons").format(season_id=season_id)

    # Sportmonks uses query param api_token (consistent with your other calls)
    resp = requests.get(url, params={"api_token": api_token})
    resp.raise_for_status()

    return _parse_season_schedule(resp.json(), season_id)


fixtures = fixtures_from_season(season_id)
df = pd.DataFrame(fixtures)
print(df)


#%%#################################
### Fetch Red Cards for a Fixture ###
####################################
from helpers.general import get_current_provider
from helpers.providers.general import get_url
from auth.auth import get_access_params

import requests
import pandas as pd

fixture_id = 19433577  # <-- set this

provider = get_current_provider()
api_token = get_access_params(provider)["api_token"]

# Your helper should return something like:
# https://api.sportmonks.com/v3/football/fixtures
fixtures_base = get_url(provider, "fixtures_by_id")

# Ensure we call fixture-by-id
#url = f"{fixtures_base.rstrip('/')}/{fixture_id}"

params = {
    "api_token": api_token,
    "include": "events,events.type,events.player,events.participant",
}

print("REQUEST URL:", url)
print("PARAMS:", params)

resp = requests.get(url, params=params)
data = resp.json()

print("HTTP:", resp.status_code)
print("API message:", data.get("message", ""))

# Hard stop on endpoint / auth / plan errors
if resp.status_code != 200 or "data" not in data:
    # This is where your "4 The requested endpoint does not exist" ends up
    raise RuntimeError(f"Request failed. HTTP={resp.status_code}, message={data.get('message')}")

fixture = data["data"]

# In v3, includes are often embedded directly under data.<relation>
events = fixture.get("events", []) or []

def event_code(ev: dict) -> str:
    # sometimes code is nested in the included type object
    t = ev.get("type")
    if isinstance(t, dict):
        c = t.get("code") or t.get("name")
        return (c or "").upper()
    if isinstance(ev.get("type"), str):
        return ev["type"].upper()
    return ""

RED_CODES = {"REDCARD", "YELLOWREDCARD"}  # direct red + 2nd-yellow red :contentReference[oaicite:1]{index=1}

rows = []
for ev in events:
    code = event_code(ev)
    if code in RED_CODES:
        rows.append({
            "event_id": ev.get("id"),
            "fixture_id": fixture_id,
            "minute": ev.get("minute"),
            "extra_minute": ev.get("extra_minute"),
            "type": code,
            "participant_id": ev.get("participant_id"),
            "player_id": ev.get("player_id"),
            "player_name": (ev.get("player") or {}).get("name") if isinstance(ev.get("player"), dict) else None,
            "reason": ev.get("reason"),
        })

df_redcards = pd.DataFrame(rows)

# Avoid KeyError when empty
if not df_redcards.empty:
    df_redcards = df_redcards.sort_values(["minute", "extra_minute"], na_position="last")

print(df_redcards)




#%%##########################
### Premium Odds Fetching ###
############################
from helpers.general import get_current_provider
from auth.auth import get_access_params

import requests
import json
import pandas as pd

# -----------------
# Config
# -----------------
league_id = 82
provider = "sportmonks"  # explicit for clarity

# -----------------
# Credentials
# -----------------
params = get_access_params(provider)
api_token = params["api_token"]

# -----------------
# API Call
# -----------------
url = "https://api.sportmonks.com/v3/football/odds/premium"

response = requests.get(
    url,
    params={
        "api_token": api_token,
        "filters": f"seasonLeagues:{league_id}",
        "per_page": 50,
    }
)

response.raise_for_status()
json_data = response.json()

print(json.dumps(json_data, indent=4))
print("Status:", response.status_code)
print("Message:", json_data.get("message", ""))


#%%############################################
### Standard Odds (Pre-Match) by Fixture ID ###
##############################################
from helpers.general import get_current_provider
from auth.auth import get_access_params

import requests
import json
import pandas as pd

# -----------------
# Config
# -----------------
fixture_id = 18863193
provider = "sportmonks"

MARKET_ID_FULLTIME_RESULT = 1
BOOKMAKER_ID_BETFAIR = 9

# -----------------
# Credentials
# -----------------
params = get_access_params(provider)
api_token = params["api_token"]

# -----------------
# API Call
# -----------------
url = f"https://api.sportmonks.com/v3/football/odds/pre-match/fixtures/{fixture_id}"

response = requests.get(
    url,
    params={
        "api_token": api_token,
        "filters": (
            f"markets:{MARKET_ID_FULLTIME_RESULT};"
            f"bookmakers:{BOOKMAKER_ID_BETFAIR}"
        ),
        # optional but useful
        "include": "market;bookmaker;fixture",
    }
)

response.raise_for_status()
json_data = response.json()

print(json.dumps(json_data, indent=4))
print("Status:", response.status_code)
print("Message:", json_data.get("message", ""))


# -----------------
# Extract 1X2 odds
# -----------------
from datetime import datetime, timezone

def _norm(x):
    return str(x or "").strip().lower()

def _parse_ts(x):
    if not x:
        return None
    s = str(x).strip()
    # SportMonks sometimes returns "YYYY-MM-DD HH:MM:SS"
    if "T" in s:
        # ISO "2023-08-12T13:45:59.000000Z"
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    try:
        # "2023-08-19 13:16:18" -> assume UTC
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None

label_map = {
    # numeric style
    "1": "home",
    "x": "draw",
    "2": "away",
    # text style
    "home": "home",
    "draw": "draw",
    "away": "away",
}

latest = {}

for o in json_data.get("data", []) or []:
    raw = o.get("label") or o.get("name")
    key = label_map.get(_norm(raw))
    if key is None:
        continue

    ts_raw = o.get("latest_bookmaker_update") or o.get("created_at")
    ts = _parse_ts(ts_raw)
    if ts is None:
        continue

    val = o.get("value")
    if val is None:
        continue
    try:
        odds = float(val)
    except Exception:
        continue

    if (key not in latest) or (ts > latest[key]["ts"]):
        latest[key] = {"odds": odds, "ts": ts}

home_odds = latest.get("home", {}).get("odds")
draw_odds = latest.get("draw", {}).get("odds")
away_odds = latest.get("away", {}).get("odds")

print(f"Home: {home_odds}")
print(f"Draw: {draw_odds}")
print(f"Away: {away_odds}")