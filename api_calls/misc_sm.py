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


#%%##########################
### Premium Odds Fetching ###
#############################
from auth.auth import get_api_token
import requests
import pandas as pd
import json
# Sportsmonks API
MONKS_API = get_api_token("sportmonks")
league_id = 82

url = "https://api.sportmonks.com/v3/football/odds/premium"

response = requests.get(
    url,
    params={
        "api_token": MONKS_API,
        "filters": f"seasonLeagues:{league_id}",
        "per_page": 50
    }
)

json_data = response.json()
print(json_data)
print(json.dumps(response.json(), indent=4))
print(response.status_code, json_data.get("message", ""))

#%%################################
### Premium Odds Fetching by ID ###
###################################
from auth.auth import get_api_token
import requests
import pandas as pd
import json
# Sportsmonks API
MONKS_API = get_api_token("sportmonks")
league_id = 82

url = "https://api.sportmonks.com/v3/football/odds/premium/fixtures/19433585"

response = requests.get(
    url,
    params={
        "api_token": MONKS_API,
        "per_page": 50
    }
)

json_data = response.json()
print(json_data)
print(json.dumps(response.json(), indent=4))
print(response.status_code, json_data.get("message", ""))





# %%
