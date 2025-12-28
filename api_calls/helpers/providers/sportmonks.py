import requests
import json
import time
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from urllib.parse import quote

from api_calls.helpers.providers.general import get_market, get_nested, get_url
from api_calls.helpers.http import get_json_with_backoff

# CHECK IF SCORES IS AVAILABLE FOR FUTURE FIXTURES
def sm_fixture(url: str, params: Dict[str, str]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    api_token = params["api_token"]

    includes = ["participants", "scores"]
    query_params = {
        "api_token": api_token,
        "include": ";".join(includes),
    }

    resp = requests.get(url, params=query_params)
    print("URL actually called:", resp.url)
    resp.raise_for_status()

    data = resp.json()
    fixture_data = data["data"]
    timezone = data.get("timezone")

    # Basic fields
    fixture_id = fixture_data.get("id")
    league_id = fixture_data.get("league_id")
    season_id = fixture_data.get("season_id")
    date = fixture_data.get("starting_at")

    # Teams
    home_team_id = away_team_id = None
    home_team_name = away_team_name = None

    for team in fixture_data.get("participants", []) or []:
        meta = team.get("meta") or {}
        loc = meta.get("location")
        if loc == "home" and home_team_id is None:
            home_team_id = team.get("id")
            home_team_name = team.get("name")
        elif loc == "away" and away_team_id is None:
            away_team_id = team.get("id")
            away_team_name = team.get("name")

    # Goals (CURRENT)
    home_goals: Optional[int] = None
    away_goals: Optional[int] = None

    for entry in fixture_data.get("scores", []) or []:
        if entry.get("description") == "CURRENT":
            sc = entry.get("score", {}) or {}
            side = sc.get("participant")  # home/away
            goals = sc.get("goals")
            if side == "home":
                home_goals = goals
            elif side == "away":
                away_goals = goals

    parsed = {
        "fixture_id": fixture_id,
        "league_id": league_id,
        "season_id": season_id,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "date": date,
        "timezone": timezone,
        "home_team_name": home_team_name,
        "away_team_name": away_team_name,
        "home_goals": home_goals,
        "away_goals": away_goals,
    }

    return data, parsed


# CHECK STRUCTURE OF LINEUPS BEFORE GAMES
def sm_lineup(url: str, params: Dict[str, str]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    api_token = params["api_token"]

    includes = ["lineups.details.type", "participants"]
    query_params = {
        "api_token": api_token,
        "include": ";".join(includes),
        # only request the detail types we need (keeps payload smaller)
        # 118 = rating, 119 = minutes-played
        "filters": "lineupDetailTypes:118,119",
    }

    #print("URL actually called:", resp.url)
    data = get_json_with_backoff(url, params=query_params, timeout=30)
    fixture_data = data["data"]

    timezone = data.get("timezone")
    date = fixture_data.get("starting_at")
    fixture_id = fixture_data.get("id")

    # Determine home + away teams
    home_team_id = away_team_id = None
    home_team_name = away_team_name = None

    for team in fixture_data.get("participants", []) or []:
        loc = (team.get("meta") or {}).get("location")
        if loc == "home":
            home_team_id = team.get("id")
            home_team_name = team.get("name")
        elif loc == "away":
            away_team_id = team.get("id")
            away_team_name = team.get("name")

    def _get_detail_value(lineup_entry: dict, type_id: int, type_code: str) -> Any:
        """
        Extract value from a lineup detail by (type_id OR type.code).
        Returns None if not present.
        """
        for d in lineup_entry.get("details", []) or []:
            if d.get("type_id") == type_id or (d.get("type") or {}).get("code") == type_code:
                return (d.get("data") or {}).get("value")
        return None

    def get_minutes_player(lineup_entry: dict) -> int:
        val = _get_detail_value(lineup_entry, type_id=119, type_code="minutes-played")
        try:
            return int(val) if val is not None else 0
        except Exception:
            return 0

    def get_rating_player(lineup_entry: dict) -> Optional[float]:
        # rating is often a decimal number
        val = _get_detail_value(lineup_entry, type_id=118, type_code="rating")
        try:
            return float(val) if val is not None else None
        except Exception:
            return None

    home_lineup = []
    away_lineup = []

    for L in fixture_data.get("lineups", []) or []:
        L = dict(L)  # avoid mutating original structure
        L["minutes_player"] = get_minutes_player(L)
        L["rating_player"] = get_rating_player(L)

        # remove heavy details payload (after extracting what we need)
        L.pop("details", None)

        if L.get("team_id") == home_team_id:
            home_lineup.append(L)
        elif L.get("team_id") == away_team_id:
            away_lineup.append(L)

    parsed = {
        "fixture_id": fixture_id,
        "date": date,
        "timezone": timezone,
        "home_team_id": home_team_id,
        "home_team_name": home_team_name,
        "away_team_id": away_team_id,
        "away_team_name": away_team_name,
        "home_lineup": home_lineup,
        "away_lineup": away_lineup,
    }

    return data, parsed

def sm_schedule(
    url: str,
    season_id: int,
    params: Dict[str, str],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    SportMonks schedule-by-season:
      GET /schedules/seasons/{season_id}
    Returns (raw_json, parsed_fixtures)
    """
    api_token = params["api_token"]

    resp = requests.get(url, params={"api_token": api_token})
    print("URL actually called:", resp.url)
    resp.raise_for_status()

    raw = resp.json()
    parsed = _parse_season_schedule_sportmonks(raw, season_id=season_id)
    return raw, parsed


def _teams_from_participants_sportmonks(
    participants: List[Dict[str, Any]]
) -> Tuple[Optional[str], Optional[str]]:
    home = away = None
    for p in participants or []:
        loc = (p.get("meta") or {}).get("location")
        if loc == "home":
            home = p.get("name")
        elif loc == "away":
            away = p.get("name")
    return home, away


def _goals_from_scores_sportmonks(
    scores: List[Dict[str, Any]]
) -> Tuple[Optional[int], Optional[int]]:
    # Prefer CURRENT
    home_goals = away_goals = None
    for s in scores or []:
        if s.get("description") == "CURRENT":
            sc = s.get("score", {}) or {}
            if sc.get("participant") == "home":
                home_goals = sc.get("goals")
            elif sc.get("participant") == "away":
                away_goals = sc.get("goals")
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


def _parse_season_schedule_sportmonks(
    schedule_json: Dict[str, Any],
    season_id: int,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for stage in schedule_json.get("data", []) or []:
        for rnd in (stage.get("rounds", []) or []):
            for fx in (rnd.get("fixtures", []) or []):
                if fx.get("season_id") != season_id:
                    continue

                home_team, away_team = _teams_from_participants_sportmonks(fx.get("participants", []) or [])
                home_goals, away_goals = _goals_from_scores_sportmonks(fx.get("scores", []) or [])

                out.append({
                    "fixture_id": fx.get("id"),
                    "date": fx.get("starting_at"),
                    "home_team_name": home_team,
                    "away_team_name": away_team,
                    "home_goals": home_goals,
                    "away_goals": away_goals,
                })

    return out

#####################
### ODDS FETCHING ###
#####################
def sm_odds_from_fixture(
    url: str,
    fixture_id: int,
    market_name: str,
    params: Dict[str, str],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    SportMonks: GET /fixtures/{fixture_id}?include=odds
    Then filter odds to the configured canonical market_name using providers_config.yaml.

    Returns: (raw_json, parsed_filtered)
    """
    api_token = params["api_token"]

    resp = requests.get(url, params={"api_token": api_token, "include": "odds"})
    print("URL actually called:", resp.url)
    resp.raise_for_status()

    raw: Dict[str, Any] = resp.json()
    fixture = raw.get("data", {}) or {}

    # Extract odds list defensively
    odds_obj = fixture.get("odds")
    odds_list: List[Dict[str, Any]] = []

    if isinstance(odds_obj, list):
        odds_list = [x for x in odds_obj if isinstance(x, dict)]
    elif isinstance(odds_obj, dict):
        data = odds_obj.get("data")
        if isinstance(data, list):
            odds_list = [x for x in data if isinstance(x, dict)]
        elif isinstance(data, dict):
            odds_list = [data]

    # Build a "base" parsed object (before filtering)
    parsed_base: Dict[str, Any] = {
        "fixture_id": fixture.get("id", fixture_id),
        "league_id": fixture.get("league_id"),
        "season_id": fixture.get("season_id"),
        "starting_at": fixture.get("starting_at"),
        "count": len(odds_list),
        "odds": odds_list,
    }

    # Provider-config-driven filtering rule
    provider = "sportmonks"  # adapter knows its provider name
    rule = get_market(provider, market_name)
    field = str(rule["field"])
    equals_lc = {str(x).strip().lower() for x in (rule.get("equals") or [])}

    filtered: List[Dict[str, Any]] = []
    for o in odds_list:
        v = get_nested(o, field) if "." in field else o.get(field)
        if isinstance(v, str) and v.strip().lower() in equals_lc:
            filtered.append(o)

    parsed_filtered: Dict[str, Any] = dict(parsed_base)
    parsed_filtered["market_name"] = market_name.strip().lower()
    parsed_filtered["count"] = len(filtered)
    parsed_filtered["odds"] = filtered

    return raw, parsed_filtered


def _get_json_with_retry(url: str, params: dict, tries: int = 2) -> dict:
    for i in range(tries):
        try:
            r = requests.get(url, params=params)
            if r.status_code == 500:
                # SportMonks quirk: 500 often means "no updates in this window"
                return {"data": []}
            r.raise_for_status()
            return r.json()
        except requests.HTTPError:
            if i < tries - 1:
                time.sleep(0.5)
                continue
            return {"data": []}


def sm_premium_odd_history(
    fixture_id: int,
    market_name: str,
    bookmaker_id: int,
    outcome_label: str,
    params: Dict[str, str],
    from_utc: Optional[str] = None,
    to_utc: Optional[str] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Resolve PremiumOdd (odd_id) for fixture+market+bookmaker+label,
    then return its PremiumOddHistory.

    If from_utc and to_utc are provided -> use updated_between endpoint.
    Else -> use "all history" endpoint (can be large).
    """
    api_token = params["api_token"]
    provider = "sportmonks"

    # ---- 1) Resolve odd_id via premium odds snapshot for this fixture ----
    url_snapshot = get_url(provider, "premium_odds_by_fixture").format(fixture_id=fixture_id)
    r1 = requests.get(url_snapshot, params={"api_token": api_token})
    print("Premium odds snapshot URL:", r1.url)
    r1.raise_for_status()
    snapshot = r1.json()

    items = snapshot.get("data", [])
    if isinstance(items, dict):
        items = [items]
    if not isinstance(items, list):
        items = []

    # Filter to canonical market_name using providers_config.yaml mapping
    rule = get_market(provider, market_name)
    field = str(rule["field"])
    equals_lc = {str(x).strip().lower() for x in (rule.get("equals") or [])}

    market_filtered: List[Dict[str, Any]] = []
    for o in items:
        if not isinstance(o, dict):
            continue
        v = get_nested(o, field) if "." in field else o.get(field)
        if isinstance(v, str) and v.strip().lower() in equals_lc:
            market_filtered.append(o)

    outcome_label_lc = outcome_label.strip().lower()
    target = next(
        (
            o for o in market_filtered
            if int(o.get("bookmaker_id", -1)) == int(bookmaker_id)
            and str(o.get("label", "")).strip().lower() == outcome_label_lc
        ),
        None
    )
    if target is None:
        raise ValueError(
            f"No PremiumOdd found for fixture_id={fixture_id}, market='{market_name}', "
            f"bookmaker_id={bookmaker_id}, label='{outcome_label}'."
        )

    odd_id = target.get("id")
    if odd_id is None:
        raise ValueError("Resolved PremiumOdd has no 'id' (odd_id).")
    
    print("Resolved odd_id:", odd_id)
    print("Snapshot value:", target.get("value"))
    print("latest_bookmaker_update:", target.get("latest_bookmaker_update"))

    def _parse_dt(s: str) -> datetime:
        """
        Accepts:
        - YYYY-MM-DD HH:MM
        - YYYY-MM-DD HH:MM:SS
        """
        s = s.strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        raise ValueError(
            f"Invalid datetime format: '{s}'. "
            "Expected 'YYYY-MM-DD HH:MM' or 'YYYY-MM-DD HH:MM:SS'"
    )

    def _fmt_dt(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d %H:%M")

    def _url_time(s: str) -> str:
        # encode space + colon safely for path segments
        return quote(s, safe="")

    # ---- 2) Fetch history via updated_between in rolling windows ----
    if (from_utc is None) != (to_utc is None):
        raise ValueError("Provide both from_utc and to_utc, or neither.")

    now = datetime.utcnow()

    # DEFAULT RANGE (adjust if you want)
    if from_utc is None and to_utc is None:
        from_dt = now - timedelta(hours=24)
        to_dt = now
    else:
        from_dt = _parse_dt(from_utc)
        to_dt = _parse_dt(to_utc)

    step = timedelta(minutes=5)
    series = []
    seen = set()

    cur = from_dt
    while cur < to_dt:
        nxt = min(cur + step, to_dt)

        url_hist = get_url(provider, "premium_odds_history_updated_between").format(
            from_utc=_url_time(_fmt_dt(cur)),
            to_utc=_url_time(_fmt_dt(nxt)),
        )

        payload = _get_json_with_retry(url_hist, {"api_token": api_token})

        items = payload.get("data", [])
        if isinstance(items, dict):
            items = [items]

        for h in items:
            if int(h.get("odd_id", -1)) != int(odd_id):
                continue
            
            print("Window", _fmt_dt(cur), "-", _fmt_dt(nxt), "returned", len(items), "history rows total")
            key = (h.get("id"), h.get("bookmaker_update"), h.get("value"))
            if key in seen:
                continue
            seen.add(key)

            series.append({
                "fixture_id": fixture_id,
                "odd_id": odd_id,
                "bookmaker_id": bookmaker_id,
                "market_name": market_name,
                "label": outcome_label,
                "bookmaker_update": h.get("bookmaker_update"),
                "value": h.get("value"),
                "probability": h.get("probability"),
                "dp3": h.get("dp3"),
                "fractional": h.get("fractional"),
                "american": h.get("american"),
            })

        cur = nxt

    series.sort(key=lambda x: x.get("bookmaker_update") or "")

    raw_bundle = {
    "premium_odds_snapshot": snapshot,
    "resolved_premium_odd": target,
    "history_mode": "updated_between_loop",
    "from_utc": _fmt_dt(from_dt),
    "to_utc": _fmt_dt(to_dt),
}

    return raw_bundle, series


def sm_team(url: str, params: Dict[str, str]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Sportmonks: GET /teams/{id}
    Returns (raw_json, parsed)
    """
    r = requests.get(url, params={"api_token": params["api_token"]}, timeout=30)
    r.raise_for_status()
    raw = r.json()

    data = raw.get("data") or {}
    # sometimes APIs return dict vs list; normalize to dict
    if isinstance(data, list):
        data = data[0] if data else {}

    parsed = {
        "team_id": data.get("id"),
        "team_name": (data.get("name") or "").strip(),
    }
    return raw, parsed