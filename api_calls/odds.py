from typing import Any, Dict, Tuple, Union, Literal, List, Optional

from helpers.general import get_current_provider
from helpers.providers.general import get_url
from auth.auth import get_access_params

ReturnMode = Literal["parsed", "full"]


def get_odds(
    fixture_id: int,
    market_name: str,
    return_mode: ReturnMode = "parsed",
) -> Union[Dict[str, Any], Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    Fetch odds for a fixture and filter to a canonical market_name.

    return_mode:
      - "parsed" (default): returns parsed+filtered odds dict
      - "full": returns (raw_json, parsed+filtered odds dict)
    """
    if not isinstance(market_name, str) or not market_name.strip():
        raise ValueError("market_name must be a non-empty string")

    provider = get_current_provider()
    params = get_access_params(provider)

    # For odds we use fixture endpoint + include=odds
    url = get_url(provider, "fixtures_by_id").format(fixture_id=fixture_id)

    if provider == "sportmonks":
        from helpers.providers.sportmonks import sm_odds_from_fixture
        raw, parsed_filtered = sm_odds_from_fixture(
            url=url,
            fixture_id=fixture_id,
            market_name=market_name,
            params=params,
        )
    else:
        raise ValueError(f"Unsupported provider: {provider}")

    if return_mode == "full":
        return raw, parsed_filtered
    if return_mode == "parsed":
        return parsed_filtered

    raise ValueError("return_mode must be either 'parsed' or 'full'")


def get_premium_odd_history(
    fixture_id: int,
    market_name: str,
    bookmaker_id: int,
    outcome_label: str,                 # "Home" / "Draw" / "Away"
    from_utc: Optional[str] = None,     # optional: "YYYY-MM-DD HH:MM:SS"
    to_utc: Optional[str] = None,       # optional: "YYYY-MM-DD HH:MM:SS"
    return_mode: ReturnMode = "parsed",
) -> Union[List[Dict[str, Any]], Tuple[Dict[str, Any], List[Dict[str, Any]]]]:
    """
    Premium odds time series for ONE bookmaker/outcome.

    Default behavior (from_utc/to_utc omitted):
      - fetch FULL PremiumOddHistory (global endpoint) and filter to the resolved odd_id

    If from_utc and to_utc are provided:
      - fetch PremiumOddHistory updated between those datetimes and filter to odd_id
    """
    provider = get_current_provider()
    params = get_access_params(provider)

    if provider == "sportmonks":
        from helpers.providers.sportmonks import sm_premium_odd_history
        raw, series = sm_premium_odd_history(
            fixture_id=fixture_id,
            market_name=market_name,
            bookmaker_id=bookmaker_id,
            outcome_label=outcome_label,
            params=params,
            from_utc=from_utc,
            to_utc=to_utc,
        )
    else:
        raise ValueError(f"Unsupported provider: {provider}")

    if return_mode == "full":
        return raw, series
    return series