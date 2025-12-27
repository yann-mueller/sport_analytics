from typing import Any, Dict, List, Tuple, Union, Literal, Optional

from helpers.general import get_current_provider
from helpers.providers.general import get_url
from auth.auth import get_access_params

ReturnMode = Literal["parsed", "full"]


def get_schedule(
    season_id: int,
    return_mode: ReturnMode = "parsed",
    provider: Optional[str] = None,
) -> Union[List[Dict[str, Any]], Tuple[Dict[str, Any], List[Dict[str, Any]]]]:
    """
    Fetch the schedule for a season.

    Parameters
    ----------
    season_id : int
    return_mode : "parsed" | "full"
        - "parsed" (default): returns parsed list of fixtures
        - "full": returns (raw_json, parsed list)
    provider : str | None
        Optional provider override. If None, uses config.yaml (fallback "sportmonks").

    Returns
    -------
    parsed_schedule OR (raw_json, parsed_schedule)
    """
    # Resolve provider
    if provider is None or not str(provider).strip():
        provider = get_current_provider(default="sportmonks")
    provider = str(provider).strip().lower()

    # Auth params
    params = get_access_params(provider)

    # URL
    url = get_url(provider, "schedules_seasons").format(season_id=season_id)

    # Dispatch
    if provider == "sportmonks":
        from helpers.providers.sportmonks import sm_schedule
        raw, parsed = sm_schedule(url=url, season_id=season_id, params=params)
    else:
        raise ValueError(f"Unsupported provider: {provider}")

    if return_mode == "full":
        return raw, parsed
    if return_mode == "parsed":
        return parsed

    raise ValueError("return_mode must be either 'parsed' or 'full'")
