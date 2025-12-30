from typing import Any, Dict, Tuple, Literal, Union, Optional

from api_calls.helpers.general import get_current_provider
from api_calls.helpers.providers.general import get_url
from api_calls.auth.auth import get_access_params

ReturnMode = Literal["parsed", "full"]


def get_player(
    player_id: int,
    return_mode: ReturnMode = "parsed",
    provider: Optional[str] = None,
) -> Union[Dict[str, Any], Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    Fetch player information by player ID.

    Parameters
    ----------
    player_id : int
    provider : str | None
        If None, uses provider from config.yaml (api.provider.name).
        If missing, falls back to "sportmonks".
    return_mode : "parsed" | "full"
        - "parsed" (default): returns parsed player dict
        - "full": returns (raw_json, parsed player dict)

    Returns
    -------
    parsed_player OR (raw_json, parsed_player)
    """
    # Resolve provider (config default, fallback to sportmonks)
    if provider is None or not str(provider).strip():
        provider = get_current_provider(default="sportmonks")
    provider = str(provider).strip().lower()

    # Auth params from api_config.yaml
    params = get_access_params(provider)

    # Provider URL
    # Sportmonks uses /players/{player_id}
    url = get_url(provider, "players_by_id").format(player_id=player_id)

    if provider == "sportmonks":
        from api_calls.helpers.providers.sportmonks import sm_player
        raw, parsed = sm_player(url=url, params=params)

    elif provider == "oddsapi":
        raise ValueError("oddsapi provider does not support players")

    else:
        raise ValueError(f"Unsupported provider: {provider}")

    if return_mode == "full":
        return raw, parsed
    if return_mode == "parsed":
        return parsed

    raise ValueError("return_mode must be either 'parsed' or 'full'")
