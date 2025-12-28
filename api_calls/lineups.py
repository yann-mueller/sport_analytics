from typing import Any, Dict, Tuple, Literal, Union, Optional

from api_calls.helpers.general import get_current_provider
from api_calls.helpers.providers.general import get_url
from api_calls.auth.auth import get_access_params

ReturnMode = Literal["parsed", "full"]


def get_lineup(
    fixture_id: int,
    return_mode: ReturnMode = "parsed",
    provider: Optional[str] = None,
) -> Union[Dict[str, Any], Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    Fetch a lineup for a fixture.

    Parameters
    ----------
    fixture_id : int
    provider : str | None
        If None, uses provider from config.yaml (api.provider.name).
        If missing, falls back to "sportmonks".
    return_mode : "parsed" | "full"
        - "parsed" (default): returns parsed lineup dict
        - "full": returns (raw_json, parsed lineup dict)

    Returns
    -------
    parsed_lineup OR (raw_json, parsed_lineup)
    """
    # Resolve provider (config default, fallback to sportmonks)
    if provider is None or not str(provider).strip():
        provider = get_current_provider(default="sportmonks")
    provider = str(provider).strip().lower()

    # Auth params from api_config.yaml
    params = get_access_params(provider)

    # Sportmonks lineups come via fixture-by-id include
    url = get_url(provider, "fixtures_by_id").format(fixture_id=fixture_id)

    if provider == "sportmonks":
        from api_calls.helpers.providers.sportmonks import sm_lineup
        raw, parsed = sm_lineup(url=url, params=params)

    elif provider == "oddsapi":
        # OddsAPI probably won't have lineups; keep placeholder for future
        raise ValueError("oddsapi provider does not support lineups")

    else:
        raise ValueError(f"Unsupported provider: {provider}")

    if return_mode == "full":
        return raw, parsed
    if return_mode == "parsed":
        return parsed

    raise ValueError("return_mode must be either 'parsed' or 'full'")