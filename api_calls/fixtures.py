from typing import Any, Dict, Tuple, Literal, Union, Optional

from helpers.general import get_current_provider
from helpers.providers.general import get_url
from auth.auth import get_access_params

ReturnMode = Literal["parsed", "full"]


def get_fixture(
    fixture_id: int,
    return_mode: ReturnMode = "parsed",
    provider: Optional[str] = None,
) -> Union[Dict[str, Any], Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    Fetch a single fixture.

    Parameters
    ----------
    fixture_id : int
    provider : str | None
        If None, uses provider from config.yaml (api.provider.name).
        If that is missing, falls back to "sportmonks".
    return_mode : "parsed" | "full"
        - "parsed" (default): return parsed fixture only
        - "full": return (raw_json, parsed_fixture)

    Returns
    -------
    parsed_fixture OR (raw_json, parsed_fixture)
    """
    # Resolve provider
    if provider is None or not str(provider).strip():
        provider = get_current_provider(default="sportmonks")
    provider = str(provider).strip().lower()

    # Auth params from api_config.yaml (your apis: mapping)
    params = get_access_params(provider)

    # URL from providers_config.yaml
    url = get_url(provider, "fixtures_by_id").format(fixture_id=fixture_id)

    # Dispatch to provider adapter
    if provider == "sportmonks":
        from helpers.providers.sportmonks import sm_fixture
        raw, parsed = sm_fixture(url=url, params=params)

    elif provider == "oddsapi":
        # You’ll implement this adapter later
        from helpers.providers.oddsapi import oa_fixture
        raw, parsed = oa_fixture(url=url, fixture_id=fixture_id, params=params)

    else:
        raise ValueError(f"Unsupported provider: {provider}")

    if return_mode == "full":
        return raw, parsed
    if return_mode == "parsed":
        return parsed

    raise ValueError("return_mode must be either 'parsed' or 'full'")