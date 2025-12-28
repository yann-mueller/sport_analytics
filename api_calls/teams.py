from __future__ import annotations

from typing import Any, Dict, Literal, Tuple, Union, Optional

from api_calls.helpers.general import get_current_provider
from api_calls.helpers.providers.general import get_url
from api_calls.auth.auth import get_access_params

ReturnMode = Literal["parsed", "full"]


def get_team(
    team_id: int,
    return_mode: ReturnMode = "parsed",
    provider: Optional[str] = None,
) -> Union[Dict[str, Any], Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    Fetch a single team by ID.

    return_mode:
      - "parsed" (default): return parsed dict
      - "full": return (raw_json, parsed dict)
    """
    if provider is None:
        provider = get_current_provider(default="sportmonks")
    provider = str(provider).strip().lower()

    params = get_access_params(provider)
    url = get_url(provider, "teams_by_id").format(team_id=team_id)

    if provider == "sportmonks":
        from api_calls.helpers.providers.sportmonks import sm_team
        raw, parsed = sm_team(url=url, params=params)
    else:
        raise ValueError(f"Unsupported provider: {provider}")

    if return_mode == "full":
        return raw, parsed
    if return_mode == "parsed":
        return parsed

    raise ValueError("return_mode must be either 'parsed' or 'full'")