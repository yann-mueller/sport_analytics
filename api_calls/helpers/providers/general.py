from pathlib import Path
from typing import Union, Any
import yaml


class ProviderURLNotFoundError(KeyError):
    pass


class EndpointNotFoundError(KeyError):
    pass


def _load_providers_cfg(yaml_path: Path) -> dict:
    # supports single-doc YAML; if you ever use multi-doc, swap to safe_load_all
    with yaml_path.open("r", encoding="utf-8") as f:
        cfg: Any = yaml.safe_load(f)

    if not isinstance(cfg, dict) or not isinstance(cfg.get("providers"), list):
        raise ValueError(f"Invalid config format in {yaml_path}. Expected top-level 'providers' list.")

    return cfg


def _default_providers_cfg_path() -> Path:
    # api_calls/helpers/providers/providers_config.yaml
    return Path(__file__).resolve().parent / "providers_config.yaml"


def get_url(
    provider: str,
    endpoint: str,
    yaml_path: Union[str, Path, None] = None,
) -> str:
    provider = provider.strip().lower()
    endpoint = endpoint.strip()

    if not provider:
        raise ValueError("provider must be a non-empty string")
    if not endpoint:
        raise ValueError("endpoint must be a non-empty string")

    if yaml_path is None:
        yaml_path = _default_providers_cfg_path()
    else:
        yaml_path = Path(yaml_path)

    cfg = _load_providers_cfg(yaml_path)

    entry = next(
        (
            p for p in cfg["providers"]
            if isinstance(p, dict) and str(p.get("name", "")).strip().lower() == provider
        ),
        None
    )
    if entry is None:
        raise ProviderURLNotFoundError(f"Provider '{provider}' not found in {yaml_path}")

    base_url = entry.get("base_url")
    endpoints = entry.get("endpoints")

    if not isinstance(base_url, str) or not base_url.strip():
        raise ValueError(f"Provider '{provider}' missing valid 'base_url' in {yaml_path}")
    if not isinstance(endpoints, dict):
        raise ValueError(f"Provider '{provider}' missing 'endpoints' mapping in {yaml_path}")

    path = endpoints.get(endpoint)
    if not isinstance(path, str) or not path.strip():
        raise EndpointNotFoundError(f"Endpoint '{endpoint}' not defined for provider '{provider}' in {yaml_path}")

    return base_url.rstrip("/") + "/" + path.lstrip("/")


class MarketNotFoundError(KeyError):
    pass


def get_market(
    provider: str,
    market_name: str,
    yaml_path: Union[str, Path, None] = None,
) -> dict:
    provider = provider.strip().lower()
    market_name = market_name.strip().lower()

    if yaml_path is None:
        yaml_path = _default_providers_cfg_path()
    else:
        yaml_path = Path(yaml_path)

    cfg = _load_providers_cfg(yaml_path)

    entry = next(
        (
            p for p in cfg["providers"]
            if isinstance(p, dict) and str(p.get("name", "")).strip().lower() == provider
        ),
        None
    )
    if entry is None:
        raise ProviderURLNotFoundError(f"Provider '{provider}' not found in {yaml_path}")

    mapping = entry.get("odds_market_mapping")
    if not isinstance(mapping, dict) or market_name not in mapping:
        raise MarketNotFoundError(
            f"Market '{market_name}' not configured for provider '{provider}' in {yaml_path}"
        )

    rule = mapping[market_name]
    if not isinstance(rule, dict) or "field" not in rule or "equals" not in rule:
        raise ValueError(
            f"Invalid odds_market_mapping for '{market_name}' (provider '{provider}')"
        )

    return rule

def get_nested(d: dict, dotted: str) -> Any:
    """
    Access nested dict values using a dotted path, e.g. "market.name".
    Returns None if any part is missing.
    """
    cur: Any = d
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur