from pathlib import Path
from typing import Union, Any
import yaml


def get_current_provider(
    yaml_path: Union[str, Path] = "config.yaml",
    default: str = "sportmonks",
) -> str:
    """
    Return the current API provider name from config.yaml.

    Expected YAML format:
      api:
        provider:
          name: "sportmonks"

    If config.yaml or api.provider.name is missing,
    returns `default`.
    """
    yaml_path = Path(yaml_path)

    # If config.yaml does not exist → return default
    if not yaml_path.exists():
        return default.strip().lower()

    with yaml_path.open("r", encoding="utf-8") as f:
        cfg: Any = yaml.safe_load(f)

    try:
        provider = cfg["api"]["provider"]["name"]
    except (TypeError, KeyError):
        return default.strip().lower()

    if not isinstance(provider, str) or not provider.strip():
        raise ValueError("Provider name must be a non-empty string")

    return provider.strip().lower()