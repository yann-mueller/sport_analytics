from __future__ import annotations
import time
import random
from typing import Any, Dict, Optional
import requests


class RateLimitError(RuntimeError):
    pass


def get_json_with_backoff(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 600,
    max_retries: int = 8,
    base_sleep: float = 1.0,
    max_sleep: float = 60.0,
    verbose: bool = True,
) -> Dict[str, Any]:
    """
    GET url and return JSON with retries/backoff on 429 and 5xx.
    Prints feedback when rate limits are hit.
    """
    attempt = 0

    while True:
        attempt += 1
        r = requests.get(url, params=params, headers=headers, timeout=timeout)

        if r.status_code == 200:
            return r.json()

        payload = None
        try:
            payload = r.json()
        except Exception:
            pass

        # ---- RATE LIMIT ----
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")

            if retry_after:
                sleep_s = float(retry_after)
            else:
                sleep_s = min(max_sleep, base_sleep * (2 ** (attempt - 1)))
                sleep_s *= (0.7 + 0.6 * random.random())

            if verbose:
                msg = payload.get("message") if isinstance(payload, dict) else None
                print(
                    f"[RATE LIMIT] 429 received "
                    f"(attempt {attempt}/{max_retries}). "
                    f"Sleeping {sleep_s:.1f}s."
                    + (f" Message: {msg}" if msg else "")
                )

            if attempt > max_retries:
                raise RateLimitError(
                    f"Rate limit exceeded after {max_retries} retries."
                )

            time.sleep(sleep_s)
            continue

        # ---- SERVER ERROR ----
        if 500 <= r.status_code < 600:
            sleep_s = min(max_sleep, base_sleep * (2 ** (attempt - 1)))
            sleep_s *= (0.7 + 0.6 * random.random())

            if verbose:
                print(
                    f"[SERVER ERROR] {r.status_code} "
                    f"(attempt {attempt}/{max_retries}). "
                    f"Sleeping {sleep_s:.1f}s."
                )

            if attempt > max_retries:
                r.raise_for_status()

            time.sleep(sleep_s)
            continue

        # ---- OTHER ERRORS ----
        r.raise_for_status()