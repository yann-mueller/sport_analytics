from __future__ import annotations

from sqlalchemy.engine import Engine
from database.auth.auth import get_engine as _get_engine


def get_engine(*, echo: bool = False) -> Engine:
    """
    Public entry point for DB engine creation.

    Credentials are resolved inside database.auth.auth (env var or db_config.yaml).
    This file is safe to commit (no secrets).
    """
    return _get_engine(echo=echo)