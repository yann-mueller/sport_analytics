import os, sys
from pathlib import Path
from odds import get_premium_odd_history
import json
import pandas as pd

history = get_premium_odd_history(
    fixture_id=19433605,
    market_name="1x2",
    bookmaker_id=16,
    outcome_label="Home",
    from_utc="2025-12-18 12:40",
    to_utc="2025-12-18 12:55",
)

print(history)