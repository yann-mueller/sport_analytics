#%%##################
### FIXTURE BY ID ###
#####################

# Jupyter Setup
import os, sys
from pathlib import Path

p = Path.cwd().resolve()
while not (p / "config.yaml").exists():
    if p.parent == p:
        raise FileNotFoundError("Could not find config.yaml in any parent directory.")
    p = p.parent

os.chdir(p)
project_root = p.parent  # parent of api_calls
sys.path.insert(0, str(project_root))

print("CWD:", Path.cwd())
print("Added to sys.path:", project_root)
# Setup Complete
from api_calls.fixtures import get_fixture
import json

raw, response = get_fixture(220294, "full")              
print(json.dumps(response, indent=4))

#%%##################
### LINEUPS BY ID ###
#####################

# Jupyter Setup
import os, sys
from pathlib import Path

p = Path.cwd().resolve()
while not (p / "config.yaml").exists():
    if p.parent == p:
        raise FileNotFoundError("Could not find config.yaml in any parent directory.")
    p = p.parent

os.chdir(p)
project_root = p.parent  # parent of api_calls
sys.path.insert(0, str(project_root))

print("CWD:", Path.cwd())
print("Added to sys.path:", project_root)
# Setup Complete

from api_calls.lineups import get_lineup
import json

response = get_lineup(220294)              
print(json.dumps(response, indent=4))

#%%##################
### PLAYER BY ID  ###
#####################

# Jupyter Setup
import os, sys
from pathlib import Path

p = Path.cwd().resolve()
while not (p / "config.yaml").exists():
    if p.parent == p:
        raise FileNotFoundError("Could not find config.yaml in any parent directory.")
    p = p.parent

os.chdir(p)
project_root = p.parent  # parent of api_calls
sys.path.insert(0, str(project_root))

print("CWD:", Path.cwd())
print("Added to sys.path:", project_root)
# Setup Complete

from api_calls.players import get_player
import json

response = get_player(997)
print(json.dumps(response, indent=4))

#%%################################
### Fetch Schedule for a Season ###
###################################

# Jupyter Setup
import os, sys
from pathlib import Path

p = Path.cwd().resolve()
while not (p / "config.yaml").exists():
    if p.parent == p:
        raise FileNotFoundError("Could not find config.yaml in any parent directory.")
    p = p.parent

os.chdir(p)
project_root = p.parent  # parent of api_calls
sys.path.insert(0, str(project_root))

print("CWD:", Path.cwd())
print("Added to sys.path:", project_root)
# Setup Complete

from api_calls.schedules import get_schedule
import pandas as pd

season_id = 25646
fixtures = get_schedule(season_id)
df = pd.DataFrame(fixtures)
print(df)

#%%###############
### ODDS BY ID ###
##################

# Jupyter Setup
import os, sys
from pathlib import Path

p = Path.cwd().resolve()
while not (p / "config.yaml").exists():
    if p.parent == p:
        raise FileNotFoundError("Could not find config.yaml in any parent directory.")
    p = p.parent

os.chdir(p)
project_root = p.parent  # parent of api_calls
sys.path.insert(0, str(project_root))

print("CWD:", Path.cwd())
print("Added to sys.path:", project_root)
# Setup Complete


from odds import get_odds
import json

parsed = get_odds(19433585, "1x2")
print(json.dumps(parsed, indent=2))

#%%#########################################
### PREMIUM ODD HISTORY (FULL) TEST CASE ###
############################################

# Jupyter Setup
import os, sys
from pathlib import Path

p = Path.cwd().resolve()
while not (p / "config.yaml").exists():
    if p.parent == p:
        raise FileNotFoundError("Could not find config.yaml in any parent directory.")
    p = p.parent

os.chdir(p)
project_root = p.parent  # parent of api_calls
sys.path.insert(0, str(project_root))

print("CWD:", Path.cwd())
print("Added to sys.path:", project_root)
# Setup Complete


from odds import get_premium_odd_history
import json
import pandas as pd

# Inputs
fixture_id = 19433605
market_name = "1x2"
bookmaker_id = 16          # <- choose one you saw in the snapshot (e.g., 32, 38, 14, ...)
outcome_label = "Home"     # <- "Home" / "Draw" / "Away"

# Full history (default)
history = get_premium_odd_history(
    fixture_id=fixture_id,
    market_name=market_name,
    bookmaker_id=bookmaker_id,
    outcome_label=outcome_label,
)

# Print as JSON (first few records)
print(json.dumps(history[:10], indent=2))

# Optional: also view as table
df = pd.DataFrame(history)
print(df.head(20))


# %%
