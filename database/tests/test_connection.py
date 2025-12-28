from sqlalchemy import text
from database.auth.auth import get_engine

engine = get_engine()

with engine.begin() as conn:
    print(conn.execute(text("select current_database(), current_user")).fetchone())
