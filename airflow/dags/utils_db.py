import os
from sqlalchemy import create_engine

def get_engine():
    user = os.getenv("WAREHOUSE_USER", "analytics")
    pwd  = os.getenv("WAREHOUSE_PASSWORD", "analytics")
    host = os.getenv("WAREHOUSE_HOST", "warehouse")
    port = os.getenv("WAREHOUSE_PORT", "5432")
    db   = os.getenv("WAREHOUSE_DB", "healthcare")
    return create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}")
