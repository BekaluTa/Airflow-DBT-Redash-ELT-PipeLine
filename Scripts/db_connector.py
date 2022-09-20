import pandas as pd
from sqlalchemy import text

from sqlalchemy import create_engine
engine = create_engine('postgresql+psycopg2://teddy:123456@localhost/traffic_data')

VEHICLE_SCHEMA = "vehicle_data_schema.sql"
TRAJECTORIES_SCHEMA = "trajectories_data_schema.sql"


def create_table():
    try:
        with engine.connect() as conn:
            for name in [VEHICLE_SCHEMA, TRAJECTORIES_SCHEMA]:
                with open(f'../schema/{name}') as file:
                    query = text(file.read())
                    conn.execute(query)
        print("Successfull")
    except Exception as e:
        print(e)


# create_table()

def insert_table(df: pd.DataFrame, name: str ):
    try:
        with engine.connect() as conn:
            df.to_sql(name=name, con=conn, if_exists='replace', index=False)
    except Exception as e:
        print(f"Failed  ---- {e}")  



