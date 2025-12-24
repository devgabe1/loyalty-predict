#%%

import pandas as pd
import sqlalchemy

#%%
def import_query(path):
    with open(path) as open_file:
        query = open_file.read()
    return query

query = import_query("life_cycle.sql")

engine_app = sqlalchemy.create_engine("sqlite:///../../data/loyalty-system/database.db")

engine_analytical = sqlalchemy.create_engine("sqlite:///../../data/analytics/database.db")



#%%
dates  = [
    "2025-05-01",
    "2025-06-01",
    "2025-07-01"
]

for i in dates:
    query_format = query.fomrat(date=i)
    df = pd.read_sql(query_format, engine_app)
    df.to_sql("life_cycle", engine_analytical, index=False, if_exists="append")
