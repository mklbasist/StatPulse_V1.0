import pandas as pd
import sqlite3

# Load CSV
df = pd.read_csv("data/test_bbb 2.csv", low_memory=False)

# Connect/create SQLite DB
conn = sqlite3.connect("data/cricpulse.db")

# Save dataframe to table 'matches'
df.to_sql("matches", conn, if_exists="replace", index=False)

conn.close()
print("Saved CSV to SQLite DB: cricpulse.db")
