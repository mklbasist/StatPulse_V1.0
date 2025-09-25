import os
import requests
import pandas as pd
import sqlite3

# Paths
os.makedirs("data", exist_ok=True)
csv_path = "data/test_bbb_2.csv"
db_path = "data/cricpulse.db"

# Download CSV if not present
if not os.path.exists(csv_path):
    print("CSV not found, downloading...")
    url = "https://drive.google.com/uc?export=download&id=12ZqcsuB91YzC2bD8fDjSQuzbyqKBjHBN"  # Direct download link
    r = requests.get(url)
    with open(csv_path, "wb") as f:
        f.write(r.content)
    print("CSV downloaded successfully!")

# Generate DB
df = pd.read_csv(csv_path, low_memory=False)
conn = sqlite3.connect(db_path)
df.to_sql("matches", conn, if_exists="replace", index=False)
conn.close()
print("DB generated successfully!")
