# query_engine.py
import pandas as pd
import re
import sqlite3
import os
import requests

# Paths
os.makedirs("data", exist_ok=True)
csv_path = "data/test_bbb_2.csv"
db_path = "data/cricpulse.db"

# Google Drive direct download link
CSV_URL = "https://drive.google.com/uc?export=download&id=12ZqcsuB91YzC2bD8fDjSQuzbyqKBjHBN"

# Download CSV if not present
if not os.path.exists(csv_path):
    print("CSV not found, downloading from Google Drive...")
    r = requests.get(CSV_URL)
    with open(csv_path, "wb") as f:
        f.write(r.content)
    print("CSV downloaded successfully!")

# Generate DB if not present
if not os.path.exists(db_path):
    print("Generating SQLite DB from CSV...")
    df_csv = pd.read_csv(csv_path, low_memory=False)
    conn = sqlite3.connect(db_path)
    df_csv.to_sql("matches", conn, if_exists="replace", index=False)
    conn.close()
    print("DB generated successfully!")

# Load SQLite DB
conn = sqlite3.connect(db_path)
df = pd.read_sql("SELECT * FROM matches", conn)
conn.close()

# Player list
PLAYERS = sorted(
    set(df["bat"].dropna().unique()).union(set(df["bowl"].dropna().unique())),
    key=lambda x: -len(str(x))
)

# Helpers
def _find_player(query_lower: str):
    for p in PLAYERS:
        if p and p.lower() in query_lower:
            return p
    return None

def _extract_country(query_lower: str):
    m = re.search(r'\bin\s+([a-z& ]+?)(?:\s|$)', query_lower)
    return m.group(1).strip() if m else None

def _extract_against(query_lower: str):
    m = re.search(r'\bagainst\s+([a-z ]+?)(?:\s|$)', query_lower)
    return m.group(1).strip() if m else None

def _filter_by_country(df_in: pd.DataFrame, country: str):
    if not country or "country" not in df_in.columns:
        return df_in
    # substring match to catch "England & Wales" etc.
    return df_in[df_in["country"].fillna("").str.lower().str.contains(country.lower(), na=False)]

def _filter_by_opposition(df_in: pd.DataFrame, opp: str):
    if not opp:
        return df_in
    masks = []
    if "team_bowl" in df_in.columns:
        masks.append(df_in["team_bowl"].fillna("").str.lower().str.contains(opp.lower(), na=False))
    if "team_bat" in df_in.columns:
        masks.append(df_in["team_bat"].fillna("").str.lower().str.contains(opp.lower(), na=False))
    if not masks:
        return df_in
    mask = masks[0]
    for m in masks[1:]:
        mask = mask | m
    return df_in[mask]

def _is_dismissal_value(val):
    if pd.isna(val):
        return False
    s = str(val).strip().lower()
    if s == "" or re.search(r'not\s*out|notout|dnb|absent|sub|retired', s):
        return False
    return True

def _count_dismissed_innings(df_bat: pd.DataFrame, dismissal_col: str):
    if dismissal_col and dismissal_col in df_bat.columns:
        dismissed_rows = df_bat[df_bat[dismissal_col].apply(_is_dismissal_value, convert_dtype=False)]
        if "p_match" in df_bat.columns and "inns" in df_bat.columns:
            return dismissed_rows.groupby(["p_match", "inns"]).ngroups
        return dismissed_rows.shape[0]
    # fallback: count unique innings
    if "p_match" in df_bat.columns and "inns" in df_bat.columns:
        return df_bat.groupby(["p_match", "inns"]).ngroups
    return df_bat.shape[0]

# Main query function
def answer_query(query: str):
    query_lower = query.lower().strip()

    if query_lower == "columns" or "show columns" in query_lower:
        return {"columns": df.columns.tolist()}

    # Detect player
    player = _find_player(query_lower)
    if not player:
        return {"error": "Player not found in dataset"}

    df_player = df[(df["bat"] == player) | (df["bowl"] == player)]

    country = _extract_country(query_lower)
    if country:
        df_player = _filter_by_country(df_player, country)

    opposition = _extract_against(query_lower)
    if opposition:
        df_player = _filter_by_opposition(df_player, opposition)

    df_bat = df_player[df_player["bat"] == player]
    dismissal_col = "dismissal" if "dismissal" in df_bat.columns else None
    outs_counted = _count_dismissed_innings(df_bat, dismissal_col)
    runs_total = int(df_bat["batruns"].sum()) if "batruns" in df_bat.columns else 0

    # Innings totals
    if {"p_match", "inns", "batruns"}.issubset(df_bat.columns):
        innings_series = df_bat.groupby(["p_match", "inns"])["batruns"].sum()
    else:
        innings_series = pd.Series(dtype="int64")

    # --- Batting Queries ---
    if "average" in query_lower or "ave" in query_lower:
        average = runs_total / outs_counted if outs_counted > 0 else None
        return {
            "player": player,
            "runs": runs_total,
            "innings_count": int(innings_series.count()) if not innings_series.empty else 0,
            "outs_counted": int(outs_counted),
            "batting_average": round(average, 2) if average is not None else None,
            "country": country,
            "against": opposition
        }

    if ("run" in query_lower or "runs" in query_lower) and "average" not in query_lower:
        return {"player": player, "runs": runs_total, "country": country, "against": opposition}

    if "50" in query_lower or "fifty" in query_lower:
        fifties = innings_series[(innings_series >= 50) & (innings_series < 100)].count() if not innings_series.empty else 0
        return {"player": player, "50s": int(fifties), "country": country, "against": opposition}

    if "100" in query_lower or "century" in query_lower:
        hundreds = innings_series[innings_series >= 100].count() if not innings_series.empty else 0
        return {"player": player, "100s": int(hundreds), "country": country, "against": opposition}

    if "highest" in query_lower or "top score" in query_lower or "best score" in query_lower:
        highest = int(innings_series.max()) if not innings_series.empty else None
        return {"player": player, "highest_score": highest, "country": country, "against": opposition}

    if "ball" in query_lower or "faced" in query_lower:
        balls = int(df_bat["ballfaced"].sum()) if "ballfaced" in df_bat.columns else 0
        return {"player": player, "balls_faced": balls, "country": country, "against": opposition}

    # --- Bowling Queries ---
    df_bowl = df_player[df_player["bowl"] == player]
    if "wicket" in query_lower:
        b_dismiss_col = "p_out" if "p_out" in df_bowl.columns else None
        wkts = _count_dismissed_innings(df_bowl, b_dismiss_col)
        return {"player": player, "wickets": int(wkts), "country": country, "against": opposition}

    if "economy" in query_lower:
        runs_conceded = int(df_bowl["bowlruns"].sum()) if "bowlruns" in df_bowl.columns else 0
        balls_bowled = df_bowl.shape[0]
        economy = (runs_conceded / (balls_bowled / 6)) if balls_bowled > 0 else None
        return {"player": player, "economy": round(economy, 2) if economy is not None else None, "country": country, "against": opposition}

    # Fallback
    return {"info": f"Query understood but not handled yet for {player}", "player": player}
