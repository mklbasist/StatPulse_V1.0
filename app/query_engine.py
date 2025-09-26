import sqlite3
import pandas as pd
import os
import re

# Path to DB in repo
db_path = os.path.join("data", "cricpulse.db")

# Load SQLite DB
conn = sqlite3.connect(db_path)
df = pd.read_sql("SELECT * FROM matches", conn)
conn.close()

# Player list
PLAYERS = sorted(
    set(df["bat"].dropna().unique()).union(set(df["bowl"].dropna().unique())),
    key=lambda x: -len(str(x))
)

# Helper functions
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

def answer_query(query: str):
    query_lower = query.lower().strip()

    player = _find_player(query_lower)
    if not player:
        return {"error": "Player not found in dataset"}

    # Filter dataset for player
    df_player = df[(df["bat"] == player) | (df["bowl"] == player)]

    country = _extract_country(query_lower)
    if country:
        df_player = df_player[df_player["country"].str.lower() == country.lower()]

    opposition = _extract_against(query_lower)
    if opposition:
        df_player = df_player[
            (df_player["team_bowl"].str.lower() == opposition.lower()) |
            (df_player["team_bat"].str.lower() == opposition.lower())
        ]

    # Batting stats
    if "run" in query_lower and "average" not in query_lower:
        runs = df_player[df_player["bat"] == player]["batruns"].sum()
        return {"player": player, "runs": int(runs), "country": country, "against": opposition}

    if "average" in query_lower or "ave" in query_lower:
        runs = df_player[df_player["bat"] == player]["batruns"].sum()
        outs = df_player[(df_player["bat"] == player) & (df_player["bat_out"].notna())].shape[0]
        average = runs / outs if outs > 0 else runs
        return {"player": player, "batting_average": round(average, 2), "country": country, "against": opposition}

    if "50" in query_lower or "fifty" in query_lower:
        innings = df_player[df_player["bat"] == player].groupby(["p_match", "inns"])["batruns"].sum()
        fifties = innings[(innings >= 50) & (innings < 100)].count()
        return {"player": player, "50s": int(fifties), "country": country, "against": opposition}

    if "100" in query_lower or "century" in query_lower:
        innings = df_player[df_player["bat"] == player].groupby(["p_match", "inns"])["batruns"].sum()
        hundreds = innings[innings >= 100].count()
        return {"player": player, "100s": int(hundreds), "country": country, "against": opposition}

    if "highest" in query_lower or "top score" in query_lower or "best score" in query_lower:
        innings = df_player[df_player["bat"] == player].groupby(["p_match", "inns"])["batruns"].sum()
        highest = innings.max() if not innings.empty else None
        return {"player": player, "highest_score": int(highest) if highest else None, "country": country, "against": opposition}

    # Bowling stats
    if "wicket" in query_lower:
        wkts = df_player[df_player["bowl"] == player]["p_out"].count()
        return {"player": player, "wickets": int(wkts), "country": country, "against": opposition}

    if "economy" in query_lower:
        bowler_df = df_player[df_player["bowl"] == player]
        runs_conceded = bowler_df["bowlruns"].sum()
        overs = bowler_df.shape[0] / 6
        economy = runs_conceded / overs if overs > 0 else 0
        return {"player": player, "economy": round(economy, 2), "country": country, "against": opposition}

    # Balls faced
    if "ball" in query_lower or "faced" in query_lower:
        balls = df_player[df_player["bat"] == player]["ballfaced"].sum()
        return {"player": player, "balls_faced": int(balls), "country": country, "against": opposition}

    return {"info": f"Query understood but not handled yet for {player}"}
