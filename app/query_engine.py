import sqlite3
import re
import os

# Path to DB
db_path = os.path.join("data", "cricpulse.db")

# -----------------------------
# Helpers
# -----------------------------
def _find_player(query_lower: str, PLAYERS):
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

def build_where(player_col, player, conditions):
    """Always start WHERE with player = ?, then add filters"""
    where = [f"{player_col} = ?"]
    where.extend(conditions)
    return "WHERE " + " AND ".join(where), [player]

# -----------------------------
# Main Query Function
# -----------------------------
def answer_query(query: str):
    query_lower = query.lower().strip()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Fetch all unique players and bowlers
    players = conn.execute("SELECT DISTINCT bat FROM matches").fetchall()
    bowlers = conn.execute("SELECT DISTINCT bowl FROM matches").fetchall()
    PLAYERS = sorted(
        [p["bat"] for p in players if p["bat"]] +
        [b["bowl"] for b in bowlers if b["bowl"]],
        key=lambda x: -len(str(x))
    )

    # Detect player
    player = _find_player(query_lower, PLAYERS)
    if not player:
        conn.close()
        return {"error": "Player not found in dataset"}

    # Filters
    country = _extract_country(query_lower)
    opposition = _extract_against(query_lower)

    # Extra filter conditions
    conditions = []
    params = []
    if country:
        conditions.append("country LIKE ?")
        params.append(f"%{country}%")
    if opposition:
        conditions.append("(team_bowl LIKE ? OR team_bat LIKE ?)")
        params.extend([f"%{opposition}%", f"%{opposition}%"])

    # -----------------------------
    # Query types
    # -----------------------------

    # Runs
    if "run" in query_lower and "average" not in query_lower:
        where_clause, base_params = build_where("bat", player, conditions)
        sql = f"SELECT SUM(batruns) as total_runs FROM matches {where_clause}"
        row = conn.execute(sql, base_params + params).fetchone()
        runs = row["total_runs"] if row and row["total_runs"] else 0
        conn.close()
        return {"player": player, "runs": int(runs), "country": country, "against": opposition}

    # Batting average
    if "average" in query_lower or "ave" in query_lower:
        where_clause, base_params = build_where("bat", player, conditions)

        sql_runs = f"SELECT SUM(batruns) as total_runs FROM matches {where_clause}"
        sql_outs = f"""
            SELECT COUNT(*) as outs
            FROM matches {where_clause}
            AND bat_out IS NOT NULL
            AND bat_out NOT LIKE '%not out%'
        """

        total_runs = conn.execute(sql_runs, base_params + params).fetchone()["total_runs"] or 0
        outs = conn.execute(sql_outs, base_params + params).fetchone()["outs"] or 0
        average = total_runs / outs if outs > 0 else total_runs
        conn.close()
        return {"player": player, "batting_average": round(average, 2), "country": country, "against": opposition}

    # 50s
    if "50" in query_lower or "fifty" in query_lower:
        where_clause, base_params = build_where("bat", player, conditions)
        sql = f"""
            SELECT p_match, inns, SUM(batruns) as runs_sum
            FROM matches {where_clause}
            GROUP BY p_match, inns
            HAVING runs_sum >= 50 AND runs_sum < 100
        """
        rows = conn.execute(sql, base_params + params).fetchall()
        conn.close()
        return {"player": player, "50s": len(rows), "country": country, "against": opposition}

    # 100s
    if "100" in query_lower or "century" in query_lower:
        where_clause, base_params = build_where("bat", player, conditions)
        sql = f"""
            SELECT p_match, inns, SUM(batruns) as runs_sum
            FROM matches {where_clause}
            GROUP BY p_match, inns
            HAVING runs_sum >= 100
        """
        rows = conn.execute(sql, base_params + params).fetchall()
        conn.close()
        return {"player": player, "100s": len(rows), "country": country, "against": opposition}

    # Highest score
    if "highest" in query_lower or "top score" in query_lower or "best score" in query_lower:
        where_clause, base_params = build_where("bat", player, conditions)
        sql = f"""
            SELECT SUM(batruns) as runs_sum
            FROM matches {where_clause}
            GROUP BY p_match, inns
            ORDER BY runs_sum DESC
            LIMIT 1
        """
        row = conn.execute(sql, base_params + params).fetchone()
        highest = row["runs_sum"] if row else None
        conn.close()
        return {"player": player, "highest_score": int(highest) if highest else None, "country": country, "against": opposition}

    # Balls faced
    if "ball" in query_lower or "faced" in query_lower:
        where_clause, base_params = build_where("bat", player, conditions)
        sql = f"SELECT SUM(ballfaced) as balls FROM matches {where_clause}"
        row = conn.execute(sql, base_params + params).fetchone()
        balls = row["balls"] or 0
        conn.close()
        return {"player": player, "balls_faced": int(balls), "country": country, "against": opposition}

    # Wickets
    if "wicket" in query_lower:
        where_clause, base_params = build_where("bowl", player, conditions)
        sql = f"SELECT COUNT(*) as wkts FROM matches {where_clause} AND p_out IS NOT NULL"
        wkts = conn.execute(sql, base_params + params).fetchone()["wkts"] or 0
        conn.close()
        return {"player": player, "wickets": int(wkts), "country": country, "against": opposition}

    # Economy
    if "economy" in query_lower:
        where_clause, base_params = build_where("bowl", player, conditions)
        sql_runs = f"""
            SELECT SUM(bowlruns) as runs_conceded, COUNT(*) as balls_bowled
            FROM matches {where_clause}
        """
        row = conn.execute(sql_runs, base_params + params).fetchone()
        runs_conceded = row["runs_conceded"] or 0
        balls_bowled = row["balls_bowled"] or 0
        overs = balls_bowled / 6 if balls_bowled > 0 else 0
        economy = runs_conceded / overs if overs > 0 else 0
        conn.close()
        return {"player": player, "economy": round(economy, 2), "country": country, "against": opposition}

    # Fallback
    conn.close()
    return {"info": f"Query understood but not handled yet for {player}"}
