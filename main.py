# main.py
from fastapi import FastAPI, Query
from app.query_engine import answer_query

app = FastAPI(title="StatPulse API")

@app.get("/query")
def query(q: str = Query(..., description="Player cricket query")):
    return answer_query(q)

@app.get("/healthz")
def health():
    return {"status": "ok"}
