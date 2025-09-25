from fastapi import FastAPI, Query
from app.query_engine import answer_query

app = FastAPI(title="CricPulse")

@app.get("/")
def root():
    return {"message": "CricPulse API is running!"}

@app.get("/query")
def get_query(q: str = Query(..., description="Your cricket query")):
    result = answer_query(q)
    return result
