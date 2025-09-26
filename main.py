from fastapi import FastAPI, Query
from app.query_engine import answer_query

app = FastAPI()

@app.get("/query")
def query(q: str = Query(..., description="Cricket query")):
    return answer_query(q)

@app.get("/healthz")
def health_check():
    return {"status": "ok"}
