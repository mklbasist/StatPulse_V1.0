from fastapi import FastAPI, Query
from app.query_engine import answer_query

app = FastAPI()   # <- this is the ASGI app Uvicorn needs

@app.get("/")
def root():
    return {"message": "Welcome to CricPulse API"}

@app.get("/query")
def query(q: str = Query(..., description="Your cricket query")):
    return answer_query(q)
