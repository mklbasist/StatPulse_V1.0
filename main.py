from fastapi import FastAPI
from app.query_engine import answer_query

app = FastAPI(title="StatPulse API")

# Homepage route
@app.get("/")
def home():
    return {
        "info": "StatPulse API is running. Use /query?q=<your query> to get cricket stats.",
        "example": "/query?q=virat kohli 50s in england"
    }

# Query endpoint
@app.get("/query")
def query(q: str):
    result = answer_query(q)
    return result
