# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.query_engine import answer_query

app = FastAPI(title="StatPulse API")

# Allow your frontend to talk to this backend
origins = [
    "https://mklbasist.github.io",  # Your GitHub Pages URL
    "http://localhost:8000",        # Local testing
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Homepage route
@app.get("/")
def home():
    return {
        "info": "StatPulse API is running. Use POST /query with {q: 'your query'} to get cricket stats.",
        "example": {"q": "virat kohli 50s in england"}
    }

# Query endpoint (POST)
@app.post("/query")
def query(body: dict):
    q = body.get("q", "")
    return answer_query(q)

# Health check
@app.get("/healthz")
def health():
    return {"status": "ok"}
