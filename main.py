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
        "info": "StatPulse API is running. Use /query?q=<your query> to get cricket stats.",
    }

# Query endpoint
@app.get("/query")
def query(q: str):
    result = answer_query(q)
    return result
