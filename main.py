from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from app.query_engine import answer_query

app = FastAPI(title="StatPulse API")

origins = [
    "https://mklbasist.github.io",
    "http://localhost:8000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {"info": "StatPulse API is running"}

@app.get("/query")
def query_get(q: str = Query(...)):
    return answer_query(q)

@app.post("/query")
async def query_post(request: Request):
    body = await request.json()
    q = body.get("q")
    return answer_query(q)

@app.get("/healthz")
def health():
    return {"status": "ok"}
