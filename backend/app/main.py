from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.routers import lists, testcases, results
from app.db.database import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(
    title="Screening Validation Platform API",
    description="OFAC and global sanctions list screening validation toolkit",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(lists.router, prefix="/api/lists", tags=["lists"])
app.include_router(testcases.router, prefix="/api/testcases", tags=["testcases"])
app.include_router(results.router, prefix="/api/results", tags=["results"])


@app.get("/api/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}
