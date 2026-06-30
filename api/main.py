"""api/main.py — FastAPI application (stub, expanded in Task 3)."""
from fastapi import FastAPI

app = FastAPI(title="Medical Telegram Warehouse API", version="0.1.0")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
