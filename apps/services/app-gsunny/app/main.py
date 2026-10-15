from fastapi import FastAPI

from app.api.router import api_router

app = FastAPI(
    title="App Gssuny API",
    version="0.1.0",
)

app.include_router(api_router, prefix="/api/v1")

@app.get("/health", tags=["System"])
async def health():
    return {"status": "ok"}


