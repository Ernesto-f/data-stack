from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import os

from database import get_db, Database
from routes import exceptions, reasons, micro_events

APP_USERNAME = os.getenv("APP_USERNAME", "admin")
APP_PASSWORD = os.getenv("APP_PASSWORD", "admin")

@asynccontextmanager
async def lifespan(app: FastAPI):
    db = Database()
    db.connect()
    app.state.db = db
    yield
    db.disconnect()

app = FastAPI(
    title="Exceptions API",
    description="API para registro de excepções de paradas — Grafana Plugin",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def verify_credentials(
    x_username: str = Header(...),
    x_password: str = Header(...),
):
    if x_username != APP_USERNAME or x_password != APP_PASSWORD:
        raise HTTPException(status_code=401, detail="Usuário ou senha inválidos")
    return x_username

app.include_router(reasons.router,      prefix="/exception-reasons", tags=["reasons"])
app.include_router(micro_events.router, prefix="/micro-events",      tags=["micro-events"])
app.include_router(
    exceptions.router,
    prefix="/exceptions",
    tags=["exceptions"],
    dependencies=[Depends(verify_credentials)],
)

@app.get("/health")
def health():
    return {"status": "ok"}

# Servir ficheiros estáticos (página HTML)
if os.path.exists("static"):
    app.mount("/", StaticFiles(directory="static", html=True), name="static")
