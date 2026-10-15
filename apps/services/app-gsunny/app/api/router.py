from FastAPI import APIRouter

from app.modules.pedidos.router import router as pedidos_router

api_router = APIRouter()

api_router.include_router(
    pedidos_router,
    prefix="/pedidos",
    tags=["pedidos"],
    )

