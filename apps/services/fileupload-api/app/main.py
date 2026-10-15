from fastapi import FastAPI

API_PREFIX = "/fileupload/api"

app = FastAPI(root_path="/fileupload/api")

from app.routers.fileupload import router as fileupload_router
from app.routers.fileupload_admin import router as fileupload_admin_router

# Base path único (sem duplicar)
app.include_router(fileupload_router)
app.include_router(fileupload_admin_router)

@app.get("/health", tags=["System"])
async def health():
    return {"status": "ok"}


