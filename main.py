# scrapper/main.py

from fastapi import FastAPI
from app.api.scrape_endpoint import router as scrape_router
from app.core.config import API_TITLE, API_DESCRIPTION, API_VERSION
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(message)s"
)

app = FastAPI(
    title=API_TITLE,
    description=API_DESCRIPTION,
    version=API_VERSION
)

app.include_router(scrape_router, prefix="/api/v1") # Added a prefix for versioning

@app.get("/")
async def read_root():
    return {"message": f"Welcome to the {API_TITLE} - v{API_VERSION}"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)