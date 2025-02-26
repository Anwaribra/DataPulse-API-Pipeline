from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi_app.routes import router

app = FastAPI(
    title="DataPulse API",
    description="API for cryptocurrency price data",
    version="0.1.0"
)

@app.get("/", tags=["Root"])
async def root():
    """Redirect to API documentation"""
    return RedirectResponse(url="/docs")

# Mount the router with prefix
app.include_router(router, prefix="/api/v1")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
