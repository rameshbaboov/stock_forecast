# main.py

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.api import register_api_routes

app = FastAPI(title="Stock Forecast App")


# Mount /static if you later add CSS/JS files under app/static
app.mount("/static", StaticFiles(directory="app/static"), name="static")


# Register all routers defined in app/api/__init__.py
register_api_routes(app)


@app.get("/")
def root():
    """
    Simple home: redirect to latest forecasts page.
    """
    return RedirectResponse(url="/forecast/latest")


# Run with: uvicorn main:app --reload
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
