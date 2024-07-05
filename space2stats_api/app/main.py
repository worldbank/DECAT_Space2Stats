from fastapi import FastAPI
from .routers import api

app = FastAPI()

app.include_router(api.router)

@app.get("/")
def read_root():
    return {"message": "Welcome to Space2Stats!"}