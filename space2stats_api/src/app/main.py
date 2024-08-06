from fastapi import FastAPI
from mangum import Mangum

from .routers import api


app = FastAPI()

app.include_router(api.router)


@app.get("/")
def read_root():
    return {"message": "Welcome to Space2Stats!"}

handler = Mangum(app)