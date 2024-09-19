"""space2stats lambda handler."""

import asyncio
import os

from mangum import Mangum

from ..settings import Settings
from .app import build_app
from .db import connect_to_db

settings = Settings()
app = build_app(settings)


@app.on_event("startup")
async def startup_event() -> None:
    """Connect to database on startup."""
    await connect_to_db(app, max_size=1)


handler = Mangum(app, lifespan="off")

if "AWS_EXECUTION_ENV" in os.environ:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.router.startup())
