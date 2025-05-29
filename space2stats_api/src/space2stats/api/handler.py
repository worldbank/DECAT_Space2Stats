"""space2stats lambda handler."""

import asyncio
import json
import os

from mangum import Mangum

from .app import build_app
from .db import connect_to_db
from .settings import Settings

settings = Settings(DB_MAX_CONN_SIZE=1)  # disable connection pooling
app = build_app(settings)


@app.on_event("startup")
async def startup_event() -> None:
    """Connect to database on startup."""
    await connect_to_db(app, settings=settings)


# Create the Mangum handler
mangum_handler = Mangum(app, lifespan="off")


# Wrap it with our error handling
def handler(event, context):
    try:
        return mangum_handler(event, context)
    except RuntimeError as e:
        if "Failed to post invocation response" in str(
            e
        ) and "Http response code: 413" in str(e):
            return {
                "statusCode": 413,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(
                    {
                        "error": "Request Entity Too Large",
                        "detail": "The response payload exceeds the Lambda limits",
                        "hint": "Try again with a smaller request or making multiple requests with smaller payloads. The factors to consider are the number of hexIds (ie. AOI), the number of fields requested, and the date range (if timeseries is requested).",
                    }
                ),
            }
        raise


if "AWS_EXECUTION_ENV" in os.environ:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.router.startup())
