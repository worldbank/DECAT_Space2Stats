"""space2stats lambda handler."""

import asyncio
import json
import os
from http import HTTPStatus

from fastapi import HTTPException
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


mangum_handler = Mangum(app, lifespan="off")


def handler(event, context):
    """
    AWS Lambda entry point with lean reactive error handling:
      - Catches FastAPI HTTPException (e.g. 413, 503)
      - Catches AWS RuntimeErrors for too-large responses
      - Everything else bubbles up as a 500
    """
    try:
        return mangum_handler(event, context)

    except HTTPException as exc:
        status = exc.status_code
        phrase = HTTPStatus(status).phrase

        # Base error payload
        error_payload = {
            "error": phrase,
            "detail": exc.detail,
        }

        # Per-status hints
        if status == 413:
            error_payload["hint"] = (
                "Try again with a smaller request or making multiple requests "
                "with smaller payloads. The factors to consider are the number of "
                "hexIds (ie. AOI), the number of fields requested, and the date range (if timeseries is requested)."
            )
        elif status == 503:
            error_payload["hint"] = (
                "The service is currently unavailable, possibly due to Lambda function timeouts "
                "or high system load. Please try again later or reduce request complexity."
            )

        return {
            "statusCode": status,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(error_payload),
        }

    except RuntimeError as e:
        # AWS Lambda may raise a RuntimeError if the *response* payload is too large
        msg = str(e)
        if any(
            kw in msg
            for kw in ["Http response code: 413", "Response payload size is too large"]
        ):
            error_payload = {
                "error": HTTPStatus(413).phrase,
                "detail": "The response payload exceeds AWS Lambda limits.",
                "hint": (
                    "Try again with a smaller request or making multiple requests "
                    "with smaller payloads. The factors to consider are the number of "
                    "hexIds (ie. AOI), the number of fields requested, and the date range (if timeseries is requested)."
                ),
            }
            return {
                "statusCode": 413,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(error_payload),
            }
        # Unhandled runtime errors → default 500
        raise


if "AWS_EXECUTION_ENV" in os.environ:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.router.startup())
