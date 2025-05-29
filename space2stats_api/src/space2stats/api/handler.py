"""space2stats lambda handler."""

import asyncio
import json
import os
import sys
from http import HTTPStatus

from fastapi import HTTPException
from mangum import Mangum

from .app import build_app
from .db import connect_to_db
from .settings import Settings

settings = Settings(DB_MAX_CONN_SIZE=1)  # disable connection pooling
app = build_app(settings)

# AWS Lambda response payload limit (6MB)
LAMBDA_RESPONSE_LIMIT = 6 * 1024 * 1024  # 6MB in bytes

# Error hint messages
ERROR_HINTS = {
    413: (
        "Try again with a smaller request or making multiple requests "
        "with smaller payloads. The factors to consider are the number of "
        "hexIds (ie. AOI), the number of fields requested, and the date range (if timeseries is requested)."
    ),
    503: (
        "The service is currently unavailable, possibly due to Lambda function timeouts "
        "or high system load. Please try again later or reduce request complexity."
    ),
}


@app.on_event("startup")
async def startup_event() -> None:
    """Connect to database on startup."""
    await connect_to_db(app, settings=settings)


mangum_handler = Mangum(app, lifespan="off")


def _create_error_response(status_code: int, detail: str, **extra_fields) -> dict:
    """Create a standardized error response."""
    error_payload = {
        "error": HTTPStatus(status_code).phrase,
        "detail": detail,
    }

    # Add hint if available
    if status_code in ERROR_HINTS:
        error_payload["hint"] = ERROR_HINTS[status_code]

    # Add any extra fields
    error_payload.update(extra_fields)

    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(error_payload),
    }


def _get_response_size(response: dict) -> int:
    """Get the size of the response body in bytes."""
    if not response or "body" not in response:
        return 0

    body = response["body"]
    if isinstance(body, str):
        return len(body.encode("utf-8"))
    return len(body) if body else 0


def handler(event, context):
    """
    AWS Lambda entry point with proactive response size checking:
      - Checks response size before returning to prevent 413 errors
      - Catches FastAPI HTTPException (e.g. 413, 503)
      - Everything else bubbles up as a 500
    """
    try:
        response = mangum_handler(event, context)

        # Check response size before returning
        body_size = _get_response_size(response)
        if body_size > LAMBDA_RESPONSE_LIMIT:
            return _create_error_response(
                413,
                "The response payload exceeds AWS Lambda limits.",
                response_size_mb=round(body_size / (1024 * 1024), 2),
                limit_mb=round(LAMBDA_RESPONSE_LIMIT / (1024 * 1024), 2),
            )

        return response

    except HTTPException as exc:
        return _create_error_response(exc.status_code, exc.detail)

    except Exception as e:
        # Log the error for debugging
        print(f"Unexpected error in lambda handler: {e}", file=sys.stderr)
        return _create_error_response(
            500, "An unexpected error occurred while processing the request."
        )


if "AWS_EXECUTION_ENV" in os.environ:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.router.startup())
