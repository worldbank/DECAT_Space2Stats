"""space2stats lambda handler."""

import asyncio
import json
import os
import sys
import time
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

# Add these near the top of handler.py
LAMBDA_TIMEOUT_SECONDS = 120  # Match your CDK timeout setting
TIMEOUT_BUFFER_SECONDS = 5  # Buffer to ensure we respond before timeout

# Error hint messages - updated to match client expectations
ERROR_HINTS = {
    413: (
        "Try again with a smaller request or making multiple requests "
        "with smaller payloads. The factors to consider are the number of "
        "hexIds (ie. AOI), the number of fields requested, and the date range (if timeseries is requested)."
    ),
    503: (
        "Try a smaller request by reducing the area of interest (AOI), number of fields requested, "
        "or date range (for timeseries). You can also break large requests into multiple smaller requests."
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


def get_remaining_time_ms(context):
    """Get remaining execution time in milliseconds."""
    if context and hasattr(context, "get_remaining_time_in_millis"):
        return context.get_remaining_time_in_millis()
    return None


def handler(event, context):
    """
    AWS Lambda entry point with proactive timeout detection.
    """
    start_time = time.time()

    try:
        # Check if we're approaching timeout before processing
        remaining_time_ms = get_remaining_time_ms(context)
        if remaining_time_ms and remaining_time_ms < (TIMEOUT_BUFFER_SECONDS * 1000):
            return _create_error_response(
                503,
                f"Request approaching lambda timeout - unable to process. "
                f"Remaining time: {remaining_time_ms}ms, Lambda timeout: {LAMBDA_TIMEOUT_SECONDS}s",
                suggestions=[
                    "Reduce the number of hexagon IDs in your request",
                    "Request fewer fields at a time",
                    "Use a smaller geographic area",
                    "For timeseries requests, use a shorter date range",
                    "Request is too complex for current timeout limits",
                ],
            )

        # Wrap the mangum handler call with timeout checking
        response = mangum_handler(event, context)

        if response.get("statusCode") == 503:
            elapsed_time = time.time() - start_time
            return _create_error_response(
                503,
                f"The request likely timed out due to processing complexity or high server load. "
                f"Request took {round(elapsed_time, 2)}s of the {LAMBDA_TIMEOUT_SECONDS}s timeout limit.",
                suggestions=[
                    "Reduce the number of hexagon IDs in your request",
                    "Request fewer fields at a time",
                    "Use a smaller geographic area",
                    "For timeseries requests, use a shorter date range",
                    "Try the request again in a few moments",
                ],
            )

        # Check response size before returning
        body_size = _get_response_size(response)
        if body_size > LAMBDA_RESPONSE_LIMIT:
            return _create_error_response(
                413,
                f"The response payload exceeds AWS Lambda limits. "
                f"Response size: {round(body_size / (1024 * 1024), 2)}MB, "
                f"Limit: {round(LAMBDA_RESPONSE_LIMIT / (1024 * 1024), 2)}MB",
            )

        return response

    except HTTPException as exc:
        return _create_error_response(exc.status_code, exc.detail)

    except Exception as e:
        elapsed_time = time.time() - start_time

        # Check if this might be a timeout-related exception
        if elapsed_time > (LAMBDA_TIMEOUT_SECONDS - TIMEOUT_BUFFER_SECONDS):
            return _create_error_response(
                503,
                f"Request timed out during processing. "
                f"Elapsed time: {round(elapsed_time, 2)}s, Lambda timeout: {LAMBDA_TIMEOUT_SECONDS}s, "
                f"Error: {type(e).__name__}",
                suggestions=[
                    "Reduce request complexity",
                    "Try breaking large requests into smaller chunks",
                    "Consider using pagination for large datasets",
                ],
            )

        # Log the error for debugging
        print(f"Unexpected error in lambda handler: {e}", file=sys.stderr)
        return _create_error_response(
            500, "An unexpected error occurred while processing the request."
        )


if "AWS_EXECUTION_ENV" in os.environ:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.router.startup())
