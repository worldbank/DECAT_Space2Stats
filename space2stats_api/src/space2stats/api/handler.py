"""space2stats lambda handler."""

import asyncio
import json
import os
import sys
from typing import Any, Dict

from mangum import Mangum

from .app import build_app
from .db import connect_to_db
from .settings import Settings

settings = Settings(DB_MAX_CONN_SIZE=1)  # disable connection pooling
app = build_app(settings)

# AWS Lambda response limit is 6MB, but be conservative with headers/encoding overhead
LAMBDA_RESPONSE_LIMIT = 5.8 * 1024 * 1024  # 5.8MB to account for overhead


@app.on_event("startup")
async def startup_event() -> None:
    """Connect to database on startup."""
    await connect_to_db(app, settings=settings)


# Create the Mangum handler
mangum_handler = Mangum(app, lifespan="off")


def check_response_size(response: Dict[str, Any]) -> Dict[str, Any]:
    """Check if response size exceeds Lambda limits and return 413 if it does."""
    try:
        # The 'body' field contains the actual response data
        body = response.get("body", "")

        # Calculate the size of the body (this is what gets sent to the client)
        if isinstance(body, str):
            body_size = len(body.encode("utf-8"))
        else:
            # Handle binary responses
            body_size = len(body) if body else 0

        # Also account for headers and other response metadata
        response_overhead = len(
            json.dumps({k: v for k, v in response.items() if k != "body"}).encode(
                "utf-8"
            )
        )

        total_size = body_size + response_overhead

        if total_size > LAMBDA_RESPONSE_LIMIT:
            # Return a 413 error response instead of the original response
            return {
                "statusCode": 413,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",  # Maintain CORS if needed
                },
                "body": json.dumps(
                    {
                        "error": "Request Entity Too Large",
                        "detail": "The response payload exceeds the Lambda limits",
                        "hint": "Try again with a smaller request or making multiple requests with smaller payloads. The factors to consider are the number of hexIds (ie. AOI), the number of fields requested, and the date range (if timeseries is requested).",
                        "response_size_mb": round(total_size / (1024 * 1024), 2),
                        "limit_mb": round(LAMBDA_RESPONSE_LIMIT / (1024 * 1024), 2),
                    }
                ),
            }

        return response

    except Exception as e:
        # If size check fails for any reason, log it and return original response
        # This allows the RuntimeError handler to catch Lambda-level issues
        print(f"Error in response size check: {e}", file=sys.stderr)
        return response


def handler(event, context):
    try:
        # Generate the full response
        response = mangum_handler(event, context)

        # Check response size before returning to AWS Lambda runtime
        checked_response = check_response_size(response)

        return checked_response

    except RuntimeError as e:
        # Fallback: catch Lambda runtime errors that slip through
        if "Failed to post invocation response" in str(
            e
        ) and "Http response code: 413" in str(e):
            return {
                "statusCode": 413,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                },
                "body": json.dumps(
                    {
                        "error": "Request Entity Too Large",
                        "detail": "The response payload exceeds the Lambda limits (caught at runtime)",
                        "hint": "Try again with a smaller request or making multiple requests with smaller payloads. The factors to consider are the number of hexIds (ie. AOI), the number of fields requested, and the date range (if timeseries is requested).",
                    }
                ),
            }
        # Re-raise other RuntimeErrors
        raise


if "AWS_EXECUTION_ENV" in os.environ:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.router.startup())
