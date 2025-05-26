import asyncio
import json

from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient
from psycopg.errors import OperationalError
from space2stats.api import app
from space2stats.api.errors import (
    database_exception_handler,
    http_exception_handler,
    validation_exception_handler,
)

client = TestClient(app)


def test_database_exception_handler():
    request = None
    exception = OperationalError("Database connection failed")
    response = asyncio.run(database_exception_handler(request, exception))

    assert response.status_code == 500
    response_data = json.loads(response.body.decode("utf-8"))
    assert response_data == {
        "error": "Database connection failed",
        "detail": "Database connection failed",
    }


def test_http_exception_handler():
    request = None
    exception = HTTPException(status_code=404, detail="Not found")
    response = asyncio.run(http_exception_handler(request, exception))

    assert response.status_code == 404
    response_data = json.loads(response.body.decode("utf-8"))
    assert response_data == {
        "error": "Not found",
    }


def test_validation_exception_handler():
    request = None
    exc = RequestValidationError(
        [
            {
                "loc": ("body", "fields"),
                "msg": "Input should be a valid list",
                "type": "type_error.list",
            }
        ]
    )
    response = asyncio.run(validation_exception_handler(request, exc))

    expected_response = {
        "error": "Validation Error",
        "details": [
            {
                "field": "body->fields",
                "message": "Input should be a valid list",
                "type": "type_error.list",
            }
        ],
        "hint": "Check if 'fields' is a valid list and all provided fields exist.",
    }

    assert response.status_code == 422
    response_data = json.loads(response.body.decode("utf-8"))
    assert response_data == expected_response


def test_http_exception_handler_413():
    request = None
    exception = HTTPException(status_code=413, detail="Request Entity Too Large")
    response = asyncio.run(http_exception_handler(request, exception))

    expected_response = {
        "error": "Request Entity Too Large",
        "detail": "The request payload exceeds the API limits",
        "hint": "Try again with a smaller request or making multiple requests with smaller payloads. The factors to consider are the number of hexIds (ie. AOI), the number of fields requested, and the date range (if timeseries is requested).",
    }

    assert response.status_code == 413
    response_data = json.loads(response.body.decode("utf-8"))
    assert response_data == expected_response


def test_http_exception_handler_503():
    request = None
    exception = HTTPException(status_code=503, detail="Service Unavailable")
    response = asyncio.run(http_exception_handler(request, exception))

    expected_response = {
        "error": "Service Unavailable",
        "detail": "The request likely timed out due to processing complexity or high server load",
        "hint": "Try a smaller request by reducing the area of interest (AOI), number of fields requested, or date range (for timeseries). You can also break large requests into multiple smaller requests.",
        "suggestions": [
            "Reduce the number of hexagon IDs in your request",
            "Request fewer fields at a time",
            "Use a smaller geographic area",
            "For timeseries requests, use a shorter date range",
            "Try the request again in a few moments",
        ],
    }

    assert response.status_code == 503
    response_data = json.loads(response.body.decode("utf-8"))
    assert response_data == expected_response
