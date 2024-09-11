import asyncio
import json

from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient
from psycopg.errors import OperationalError
from space2stats.app import app
from space2stats.errors import (
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
