from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg import OperationalError


async def database_exception_handler(request: Request, exc: OperationalError):
    return JSONResponse(
        status_code=500,
        content={"error": "Database connection failed", "detail": str(exc)},
    )


async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail},
    )


# Custom handler for validation errors (422 Unprocessable Entity)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = exc.errors()
    detailed_errors = [
        {
            "field": "->".join(map(str, err.get("loc", []))),
            "message": err.get("msg"),
            "type": err.get("type", "Unknown error type"),
        }
        for err in errors
    ]

    return JSONResponse(
        status_code=422,
        content={
            "error": "Validation Error",
            "details": detailed_errors,
            "hint": "Check if 'fields' is a valid list and all provided fields exist.",
        },
    )


def add_exception_handlers(app):
    app.add_exception_handler(OperationalError, database_exception_handler)
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
