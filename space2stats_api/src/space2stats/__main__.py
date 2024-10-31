import os

try:
    import uvicorn  # noqa

except ImportError:  # pragma: nocover
    uvicorn = None  # type: ignore


if __name__ == "__main__":
    assert (
        uvicorn is not None
    ), "uvicorn must be installed: `python -m pip install 'space2stats[server]'`"

    uvicorn.run(
        app="space2stats.api.app:build_app",
        host=os.getenv("UVICORN_HOST", "127.0.0.1"),
        port=int(os.getenv("UVICORN_PORT", "8000")),
        root_path=os.getenv("UVICORN_ROOT_PATH", ""),
        log_level="info",
        factory=True,
        reload=True,
    )
