from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PGHOST: str
    PGPORT: int
    PGDATABASE: str
    PGUSER: str
    PGPASSWORD: str
    PGTABLENAME: str

    # Bucket for large responses
    S3_BUCKET_NAME: str

    # see https://www.psycopg.org/psycopg3/docs/api/pool.html#the-connectionpool-class for options
    DB_MIN_CONN_SIZE: int = 1
    DB_MAX_CONN_SIZE: int = 10

    # Maximum number of requests that can be queued to the pool
    DB_MAX_QUERIES: int = 50000

    # Maximum time, in seconds, that a connection can stay unused in the pool before being closed, and the pool shrunk.
    DB_MAX_IDLE: float = 300

    # Number of background worker threads used to maintain the pool state
    DB_NUM_WORKERS: int = 3

    @property
    def DB_CONNECTION_STRING(self) -> str:
        host_port = f"host={self.PGHOST} port={self.PGPORT}"
        db_user = f"dbname={self.PGDATABASE} user={self.PGUSER}"
        return f"{host_port} {db_user} password={self.PGPASSWORD}"

    model_config = {
        "env_file": "local_db.env",
        "extra": "ignore",
    }
