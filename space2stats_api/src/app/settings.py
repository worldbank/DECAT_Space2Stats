from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_TABLE_NAME: str

    @property
    def DB_CONNECTION_STRING(self) -> str:
        host_port = f"host={self.DB_HOST} port={self.DB_PORT}"
        db_user = f"dbname={self.DB_NAME} user={self.DB_USER}"
        return f"{host_port} {db_user} password={self.DB_PASSWORD}"
