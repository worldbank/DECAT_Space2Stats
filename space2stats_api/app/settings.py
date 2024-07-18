from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_TABLE_NAME: str

    model_config = SettingsConfigDict(env_file="../db.env")


settings = Settings()
