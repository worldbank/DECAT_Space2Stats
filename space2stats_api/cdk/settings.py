from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    CDK_DEFAULT_ACCOUNT: str
    CDK_DEFAULT_REGION: str
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_TABLE_NAME: str

    model_config = SettingsConfigDict(env_file="./aws.env")

settings = Settings()
