from pydantic_settings import BaseSettings


class AppSettings(BaseSettings):
    DB_HOST: str
    DB_PORT: str
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_TABLE_NAME: str


class DeploymentSettings(BaseSettings):
    CDK_DEFAULT_ACCOUNT: str
    CDK_DEFAULT_REGION: str
    CDK_CERTIFICATE_ARN: str
    CDK_DOMAIN_NAME: str
