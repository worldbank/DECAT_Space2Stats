from typing import Optional
from pydantic_settings import BaseSettings


class AppSettings(BaseSettings):
    PGHOST: str
    PGPORT: str
    PGDATABASE: str
    PGUSER: str
    PGPASSWORD: str
    PGTABLENAME: str


class DeploymentSettings(BaseSettings):
    CDK_DEFAULT_ACCOUNT: str
    CDK_DEFAULT_REGION: str
    CDK_CERTIFICATE_ARN: str
    CDK_DOMAIN_NAME: Optional[str]
    STAGE: str = "dev"
