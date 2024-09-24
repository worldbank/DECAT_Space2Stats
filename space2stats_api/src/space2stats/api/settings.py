from ..settings import Settings as DbSettings


class Settings(DbSettings):
    # Bucket for large responses
    S3_BUCKET_NAME: str
