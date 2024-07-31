import os

from pydantic_settings import BaseSettings, SettingsConfigDict


current_directory = os.getcwd()    
env_file_name = 'local_db.env'

if os.path.basename(current_directory) == "space2stats_api":
    env_path = os.path.join(current_directory, env_file_name)
else:
    env_path = os.path.join(current_directory, env_file_name)

class Settings(BaseSettings):
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_TABLE_NAME: str
        
    model_config = SettingsConfigDict(env_file=env_path)


settings = Settings()
