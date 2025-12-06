from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    ENV: str = "dev"
    AUTH_URL: str
    ROBOTS_URL: str
    ROBOTS_LOGIN: str
    AUTH_TIMEOUT: float = 2.0
    ROBOTS_TIMEOUT: float = 2.0
    ROBOT_STATS_URL: str
    ROBOT_STATS_HEARTBEAT: str
    KAFKA_BOOTSTRAP_SERVER: str
    HEARTBEAT_TOPIC: str
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        
@lru_cache
def get_settings() -> Settings:
    return Settings()