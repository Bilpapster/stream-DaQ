from functools import lru_cache
from typing import Literal

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

ENV_FILE_PATH = "config/.env"


class Settings(BaseSettings):
    input_connector: Literal["python", "kafka"]
    autocommit_duration_ms: int
    pathway_threads: int

    kafka_bootstrap_servers: str
    kafka_group_id: str
    kafka_session_timeout_ms: str
    kafka_topic: str

    class Config:
        case_sensitive = False
        env_file = ENV_FILE_PATH


@lru_cache()
def get_settings():
    load_dotenv(ENV_FILE_PATH)  # make sure variables in .env file are propagated to environment
    return Settings()
