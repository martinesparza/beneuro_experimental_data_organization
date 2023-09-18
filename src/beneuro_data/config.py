from pydantic.v1 import BaseSettings
from pathlib import Path


class Config(BaseSettings):
    LOCAL_PATH: Path
    REMOTE_PATH: Path

    class Config:
        env_file = ".env"


config = Config()
