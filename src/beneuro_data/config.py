from pydantic.v1 import BaseSettings
from pathlib import Path
from dotenv import find_dotenv


class Config(BaseSettings):
    LOCAL_PATH: Path
    REMOTE_PATH: Path

    class Config:
        env_file = find_dotenv(".env")


config = Config()
