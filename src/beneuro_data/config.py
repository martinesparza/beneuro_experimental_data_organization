from pydantic.v1 import BaseSettings
from pathlib import Path


def _get_env_path():
    package_path = Path(__file__).absolute().parent.parent.parent
    return package_path / ".env"


class Config(BaseSettings):
    LOCAL_PATH: Path
    REMOTE_PATH: Path
    IGNORED_SUBJECT_LEVEL_DIRS: list[str] = []

    class Config:
        env_file = _get_env_path()


def _load_config():
    if not _get_env_path().exists():
        raise FileNotFoundError(
            "Config file not found. Run `bnd init-config` to create one."
        )

    return Config()
