from pathlib import Path

from pydantic.v1 import BaseSettings

def _get_package_path() -> Path:
    """
    Returns the path to the package directory.
    """
    return Path(__file__).absolute().parent.parent.parent


def _get_env_path() -> Path:
    """
    Returns the path to the .env file containing the configuration settings.
    """
    package_path = _get_package_path()
    return package_path / ".env"


class Config(BaseSettings):
    LOCAL_PATH: Path
    REMOTE_PATH: Path
    IGNORED_SUBJECT_LEVEL_DIRS: tuple[str, ...] = ("treadmill-calibration",)
    WHITELISTED_FILES_IN_ROOT: tuple[str, ...] = (
        "comment.txt",
        "traj_plan.txt",
        "trajectory.txt",
        "channel_map.txt",
    )
    EXTENSIONS_TO_RENAME_AND_UPLOAD: tuple[str, ...] = (".txt",)

    @staticmethod
    def get_animal_name(session_name) -> str:
        return session_name[:4]

    def get_local_session_path(self, session_name: str, processed_or_raw: str):
        if processed_or_raw not in ['processed', 'raw']:
            raise ValueError(f'{processed_or_raw} option not supported. Input must be either'
                             f'"processed" or "raw"')
        animal = self.get_animal_name(session_name)
        local_session_path = self.LOCAL_PATH / processed_or_raw / animal / session_name
        return local_session_path


    class Config:
        env_file = _get_env_path()


def _load_config() -> Config:
    """
    Loads the configuration settings from the .env file and returns it as a Config object.
    """
    if not _get_env_path().exists():
        raise FileNotFoundError("Config file not found. Run `bnd init` to create one.")

    return Config()
