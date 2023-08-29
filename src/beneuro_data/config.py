from pydantic.v1 import SecretStr, BaseSettings


class Config(BaseSettings):
    USERNAME: str
    PASSWORD: SecretStr

    REMOTE_SERVER_ADDRESS: str
    LOCAL_PATH: str
    REMOTE_PATH: str

    class Config:
        env_file = ".env"


config = Config()
