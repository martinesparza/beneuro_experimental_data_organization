import os

from fabric import Connection

from beneuro_data.config import Config

config = Config()

def folder_exists(local_or_remote: str, path: str):
    if local_or_remote == 'local':
        return _local_folder_exists(path)
    elif local_or_remote == 'remote':
        return _remote_folder_exists(path)
    else:
        raise ValueError(f'Invalid local_or_remote {local_or_remote}')

def _local_folder_exists(path: str):
    return os.path.exists(path)

def _remote_folder_exists(path: str):
    with Connection(host=config.REMOTE_SERVER_ADDRESS, user=config.USERNAME, connect_kwargs={'password': config.PASSWORD.get_secret_value()}) as c:
        with c.sftp() as sftp:
            try:
                # if we can read the contents, then the folder exists
                sftp.listdir(path)
                return True
            except FileNotFoundError:
                return False

def make_folder(local_or_remote: str, path: str):
    if local_or_remote == 'local':
        return _make_local_folder(path)
    elif local_or_remote == 'remote':
        return _make_remote_folder(path)
    else:
        raise ValueError(f'Invalid local_or_remote {local_or_remote}')

def _make_local_folder(path: str):
    return os.makedirs(path)

def _make_remote_folder(path: str):
    with Connection(host=config.REMOTE_SERVER_ADDRESS, user=config.USERNAME, connect_kwargs={'password': config.PASSWORD.get_secret_value()}) as c:
        with c.sftp() as sftp:
            sftp.mkdir(path)
