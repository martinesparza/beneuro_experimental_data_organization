import pytest

import os
from pydantic import BaseSettings
from ruamel.yaml import YAML

from beneuro_data.data_transfer import Subject

from generate_directory_structure_test_cases import create_directory_structure_from_dict


# class TestConfig(BaseSettings):
#    LOCAL_PATH: str
#    REMOTE_PATH: str


test_dir_path = os.path.dirname(__file__)
yaml_folder = os.path.join(test_dir_path, "directory_structure_test_yamls")


def test_subject(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    for mouse_name in ["M011", "M015", "M016"]:
        with open(os.path.join(yaml_folder, f"{mouse_name}.yaml"), "r") as f:
            mouse_dir_dict = YAML().load(f)

        create_directory_structure_from_dict(
            mouse_dir_dict,
            str(raw_dir),
        )

    for mouse_name in ["M011", "M015", "M016"]:
        subject = Subject(mouse_name, False, local_base_path=str(tmp_path))
        # print(subject.get_path("local", "raw"))
        print(subject.list_local_session_folders("raw"))
