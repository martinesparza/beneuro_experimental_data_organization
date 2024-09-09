import os
from pathlib import Path

import pytest
from test_data_validation import (
    DIRECTORY_STRUCTURE_YAML_FOLDER,
    _prepare_directory_structure,
)

from beneuro_data.data_validation import validate_raw_videos_of_session
from beneuro_data.video_renaming import rename_raw_videos_of_session

test_cases = [
    ("M011_wrong_video_filenames.yaml", "M011_2023_04_04_16_00"),
    ("M011_wrong_video_folder.yaml", "M011_2023_04_04_16_00"),
    ("M011_old_video_naming.yaml", "M011_2023_04_04_16_00"),
    ("M011_videos_in_root.yaml", "M011_2023_04_04_16_00"),
    ("M011_videos_in_root_and_wrong_name.yaml", "M011_2023_04_04_16_00"),
]


@pytest.mark.parametrize(("yaml_filename", "session_name"), test_cases)
def test_rename_raw_videos_of_session(
    tmp_path: Path,
    yaml_filename: str,
    session_name: str,
):
    subject_name = os.path.splitext(yaml_filename)[0].split("_")[0]
    subject_dir = tmp_path / "raw" / subject_name

    session_path = subject_dir / session_name

    _prepare_directory_structure(tmp_path, DIRECTORY_STRUCTURE_YAML_FOLDER, yaml_filename)

    # make sure the original structure is invalid
    with pytest.raises(ValueError):
        validate_raw_videos_of_session(session_path, subject_name, True)

    # rename the video folder and/or files
    rename_raw_videos_of_session(session_path, subject_name, False)

    # make sure the new structure is valid
    assert validate_raw_videos_of_session(session_path, subject_name, True)
