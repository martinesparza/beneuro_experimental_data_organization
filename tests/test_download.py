import os
from dataclasses import dataclass
from typing import Optional, Type

import pytest
from test_data_validation import (
    EXTENSIONS_TO_RENAME_AND_UPLOAD,
    WHITELISTED_FILES_IN_ROOT,
    _prepare_directory_structure,
)

from beneuro_data.data_transfer import download_raw_session
from beneuro_data.data_validation import WrongNumberOfFilesError

TEST_DIR_PATH = os.path.dirname(__file__)
DOWNLOAD_RAW_SESSION_TEST_FOLDER = os.path.join(
    TEST_DIR_PATH, "download_raw_session_test_yamls"
)


@dataclass
class RawSessionDownloadTestCase:
    yaml_name: str
    session_name: str
    expected_error: Optional[Type]
    error_message: Optional[str]
    include_behavior: bool
    include_ephys: bool
    include_videos: bool

    @property
    def mouse_name(self) -> str:
        return os.path.splitext(self.yaml_name)[0].split("_")[0]


session_download_test_cases = [
    RawSessionDownloadTestCase(
        "M011_correct.yaml",
        "M011_2023_04_04_16_00",
        None,
        None,
        True,
        True,
        True,
    ),
    RawSessionDownloadTestCase(
        "M011_no_videos.yaml",
        "M011_2023_04_04_16_00",
        Warning,
        "Skipping videos",
        True,
        True,
        True,
    ),
    RawSessionDownloadTestCase(
        "M011_no_ephys.yaml",
        "M011_2023_04_04_16_00",
        Warning,
        "Skipping ephys",
        True,
        True,
        True,
    ),
    RawSessionDownloadTestCase(
        "M011_no_extra_files.yaml",
        "M011_2023_04_04_16_00",
        Warning,
        "Skipping extra files",
        True,
        True,
        True,
    ),
    RawSessionDownloadTestCase(
        "M011_wrong_behavior.yaml",
        "M011_2023_04_04_16_00",
        Warning,
        "Skipping behavioral",
        True,
        True,
        True,
    ),
    # TODO not wanting to download bad behavior should not warn
    # the test itself is incorrect for this case
    # use data validation functions on both sides to compare list of files
    # RawSessionDownloadTestCase(
    #    "M011_wrong_behavior.yaml",
    #    "M011_2023_04_04_16_00",
    #    None,
    #    None,
    #    False,
    #    True,
    #    True,
    # ),
]


@pytest.mark.parametrize(
    "test_case",
    session_download_test_cases,
    ids=[tc.yaml_name for tc in session_download_test_cases],
)
def test_download_raw_session(tmp_path, test_case: RawSessionDownloadTestCase):
    source_base_path = tmp_path / "source_dir"
    dest_base_path = tmp_path / "dest_dir"

    (source_base_path / "raw").mkdir(parents=True, exist_ok=True)
    (dest_base_path / "raw").mkdir(parents=True, exist_ok=True)

    _prepare_directory_structure(
        source_base_path, DOWNLOAD_RAW_SESSION_TEST_FOLDER, test_case.yaml_name
    )

    source_subject_path = source_base_path / "raw" / test_case.mouse_name
    dest_subject_path = dest_base_path / "raw" / test_case.mouse_name

    source_session_path = source_subject_path / test_case.session_name
    dest_session_path = dest_subject_path / test_case.session_name

    if test_case.expected_error is None:
        download_raw_session(
            source_session_path,
            test_case.mouse_name,
            dest_base_path,
            source_base_path,
            test_case.include_behavior,
            test_case.include_ephys,
            test_case.include_videos,
            WHITELISTED_FILES_IN_ROOT,
            EXTENSIONS_TO_RENAME_AND_UPLOAD,
        )

        import filecmp

        report = filecmp.dircmp(source_session_path, dest_session_path)

        assert len(report.left_only) == 0
        assert len(report.right_only) == 0

    elif issubclass(test_case.expected_error, Warning):
        with pytest.warns(test_case.expected_error, match=test_case.error_message):
            download_raw_session(
                source_session_path,
                test_case.mouse_name,
                dest_base_path,
                source_base_path,
                test_case.include_behavior,
                test_case.include_ephys,
                test_case.include_videos,
                WHITELISTED_FILES_IN_ROOT,
                EXTENSIONS_TO_RENAME_AND_UPLOAD,
            )
