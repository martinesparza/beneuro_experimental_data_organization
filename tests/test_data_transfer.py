import os
from dataclasses import dataclass
from typing import Optional, Type

import pytest

from beneuro_data.data_transfer import sync_subject_dir, upload_raw_session
from beneuro_data.data_validation import WrongNumberOfFilesError

from test_data_validation import (
    _prepare_directory_structure,
    WHITELISTED_FILES_IN_ROOT,
    EXTENSIONS_TO_RENAME_AND_UPLOAD,
)

TEST_DIR_PATH = os.path.dirname(__file__)
UPLOAD_RAW_SESSION_YAML_FOLDER = os.path.join(
    TEST_DIR_PATH, "upload_raw_session_test_yamls"
)


@dataclass
class SubjectSyncTestCase:
    subject_name: str
    processing_level: str
    set_up_local_parent_dir: bool
    set_up_local_subject_dir: bool
    set_up_server_dir: bool
    expected_error: Optional[Type]
    expected_error_message: Optional[str]


subject_test_cases = [
    # happy raw
    SubjectSyncTestCase(
        "M011",
        "raw",
        True,
        True,
        True,
        None,
        None,
    ),
    # happy processed
    SubjectSyncTestCase(
        "M011",
        "processed",
        True,
        True,
        True,
        None,
        None,
    ),
    SubjectSyncTestCase(
        "M011",
        "invalid_proc_level",
        True,
        True,
        True,
        ValueError,
        "Invalid value for processing_level",
    ),
    SubjectSyncTestCase(
        "M011",
        "raw",
        True,
        False,
        False,
        FileNotFoundError,
        "Local subject directory does not exist",
    ),
    SubjectSyncTestCase(
        "M011",
        "processed",
        True,
        False,
        False,
        FileNotFoundError,
        "Local subject directory does not exist",
    ),
    SubjectSyncTestCase(
        "M011",
        "raw",
        False,
        False,
        False,
        FileNotFoundError,
        "Local 'raw' directory does not exist",
    ),
    SubjectSyncTestCase(
        "M011",
        "raw",
        True,
        True,
        False,
        FileNotFoundError,
        "Remote 'raw' directory does not exist",
    ),
    SubjectSyncTestCase(
        "M011",
        "processed",
        True,
        True,
        False,
        FileNotFoundError,
        "Remote 'processed' directory does not exist",
    ),
]


@pytest.mark.parametrize("test_case", subject_test_cases)
def test_sync_subject_only(tmp_path, test_case: SubjectSyncTestCase):
    source_base_path = tmp_path / "source_dir"
    dest_base_path = tmp_path / "dest_dir"

    # locally create the raw/processed directory
    if test_case.set_up_local_parent_dir:
        (source_base_path / test_case.processing_level).mkdir(parents=True, exist_ok=False)

    # locally create the subject's directory
    if test_case.set_up_local_subject_dir:
        (source_base_path / test_case.processing_level / test_case.subject_name).mkdir(
            parents=True, exist_ok=False
        )

    # on the "server" create the raw/processed directory
    if test_case.set_up_server_dir:
        (dest_base_path / test_case.processing_level).mkdir(parents=True, exist_ok=False)

    # run the test
    if test_case.expected_error is None:
        # when it should go right, check that the directory was created
        sync_subject_dir(
            test_case.subject_name,
            test_case.processing_level,
            source_base_path,
            dest_base_path,
        )

        assert (
            dest_base_path / test_case.processing_level / test_case.subject_name
        ).exists()

    elif issubclass(test_case.expected_error, BaseException):
        with pytest.raises(
            test_case.expected_error, match=test_case.expected_error_message
        ):
            sync_subject_dir(
                test_case.subject_name,
                test_case.processing_level,
                source_base_path,
                dest_base_path,
            )


@dataclass
class RawSessionUploadTestCase:
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


raw_session_upload_test_cases = [
    RawSessionUploadTestCase(
        "M011_correct.yaml",
        "M011_2023_04_04_16_00",
        None,
        None,
        True,
        True,
        True,
    ),
    RawSessionUploadTestCase(
        "M011_with_extra_txt_files.yaml",
        "M011_2023_04_04_16_00",
        None,
        None,
        True,
        True,
        True,
    ),
    RawSessionUploadTestCase(
        "M011_with_traj_plan_txt.yaml",
        "M011_2023_04_04_16_00",
        None,
        None,
        True,
        True,
        True,
    ),
    RawSessionUploadTestCase(
        "M011_with_comment_txt.yaml",
        "M011_2023_04_04_16_00",
        None,
        None,
        True,
        True,
        True,
    ),
    RawSessionUploadTestCase(
        "M011_correct_but_no_behavior.yaml",
        "M011_2023_04_04_16_00",
        WrongNumberOfFilesError,
        "Expected 2 files with extension .pca",
        True,
        True,
        False,
    ),
    RawSessionUploadTestCase(
        "M011_correct_but_no_ephys.yaml",
        "M011_2023_04_04_16_00",
        FileNotFoundError,
        "no recordings found",
        True,
        True,
        False,
    ),
    RawSessionUploadTestCase(
        "M011_correct_but_no_videos.yaml",
        "M011_2023_04_04_16_00",
        FileNotFoundError,
        "no video folder found",
        True,
        True,
        True,
    ),
    # not having to upload missing behavior should not raise error
    RawSessionUploadTestCase(
        "M011_correct_but_no_behavior.yaml",
        "M011_2023_04_04_16_00",
        None,
        None,
        False,
        True,
        False,
    ),
    # not having to upload missing ephys should not raise error
    RawSessionUploadTestCase(
        "M011_correct_but_no_ephys.yaml",
        "M011_2023_04_04_16_00",
        None,
        None,
        True,
        False,
        False,
    ),
    # not having to upload missing videos should not raise error
    RawSessionUploadTestCase(
        "M011_correct_but_no_videos.yaml",
        "M011_2023_04_04_16_00",
        None,
        None,
        True,
        True,
        False,
    ),
    RawSessionUploadTestCase(
        "M011_wrong_video_folder_name.yaml",
        "M011_2023_04_04_16_00",
        ValueError,
        "Found .avi file in unexpected location",
        True,
        True,
        True,
    ),
]


@pytest.mark.parametrize(
    "test_case",
    raw_session_upload_test_cases,
    ids=[tc.yaml_name for tc in raw_session_upload_test_cases],
)
def test_upload_raw_session(tmp_path, test_case: RawSessionUploadTestCase):
    source_base_path = tmp_path / "source_dir"
    dest_base_path = tmp_path / "dest_dir"

    (source_base_path / "raw").mkdir(parents=True, exist_ok=True)
    (dest_base_path / "raw").mkdir(parents=True, exist_ok=True)

    _prepare_directory_structure(
        source_base_path, UPLOAD_RAW_SESSION_YAML_FOLDER, test_case.yaml_name
    )

    source_subject_path = source_base_path / "raw" / test_case.mouse_name
    dest_subject_path = dest_base_path / "raw" / test_case.mouse_name

    source_session_path = source_subject_path / test_case.session_name
    dest_session_path = dest_subject_path / test_case.session_name

    if test_case.expected_error is None:
        upload_raw_session(
            source_session_path,
            test_case.mouse_name,
            source_base_path,
            dest_base_path,
            test_case.include_behavior,
            test_case.include_ephys,
            test_case.include_videos,
            True,
            WHITELISTED_FILES_IN_ROOT,
            EXTENSIONS_TO_RENAME_AND_UPLOAD,
        )

        import filecmp

        report = filecmp.dircmp(source_session_path, dest_session_path)

        assert len(report.left_only) == 0
        assert len(report.right_only) == 0

    elif issubclass(test_case.expected_error, BaseException):
        with pytest.raises(test_case.expected_error, match=test_case.error_message):
            upload_raw_session(
                source_session_path,
                test_case.mouse_name,
                source_base_path,
                dest_base_path,
                test_case.include_behavior,
                test_case.include_ephys,
                test_case.include_videos,
                True,
                WHITELISTED_FILES_IN_ROOT,
                EXTENSIONS_TO_RENAME_AND_UPLOAD,
            )
