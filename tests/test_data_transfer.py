import pytest

from dataclasses import dataclass
from typing import Optional, Type

from beneuro_data.data_transfer import sync_subject_dir


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
