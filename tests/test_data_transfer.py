import pytest

from dataclasses import dataclass
from typing import Optional, Type

from beneuro_data.data_validation import Subject
from beneuro_data.data_transfer import sync_subject


@dataclass
class SubjectSyncTestCase:
    subject_name: str
    processing_level: str
    set_up_local_dir: bool
    set_up_server_dir: bool
    expected_error: Optional[Type]
    expected_error_message: Optional[str]


subject_test_cases = [
    SubjectSyncTestCase("M011", "raw", True, True, None, None),
    SubjectSyncTestCase("M011", "processed", True, True, None, None),
    SubjectSyncTestCase(
        "M011",
        "invalid_proc_level",
        True,
        True,
        ValueError,
        "Invalid value for processing_level",
    ),
    SubjectSyncTestCase(
        "M011", "raw", False, True, AssertionError, "Subject has no local folder"
    ),
    SubjectSyncTestCase(
        "M011", "processed", False, True, AssertionError, "Subject has no local folder"
    ),
    SubjectSyncTestCase(
        "M011", "raw", True, False, AssertionError, "parent directory does not exist"
    ),
    SubjectSyncTestCase(
        "M011", "processed", True, False, AssertionError, "parent directory does not exist"
    ),
]


@pytest.mark.parametrize("test_case", subject_test_cases)
def test_sync_subject_only(tmp_path, test_case: SubjectSyncTestCase):
    source_base_path = tmp_path / "source_dir"
    dest_base_path = tmp_path / "dest_dir"

    # locally create the subject's directory
    if test_case.set_up_local_dir:
        (source_base_path / test_case.processing_level / test_case.subject_name).mkdir(
            parents=True, exist_ok=False
        )
    # on the "server" create the raw/processed directory
    if test_case.set_up_server_dir:
        (dest_base_path / test_case.processing_level).mkdir(parents=True, exist_ok=False)

    subject = Subject(
        test_case.subject_name,
        False,
        source_base_path,
        dest_base_path,
    )

    if test_case.expected_error is None:
        sync_subject(subject, test_case.processing_level)
        assert (
            dest_base_path / test_case.processing_level / test_case.subject_name
        ).exists()
    elif issubclass(test_case.expected_error, BaseException):
        with pytest.raises(
            test_case.expected_error, match=test_case.expected_error_message
        ):
            sync_subject(subject, test_case.processing_level)
