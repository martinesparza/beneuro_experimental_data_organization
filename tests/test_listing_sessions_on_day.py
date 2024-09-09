import datetime
from dataclasses import dataclass
from pathlib import Path

import pytest
from test_data_validation import _prepare_directory_structure

from beneuro_data.query_sessions import (
    list_all_sessions_on_day,
    list_subject_sessions_on_day,
)

TEST_DIR_PATH = Path(__file__).parent
DAYS_SESSIONS_TEST_YAML_FOLDER = TEST_DIR_PATH / "list_days_sessions_test_yamls"


@dataclass
class DaysSessionsPerSubjectTestCase:
    yaml_name: str
    sessions_on_day: str
    day: datetime.date
    subject_name: str


single_subject_test_cases = [
    DaysSessionsPerSubjectTestCase(
        "one_session.yaml", ["M015_2023_08_04_14_30"], datetime.date(2023, 8, 4), "M015"
    ),
    DaysSessionsPerSubjectTestCase(
        "two_sessions.yaml",
        ["M011_2023_08_04_14_30", "M011_2023_08_04_18_00"],
        datetime.date(2023, 8, 4),
        "M011",
    ),
]


@pytest.mark.parametrize("test_case", single_subject_test_cases)
def test_list_subject_sessions_on_day(
    tmp_path: Path, test_case: DaysSessionsPerSubjectTestCase
):
    _prepare_directory_structure(
        tmp_path, DAYS_SESSIONS_TEST_YAML_FOLDER, test_case.yaml_name
    )

    subject_path = tmp_path / "raw" / test_case.subject_name

    sessions_on_day = list_subject_sessions_on_day(subject_path, test_case.day)

    # they don't have to be in the same order, so test for equality of sets
    assert set(sessions_on_day) == set(test_case.sessions_on_day)


@dataclass
class DaysSessionsMultipleSubjectsTestCase:
    yaml_names: list[str]
    sessions_on_day: list[tuple[str, str]]
    day: datetime.date
    ignored_subject_level_dirs: tuple[str, ...] = ()


multiple_subjects_test_cases = [
    DaysSessionsMultipleSubjectsTestCase(
        ["one_session.yaml", "two_sessions.yaml"],
        [
            ("M015", "M015_2023_08_04_14_30"),
            ("M011", "M011_2023_08_04_14_30"),
            ("M011", "M011_2023_08_04_18_00"),
        ],
        datetime.date(2023, 8, 4),
        tuple(),
    ),
    DaysSessionsMultipleSubjectsTestCase(
        ["one_session.yaml", "two_sessions.yaml", "treadmill_calibration_to_ignore.yaml"],
        [
            ("M015", "M015_2023_08_04_14_30"),
            ("M011", "M011_2023_08_04_14_30"),
            ("M011", "M011_2023_08_04_18_00"),
        ],
        datetime.date(2023, 8, 4),
        ("treadmill-calibration",),
    ),
]


@pytest.mark.parametrize("test_case", multiple_subjects_test_cases)
def test_list_all_sessions_on_day(
    tmp_path: Path, test_case: DaysSessionsMultipleSubjectsTestCase
):
    # _prepare_directory_structure can only set up directories for one subject
    # because it (re)creates the "raw" directory
    wipe_raw_dir = True
    for yaml_name in test_case.yaml_names:
        _prepare_directory_structure(
            tmp_path,
            DAYS_SESSIONS_TEST_YAML_FOLDER,
            yaml_name,
            wipe_raw_dir=wipe_raw_dir,
        )
        wipe_raw_dir = False

    raw_dir_path = tmp_path / "raw"

    sessions_on_day = list_all_sessions_on_day(
        raw_dir_path, test_case.day, test_case.ignored_subject_level_dirs
    )

    # they don't have to be in the same order, so test for equality of sets
    assert set(sessions_on_day) == set(test_case.sessions_on_day)
