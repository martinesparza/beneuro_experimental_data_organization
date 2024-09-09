from dataclasses import dataclass
from pathlib import Path

import pytest
from test_data_validation import _prepare_directory_structure

from beneuro_data.query_sessions import get_last_session_path

TEST_DIR_PATH = Path(__file__).parent
SUBJECT_SESSION_LIST_YAML_FOLDER = TEST_DIR_PATH / "subject_session_list_yamls"


@dataclass
class LastSessionTestCase:
    yaml_name: str
    last_session_name: str
    subject_name: str


last_session_test_cases = [
    LastSessionTestCase("happy_case.yaml", "M015_2023_08_04_14_30", "M015"),
    LastSessionTestCase("happy_case_same_date.yaml", "M015_2023_08_04_14_30", "M015"),
    LastSessionTestCase(
        "last_one_has_wrong_date_format.yaml", "M015_2023_07_27_13_00", "M015"
    ),
]


@pytest.mark.parametrize("test_case", last_session_test_cases)
def test_get_last_session_path(tmp_path: Path, test_case: LastSessionTestCase):
    _prepare_directory_structure(
        tmp_path, SUBJECT_SESSION_LIST_YAML_FOLDER, test_case.yaml_name
    )

    subject_path = tmp_path / "raw" / test_case.subject_name

    last_session_found = get_last_session_path(subject_path, test_case.subject_name)

    assert last_session_found.name == test_case.last_session_name
