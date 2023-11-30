import pytest

from dataclasses import dataclass
from pathlib import Path
import os
from typing import Optional, Type

from beneuro_data.extra_file_handling import (
    _rename_whitelisted_files_in_root,
    _rename_extra_files_with_extension,
)

from test_data_validation import TEST_DIR_PATH, _prepare_directory_structure

EXTRA_FILE_RENAMING_YAML_FOLDER = Path(TEST_DIR_PATH) / "extra_file_renaming_test_yamls"

WHITELISTED_FILES_IN_ROOT = (
    "comment.txt",
    "traj_plan.txt",
)

EXTENSIONS_TO_RENAME_AND_UPLOAD = (".txt",)


@dataclass
class WhitelistedFilesInRootTestCase:
    yaml_name: str
    session_name: str
    orig_filenames: tuple[str, ...]
    expected_new_filenames: tuple[str, ...]
    expected_error: Optional[Type]


whitelisted_in_root_test_cases = [
    WhitelistedFilesInRootTestCase(
        "files_in_root_happy.yaml",
        "M011_2023_04_04_16_00",
        ("comment.txt", "traj_plan.txt"),
        ("M011_2023_04_04_16_00_comment.txt", "M011_2023_04_04_16_00_traj_plan.txt"),
        None,
    ),
    WhitelistedFilesInRootTestCase(
        "both_short_and_renamed_already_exist.yaml",
        "M011_2023_04_04_16_00",
        ("comment.txt", "M011_2023_04_04_16_00_comment.txt"),
        (),
        FileExistsError,
    ),
]


@pytest.mark.parametrize(
    "test_case",
    whitelisted_in_root_test_cases,
    ids=[tc.yaml_name for tc in whitelisted_in_root_test_cases],
)
def test_rename_whitelisted_files_in_root(
    tmp_path: Path,
    test_case: WhitelistedFilesInRootTestCase,
):
    subject_name = os.path.splitext(test_case.session_name)[0].split("_")[0]
    subject_dir = tmp_path / "raw" / subject_name
    session_path = subject_dir / test_case.session_name

    _prepare_directory_structure(
        tmp_path, EXTRA_FILE_RENAMING_YAML_FOLDER, test_case.yaml_name
    )

    # just making sure that the files are in their original place
    for fname in test_case.orig_filenames:
        assert (session_path / fname).exists()

    if test_case.expected_error is None:
        # rename the whitelisted files
        _rename_whitelisted_files_in_root(session_path, WHITELISTED_FILES_IN_ROOT)

        # make sure the files are in their new place
        # and the old ones are gone
        for fname in test_case.expected_new_filenames:
            assert (session_path / fname).exists()

        for fname in test_case.orig_filenames:
            assert not (session_path / fname).exists()

    elif issubclass(test_case.expected_error, BaseException):
        with pytest.raises(test_case.expected_error):
            _rename_whitelisted_files_in_root(session_path, WHITELISTED_FILES_IN_ROOT)


@dataclass
class ExtraFilesWithExtensionTestCase:
    yaml_name: str
    session_name: str
    orig_filenames: tuple[Path, ...]
    expected_new_filenames: tuple[Path, ...]
    expected_error: Optional[Type]


extension_test_cases = [
    ExtraFilesWithExtensionTestCase(
        "trajectory_and_channel_map.yaml",
        "M011_2023_04_04_16_00",
        (
            Path("M011_2023_04_04_16_00_g1/trajectory.txt"),
            Path("M011_2023_04_04_16_00_g1/channel_map.txt"),
        ),
        (
            Path("M011_2023_04_04_16_00_g1/M011_2023_04_04_16_00_trajectory.txt"),
            Path("M011_2023_04_04_16_00_g1/M011_2023_04_04_16_00_channel_map.txt"),
        ),
        None,
    ),
    ExtraFilesWithExtensionTestCase(
        "trajectory_with_name_already_good.yaml",
        "M011_2023_04_04_16_00",
        (Path("M011_2023_04_04_16_00_g1/M011_2023_04_04_16_00_trajectory.txt"),),
        (Path("M011_2023_04_04_16_00_g1/M011_2023_04_04_16_00_trajectory.txt"),),
        None,
    ),
]


@pytest.mark.parametrize(
    "test_case", extension_test_cases, ids=[tc.yaml_name for tc in extension_test_cases]
)
def test_rename_extra_files_with_extension(
    tmp_path: Path,
    test_case: ExtraFilesWithExtensionTestCase,
):
    subject_name = os.path.splitext(test_case.session_name)[0].split("_")[0]
    subject_dir = tmp_path / "raw" / subject_name
    session_path = subject_dir / test_case.session_name

    _prepare_directory_structure(
        tmp_path, EXTRA_FILE_RENAMING_YAML_FOLDER, test_case.yaml_name
    )

    # just making sure that the files are in their original place
    for fname in test_case.orig_filenames:
        assert (session_path / fname).exists()

    if test_case.expected_error is None:
        # rename the whitelisted files
        for extension in EXTENSIONS_TO_RENAME_AND_UPLOAD:
            _rename_extra_files_with_extension(session_path, extension)

        # make sure the files are in their new place
        # and the old ones are gone
        for fname in test_case.expected_new_filenames:
            assert (session_path / fname).exists()

        for fname in test_case.orig_filenames:
            # if the file's name was already the expected one, it should still be there
            if fname in test_case.expected_new_filenames:
                continue
            assert not (session_path / fname).exists()

    elif issubclass(test_case.expected_error, BaseException):
        with pytest.raises(test_case.expected_error):
            for extension in EXTENSIONS_TO_RENAME_AND_UPLOAD:
                _rename_extra_files_with_extension(session_path, extension)
