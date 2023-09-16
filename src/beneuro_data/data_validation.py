import re
import warnings
from datetime import datetime
from pathlib import Path


def validate_raw_session(session_path: Path, subject_name: str):
    validate_raw_ephys_data_of_session(session_path, subject_name)
    validate_raw_behavioral_data_of_session(session_path, subject_name)


def validate_session_path(session_path: Path, subject_name: str):
    # 1. has to start with the subject's name
    # 2. has to have an underscore after the subject's name
    # 3. has to end with a date in the correct format

    folder_name = session_path.name

    if not session_path.exists():
        raise FileNotFoundError(f"Session folder does not exist: {session_path}")

    if not folder_name.startswith(subject_name):
        raise ValueError(
            f"Folder name has to start with subject name. Got {folder_name} with subject name {subject_name}"
        )

    if folder_name[len(subject_name)] != "_":
        raise ValueError(
            f"Folder name has to have an underscore after subject name. Got {folder_name}."
        )

    # this is the expected format the end of the session folder should have
    # e.g. M016_2023_08_15_16_00
    date_format: str = "%Y_%m_%d_%H_%M"

    # ideally the part after the subject_ is the date and time
    extracted_date_str = folder_name[len(subject_name) + 1 :]

    # parsing the string to datetime, then generating the correctly formatted string to compare to
    try:
        correct_str = datetime.strptime(extracted_date_str, date_format).strftime(
            date_format
        )
    except ValueError:
        raise ValueError(
            f"{extracted_date_str} doesn't match expected format of {date_format}"
        )

    if extracted_date_str != correct_str:
        raise ValueError(
            f"{extracted_date_str} doesn't match expected format of {correct_str}"
        )

    return True


def validate_raw_behavioral_data_of_session(session_path: Path, subject_name: str):
    # validate that the session's path and folder name are in the expected format
    validate_session_path(session_path, subject_name)

    # look for files that match the patterns that pycontrol files should have
    # validate their number
    pycontrol_ending_pattern_per_extension = {
        ".pca": rf"_MotSen\d-(X|Y)\.pca",
        ".txt": r"\.txt",
    }

    expected_number_of_pycontrol_files_per_extension = {
        ".pca": 2,
        ".txt": 1,
    }

    pycontrol_start_pattern = rf"^{re.escape(session_path.name)}"
    # pycontrol_middle_pattern = rf'{self.session.date.strftime("%Y-%m-%d")}-\d{{6}}'
    pycontrol_middle_pattern = r".*"

    for extension in pycontrol_ending_pattern_per_extension.keys():
        pycontrol_pattern_for_extension = (
            pycontrol_start_pattern
            + pycontrol_middle_pattern
            + pycontrol_ending_pattern_per_extension[extension]
        )

        # this one is not that precise
        all_files_with_extension = list(session_path.glob(rf"*{extension}"))
        for ext_file in all_files_with_extension:
            if re.match(pycontrol_pattern_for_extension, ext_file.name) is None:
                raise ValueError(
                    f"Filename does not match expected pattern for PyControl {extension} files: {ext_file}"
                )

        # at this point if there was no fail, then all files match the pycontrol pattern
        n_files_found = len(all_files_with_extension)
        n_files_expected = expected_number_of_pycontrol_files_per_extension[extension]

        if n_files_found != n_files_expected:
            raise ValueError(
                f"Expected {n_files_expected} files with extension {extension}. Found {n_files_found}"
            )

    # check if there is a folder for .py files that run the task
    # warn if there is none
    # if there is, make sure that only one .py file is in there
    pycontrol_py_foldername = "run_task-task_files"
    py_folder = session_path / pycontrol_py_foldername

    if not py_folder.exists():
        warnings.warn(f"No PyControl task folder found in {session_path}")
    else:
        n_python_files_found = len(list(py_folder.glob("*.py")))
        if n_python_files_found > 1:
            raise ValueError(f"More than one .py files found in task folder {session_path}")
        if n_python_files_found == 0:
            raise FileNotFoundError(f"No .py file found in task folder {session_path}")

    return True


def validate_raw_ephys_data_of_session(session_path: Path, subject_name: str):
    # validate that the session's path and folder name are in the expected format
    validate_session_path(session_path, subject_name)

    # list the folders that look like recordings -> end with _gx
    spikeglx_recording_folder_pathlib_pattern = "*_g?"
    recording_folder_paths = list(
        session_path.glob(spikeglx_recording_folder_pathlib_pattern)
    )

    # ideally there is only one recording in a session.
    # warn if there are more
    if len(recording_folder_paths) > 1:
        warnings.warn(f"More than one raw ephys recordings found in {session_path}.")

    # validate the structure in the recording folders that we found
    for recording_path in recording_folder_paths:
        validate_raw_ephys_recording(recording_path)

    # search subfolders for spikeglx filetypes and make sure that all of them are in the recording folders found
    spikeglx_endings = [".lf.meta", ".lf.bin", ".ap.meta", ".ap.bin"]
    for ending in spikeglx_endings:
        for spikeglx_filepath in session_path.glob(f"**/*{ending}"):
            if not any(
                spikeglx_filepath.is_relative_to(recording_path)
                for recording_path in recording_folder_paths
            ):
                raise ValueError(
                    f"{spikeglx_filepath} is not in any known recording folders."
                )


def validate_raw_ephys_recording(gid_folder_path: Path):
    # extract gx part
    gid = extract_gid(gid_folder_path.name)

    # validate that the folder name has the expected structure
    session_name = gid_folder_path.parent.name
    expected_folder_name = f"{session_name}_{gid}"
    if gid_folder_path.name != expected_folder_name:
        raise ValueError(
            f"Folder name {gid_folder_path.name} does not match expected format {expected_folder_name}"
        )

    # validate that there are only subfolders in the recording's folder
    # hidden files are allowed
    probe_subfolders = []
    for child in gid_folder_path.iterdir():
        # hidden files are allowed
        if child.match(".*"):
            continue

        if not child.is_dir():
            raise ValueError("Only folders are allowed in the ephys recordings folder")

        # the directories should be the probes' subfolders
        probe_subfolders.append(child)

    # validate that the probe subfolders have the expected name
    probe_subfolder_pattern = rf"{gid_folder_path.name}_imec\d$"
    for probe_folder in probe_subfolders:
        if re.match(probe_subfolder_pattern, probe_folder.name) is None:
            raise ValueError(
                f"The following folder name doesn't match the expected format for probes: {probe_folder}"
            )

    # validate that the subfolders have .lf.meta, .lf.bin, .ap.meta, .ap.bin files with the expected names
    expected_endings = [".lf.meta", ".lf.bin", ".ap.meta", ".ap.bin"]
    for probe_folder in probe_subfolders:
        imec_str = probe_folder.name.split("_")[-1]
        expected_filenames = {
            f"{gid_folder_path.name}_t0.{imec_str}{ending}" for ending in expected_endings
        }
        found_filenames = {p.name for p in probe_folder.iterdir()}
        if found_filenames != expected_filenames:
            raise ValueError(
                f"Files in probe directory do not match the expected pattern. {probe_folder}"
            )

    return True


def extract_gid(folder_name: str):
    # TODO can there be multiple numbers after the _g?
    # SPIKEGLX_RECORDING_PATTERN = r"_g(\d+)$"
    SPIKEGLX_RECORDING_PATTERN = r"_g(\d)$"

    gid_search_result = re.search(SPIKEGLX_RECORDING_PATTERN, folder_name)

    if gid_search_result is None:
        raise ValueError(f"Could not extract correct recording ID from {folder_name}")

    return gid_search_result.group(0)[1:]
