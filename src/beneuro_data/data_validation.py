import re
import warnings
from datetime import datetime
from pathlib import Path
from typing import Optional

# this is the expected format the end of the session folder should have
# e.g. M016_2023_08_15_16_00
EXPECTED_DATE_FORMAT: str = "%Y_%m_%d_%H_%M"


class WrongNumberOfFilesError(Exception):
    pass


def validate_raw_session(
    session_path: Path,
    subject_name: str,
    include_behavior: bool,
    include_ephys: bool,
    include_videos: bool,
    whitelisted_files_in_root: tuple[str, ...],
    allowed_extensions_not_in_root: tuple[str, ...],
):
    behavior_files = []
    ephys_folder_paths = []
    video_folder_path = None

    if include_behavior:
        behavior_files = validate_raw_behavioral_data_of_session(
            session_path, subject_name, whitelisted_files_in_root
        )
    if include_ephys:
        ephys_folder_paths = validate_raw_ephys_data_of_session(
            session_path, subject_name, allowed_extensions_not_in_root
        )
    if include_videos:
        video_folder_path = validate_raw_videos_of_session(session_path, subject_name)

    return behavior_files, ephys_folder_paths, video_folder_path


def validate_date_format(extracted_date_str: str) -> bool:
    # parsing the string to datetime, then generating the correctly formatted string to compare to
    try:
        correct_str = datetime.strptime(extracted_date_str, EXPECTED_DATE_FORMAT).strftime(
            EXPECTED_DATE_FORMAT
        )
    except ValueError:
        raise ValueError(
            f"{extracted_date_str} doesn't match expected format of {EXPECTED_DATE_FORMAT}"
        )

    if extracted_date_str != correct_str:
        raise ValueError(
            f"{extracted_date_str} doesn't match expected format of {correct_str}"
        )

    return True


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

    # ideally the part after the subject_ is the date and time
    extracted_date_str = folder_name[len(subject_name) + 1 :]

    validate_date_format(extracted_date_str)

    return True


def validate_raw_behavioral_data_of_session(
    session_path: Path,
    subject_name: str,
    whitelisted_files_in_root: tuple[str, ...],
    warn_if_no_pycontrol_py_folder: bool = True,
) -> list[Path]:
    # validate that the session's path and folder name are in the expected format
    validate_session_path(session_path, subject_name)

    # start making a list of files we find
    behavioral_data_files = []

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
            # sometimes the experimenter leaves comments in a comment.txt file
            if ext_file.name in whitelisted_files_in_root:
                # include this file in the list of behavioral data files found
                # NOTE not adding this, it will be done when processing the extra files
                # behavioral_data_files.append(ext_file)
                # remove it from the list that will be compared to the expected number of pycontrol files
                all_files_with_extension.remove(ext_file)
                # skip testing if it matches the pycontrol pattern
                continue

            if re.match(pycontrol_pattern_for_extension, ext_file.name) is None:
                raise ValueError(
                    f"Filename does not match expected pattern for PyControl {extension} files and is not in the whitelist: {ext_file}"
                )

        # at this point if there was no fail, then all files match the pycontrol pattern
        n_files_found = len(all_files_with_extension)
        n_files_expected = expected_number_of_pycontrol_files_per_extension[extension]

        if n_files_found != n_files_expected:
            raise WrongNumberOfFilesError(
                f"Expected {n_files_expected} files with extension {extension}. Found {n_files_found}"
            )

        # if there was no error, add the files to the list
        behavioral_data_files.extend(all_files_with_extension)

    # check if there is a folder for .py files that run the task
    # warn if there is none
    # if there is, make sure that only one .py file is in there
    pycontrol_py_foldername = "run_task-task_files"
    py_folder = session_path / pycontrol_py_foldername

    if not py_folder.exists():
        if warn_if_no_pycontrol_py_folder:
            warnings.warn(f"No PyControl task folder found in {session_path}")
    else:
        python_files_found = list(py_folder.glob("*.py"))

        if len(python_files_found) > 1:
            raise ValueError(f"More than one .py files found in task folder {session_path}")
        if len(python_files_found) == 0:
            raise FileNotFoundError(f"No .py file found in task folder {session_path}")

        behavioral_data_files.append(python_files_found[0])

    return behavioral_data_files


def validate_raw_ephys_data_of_session(
    session_path: Path,
    subject_name: str,
    allowed_extensions_not_in_root: tuple[str, ...],
) -> list[Path]:
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
        validate_raw_ephys_recording(recording_path, allowed_extensions_not_in_root)

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

    return recording_folder_paths


def validate_raw_ephys_recording(
    gid_folder_path: Path, allowed_extensions_not_in_root: tuple[str, ...]
):
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

        # files with some extensions are allowed and will be renamed and uploaded
        if child.suffix in allowed_extensions_not_in_root:
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


def validate_raw_videos_of_session(
    session_path: Path,
    subject_name: str,
    warn_if_no_video_folder: bool = True,
) -> Optional[Path]:
    # validate that the session's path and folder name are in the expected format
    validate_session_path(session_path, subject_name)

    video_extension = ".avi"

    # video folder's name should be the same as the session's name
    session_name = session_path.name
    video_folder_name = session_name + "_cameras"
    video_folder_path = session_path / video_folder_name

    expected_video_filename_start = rf"{session_name}_camera_"

    if not video_folder_path.exists():
        if warn_if_no_video_folder:
            warnings.warn(f"No correctly named video folder found in {session_path}")

        video_folder_exists = False
    else:
        video_folder_exists = True

        # validate that the folder contains .avi files and a metadata.csv
        avi_files = list(video_folder_path.glob(rf"*{video_extension}"))

        if len(avi_files) == 0:
            raise FileNotFoundError(
                f"No video files found in video folder: {video_folder_path}"
            )

        for avi_file in avi_files:
            if not avi_file.name.startswith(expected_video_filename_start):
                raise ValueError(
                    f"Video filename does not start with {expected_video_filename_start}: {avi_file}"
                )

        # make sure the only remaining file is the metadata.csv
        remaining_files_in_folder = set(video_folder_path.iterdir()).difference(avi_files)

        if (video_folder_path / "metadata.csv") not in remaining_files_in_folder:
            raise FileNotFoundError(
                f"Could not find metadata.csv in video folder {video_folder_path}"
            )

        if len(remaining_files_in_folder) > 1:
            raise ValueError(f"Found unexpected files in video folder {video_folder_path}")

    # make sure there are no avi files in another directory
    for avi_path in session_path.glob(rf"**/*{video_extension}"):
        if not avi_path.is_relative_to(video_folder_path):
            raise ValueError(
                f"Found {video_extension} file in unexpected location: {avi_path}. Expected it to be in {video_folder_path}"
            )

    # make sure there are no metadata.csv files in another directory
    for metadata_path in session_path.glob("**/metadata.csv"):
        if not metadata_path.is_relative_to(video_folder_path):
            raise ValueError(
                f"Found metadata.csv file in unexpected location: {metadata_path}. Expected it to be in {video_folder_path}"
            )

    if video_folder_exists:
        return video_folder_path

    return None
