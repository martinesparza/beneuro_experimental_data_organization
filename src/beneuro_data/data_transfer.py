from pathlib import Path
import warnings

from beneuro_data.data_validation import (
    validate_raw_behavioral_data_of_session,
    validate_raw_ephys_data_of_session,
    validate_raw_session,
    validate_raw_videos_of_session,
)
from beneuro_data.extra_file_handling import (
    _find_extra_files_with_extensions,
    _find_whitelisted_files_in_root,
    rename_extra_files_in_session,
)
from beneuro_data.data_transfer_helpers import (
    _source_to_dest,
    _copy_list_of_files,
    _check_list_of_files_before_copy,
    _validate_session_is_raw_and_in_root,
)
from beneuro_data.validate_argument import validate_argument
from beneuro_data.video_renaming import rename_raw_videos_of_session


def upload_raw_session(
    local_session_path: Path,
    subject_name: str,
    local_root: Path,
    remote_root: Path,
    include_behavior: bool,
    include_ephys: bool,
    include_videos: bool,
    include_extra_files: bool,
    whitelisted_files_in_root: tuple[str, ...],
    allowed_extensions_not_in_root: tuple[str, ...],
    rename_videos_first: bool,
    rename_extra_files_first: bool,
) -> bool:
    """
    Uploads a raw session to the remote server.

    Parameters
    ----------
    local_session_path : Path
        Path to the session on the local computer.
    subject_name : str
        Name of the subject. (Needed for validation.)
    local_root : Path
        The root directory of the local data (contains the "raw" and "processed" directories).
    remote_root : Path
        The root directory of the remote data (contains the "raw" and "processed" directories).
    include_behavior : bool
        Whether to upload the behavioral data.
    include_ephys : bool
        Whether to upload the ephys data.
    include_videos : bool
        Whether to upload the video data.
    include_extra_files : bool
        Whether to upload extra files such as comment.txt, traj_plan.txt, etc.
    whitelisted_files_in_root : tuple[str, ...]
        A tuple of filenames that are allowed in the root of the session directory.
    allowed_extensions_not_in_root : tuple[str, ...]
        A tuple of file extensions that are allowed in the session directory excluding the root level.
        E.g. (".txt", )
        For what's allowed in the root, use `whitelisted_files_in_root`.
    rename_videos_first : bool
        Whether to rename the videos to the convention before uploading them.
        Result is `<session_name>_cameras/<session_name>_<camera>_i.avi`
    rename_extra_files_first : bool
        Whether to rename the extra files to the convention before uploading them.
        Results in `<session_name>_<original_filename>`

    Returns
    -------
    Returns True if the upload was successful, raises an error otherwise.
    """
    # have to rename first so that validation passes
    if rename_videos_first:
        if not include_videos:
            raise ValueError(
                "Do not rename videos with upload_raw_session if you're not uploading them."
            )

        rename_raw_videos_of_session(local_session_path, subject_name)

    if rename_extra_files_first:
        if not include_extra_files:
            raise ValueError(
                "Do not rename extra files with upload_raw_session if you're not uploading them."
            )

        rename_extra_files_in_session(
            local_session_path, whitelisted_files_in_root, allowed_extensions_not_in_root
        )

    # TODO maybe check separately to have the option to upload things that are valid
    # check everything we want to upload
    behavior_files, ephys_files, video_files = validate_raw_session(
        local_session_path,
        subject_name,
        include_behavior,
        include_ephys,
        include_videos,
        whitelisted_files_in_root,
        allowed_extensions_not_in_root,
    )

    if include_behavior:
        upload_raw_behavioral_data(
            local_session_path,
            subject_name,
            local_root,
            remote_root,
            whitelisted_files_in_root,
        )
    if include_ephys:
        upload_raw_ephys_data(
            local_session_path,
            subject_name,
            local_root,
            remote_root,
            allowed_extensions_not_in_root,
        )
    if include_videos:
        upload_raw_videos(
            local_session_path,
            subject_name,
            local_root,
            remote_root,
        )
    if include_extra_files:
        upload_extra_files(
            local_session_path,
            subject_name,
            local_root,
            remote_root,
            whitelisted_files_in_root,
            allowed_extensions_not_in_root,
        )

    return True


def upload_raw_behavioral_data(
    local_session_path: Path,
    subject_name: str,
    local_root: Path,
    remote_root: Path,
    whitelisted_files_in_root: tuple[str, ...],
) -> list[Path]:
    """
    Uploads raw behavioral data of a session to the remote server.

    Parameters
    ----------
    local_session_path : Path
        Path to the session on the local computer.
    subject_name : str
        Name of the subject. (Needed for validation.)
    local_root : Path
        The root directory of the local data (contains the "raw" and "processed" directories).
    remote_root : Path
        The root directory of the remote data (contains the "raw" and "processed" directories).
    whitelisted_files_in_root : tuple[str, ...]
        A tuple of filenames that are allowed in the root of the session directory.

    Returns
    -------
    Returns paths to the remote files that were copied.
    """
    if_exists = "error_if_different"

    _validate_session_is_raw_and_in_root(local_session_path, local_root)

    # validate local data and see if things can be copied
    local_file_paths, remote_file_paths = _prepare_copying_raw_behavioral_data(
        local_session_path,
        subject_name,
        local_root,
        remote_root,
        whitelisted_files_in_root,
        if_exists=if_exists,
        warn_if_no_pycontrol_py_folder=True,
    )

    # copy the files to the server
    _copy_list_of_files(local_file_paths, remote_file_paths, if_exists)

    # check that the files are there and valid
    remote_file_paths_there = validate_raw_behavioral_data_of_session(
        _source_to_dest(local_session_path, local_root, remote_root),
        subject_name,
        whitelisted_files_in_root,
        warn_if_no_pycontrol_py_folder=False,
    )

    if sorted(remote_file_paths_there) != sorted(remote_file_paths):
        raise RuntimeError(
            f"Something went wrong during uploading raw behavioral data for session {local_session_path}"
        )

    return remote_file_paths_there


def upload_raw_ephys_data(
    local_session_path: Path,
    subject_name: str,
    local_root: Path,
    remote_root: Path,
    allowed_extensions_not_in_root: tuple[str, ...],
) -> list[Path]:
    """
    Uploads raw electrophysiology data of a session to the remote server.

    Parameters
    ----------
    local_session_path : Path
        Path to the session on the local computer.
    subject_name : str
        Name of the subject. (Needed for validation.)
    local_root : Path
        The root directory of the local data (contains the "raw" and "processed" directories).
    remote_root : Path
        The root directory of the remote data (contains the "raw" and "processed" directories).
    allowed_extensions_not_in_root : tuple[str, ...]
        A tuple of file extensions that are allowed in the session directory excluding the root level.
        E.g. (".txt", )
        For what's allowed in the root, use `whitelisted_files_in_root`.

    Returns
    -------
    Returns a list of the remote folders that were created.
    """
    if_exists = "error_if_different"

    _validate_session_is_raw_and_in_root(local_session_path, local_root)

    local_file_paths, remote_file_paths = _prepare_copying_raw_ephys_data(
        local_session_path,
        subject_name,
        local_root,
        remote_root,
        allowed_extensions_not_in_root,
        if_exists,
    )

    if len(local_file_paths) == 0:
        raise FileNotFoundError(
            f"Trying to upload raw ephys data but no recordings found in session {local_session_path}"
        )

    _copy_list_of_files(local_file_paths, remote_file_paths, if_exists)

    # check that the files are there and valid
    remote_files_found = validate_raw_ephys_data_of_session(
        _source_to_dest(local_session_path, local_root, remote_root),
        subject_name,
        allowed_extensions_not_in_root,
    )

    return remote_files_found


def upload_raw_videos(
    local_session_path: Path,
    subject_name: str,
    local_root: Path,
    remote_root: Path,
) -> Path:
    """
    Uploads videos of a session to the remote server.

    Parameters
    ----------
    local_session_path : Path
        Path to the session on the local computer.
    subject_name : str
        Name of the subject. (Needed for validation.)
    local_root : Path
        The root directory of the local data (contains the "raw" and "processed" directories).
    remote_root : Path
        The root directory of the remote data (contains the "raw" and "processed" directories).

    Returns
    -------
    Path to the remote video folder.
    """
    if_exists = "error_if_different"

    _validate_session_is_raw_and_in_root(local_session_path, local_root)

    # validate that there are local videos
    local_file_paths = validate_raw_videos_of_session(local_session_path, subject_name)

    if len(local_file_paths) == 0:
        raise FileNotFoundError(
            f"Trying to upload raw videos but no video folder found in session {local_session_path}"
        )

    remote_file_paths = _source_to_dest(local_file_paths, local_root, remote_root)

    local_file_paths, remote_file_paths = _prepare_copying_raw_videos(
        local_session_path,
        subject_name,
        local_root,
        remote_root,
        warn_if_no_video_folder=True,
        if_exists=if_exists,
    )

    _copy_list_of_files(local_file_paths, remote_file_paths, if_exists)

    # check that the folder is there and valid
    remote_video_files_found = validate_raw_videos_of_session(
        _source_to_dest(local_session_path, local_root, remote_root),
        subject_name,
    )

    if len(remote_video_files_found) == 0:
        raise FileNotFoundError(
            "Something went wrong during uploading raw videos. Data not found on the server."
        )

    return remote_video_files_found


def upload_extra_files(
    local_session_path: Path,
    subject_name: str,
    local_root: Path,
    remote_root: Path,
    whitelisted_files_in_root: tuple[str, ...],
    allowed_extensions_not_in_root: tuple[str, ...],
) -> bool:
    """
    Renames and uploads extra files such as comment.txt, traj_plan.txt, etc. to the remote server.

    Parameters
    ----------
    local_session_path : Path
        Path to the session on the local computer.
    subject_name : str
        Name of the subject. (Needed for validation.)
    local_root : Path
        The root directory of the local data (contains the "raw" and "processed" directories).
    remote_root : Path
        The root directory of the remote data (contains the "raw" and "processed" directories).
    whitelisted_files_in_root : tuple[str, ...]
        A tuple of filenames that are allowed in the root of the session directory.
    allowed_extensions_not_in_root : tuple[str, ...]
        A tuple of file extensions that are allowed in the session directory excluding the root level.
        E.g. (".txt", )
        For what's allowed in the root, use `whitelisted_files_in_root`.

    Returns
    -------
    Returns True if the upload was successful, raises an error otherwise.
    """
    if_exists = "error_if_different"

    # have to rename the extra files first so that validation passes
    rename_extra_files_in_session(
        local_session_path, whitelisted_files_in_root, allowed_extensions_not_in_root
    )

    # find files and check if they can be copied without problems
    local_file_paths, remote_file_paths = _prepare_copying_raw_extra_files(
        local_session_path,
        local_root,
        remote_root,
        whitelisted_files_in_root,
        allowed_extensions_not_in_root,
        if_exists,
    )

    # copy the files to the server
    _copy_list_of_files(local_file_paths, remote_file_paths, if_exists)

    return remote_file_paths


def download_raw_session(
    remote_session_path: Path,
    subject_name: str,
    local_base_path: Path,
    remote_base_path: Path,
    include_behavior: bool,
    include_ephys: bool,
    include_videos: bool,
    whitelisted_files_in_root: tuple[str, ...],
    allowed_extensions_not_in_root: tuple[str, ...],
):
    """
    Downloads a raw session from the remote server to the local computer.
    Handling of errors is different than in the case of uploads: here try to download
    everything that is valid even if others are not.

    """
    remote_behavior_files = []
    remote_ephys_files = []
    remote_video_files = []
    remote_extra_files = []

    if_exists = "error_if_different"

    # check what data the session has on the server
    if include_behavior:
        try:
            (
                remote_behavior_files,
                local_behavior_files,
            ) = _prepare_copying_raw_behavioral_data(
                remote_session_path,
                subject_name,
                remote_base_path,
                local_base_path,
                whitelisted_files_in_root,
                if_exists,
                warn_if_no_pycontrol_py_folder=False,
            )
        except Exception as e:
            warnings.warn(f"Skipping behavioral data because of: {type(e).__name__}: {e}")
            include_behavior = False

    if include_ephys:
        try:
            remote_ephys_files, local_ephys_files = _prepare_copying_raw_ephys_data(
                remote_session_path,
                subject_name,
                remote_base_path,
                local_base_path,
                allowed_extensions_not_in_root,
                if_exists=if_exists,
            )
        except Exception as e:
            warnings.warn(f"Skipping ephys data because of: {type(e).__name__}: {e}")
            include_ephys = False

    if include_videos:
        try:
            remote_video_files, local_video_files = _prepare_copying_raw_videos(
                remote_session_path,
                subject_name,
                remote_base_path,
                local_base_path,
                warn_if_no_video_folder=False,
                if_exists=if_exists,
            )
        except Exception as e:
            warnings.warn(f"Skipping videos because of: {type(e).__name__}: {e}")
            include_videos = False

    # always try to include extra files
    try:
        remote_extra_files, local_extra_files = _prepare_copying_raw_extra_files(
            remote_session_path,
            remote_base_path,
            local_base_path,
            whitelisted_files_in_root,
            allowed_extensions_not_in_root,
            if_exists,
        )
        include_extra_files = True
    except Exception as e:
        warnings.warn(f"Skipping extra files because of: {type(e).__name__}: {e}")
        include_extra_files = False

    if include_behavior and len(remote_behavior_files) == 0:
        warnings.warn("Skipping behavioral data because it was not found. ")
        include_behavior = False

    if include_ephys and len(remote_ephys_files) == 0:
        warnings.warn("Skipping ephys data because it was not found. ")
        include_ephys = False

    if include_videos and len(remote_video_files) == 0:
        warnings.warn("Skipping videos because they were not found. ")
        include_videos = False

    if include_extra_files and len(remote_extra_files) == 0:
        warnings.warn("Skipping extra files because they were not found. ")
        include_extra_files = False

    if include_behavior:
        _copy_list_of_files(remote_behavior_files, local_behavior_files, if_exists)
    if include_ephys:
        _copy_list_of_files(remote_ephys_files, local_ephys_files, if_exists)
    if include_videos:
        _copy_list_of_files(remote_video_files, local_video_files, if_exists)
    if include_extra_files:
        _copy_list_of_files(remote_extra_files, local_extra_files, if_exists)

    local_session_path = _source_to_dest(
        remote_session_path, remote_base_path, local_base_path
    )

    validate_raw_session(
        local_session_path,
        subject_name,
        include_behavior,
        include_ephys,
        include_videos,
        whitelisted_files_in_root,
        allowed_extensions_not_in_root,
    )

    return local_session_path


def _prepare_copying_raw_behavioral_data(
    source_session_path: Path,
    subject_name: str,
    source_base_path: Path,
    dest_base_path: Path,
    whitelisted_files_in_root: tuple[str, ...],
    if_exists: str,
    warn_if_no_pycontrol_py_folder: bool,
):
    """
    Prepare copying raw behavioral data from the source to the destination.
    1. Validate the source data
    2. List the source files and their destinations
    3. Check if the files can be copied.

    Parameters
    ----------
    source_session_path : Path
        Path to the session on the source computer.
    subject_name : str
        Name of the subject. (Needed for validation.)
    source_base_path : Path
        The root directory of the source data.
    dest_base_path : Path
        The root directory of the destination data.
        Has to be at the same level as the source_base_path.
    whitelisted_files_in_root : tuple[str, ...]
        A tuple of filenames that are allowed in the root of the session directory.
    if_exists: str
        Behavior when the file already exists. Can be one of:
            - "overwrite": Overwrite the file.
            - "skip": Skip the file.
            - "error_if_different": Raise an error if the file is different. (Recommended.)
            - "error": Raise an error.
    warn_if_no_pycontrol_py_folder: bool
        Whether to warn if the folder with the PyControl .py is not found.

    Returns
    -------
    source_behavior_files, dest_behavior_files : list[Path], list[Path]
        Lists of source and destination paths for the behavioral data.
    """
    source_behavior_files = validate_raw_behavioral_data_of_session(
        source_session_path,
        subject_name,
        whitelisted_files_in_root,
        warn_if_no_pycontrol_py_folder=warn_if_no_pycontrol_py_folder,
    )

    dest_behavior_files = _source_to_dest(
        source_behavior_files, source_base_path, dest_base_path
    )

    _check_list_of_files_before_copy(source_behavior_files, dest_behavior_files, if_exists)

    return source_behavior_files, dest_behavior_files


def _prepare_copying_raw_ephys_data(
    source_session_path: Path,
    subject_name: str,
    source_base_path: Path,
    dest_base_path: Path,
    allowed_extensions_not_in_root: tuple[str, ...],
    if_exists: str,
):
    """
    Same as _prepare_copying_raw_behavioral_data but for ephys data.
    """
    source_ephys_files = validate_raw_ephys_data_of_session(
        source_session_path, subject_name, allowed_extensions_not_in_root
    )

    dest_ephys_files = _source_to_dest(source_ephys_files, source_base_path, dest_base_path)

    _check_list_of_files_before_copy(source_ephys_files, dest_ephys_files, if_exists)

    return source_ephys_files, dest_ephys_files


def _prepare_copying_raw_videos(
    source_session_path: Path,
    subject_name: str,
    source_base_path: Path,
    dest_base_path: Path,
    warn_if_no_video_folder: bool,
    if_exists: str,
):
    """
    Same as _prepare_copying_raw_behavioral_data but for the videos.
    """
    source_video_files = validate_raw_videos_of_session(
        source_session_path,
        subject_name,
        warn_if_no_video_folder=warn_if_no_video_folder,
    )

    dest_video_files = _source_to_dest(source_video_files, source_base_path, dest_base_path)

    _check_list_of_files_before_copy(source_video_files, dest_video_files, if_exists)

    return source_video_files, dest_video_files


def _prepare_copying_raw_extra_files(
    source_session_path: Path,
    source_base_path: Path,
    dest_base_path: Path,
    whitelisted_files_in_root: tuple[str, ...],
    allowed_extensions_not_in_root: tuple[str, ...],
    if_exists: str,
):
    """
    Same as _prepare_copying_raw_behavioral_data but for the extra files.
    """
    source_extra_files_not_in_root = _find_extra_files_with_extensions(
        source_session_path, allowed_extensions_not_in_root
    )
    source_extra_files_in_root = _find_whitelisted_files_in_root(
        source_session_path, whitelisted_files_in_root
    )
    source_extra_files = source_extra_files_not_in_root + source_extra_files_in_root

    dest_extra_files = _source_to_dest(source_extra_files, source_base_path, dest_base_path)

    _check_list_of_files_before_copy(source_extra_files, dest_extra_files, if_exists)

    return source_extra_files, dest_extra_files


@validate_argument("processing_level", ["raw", "processed"])
def sync_subject_dir(
    subject_name: str, processing_level: str, local_root: Path, remote_root: Path
) -> None:
    """
    Make sure the subject's directory at the given processing level (processed or raw)
    exists locally, and create it remotely if it doesn't exist.

    Parameters
    ----------
    subject_name : str
        The name of the subject.
    processing_level : str
        The processing level of the data. Can be either "raw" or "processed".
    local_root : Path
        The root directory of the local data (contains the "raw" and "processed" directories).
    remote_root : Path
        The root directory of the remote data (contains the "raw" and "processed" directories).

    Returns
    -------
    None
    """
    local_subject_dir = local_root / processing_level / subject_name
    remote_subject_dir = remote_root / processing_level / subject_name

    if not local_subject_dir.parent.exists():
        raise FileNotFoundError(f"Local '{processing_level}' directory does not exist.")

    if not local_subject_dir.exists():
        raise FileNotFoundError(
            f"Local subject directory does not exist: {local_subject_dir}"
        )

    if not local_subject_dir.is_dir():
        raise NotADirectoryError(
            f"Local subject directory is not a directory: {local_subject_dir}"
        )

    if not remote_subject_dir.parent.exists():
        raise FileNotFoundError(f"Remote '{processing_level}' directory does not exist.")

    if not remote_subject_dir.exists():
        remote_subject_dir.mkdir()

    if not remote_subject_dir.exists():
        raise FileNotFoundError(
            f"Could not create remote subject direcory: {remote_subject_dir}"
        )
