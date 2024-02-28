import filecmp
import shutil
from pathlib import Path
from typing import Union

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
from beneuro_data.validate_argument import validate_argument
from beneuro_data.video_renaming import rename_raw_videos_of_session


def _source_to_dest(
    source_paths: Union[Path, list[Path]], source_root: Path, dest_root: Path
) -> Union[Path, list[Path]]:
    """
    Determine the destination path for a local path or a list of local paths.

    Parameters
    ----------
    source_paths : Union[Path, list[Path]]
        The source path or a list of source paths.
    source_root : Path
        The root directory of the source paths.
    dest_root : Path
        The root directory of the destination paths.
        Should be at the same level as the source_root.

    Returns
    -------
    If source_paths is a Path, returns the destination path.
    If source_paths is a list of Paths, returns a list of destination paths.
    """
    if isinstance(source_paths, Path):
        return dest_root / source_paths.relative_to(source_root)
    elif isinstance(source_paths, list):
        return [dest_root / p.relative_to(source_root) for p in source_paths]
    else:
        raise ValueError(f"Invalid type for source_paths: {type(source_paths)}")


def _copy_list_of_files(
    source_file_paths: list[Path], dest_file_paths: list[Path], if_exists: str
):
    """
    Copies a list of files from one directory to another.

    Parameters
    ----------
    source_file_paths : list[Path]
        List of paths to the source files.
    dest_file_paths : list[Path]
        List of paths where the files will be copied to.
    if_exists: str
        Behavior when the file already exists. Can be one of:
            - "overwrite": Overwrite the file.
            - "skip": Skip the file.
            - "error_if_different": Raise an error if the file is different.
            - "error": Raise an error.
    """
    for source_path, dest_path in zip(source_file_paths, dest_file_paths):
        # create the parent directory if it doesn't exist
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # if the file doesn't exist, copy it and move to the next one
        if not dest_path.exists():
            shutil.copy2(source_path, dest_path)
            continue

        # handle the case when the file exists
        if if_exists == "overwrite":
            shutil.copy2(source_path, dest_path)

        elif if_exists == "skip":
            continue

        elif if_exists == "error_if_different":
            # compare based on content
            if filecmp.cmp(source_path, dest_path, shallow=False):
                continue

            raise FileExistsError(f"File already exists and is different: {dest_path}")

        elif if_exists == "error":
            raise FileExistsError(f"File already exists: {dest_path}")

        else:
            raise ValueError(f"Invalid value for if_exists: {if_exists}")


def _check_list_of_files_before_copy(
    source_file_paths: list[Path], dest_file_paths: list[Path], if_exists: str
):
    """
    Check if the files can be copied before calling _copy_list_of_files.

    Parameters
    ----------
    source_file_paths : list[Path]
        List of paths to the source files.
    dest_file_paths : list[Path]
        List of paths where the files will be copied to.
    if_exists: str
        Behavior when the file already exists. Can be one of:
            - "error": Raise an error.
            - "error_if_different": Raise an error if a file is different.
            - "overwrite": Overwriting should always work.
            - "skip": Skipping should always work.
    """
    for source_path, dest_path in zip(source_file_paths, dest_file_paths):
        # if the destination file doesn't exist, copying will be fine
        if not dest_path.exists():
            continue

        # handle the case when the file exists

        # overwriting should always work
        if if_exists == "overwrite":
            continue

        # skipping should always work
        elif if_exists == "skip":
            continue

        # if the file is different, raise an error
        elif if_exists == "error_if_different":
            # compare based on content
            if filecmp.cmp(source_path, dest_path, shallow=False):
                continue
            raise FileExistsError(f"File already exists and is different: {dest_path}")

        elif if_exists == "error":
            raise FileExistsError(f"File already exists: {dest_path}")

        else:
            raise ValueError(f"Invalid value for if_exists: {if_exists}")


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
    behavior_files, ephys_folder_paths, video_folder_path = validate_raw_session(
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


# TODO rename this or merge it with the validate_session_path function
def _validate_local_session_path(
    local_session_path: Path, local_root: Path, processing_level: str
) -> None:
    """
    1. Make sure the session_path is actually a subdirectory of the local_root.
    2. Make sure the session_path has processing_level (most likely "raw") in it.
    """
    # 1 make sure the session_path is actually a subdirectory of the local_root
    if not local_session_path.is_relative_to(local_root):
        raise ValueError(
            f"Session path is not a subdirectory of the local root: {local_session_path}"
        )
    # 2 make sure the session_path has "raw" in it
    if not (processing_level in local_session_path.parts):
        raise ValueError(
            f"Trying to upload a raw session, but the session's path does not contain '{processing_level}': {local_session_path}"
        )


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
    _validate_local_session_path(local_session_path, local_root, "raw")

    # 1. make sure locally the things are valid
    # TODO how about this one returning a dataclass with some diagnostics like pycontrol folder exists or not?
    local_file_paths = validate_raw_behavioral_data_of_session(
        local_session_path,
        subject_name,
        whitelisted_files_in_root,
        warn_if_no_pycontrol_py_folder=True,
    )

    # 2. check if there are remote files already
    remote_session_path = _source_to_dest(local_session_path, local_root, remote_root)
    remote_file_paths = _source_to_dest(local_file_paths, local_root, remote_root)

    _check_list_of_files_before_copy(
        local_file_paths, remote_file_paths, "error_if_different"
    )

    _copy_list_of_files(local_file_paths, remote_file_paths, "error_if_different")

    # 5. check that the files are there and they are identical
    remote_file_paths_there = validate_raw_behavioral_data_of_session(
        remote_session_path,
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
    _validate_local_session_path(local_session_path, local_root, "raw")

    # make sure locally the things are valid
    local_recording_folders = validate_raw_ephys_data_of_session(
        local_session_path, subject_name, allowed_extensions_not_in_root
    )
    if len(local_recording_folders) == 0:
        raise FileNotFoundError(
            f"Trying to upload raw ephys data but no recordings found in session {local_session_path}"
        )

    # 2. check if there are remote files already
    local_file_paths = [
        p
        for recording_folder in local_recording_folders
        for p in recording_folder.glob("**/*")
        if p.is_file()
    ]

    remote_file_paths = _source_to_dest(local_file_paths, local_root, remote_root)

    _check_list_of_files_before_copy(
        local_file_paths, remote_file_paths, "error_if_different"
    )

    _copy_list_of_files(local_file_paths, remote_file_paths, "error_if_different")

    # check that the files are there and valid
    remote_recording_folders = validate_raw_ephys_data_of_session(
        _source_to_dest(local_session_path, local_root, remote_root),
        subject_name,
        allowed_extensions_not_in_root,
    )

    return remote_recording_folders


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
    _validate_local_session_path(local_session_path, local_root, "raw")

    # validate that there are local videos
    local_video_folder_path = validate_raw_videos_of_session(
        local_session_path, subject_name
    )

    if local_video_folder_path is None:
        raise FileNotFoundError(
            f"Trying to upload raw videos but no video folder found in session {local_session_path}"
        )

    local_file_paths = [p for p in local_video_folder_path.glob("**/*") if p.is_file()]
    remote_file_paths = _source_to_dest(local_file_paths, local_root, remote_root)

    _check_list_of_files_before_copy(
        local_file_paths, remote_file_paths, "error_if_different"
    )

    _copy_list_of_files(local_file_paths, remote_file_paths, "error_if_different")

    # check that the folder is there and valid
    remote_video_folder_found = validate_raw_videos_of_session(
        _source_to_dest(local_session_path, local_root, remote_root),
        subject_name,
    )

    if remote_video_folder_found is None:
        raise FileNotFoundError(
            "Something went wrong during uploading raw videos. Data not found on the server."
        )

    return remote_video_folder_found


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
    # 0. rename extra files first
    # have to rename first so that validation passes
    rename_extra_files_in_session(
        local_session_path, whitelisted_files_in_root, allowed_extensions_not_in_root
    )

    # 1. find whitelisted files
    whitelisted_paths_in_root = _find_whitelisted_files_in_root(
        local_session_path, whitelisted_files_in_root
    )

    # 2. find extra files with allowed extensions
    extra_files_with_allowed_extensions = _find_extra_files_with_extensions(
        local_session_path, allowed_extensions_not_in_root
    )

    local_file_paths = whitelisted_paths_in_root + extra_files_with_allowed_extensions
    remote_file_paths = _source_to_dest(local_file_paths, local_root, remote_root)

    # 3. copy the files to the server
    _copy_list_of_files(local_file_paths, remote_file_paths, "error_if_different")

    return remote_file_paths


# NOTE This will fail, but it's expected and should be rewritten
def download_raw_session(
    remote_session_path: Path,
    subject_name: str,
    local_base_path: Path,
    remote_base_path: Path,
    include_behavior: bool,
    include_ephys: bool,
    include_videos: bool,
    whitelisted_files_in_root: tuple[str, ...],
):
    # check what data the session has on the server
    behavior_files, ephys_folder_paths, video_folder_path = validate_raw_session(
        remote_session_path, subject_name, True, True, True, whitelisted_files_in_root
    )

    include_behavior = include_behavior and len(behavior_files) > 0
    include_ephys = include_ephys and len(ephys_folder_paths) > 0
    include_videos = include_videos and video_folder_path is not None

    # downloading is just uploading the other way around
    upload_raw_session(
        remote_session_path,
        subject_name,
        # config.REMOTE_PATH,
        # config.LOCAL_PATH,
        remote_base_path,
        local_base_path,
        include_behavior,
        include_ephys,
        include_videos,
    )

    local_session_path = local_base_path / remote_session_path.relative_to(remote_base_path)

    return local_session_path
