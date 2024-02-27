import os
import shutil
from pathlib import Path

from beneuro_data.data_validation import (
    validate_raw_behavioral_data_of_session,
    validate_raw_ephys_data_of_session,
    validate_raw_session,
    validate_raw_videos_of_session,
)
from beneuro_data.extra_file_handling import (
    _find_extra_files_with_extension,
    _find_whitelisted_files_in_root,
    rename_extra_files_in_session,
)
from beneuro_data.validate_argument import validate_argument
from beneuro_data.video_renaming import rename_raw_videos_of_session


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

    # check everything we want to upload
    validate_raw_session(
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
):
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
    Returns True if the upload was successful, raises an error otherwise.
    """
    # have to rename first so that validation passes
    processing_level = "raw"
    remote_folders_created = []

    # 0.1 make sure the session_path is actually a subdirectory of the local_root
    if not local_session_path.is_relative_to(local_root):
        raise ValueError(
            f"Session path is not a subdirectory of the local root: {local_session_path}"
        )
    # 0.2 make sure the session_path has "raw" in it
    if not ("raw" in local_session_path.parts):
        raise ValueError(
            f"Trying to upload a raw session, but the session's path does not contain 'raw': {local_session_path}"
        )
    # 1. make sure locally the things are valid
    # TODO how about this one returning a dataclass with some diagnostics like pycontrol folder exists or not?
    local_file_paths = validate_raw_behavioral_data_of_session(
        local_session_path,
        subject_name,
        whitelisted_files_in_root,
        warn_if_no_pycontrol_py_folder=True,
    )

    # 2. check if there are remote files already
    remote_session_path = remote_root / local_session_path.relative_to(local_root)

    remote_file_paths = [
        remote_session_path / local_path.relative_to(local_session_path)
        for local_path in local_file_paths
    ]

    for remote_path in remote_file_paths:
        if remote_path.exists():
            raise FileExistsError(f"Remote file already exists: {remote_path}")

    # 3. if not, create the subject directory if needed
    remote_subject_dir = remote_root / processing_level / subject_name
    if not remote_subject_dir.exists():
        sync_subject_dir(subject_name, processing_level, local_root, remote_root)
        remote_folders_created.append(remote_subject_dir)

    # if it doesn't exist, create the session's remote directory
    if not remote_session_path.exists():
        remote_session_path.mkdir()
        remote_folders_created.append(remote_session_path)

    # 4. copy files in order
    files_already_copied = []
    for local_path, remote_path in zip(local_file_paths, remote_file_paths):
        try:
            # for the .py file create the parent directory
            if not remote_path.parent.exists():
                assert remote_path.parent.is_relative_to(remote_session_path)
                remote_path.parent.mkdir()

            shutil.copy2(local_path, remote_path)
            files_already_copied.append(remote_path)
        except:
            print(
                f"Error copying {local_path} to {remote_path}. Rolling back changes made so far."
            )
            for p in files_already_copied:
                try:
                    p.unlink()
                except:
                    pass
            for p in remote_folders_created:
                try:
                    shutil.rmtree(p)
                except:
                    pass

            raise RuntimeError(
                f"Error uploading raw behavioral data from {local_session_path}.\nSee stacktrace above."
            )

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

    return True


def upload_raw_ephys_data(
    local_session_path: Path,
    subject_name: str,
    local_root: Path,
    remote_root: Path,
    allowed_extensions_not_in_root: tuple[str, ...],
) -> bool:
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
    Returns True if the upload was successful, raises an error otherwise.
    """
    # have to rename first so that validation passes
    processing_level = "raw"
    remote_folders_created = []

    # 0.1 make sure the session_path is actually a subdirectory of the local_root
    if not local_session_path.is_relative_to(local_root):
        raise ValueError(
            f"Session path is not a subdirectory of the local root: {local_session_path}"
        )
    # 0.2 make sure the session_path has "raw" in it
    if not ("raw" in local_session_path.parts):
        raise ValueError(
            f"Trying to upload a raw session, but the session's path does not contain 'raw': {local_session_path}"
        )

    # make sure locally the things are valid
    local_recording_folder_paths = validate_raw_ephys_data_of_session(
        local_session_path, subject_name, allowed_extensions_not_in_root
    )
    if len(local_recording_folder_paths) == 0:
        raise FileNotFoundError(
            f"Trying to upload raw ephys data but no recordings found in session {local_session_path}"
        )

    # 2. check if there are remote files already
    remote_session_path = remote_root / local_session_path.relative_to(local_root)
    spikeglx_recording_folder_pathlib_pattern = "*_g?"
    remote_recording_folder_paths_already_there = list(
        remote_session_path.glob(spikeglx_recording_folder_pathlib_pattern)
    )
    if len(remote_recording_folder_paths_already_there) > 0:
        raise FileExistsError(
            f"Remote recording folders already exist: {remote_recording_folder_paths_already_there}"
        )

    remote_recording_folder_paths = [
        remote_session_path / local_recording.relative_to(local_session_path)
        for local_recording in local_recording_folder_paths
    ]

    remote_folders_created = []
    for local_path, remote_path in zip(
        local_recording_folder_paths, remote_recording_folder_paths
    ):
        try:
            shutil.copytree(local_path, remote_path, copy_function=shutil.copy2)
            remote_folders_created.append(remote_path)
        except:
            print(
                f"Error copying {local_path} to {remote_path}. Rolling back changes made so far."
            )

            for p in remote_folders_created:
                shutil.rmtree(p)

            raise RuntimeError(
                f"Error uploading raw ephys data from {local_session_path}.\nSee stacktrace above."
            )

    return True


def upload_raw_videos(
    local_session_path: Path,
    subject_name: str,
    local_root: Path,
    remote_root: Path,
) -> bool:
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
    Returns True if the upload was successful, raises an error otherwise.
    """
    # have to rename first so that validation passes
    processing_level = "raw"

    # figure out video folder name
    # look if there is a video folder locally
    # check if there is a video folder remotely
    # upload if needed

    # before that make sure that we're getting something "raw"
    # 0.1 make sure the session_path is actually a subdirectory of the local_root
    if not local_session_path.is_relative_to(local_root):
        raise ValueError(
            f"Session path is not a subdirectory of the local root: {local_session_path}"
        )
    # 0.2 make sure the session_path has "raw" in it
    if not ("raw" in local_session_path.parts):
        raise ValueError(
            f"Trying to upload a raw session, but the session's path does not contain 'raw': {local_session_path}"
        )

    # validate that there are local videos
    local_video_folder_path = validate_raw_videos_of_session(
        local_session_path, subject_name
    )

    if local_video_folder_path is None:
        raise FileNotFoundError(
            f"Trying to upload raw videos but no video folder found in session {local_session_path}"
        )

    remote_folders_created = []

    # check if there are remote files already
    remote_session_path = remote_root / local_session_path.relative_to(local_root)
    remote_video_folder_path = remote_root / local_video_folder_path.relative_to(local_root)

    remote_subject_dir = remote_root / processing_level / subject_name
    if not remote_subject_dir.exists():
        sync_subject_dir(subject_name, processing_level, local_root, remote_root)
        remote_folders_created.append(remote_subject_dir)

    if not remote_session_path.exists():
        remote_session_path.mkdir()
        remote_folders_created.append(remote_session_path)

    # try copying the video folder
    try:
        shutil.copytree(
            local_video_folder_path, remote_video_folder_path, copy_function=shutil.copy2
        )
    # clean up if it fails
    except:
        # remove the remote video folder
        try:
            shutil.rmtree(remote_video_folder_path)
        except:
            pass

        # remove the other folders we created along the way
        for p in remote_folders_created:
            try:
                shutil.rmtree(p)
            except:
                pass

        raise RuntimeError(
            f"Error uploading raw videos from {local_session_path}.\nSee stacktrace above."
        )

    return True


def upload_extra_files(
    local_session_path: Path,
    subject_name: str,
    local_root: Path,
    remote_root: Path,
    whitelisted_files_in_root: tuple[str, ...],
    allowed_extensions_not_in_root: tuple[str, ...],
) -> bool:
    """
    Uploads extra files such as comment.txt, traj_plan.txt, etc. to the remote server.

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
    # have to rename first so that validation passes
    remote_session_path = remote_root / local_session_path.relative_to(local_root)

    # TODO have an option to rename the extra files or not?
    # rename extra files first
    rename_extra_files_in_session(
        local_session_path, whitelisted_files_in_root, allowed_extensions_not_in_root
    )

    # 1. rename whitelisted files
    whitelisted_paths_in_root = _find_whitelisted_files_in_root(
        local_session_path, whitelisted_files_in_root
    )

    # 2. find extra files with allowed extensions
    extra_files_with_allowed_extensions = []
    for extension in allowed_extensions_not_in_root:
        extra_files_with_allowed_extensions.extend(
            _find_extra_files_with_extension(local_session_path, extension)
        )

    # 4. copy the files to the server
    for local_path in whitelisted_paths_in_root + extra_files_with_allowed_extensions:
        remote_path = remote_session_path / local_path.relative_to(local_session_path)
        if remote_path.exists():
            # it was most likely already copied when copying the recording folders
            print(f"Skipping copying because emote file already exists: {remote_path}")
            continue
        shutil.copy2(local_path, remote_path)

    return True


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
