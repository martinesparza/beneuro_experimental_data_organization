import os
import shutil
from pathlib import Path

from beneuro_data.data_validation import (
    validate_raw_behavioral_data_of_session,
    validate_raw_ephys_data_of_session,
    validate_raw_videos_of_session,
    validate_raw_session,
)

from beneuro_data.validate_argument import validate_argument

from beneuro_data.rename_extra_files import (
    rename_whitelisted_files_in_root,
    rename_extra_files_with_extension,
)


@validate_argument("processing_level", ["raw", "processed"])
def sync_subject_dir(
    subject_name: str, processing_level: str, local_root: Path, remote_root: Path
):
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
):
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
):
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


def upload_raw_videos(
    local_session_path: Path,
    subject_name: str,
    local_root: Path,
    remote_root: Path,
):
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
):
    remote_session_path = remote_root / local_session_path.relative_to(local_root)

    # 1. rename whitelisted files in root and upload them
    new_whitelisted_paths_in_root = rename_whitelisted_files_in_root(
        local_session_path, whitelisted_files_in_root
    )

    for local_path in new_whitelisted_paths_in_root:
        remote_path = remote_session_path / local_path.relative_to(local_session_path)
        if remote_path.exists():
            raise FileExistsError(f"Remote file already exists: {remote_path}")
        shutil.copy2(local_path, remote_path)

    # 2. rename extra files with the allowed extensions and upload them
    extra_files_with_allowed_extensions = []
    for extension in allowed_extensions_not_in_root:
        extra_files_with_allowed_extensions.extend(
            rename_extra_files_with_extension(local_session_path, extension)
        )

    for local_path in extra_files_with_allowed_extensions:
        remote_path = remote_session_path / local_path.relative_to(local_session_path)
        if remote_path.exists():
            raise FileExistsError(f"Remote file already exists: {remote_path}")
        shutil.copy2(local_path, remote_path)


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
