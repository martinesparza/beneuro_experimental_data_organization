import os
import shutil
from pathlib import Path

from beneuro_data.data_validation import (
    validate_raw_behavioral_data_of_session,
    validate_raw_ephys_data_of_session,
)

from beneuro_data.validate_argument import validate_argument


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


# def upload_raw_session(
#    session_path: Path,
#    subject_name: str,
#    local_root: Path,
#    remote_root: Path,
# ):
#    pass


def upload_raw_behavioral_data(
    session_path: Path,
    subject_name: str,
    local_root: Path,
    remote_root: Path,
):
    # 1. make sure locally the things are valid
    validate_raw_behavioral_data_of_session(session_path, subject_name)
    # TODO how about this previous one returning a dataclass with some diagnostics like pycontrol folder exists or not?
    # 2. check if there are remote files already
    # 3. if not, create top level directory
    # 4. copy files in order
    # 4.1 pycontrol output files
    # 4.2 pycontrol py folder if exists
    # 5. check if files are there and they are identical


# @validate_argument("processing_level", ["raw", "processed"])
# def sync_subject(subject: Subject, processing_level: str) -> bool:
#     # 1. make sure locally the things are valid
#     # 2. check if there are remote files already
#     # 3. if not, create top level directory
#     assert subject.local_store.has_folder(processing_level), "Subject has no local folder"
#     assert not subject.remote_store.has_folder(
#         processing_level
#     ), "Subject already has remote folder"

#     if not subject.remote_store.has_folder(processing_level):
#         subject.remote_store.create_folder(processing_level)

#     if not subject.remote_store.has_folder(processing_level):
#         raise FileNotFoundError(f"{subject.name} has no remote {processing_level} folder.")

#     return True


# def upload_raw_session(session: Session, dry_run: bool):
#     assert session.has_folder("local"), "Session has no local folder"

#     assert session.all_local_ephys_recordings_loaded(
#         "raw"
#     ), "Not all ephys recordings are loaded"

#     assert session.behavioral_data is not None, "Behavioral data is not loaded"

#     if not session.subject.has_folder("remote", "raw"):
#         # session.subject.create_folder("remote", "raw")
#         sync_subject(session.subject, "raw")

#     if not session.has_folder("remote", "raw"):
#         session.create_folder("remote", "raw")

#     local_remote_pairs = []

#     # it can be that the session already has a remote folder with behavioral data but no ephys
#     # in that case, shutil.copytree will fail because the destination folder already exists
#     # so instead just loop throught the ephys recordings and copy them one by one
#     for recording in session.ephys_recordings:
#         if recording.has_folder("remote", "raw"):
#             raise FileExistsError(
#                 f"Remote folder already exists: {recording.get_path('remote', 'raw')}"
#             )

#         local_remote_pairs.append(
#             (
#                 recording.get_path("local", "raw"),
#                 recording.get_path("remote", "raw"),
#                 "folder",
#             )
#         )

#     behavior = session.behavioral_data
#     if behavior._pycontrol_task_folder_exists():
#         local_pycontrol_py_folder_name = behavior._get_pycontrol_py_folder_path()
#         pycontrol_py_file_name = behavior._pycontrol_task_py_file_name()
#         local_remote_pairs.append(
#             (
#                 os.path.join(local_pycontrol_py_folder_name, pycontrol_py_file_name),
#                 os.path.join(behavior.get_path("remote", "raw"), pycontrol_py_file_name),
#                 "file",
#             )
#         )

#     for extension in behavior._pycontrol_extensions:
#         matching_filenames = [
#             fname
#             for fname in os.listdir(behavior.get_path("local", "raw"))
#             if os.path.splitext(fname)[1] == extension
#         ]
#         for fname in matching_filenames:
#             local_path = os.path.join(behavior.get_path("local", "raw"), fname)
#             remote_path = os.path.join(behavior.get_path("remote", "raw"), fname)

#             local_remote_pairs.append(
#                 (
#                     local_path,
#                     remote_path,
#                     "file",
#                 )
#             )

#     for local_path, remote_path, filetype in local_remote_pairs:
#         if dry_run:
#             print(
#                 local_path,
#                 " -> ",
#                 remote_path,
#             )
#         else:
#             if filetype == "file":
#                 shutil.copy(local_path, remote_path)
#             elif filetype == "folder":
#                 shutil.copytree(local_path, remote_path)
#             else:
#                 raise ValueError(f"Unknown filetype: {filetype}")
