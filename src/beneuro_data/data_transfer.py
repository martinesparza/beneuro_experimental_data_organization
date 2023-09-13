import os
import shutil

from beneuro_data.data_validation import Subject, Session
from beneuro_data.validate_argument import validate_argument

# TODO: how do we sync the .profile file?

# Subject.get_valid_sessions("local", "raw") should get the sessions
# that have a valid directory structure and are ready to be copied


@validate_argument("processing_level", ["raw", "processed"])
def sync_subject(subject: Subject, processing_level: str) -> bool:
    # 1. make sure locally the things are valid
    # 2. check if there are remote files already
    # 3. if not, create top level directory
    assert subject.has_folder("local", processing_level), "Subject has no local folder"

    if not subject.has_folder("remote", processing_level):
        subject.create_folder("remote", processing_level)

    assert subject.has_folder("remote", processing_level), "Could not create remote folder"

    return True


def upload_raw_session(session: Session, dry_run: bool):
    assert session.has_folder("local", "raw"), "Session has no local folder"

    assert session.all_local_ephys_recordings_loaded(
        "raw"
    ), "Not all ephys recordings are loaded"

    assert session.behavioral_data is not None, "Behavioral data is not loaded"

    if not session.subject.has_folder("remote", "raw"):
        # session.subject.create_folder("remote", "raw")
        sync_subject(session.subject, "raw")

    if not session.has_folder("remote", "raw"):
        session.create_folder("remote", "raw")

    local_remote_pairs = []

    # it can be that the session already has a remote folder with behavioral data but no ephys
    # in that case, shutil.copytree will fail because the destination folder already exists
    # so instead just loop throught the ephys recordings and copy them one by one
    for recording in session.ephys_recordings:
        if recording.has_folder("remote", "raw"):
            raise FileExistsError(
                f"Remote folder already exists: {recording.get_path('remote', 'raw')}"
            )

        local_remote_pairs.append(
            (
                recording.get_path("local", "raw"),
                recording.get_path("remote", "raw"),
                "folder",
            )
        )

    behavior = session.behavioral_data
    if behavior._pycontrol_task_folder_exists():
        local_pycontrol_py_folder_name = behavior._get_pycontrol_py_folder_path()
        pycontrol_py_file_name = behavior._pycontrol_task_py_file_name()
        local_remote_pairs.append(
            (
                os.path.join(local_pycontrol_py_folder_name, pycontrol_py_file_name),
                os.path.join(behavior.get_path("remote", "raw"), pycontrol_py_file_name),
                "file",
            )
        )

    for extension in behavior._pycontrol_extensions:
        matching_filenames = [
            fname
            for fname in os.listdir(behavior.get_path("local", "raw"))
            if os.path.splitext(fname)[1] == extension
        ]
        for fname in matching_filenames:
            local_path = os.path.join(behavior.get_path("local", "raw"), fname)
            remote_path = os.path.join(behavior.get_path("remote", "raw"), fname)

            local_remote_pairs.append(
                (
                    local_path,
                    remote_path,
                    "file",
                )
            )

    for local_path, remote_path, filetype in local_remote_pairs:
        if dry_run:
            print(
                local_path,
                " -> ",
                remote_path,
            )
        else:
            if filetype == "file":
                shutil.copy(local_path, remote_path)
            elif filetype == "folder":
                shutil.copytree(local_path, remote_path)
            else:
                raise ValueError(f"Unknown filetype: {filetype}")
