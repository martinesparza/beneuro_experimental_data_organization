from pathlib import Path
from datetime import datetime

from .data_validation import validate_session_path, EXPECTED_DATE_FORMAT


def list_subject_sessions(
    subject_path: Path, subject_name: str
) -> tuple[list[Path], list[Path]]:
    """
    Lists all sessions for a given subject.

    Parameters
    ----------
    subject_name : str
        Name of the experimental subject.
    subject_path : Path
        Path to the subject's folder.

    Returns
    -------
    valid_subject_sessions : list[Path]
        List of folders with a valid name for a session.
    invalid_subject_sessions : list[Path]
        List of other folders.
    """
    valid_subject_sessions = []
    invalid_subject_sessions = []
    for session_path in subject_path.iterdir():
        if session_path.is_dir():
            try:
                validate_session_path(session_path, subject_name)
            except ValueError:
                invalid_subject_sessions.append(session_path)
            else:
                valid_subject_sessions.append(session_path)

    return valid_subject_sessions, invalid_subject_sessions


def get_last_session_path(subject_path: Path, subject_name: str) -> Path:
    """
    Returns the path to the last session of a subject.
    """
    valid_subject_sessions, invalid_subject_sessions = list_subject_sessions(
        subject_path, subject_name
    )

    if len(valid_subject_sessions) == 0:
        raise ValueError(
            f"No valid sessions found for subject {subject_name} in {subject_path}"
        )

    dates = []
    for session_path in valid_subject_sessions:
        session_name = session_path.name
        # ideally the part after the subject_ is the date and time
        session_date = datetime.strptime(
            session_name[len(subject_name) + 1 :],
            EXPECTED_DATE_FORMAT,
        )
        dates.append(session_date)

    last_session_index = dates.index(max(dates))

    return valid_subject_sessions[last_session_index]
