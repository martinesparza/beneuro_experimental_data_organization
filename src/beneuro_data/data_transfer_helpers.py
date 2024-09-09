import filecmp
import shutil
from pathlib import Path
from typing import Union


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
) -> None:
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


def _validate_session_is_raw_and_in_root(session_path: Path, base_path: Path) -> None:
    """
    1. Make sure the session_path is actually a subdirectory of the base_path.
    2. Make sure the session_path has "raw" in it.
    """
    # 1 make sure the session_path is actually a subdirectory of the base_path
    if not session_path.is_relative_to(base_path):
        raise ValueError(
            f"Session path is not a subdirectory of the local root: {session_path}"
        )
    # 2 make sure the session_path has "raw" in it
    if not ("raw" in session_path.parts):
        raise ValueError(
            f"Trying to upload a raw session, but the session's path does not contain 'raw': {session_path}"
        )
