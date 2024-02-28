from pathlib import Path


def _find_whitelisted_files_in_root(
    session_path: Path,
    whitelisted_files_in_root: tuple[str, ...],
) -> list[Path]:
    """
    Find files in the root of the session folder with the given filenames.

    Parameters
    ----------
    session_path : Path
        Path to the session folder.
    whitelisted_files_in_root : tuple[str, ...]
        A tuple of filenames that are allowed in the root of the session directory.
        These will be usually read from the config file.

    Returns
    -------
    List of paths to the files found in the root of the session folder.
    """
    session_name = session_path.name

    files_found = []
    for filename in whitelisted_files_in_root:
        # try the filename as is
        if (session_path / filename).exists():
            files_found.append(session_path / filename)

        # try the filename with the session name in front of it
        if (session_path / (session_name + "_" + filename)).exists():
            files_found.append(session_path / (session_name + "_" + filename))

    return files_found


def _find_extra_files_with_extension(session_path: Path, extension: str) -> list[Path]:
    """
    Find files with the given extension anywhere in the session folder but the root.

    Parameters
    ----------
    session_path : Path
        Path to the session folder.
    extension : str
        The extension to look for.

    Returns
    -------
    List of paths to the files found.
    """
    # list all files with the given extension excluding the ones in root
    return [p for p in session_path.glob(f"**/*{extension}") if p.parent != session_path]


def _find_extra_files_with_extensions(
    session_path: Path, extensions: tuple[str, ...]
) -> list[Path]:
    """
    Find files with all the given extensions anywhere in the session folder but the root.

    Parameters
    ----------
    session_path : Path
        Path to the session folder.
    extensions : tuple[str, ...]
        The list of extensions to look for.

    Returns
    -------
    List of paths to the files found.
    """
    extra_files_with_allowed_extensions = []
    for extension in extensions:
        extra_files_with_allowed_extensions.extend(
            _find_extra_files_with_extension(session_path, extension)
        )

    return extra_files_with_allowed_extensions


def _rename_files_to_start_with_session_name(
    session_path: Path,
    files_to_rename: list[Path],
) -> None:
    """
    If they don't already start with the session name, rename the files on disk
    by prepending the session name to the original filename.

    Example: "comment.txt" -> "M020_2023_11_30_11_20_comment.txt"

    Parameters
    ----------
    session_path : Path
        Path to the session folder.
    files_to_rename : list[Path]
        List of paths to the files to rename.

    Returns
    -------
    None, but the files are renamed on disk.
    """
    for file_path in files_to_rename:
        # if it already has the session name in the beginning, skip it
        if file_path.name.startswith(session_path.name):
            continue

        new_filename = session_path.name + "_" + file_path.name
        new_path = file_path.with_name(new_filename)
        if new_path.exists():
            raise FileExistsError(f"Aborting renaming. {new_path} already exists.")
        file_path.rename(new_path)


def _rename_whitelisted_files_in_root(
    session_path: Path,
    whitelisted_files_in_root: tuple[str, ...],
) -> None:
    """
    Find and rename the files with the given filenames in the root of the session folder
    such that they start with the session name.

    Example: "comment.txt" -> "M020_2023_11_30_11_20_comment.txt"

    Parameters
    ----------
    session_path : Path
        Path to the session folder.
    whitelisted_files_in_root : tuple[str, ...]
        A tuple of filenames that are allowed in the root of the session directory.
        These will be usually read from the config file.

    Returns
    -------
    None, but the files are renamed on disk.
    """
    _rename_files_to_start_with_session_name(
        session_path,
        _find_whitelisted_files_in_root(session_path, whitelisted_files_in_root),
    )


def _rename_extra_files_with_extension(session_path: Path, extension: str) -> None:
    """
    Find and rename the files with the given extension within the session folder (except the
    root) such that they start with the session name.

    Example: "traj_plan.txt" -> "M020_2023_11_30_11_20_traj_plan.txt"

    Parameters
    ----------
    session_path : Path
        Path to the session folder.
    extension : str
        The extension to look for.

    Returns
    -------
    None, but the files are renamed on disk.
    """
    _rename_files_to_start_with_session_name(
        session_path,
        _find_extra_files_with_extension(session_path, extension),
    )


def rename_extra_files_in_session(
    session_path: Path,
    whitelisted_files_in_root: tuple[str, ...],
    extensions_to_rename_and_upload: tuple[str, ...],
) -> None:
    """
    Find and rename the extra files in the session such that they start with the session name.
    These files are:
        - files with the given filenames in the root of the session folder
        - files with the given extensions anywhere in the session folder but the root

    Example: "traj_plan.txt" -> "M020_2023_11_30_11_20_traj_plan.txt"

    Parameters
    ----------
    session_path : Path
        Path to the session folder.
    whitelisted_files_in_root : tuple[str, ...]
        A tuple of filenames that are allowed in the root of the session directory.
        These will be usually read from the config file.
    extensions_to_rename_and_upload : tuple[str, ...]
        The extension to look for.

    Returns
    -------
    None, but the files are renamed on disk.
    """
    _rename_whitelisted_files_in_root(session_path, whitelisted_files_in_root)

    for extension in extensions_to_rename_and_upload:
        _rename_extra_files_with_extension(session_path, extension)
