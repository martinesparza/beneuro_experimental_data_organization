from pathlib import Path


def _find_whitelisted_files_in_root(
    session_path: Path,
    whitelisted_files_in_root: tuple[str, ...],
) -> list[Path]:
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
    # list all files with the given extension excluding the ones in root
    return [p for p in session_path.glob(f"**/*{extension}") if p.parent != session_path]


def _rename_files_to_start_with_session_name(
    session_path: Path,
    files_to_rename: list[Path],
) -> None:
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
    _rename_files_to_start_with_session_name(
        session_path,
        _find_whitelisted_files_in_root(session_path, whitelisted_files_in_root),
    )


def _rename_extra_files_with_extension(session_path: Path, extension: str) -> None:
    _rename_files_to_start_with_session_name(
        session_path,
        _find_extra_files_with_extension(session_path, extension),
    )


def rename_extra_files_in_session(
    session_path: Path,
    whitelisted_files_in_root: tuple[str, ...],
    extensions_to_rename_and_upload: tuple[str, ...],
) -> None:
    _rename_whitelisted_files_in_root(session_path, whitelisted_files_in_root)

    for extension in extensions_to_rename_and_upload:
        _rename_extra_files_with_extension(session_path, extension)
