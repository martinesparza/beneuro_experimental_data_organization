from pathlib import Path


def rename_whitelisted_files_in_root(
    session_path: Path,
    whitelisted_files_in_root: tuple[str, ...],
) -> list[Path]:
    new_paths = []
    for filename in whitelisted_files_in_root:
        current_path = session_path / filename
        if current_path.exists():
            new_filename = session_path.name + "_" + filename
            new_path = session_path / new_filename
            if new_path.exists():
                raise FileExistsError(f"Aborting renaming. {new_path} already exists.")
            current_path.rename(new_path)
            new_paths.append(new_path)

    return new_paths


def rename_extra_files_with_extension(session_path: Path, extension: str) -> list[Path]:
    # list all files with the given extension excluding the ones in root
    files_found = [
        p for p in session_path.glob(f"**/*{extension}") if p.parent != session_path
    ]

    new_paths = []
    for file_path in files_found:
        # if it already has the session name in the beginning, skip it
        if file_path.name.startswith(session_path.name):
            continue

        new_filename = session_path.name + "_" + file_path.name
        new_path = file_path.with_name(new_filename)
        if new_path.exists():
            raise FileExistsError(f"Aborting renaming. {new_path} already exists.")
        file_path.rename(new_path)
        new_paths.append(new_path)

    return new_paths
