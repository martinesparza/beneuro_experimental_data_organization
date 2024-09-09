from pathlib import Path

from rich import print as rprint

from beneuro_data.data_validation import (
    validate_raw_videos_of_session,
    validate_session_path,
)


def rename_raw_videos_of_session(
    session_path: Path,
    subject_name: str,
    verbose: bool = False,
) -> list[tuple[Path, Path]]:
    """
    Rename the raw videos saved by Jarvis to the expected format.

    Parameters
    ----------
    session_path : Path
        Path to the session folder.
    subject_name : str
        Name of the subject, e.g. M017
    verbose: bool, default False
        List the files that were renamed.

    Returns
    -------
    list of pairs of old_path, new_path
    """
    # validate that the session's path and folder name are in the expected format
    validate_session_path(session_path, subject_name)

    # NOTE following is duplicate code, could be moved out into a function/config
    video_extension = ".avi"

    session_name = session_path.name
    expected_video_folder_name = session_name + "_cameras"
    expected_video_folder_path = session_path / expected_video_folder_name

    expected_video_filename_start = f"{session_name}_camera_"

    old_and_new_paths = []

    try:
        validate_raw_videos_of_session(session_path, subject_name, False)
    except ValueError as e:
        if "file in unexpected location" in str(e):
            # there are videos in the wrong folder, don't know about the filenames yet

            found_video_folders = set(
                video_file_path.parent
                for video_file_path in session_path.glob(f"**/*{video_extension}")
            )

            # make sure all the videos are in one folder
            assert len(found_video_folders) == 1
            found_video_folder = found_video_folders.pop()

            if expected_video_folder_path.exists():
                raise FileExistsError(
                    f"Aborting renaming. {found_video_folder} and {expected_video_folder_path} both exist"
                )

            # if the videos are in the session's root
            if found_video_folder == session_path:
                # create the expected folder and move the videos and metadata.csv in there
                expected_video_folder_path.mkdir()

                # move the videos
                for video_file_path in session_path.glob(f"**/*{video_extension}"):
                    dest_path = expected_video_folder_path / video_file_path.name
                    video_file_path.rename(dest_path)

                    old_and_new_paths.append((video_file_path, dest_path))

                # move the metadata.csv
                metadata_path = found_video_folder / "metadata.csv"
                assert metadata_path.exists()
                metadata_path.rename(expected_video_folder_path / metadata_path.name)

                old_and_new_paths.append(
                    (metadata_path, expected_video_folder_path / metadata_path.name)
                )
            # otherwise just rename the folder
            else:
                found_video_folder.rename(expected_video_folder_path)

                old_and_new_paths.append((found_video_folder, expected_video_folder_path))

            # at this point the folder should be as expected,
            # but filenames could be wrong, so try again
            # and potentially add the files to the list
            old_and_new_paths.extend(
                rename_raw_videos_of_session(session_path, subject_name, False)
            )

        if "Video filename does not start with" in str(e):
            # wrong filenames, correct folder

            # folder should be as expected
            assert expected_video_folder_path.exists()
            assert expected_video_folder_path.is_dir()

            for old_path in expected_video_folder_path.glob(f"*{video_extension}"):
                # assuming the camera's filename is somethin like Camera_0.avi
                camera_id = int(old_path.stem.split("_")[-1])

                new_path = old_path.with_stem(
                    expected_video_filename_start + str(camera_id)
                )

                if new_path.exists():
                    raise FileExistsError(f"Aborting renaming. {new_path} already exists")

                old_path.rename(new_path)
                old_and_new_paths.append((old_path, new_path))

    except Exception as e:
        raise e

    if verbose:
        if len(old_and_new_paths) == 0:
            rprint(f"{session_path} looks good, no renaming needed.")
        else:
            for old_path, new_path in old_and_new_paths:
                rprint(
                    str(old_path.relative_to(session_path)),
                    "->",
                    str(new_path.relative_to(session_path)),
                )

    return old_and_new_paths
