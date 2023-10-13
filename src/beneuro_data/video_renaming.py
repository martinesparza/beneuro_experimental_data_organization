from pathlib import Path

from beneuro_data.data_validation import validate_raw_videos_of_session


def rename_raw_videos_of_session(
    session_path: Path,
    subject_name: str,
):
    video_extension = ".avi"

    session_name = session_path.name
    expected_video_folder_name = session_name + "_cameras"
    expected_video_folder_path = session_path / expected_video_folder_name

    expected_video_filename_start = f"{session_name}_camera_"

    try:
        validate_raw_videos_of_session(session_path, subject_name, False)
    except ValueError as e:
        if "file in unexpected location" in str(e):
            # print("Wrong folder, don't know about the filenames yet")

            found_video_folders = set(
                video_file_path.parent
                for video_file_path in session_path.glob(f"**/*{video_extension}")
            )

            assert len(found_video_folders) == 1
            found_video_folder = found_video_folders.pop()

            if expected_video_folder_path.exists():
                raise FileExistsError(
                    f"Aborting renaming. {found_video_folder} and {expected_video_folder_path} both exist"
                )
            found_video_folder.rename(expected_video_folder_path)
            # print(found_video_folder, "->", expected_video_folder_path)

            # folder should be as expected, but filenames could be wrong, so try again
            rename_raw_videos_of_session(session_path, subject_name)

        if "Video filename does not start with" in str(e):
            # print("Wrong filenames, correct folder")

            # folder should be as expected
            assert expected_video_folder_path.exists()
            assert expected_video_folder_path.is_dir()

            for file in expected_video_folder_path.glob(f"*{video_extension}"):
                # assuming the camera thing is correct
                camera_id = int(file.stem.split("_")[-1])

                old_path = file
                new_path = old_path.with_stem(
                    expected_video_filename_start + str(camera_id)
                )

                if new_path.exists():
                    raise FileExistsError(f"Aborting renaming. {new_path} already exists")
                old_path.rename(new_path)
                # print(old_path, "->", new_path)

    except Exception as e:
        raise e
    else:
        print(f"{session_path} looks good, no renaming needed.")
