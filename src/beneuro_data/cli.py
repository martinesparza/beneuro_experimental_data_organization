import datetime
from pathlib import Path
from typing import List, Optional

import typer
from rich import print
from typing_extensions import Annotated

from beneuro_data.config import _get_env_path, _get_package_path, _load_config
from beneuro_data.data_transfer import download_raw_session, upload_raw_session
from beneuro_data.data_validation import validate_raw_session
from beneuro_data.extra_file_handling import rename_extra_files_in_session
from beneuro_data.query_sessions import (
    get_last_session_path,
    list_all_sessions_on_day,
    list_subject_sessions_on_day,
)
from beneuro_data.update_bnd import check_for_updates, update_bnd
from beneuro_data.video_renaming import rename_raw_videos_of_session

app = typer.Typer()


@app.command()
def nwb_to_pyaldata(
    local_session_path: Annotated[
        Path,
        typer.Argument(
            help="Path to processed session directory"
        ),
    ],
    default_channel_map: Annotated[
        bool,
        typer.Option(
            "--default-channel-map/--custom-channel-map",
            help="Use the default channel from .nwb file or use a custom one available in processed session folder"),
    ] = True,
):
    """
    Convert a session's data to from NWB to pyaldata form, using a default channel map or a custom one

    """
    from beneuro_data.conversion.convert_nwb_to_pyaldata import convert_nwb_to_pyaldata

    config = _load_config()

    # TODO: Make custom channel map option in case we dont agree with pinpoint

    # TODO: Implement main argument to be session name and automatically look for NWB
    #  file in processed

    if not local_session_path.absolute().is_dir():
        raise ValueError("Session path must be a directory.")
    if not local_session_path.absolute().exists():
        raise ValueError("Session path does not exist.")
    if not local_session_path.absolute().is_relative_to(config.LOCAL_PATH):
        raise ValueError("Session path must be inside the local root folder.")

    nwbfiles = list(local_session_path.glob('*.nwb'))
    if len(nwbfiles) > 1:
        raise FileNotFoundError("Too many nwb files in session folder!")
    elif not nwbfiles:
        raise FileNotFoundError("No .nwb file found in session folder!")

    nwbfile_path = nwbfiles[0].absolute()

    # Run conversion
    convert_nwb_to_pyaldata(
        nwbfile_path,
    )


@app.command()
def to_nwb(
    local_session_path: Annotated[
        Path,
        typer.Argument(help="Path to session directory. Can be relative or absolute"),
    ],
    subject_name: Annotated[
        str,
        typer.Argument(
            help="Name of the subject the session belongs to. (Used for confirmation.)"
        ),
    ],
    run_kilosort: Annotated[
        bool,
        typer.Option("--kilosort/--no-kilosort", help="Run Kilosort 4 or not"),
    ] = True,
    sort_probe: Annotated[
        Optional[List[str]],
        typer.Option(
            help="Probes to run Kilosort on. If not given, all probes are processed."
        ),
    ] = None,
    clean_up_temp_files: Annotated[
        bool,
        typer.Option(
            "--clean-up-temp-files/--keep-temp-files",
            help="Keep the binary files created by Kilosort or not. They are huge, but needed for running Phy later.",
        ),
    ] = True,
    verbose_kilosort: Annotated[
        bool,
        typer.Option(help="Run Kilosort in verbose mode."),
    ] = True,
):
    """
    Convert a session's data to NWB, optionally running Kilosort before.

    Includes:

        - behavioral events and ball position from PyControl

        - session and subject details from the .profile file

        - spikes from Kilosort

        - pose estimation from sleap-anipose



    Note that you will need some extra dependencies that are not installed by default.

    You can install them by running `poetry install --with processing` in bnd's root folder (which you can with with `bnd show-config`).



    \b
    Basic usage:
        `bnd to-nwb . M027`

    \b
    Skip running Kilosort:
        `bnd to-nwb . M027 --no-kilosort`

    \b
    Running Kilosort on only selected probes:
        `bnd to-nwb . M027 --sort-probe imec0 --sort-probe imec1`
    """
    # this will throw an error if the dependencies are not available
    from beneuro_data.conversion.convert_to_nwb import convert_to_nwb

    config = _load_config()

    if not local_session_path.absolute().is_dir():
        raise ValueError("Session path must be a directory.")
    if not local_session_path.absolute().exists():
        raise ValueError("Session path does not exist.")
    if not local_session_path.absolute().is_relative_to(config.LOCAL_PATH):
        raise ValueError("Session path must be inside the local root folder.")

    if len(sort_probe) != 0:
        if len(set(sort_probe)) != len(sort_probe):
            raise ValueError(f"Duplicate probe names found in {sort_probe}.")
        # append .ap to each probe name
        stream_names_to_process = [f"{probe}.ap" for probe in sort_probe]
        # check that they are all in the session folder somewhere
        for stream_name in stream_names_to_process:
            if len(list(local_session_path.glob(f"**/*{stream_name}.bin"))) == 0:
                raise ValueError(
                    f"No file found for {stream_name} in {local_session_path.absolute()}"
                )
    else:
        stream_names_to_process = None

    convert_to_nwb(
        local_session_path.absolute(),
        subject_name,
        config.LOCAL_PATH,
        config.WHITELISTED_FILES_IN_ROOT,
        config.EXTENSIONS_TO_RENAME_AND_UPLOAD,
        run_kilosort,
        stream_names_to_process,
        clean_up_temp_files,
        verbose_kilosort,
    )


@app.command()
def download_last(
    subject_name: Annotated[
        str,
        typer.Argument(help="Name of the subject the session belongs to."),
    ],
    include_behavior: Annotated[
        bool,
        typer.Option(
            "--include-behavior/--ignore-behavior",
            help="Download behavioral data or not.",
        ),
    ] = None,
    include_ephys: Annotated[
        bool,
        typer.Option(
            "--include-ephys/--ignore-ephys",
            help="Download ephys data or not.",
        ),
    ] = None,
    include_videos: Annotated[
        bool,
        typer.Option(
            "--include-videos/--ignore-videos",
            help="Download video data or not.",
        ),
    ] = None,
    # include_extra_files: Annotated[
    #    bool,
    #    typer.Option(
    #        "--include-extra-files/--ignore-extra-files",
    #        help="Download extra files that are created by the experimenter or other software.",
    #    ),
    # ] = True,
    processing_level: Annotated[
        str, typer.Argument(help="Processing level of the session. raw or processed.")
    ] = "raw",
):
    """
    Download (raw) experimental data in the last session of a subject from the remote server.

    Example usage:
        `bnd upload-last M017`
    """
    if processing_level != "raw":
        raise NotImplementedError("Sorry, only raw data is supported for now.")

    config = _load_config()

    subject_path = config.REMOTE_PATH / processing_level / subject_name

    # first get the last valid session
    # and ask the user if this is really the session they want to upload
    last_session_path = get_last_session_path(subject_path, subject_name).absolute()

    typer.confirm(
        f"{last_session_path.name} is the last session for {subject_name}. Download?",
        abort=True,
    )

    # then ask the user if they want to include behavior, ephys, and videos
    # only ask the ones that are not specified as a CLI option
    if include_behavior is None:
        include_behavior = typer.confirm("Include behavior?")
    if include_ephys is None:
        include_ephys = typer.confirm("Include ephys?")
    if include_videos is None:
        include_videos = typer.confirm("Include videos?")

    if all([not include_behavior, not include_ephys, not include_videos]):
        raise ValueError("At least one data type must be included.")

    download_raw_session(
        last_session_path,
        subject_name,
        config.LOCAL_PATH,
        config.REMOTE_PATH,
        include_behavior,
        include_ephys,
        include_videos,
        config.WHITELISTED_FILES_IN_ROOT,
        config.EXTENSIONS_TO_RENAME_AND_UPLOAD,
    )

    return True


@app.command()
def download_session(
    remote_session_path: Annotated[
        Path, typer.Argument(help="Path to session directory. Can be relative or absolute.")
    ],
    subject_name: Annotated[
        str,
        typer.Argument(
            help="Name of the subject the session belongs to. (Used for confirmation.)"
        ),
    ],
    include_behavior: Annotated[
        bool,
        typer.Option(
            "--include-behavior/--ignore-behavior",
            help="Download behavioral data or not.",
            prompt=True,
        ),
    ],
    include_ephys: Annotated[
        bool,
        typer.Option(
            "--include-ephys/--ignore-ephys",
            help="Download ephys data or not.",
            prompt=True,
        ),
    ],
    include_videos: Annotated[
        bool,
        typer.Option(
            "--include-videos/--ignore-videos",
            help="Download video data or not.",
            prompt=True,
        ),
    ],
    # extra files are always included
    # TODO do we want this to stay this way?
    # include_extra_files: Annotated[
    #    bool,
    #    typer.Option(
    #        "--include-extra-files/--ignore-extra-files",
    #        help="Download extra files that are created by the experimenter or other software.",
    #    ),
    # ] = True,
    processing_level: Annotated[
        str, typer.Argument(help="Processing level of the session. raw or processed.")
    ] = "raw",
):
    """
    Download (raw) experimental data in a given session from the remote server.

    Example usage after navigating to session folder on RDS:
        `bnd download-session . M017`
    """
    if processing_level != "raw":
        raise NotImplementedError("Sorry, only raw data is supported for now.")

    if all([not include_behavior, not include_ephys, not include_videos]):
        raise ValueError("At least one data type must be included.")

    config = _load_config()

    download_raw_session(
        remote_session_path.absolute(),
        subject_name,
        config.LOCAL_PATH,
        config.REMOTE_PATH,
        include_behavior,
        include_ephys,
        include_videos,
        config.WHITELISTED_FILES_IN_ROOT,
        config.EXTENSIONS_TO_RENAME_AND_UPLOAD,
    )

    return True


@app.command()
def dl(
    session_name: Annotated[
        str, typer.Argument(help="Name of session: M123_2000_02_03_14_15")
    ],
    include_behavior: Annotated[
        bool,
        typer.Option(
            "--include-behavior/--ignore-behavior",
            "-b/-B",
            help="Download behavioral data (-b) or not (-B).",
        ),
    ] = True,
    include_ephys: Annotated[
        bool,
        typer.Option(
            "--include-ephys/--ignore-ephys",
            "-e/-E",
            help="Download ephys data (-e) or not (-E).",
        ),
    ] = False,
    include_videos: Annotated[
        bool,
        typer.Option(
            "--include-videos/--ignore-videos",
            "-v/-V",
            help="Download video data (-v) or not (-V).",
        ),
    ] = False,
    processing_level: Annotated[
        str, typer.Argument(help="Processing level of the session. raw or processed.")
    ] = "raw",
):
    """
    Download (raw) experimental data from a given session from the remote server.

    Example usage to download everything:
        `bnd dl M017_2024_03_12_18_45 -ev`
    """
    animal = session_name[:4]
    if processing_level != "raw":
        raise NotImplementedError("Sorry, only raw data is supported for now.")

    if all([not include_behavior, not include_ephys, not include_videos]):
        raise ValueError("At least one data type must be included.")

    config = _load_config()

    download_raw_session(
        remote_session_path=config.REMOTE_PATH / processing_level / animal / session_name,
        subject_name=animal,
        local_base_path=config.LOCAL_PATH,
        remote_base_path=config.REMOTE_PATH,
        include_behavior=include_behavior,
        include_ephys=include_ephys,
        include_videos=include_videos,
        whitelisted_files_in_root=config.WHITELISTED_FILES_IN_ROOT,
        allowed_extensions_not_in_root=config.EXTENSIONS_TO_RENAME_AND_UPLOAD,
    )
    print(f"Session {session_name} downloaded.")

    return True


@app.command()
def kilosort_session(
    local_session_path: Annotated[
        Path, typer.Argument(help="Path to session directory. Can be relative or absolute")
    ],
    subject_name: Annotated[
        str,
        typer.Argument(
            help="Name of the subject the session belongs to. (Used for confirmation.)"
        ),
    ],
    probes: Annotated[
        Optional[List[str]],
        typer.Argument(
            help="List of probes to process. If nothing is given, all probes are processed."
        ),
    ] = None,
    clean_up_temp_files: Annotated[
        bool,
        typer.Option(
            "--clean-up-temp-files/--keep-temp-files",
            help="Keep the binary files created or not. They are huge, but needed for running Phy later.",
        ),
    ] = True,
    verbose: Annotated[
        bool, typer.Option(help="Print info about what is being run.")
    ] = True,
):
    """
    Run Kilosort 4 on a session and save the results in the processed folder.

    Note that you will need some extra dependencies that are not installed by default. You can install them by running `poetry install --with processing` in bnd's root folder.

    \b
    Basic usage:
        `bnd kilosort-session . M020`

    \b
    Only sorting specific probes:
        `bnd kilosort-session . M020 imec0`
        `bnd kilosort-session . M020 imec0 imec1`

    \b
    Keeping binary files useful for Phy:
        `bnd kilosort-session . M020 --keep-temp-files`

    \b
    Suppressing output:
        `bnd kilosort-session . M020 --no-verbose`
    """
    # this will throw an error if the dependencies are not available
    from beneuro_data.spike_sorting import run_kilosort_on_session_and_save_in_processed

    config = _load_config()

    if not local_session_path.absolute().is_dir():
        raise ValueError("Session path must be a directory.")
    if not local_session_path.absolute().exists():
        raise ValueError("Session path does not exist.")
    if not local_session_path.absolute().is_relative_to(config.LOCAL_PATH):
        raise ValueError("Session path must be inside the local root folder.")

    if len(probes) != 0:
        if len(set(probes)) != len(probes):
            raise ValueError(f"Duplicate probe names found in {probes}.")
        # append .ap to each probe name
        stream_names_to_process = [f"{probe}.ap" for probe in probes]
        # check that they are all in the session folder somewhere
        for stream_name in stream_names_to_process:
            if len(list(local_session_path.glob(f"**/*{stream_name}.bin"))) == 0:
                raise ValueError(
                    f"No file found for {stream_name} in {local_session_path.absolute()}"
                )
    else:
        stream_names_to_process = None

    run_kilosort_on_session_and_save_in_processed(
        local_session_path.absolute(),
        subject_name,
        config.LOCAL_PATH,
        config.EXTENSIONS_TO_RENAME_AND_UPLOAD,
        stream_names_to_process=stream_names_to_process,
        clean_up_temp_files=clean_up_temp_files,
        verbose=verbose,
    )


@app.command()
def validate_session(
    session_path: Annotated[
        Path, typer.Argument(help="Path to session directory. Can be relative or absolute")
    ],
    subject_name: Annotated[
        str,
        typer.Argument(
            help="Name of the subject the session belongs to. (Used for confirmation.)"
        ),
    ],
    processing_level: Annotated[
        str, typer.Argument(help="Processing level of the session. raw or processed.")
    ] = "raw",
    check_behavior: Annotated[
        bool,
        typer.Option(
            "--check-behavior/--ignore-behavior", help="Check behavioral data or not."
        ),
    ] = True,
    check_ephys: Annotated[
        bool,
        typer.Option("--check-ephys/--ignore-ephys", help="Check ephys data or not."),
    ] = True,
    check_videos: Annotated[
        bool,
        typer.Option("--check-videos/--ignore-videos", help="Check videos data or not."),
    ] = True,
):
    """
    Validate experimental data in a given session.

    E.g. to check all kinds of data in the current working directory which is supposedly a session of subject called M017:

        `bnd validate-session . M017`
    """
    if processing_level != "raw":
        raise NotImplementedError("Sorry, only raw data is supported for now.")

    if all([not check_behavior, not check_ephys, not check_videos]):
        raise ValueError("At least one data type must be checked.")

    if not session_path.absolute().is_dir():
        raise ValueError("Session path must be a directory.")
    if not session_path.absolute().exists():
        raise ValueError("Session path does not exist.")

    config = _load_config()

    validate_raw_session(
        session_path.absolute(),
        subject_name,
        check_behavior,
        check_ephys,
        check_videos,
        config.WHITELISTED_FILES_IN_ROOT,
        config.EXTENSIONS_TO_RENAME_AND_UPLOAD,
    )


@app.command()
def validate_last(
    subject_name: Annotated[
        str,
        typer.Argument(
            help="Name of the subject the session belongs to. (Used for confirmation.)"
        ),
    ],
    processing_level: Annotated[
        str, typer.Argument(help="Processing level of the session. raw or processed.")
    ] = "raw",
    check_locally: Annotated[
        bool,
        typer.Option("--local/--remote", help="Check local or remote data."),
    ] = True,
    check_behavior: Annotated[
        bool,
        typer.Option(
            "--check-behavior/--ignore-behavior", help="Check behavioral data or not."
        ),
    ] = True,
    check_ephys: Annotated[
        bool,
        typer.Option("--check-ephys/--ignore-ephys", help="Check ephys data or not."),
    ] = True,
    check_videos: Annotated[
        bool,
        typer.Option("--check-videos/--ignore-videos", help="Check videos data or not."),
    ] = True,
):
    """
    Validate experimental data in the last session of a subject.

    Example usage:
        `bnd validate-last M017`
    """
    if processing_level != "raw":
        raise NotImplementedError("Sorry, only raw data is supported for now.")

    if all([not check_behavior, not check_ephys, not check_videos]):
        raise ValueError("At least one data type must be checked.")

    config = _load_config()

    root_path = config.LOCAL_PATH if check_locally else config.REMOTE_PATH

    subject_path = root_path / processing_level / subject_name

    # get the last valid session
    last_session_path = get_last_session_path(subject_path, subject_name).absolute()

    print(f"[bold]Last session found: [green]{last_session_path.name}", end="\n\n")

    validate_raw_session(
        last_session_path,
        subject_name,
        check_behavior,
        check_ephys,
        check_videos,
        config.WHITELISTED_FILES_IN_ROOT,
        config.EXTENSIONS_TO_RENAME_AND_UPLOAD,
    )


@app.command()
def rename_videos(
    session_path: Annotated[
        Path, typer.Argument(help="Path to session directory. Can be relative or absolute")
    ],
    subject_name: Annotated[
        str,
        typer.Argument(
            help="Name of the subject the session belongs to. (Used for confirmation.)"
        ),
    ],
    processing_level: Annotated[
        str, typer.Argument(help="Processing level of the session. raw or processed.")
    ] = "raw",
    verbose: Annotated[
        bool,
        typer.Option(help="Print the list of files that were renamed."),
    ] = False,
):
    """
    Rename the raw videos saved by Jarvis during a session to the convention we use.

    Example usage:

        `bnd rename-videos . M017 --verbose`

        `bnd rename-videos /absolute/path/to/session M017 --verbose`
    """
    if processing_level != "raw":
        raise NotImplementedError("Sorry, only raw data is supported for now.")

    if not session_path.absolute().is_dir():
        raise ValueError("Session path must be a directory.")
    if not session_path.absolute().exists():
        raise ValueError("Session path does not exist.")

    rename_raw_videos_of_session(
        session_path.absolute(),
        subject_name,
        verbose,
    )


@app.command()
def rename_extra_files(
    session_path: Annotated[
        Path, typer.Argument(help="Path to session directory. Can be relative or absolute")
    ],
    subject_name: Annotated[
        str,
        typer.Argument(
            help="Name of the subject the session belongs to. (Used for confirmation.)"
        ),
    ],
):
    """
    Rename the extra files (found based on the config) to the convention we use.

    Example usage:

        `bnd rename-extra-files . M017`

        `bnd rename-extra-files /absolute/path/to/session M017`
    """
    if not session_path.absolute().is_dir():
        raise ValueError("Session path must be a directory.")
    if not session_path.absolute().exists():
        raise ValueError("Session path does not exist.")

    config = _load_config()

    rename_extra_files_in_session(
        session_path.absolute(),
        tuple(config.WHITELISTED_FILES_IN_ROOT),
        tuple(config.EXTENSIONS_TO_RENAME_AND_UPLOAD),
    )


@app.command()
def upload_session(
    local_session_path: Annotated[
        Path, typer.Argument(help="Path to session directory. Can be relative or absolute")
    ],
    subject_name: Annotated[
        str,
        typer.Argument(
            help="Name of the subject the session belongs to. (Used for confirmation.)"
        ),
    ],
    include_behavior: Annotated[
        bool,
        typer.Option(
            "--include-behavior/--ignore-behavior",
            help="Upload behavioral data or not.",
            prompt=True,
        ),
    ],
    include_ephys: Annotated[
        bool,
        typer.Option(
            "--include-ephys/--ignore-ephys", help="Upload ephys data or not.", prompt=True
        ),
    ],
    include_videos: Annotated[
        bool,
        typer.Option(
            "--include-videos/--ignore-videos",
            help="Upload video data or not.",
            prompt=True,
        ),
    ],
    include_extra_files: Annotated[
        bool,
        typer.Option(
            "--include-extra-files/--ignore-extra-files",
            help="Upload extra files that are created by the experimenter or other software.",
        ),
    ] = True,
    rename_videos_first: Annotated[
        bool,
        typer.Option(
            "--rename-videos/--no-rename-videos",
            help="Rename videos before validating and uploading. Defaults to True if including videos, to False if not.",
        ),
    ] = None,
    rename_extra_files_first: Annotated[
        bool,
        typer.Option(
            "--rename-extra-files/--no-rename-extra-files",
            help="Rename extra files (e.g. comment.txt) before validating and uploading.",
        ),
    ] = True,
    processing_level: Annotated[
        str, typer.Argument(help="Processing level of the session. raw or processed.")
    ] = "raw",
):
    """
    Upload (raw) experimental data in a given session to the remote server.

    Example usage:
        `bnd upload-session . M017`
    """
    if processing_level != "raw":
        raise NotImplementedError("Sorry, only raw data is supported for now.")

    if all([not include_behavior, not include_ephys, not include_videos]):
        raise ValueError("At least one data type must be included.")

    # if videos are included, rename them first if not specified otherwise
    if rename_videos_first is None:
        rename_videos_first = include_videos

    if rename_videos_first and not include_videos:
        raise ValueError(
            "Do not rename videos if you're not uploading them. (Meaning --ignore-videos and --rename-videos are not allowed together.)"
        )

    config = _load_config()

    upload_raw_session(
        local_session_path.absolute(),
        subject_name,
        config.LOCAL_PATH,
        config.REMOTE_PATH,
        include_behavior,
        include_ephys,
        include_videos,
        include_extra_files,
        config.WHITELISTED_FILES_IN_ROOT,
        config.EXTENSIONS_TO_RENAME_AND_UPLOAD,
        rename_videos_first,
        rename_extra_files_first,
    )

    return True


@app.command()
def up(
    session_or_animal_name: Annotated[
        str, typer.Argument(help="Animal or session name: M123 or M123_2000_02_03_14_15")
    ],
    include_behavior: Annotated[
        bool,
        typer.Option(
            "--include-behavior/--ignore-behavior",
            "-b/-B",
            help="Upload behavioral data (-b) or not (-B).",
        ),
    ] = True,
    include_ephys: Annotated[
        bool,
        typer.Option(
            "--include-ephys/--ignore-ephys",
            "-e/-E",
            help="Upload ephys data (-e) or not (-E).",
        ),
    ] = False,
    include_videos: Annotated[
        bool,
        typer.Option(
            "--include-videos/--ignore-videos",
            "-v/-V",
            help="Upload video data (-v) or not (-V).",
        ),
    ] = False,
    include_extra_files: Annotated[
        bool,
        typer.Option(
            "--include-extra-files/--ignore-extra-files",
            help="Upload extra files that are created by the experimenter or other software.",
        ),
    ] = True,
    rename_videos_first: Annotated[
        bool,
        typer.Option(
            "--rename/--no-rename",
            help="Rename videos before validating and uploading.",
        ),
    ] = None,
    rename_extra_files_first: Annotated[
        bool,
        typer.Option(
            "--rename-extra/--no-rename-extra",
            help="Rename extra files (e.g. comment.txt) before validating and uploading.",
        ),
    ] = True,
    processing_level: Annotated[
        str, typer.Argument(help="Processing level of the session. raw or processed.")
    ] = "raw",
):
    """
    Upload (raw) experimental data to the remote server.

    Example usage to upload everything of a given session:
        `bnd up M017_2024_03_12_18_45 -ev`
    Example to upload the videos and ephys of the last session of a subject:
        `bnd up M017 -evB`
    """
    animal = session_or_animal_name[:4]

    if processing_level != "raw":
        raise NotImplementedError("Sorry, only raw data is supported for now.")

    if all([not include_behavior, not include_ephys, not include_videos]):
        raise ValueError("At least one data type must be included.")

    # if videos are included, rename them first if not specified otherwise
    if rename_videos_first is None:
        rename_videos_first = include_videos

    if rename_videos_first and not include_videos:
        raise ValueError(
            "Do not rename videos if you're not uploading them. (Meaning --ignore-videos and --rename-videos are not allowed together.)"
        )

    config = _load_config()

    if len(session_or_animal_name) > 4:  # session name is given
        up_done = upload_raw_session(
            config.LOCAL_PATH / processing_level / animal / session_or_animal_name,
            animal,
            config.LOCAL_PATH,
            config.REMOTE_PATH,
            include_behavior,
            include_ephys,
            include_videos,
            include_extra_files,
            config.WHITELISTED_FILES_IN_ROOT,
            config.EXTENSIONS_TO_RENAME_AND_UPLOAD,
            rename_videos_first,
            rename_extra_files_first,
        )
        message = f"Session {session_or_animal_name} uploaded."
    else:  # only animal name is given
        up_done = upload_last(
            animal,
            include_behavior,
            include_ephys,
            include_videos,
            include_extra_files,
            rename_videos_first,
            rename_extra_files_first,
            processing_level,
        )
        message = f"Last session of {animal} uploaded."
    if up_done:
        print(message)

    return True


@app.command()
def upload_last(
    subject_name: Annotated[
        str,
        typer.Argument(help="Name of the subject the session belongs to."),
    ],
    include_behavior: Annotated[
        bool,
        typer.Option(
            "--include-behavior/--ignore-behavior",
            help="Upload behavioral data or not.",
        ),
    ] = None,
    include_ephys: Annotated[
        bool,
        typer.Option(
            "--include-ephys/--ignore-ephys",
            help="Upload ephys data or not.",
        ),
    ] = None,
    include_videos: Annotated[
        bool,
        typer.Option(
            "--include-videos/--ignore-videos",
            help="Upload video data or not.",
        ),
    ] = None,
    include_extra_files: Annotated[
        bool,
        typer.Option(
            "--include-extra-files/--ignore-extra-files",
            help="Upload extra files that are created by the experimenter or other software.",
        ),
    ] = True,
    rename_videos_first: Annotated[
        bool,
        typer.Option(
            "--rename-videos/--no-rename-videos",
            help="Rename videos before validating and uploading. Defaults to True if including videos, to False if not.",
        ),
    ] = None,
    rename_extra_files_first: Annotated[
        bool,
        typer.Option(
            "--rename-extra-files/--no-rename-extra-files",
            help="Rename extra files (e.g. comment.txt) before validating and uploading.",
        ),
    ] = True,
    processing_level: Annotated[
        str, typer.Argument(help="Processing level of the session. raw or processed.")
    ] = "raw",
):
    """
    Upload (raw) experimental data in the last session of a subject to the remote server.

    Example usage:
        `bnd upload-last M017`
    """
    if processing_level != "raw":
        raise NotImplementedError("Sorry, only raw data is supported for now.")

    config = _load_config()

    subject_path = config.LOCAL_PATH / processing_level / subject_name

    # first get the last valid session
    # and ask the user if this is really the session they want to upload
    last_session_path = get_last_session_path(subject_path, subject_name).absolute()

    typer.confirm(
        f"{last_session_path.name} is the last session for {subject_name}. Upload?",
        abort=True,
    )

    # then ask the user if they want to include behavior, ephys, and videos
    # only ask the ones that are not specified as a CLI option
    if include_behavior is None:
        include_behavior = typer.confirm("Include behavior?")
    if include_ephys is None:
        include_ephys = typer.confirm("Include ephys?")
    if include_videos is None:
        include_videos = typer.confirm("Include videos?")

    if all([not include_behavior, not include_ephys, not include_videos]):
        raise ValueError("At least one data type must be included.")

    # if videos are included, rename them first if not specified otherwise
    if rename_videos_first is None:
        rename_videos_first = include_videos

    if rename_videos_first and not include_videos:
        raise ValueError(
            "Do not rename videos if you're not uploading them. (Meaning --ignore-videos and --rename-videos are not allowed together.)"
        )

    upload_raw_session(
        last_session_path,
        subject_name,
        config.LOCAL_PATH,
        config.REMOTE_PATH,
        include_behavior,
        include_ephys,
        include_videos,
        include_extra_files,
        config.WHITELISTED_FILES_IN_ROOT,
        config.EXTENSIONS_TO_RENAME_AND_UPLOAD,
        rename_videos_first,
        rename_extra_files_first,
    )

    return True


@app.command()
def validate_sessions(
    subject_name: Annotated[str, typer.Argument(help="Subject name.")],
    processing_level: Annotated[
        str,
        typer.Argument(help="Processing level of the session. raw or processed."),
    ],
    check_locally: Annotated[
        bool,
        typer.Option("--local/--remote", help="Check local or remote data."),
    ] = True,
    check_behavior: Annotated[
        bool,
        typer.Option(
            "--check-behavior/--ignore-behavior", help="Check behavioral data or not."
        ),
    ] = True,
    check_ephys: Annotated[
        bool,
        typer.Option("--check-ephys/--ignore-ephys", help="Check ephys data or not."),
    ] = True,
    check_videos: Annotated[
        bool,
        typer.Option("--check-videos/--ignore-videos", help="Check videos data or not."),
    ] = True,
    rename_videos_first: Annotated[
        bool,
        typer.Option(
            "--rename-videos/--no-rename-videos",
            help="Rename videos before validating and uploading.",
        ),
    ] = True,
    rename_extra_files_first: Annotated[
        bool,
        typer.Option(
            "--rename-extra-files/--no-rename-extra-files",
            help="Rename extra files (e.g. comment.txt) before validating and uploading.",
        ),
    ] = True,
):
    """
    Validate (raw) experimental data in all sessions of a given subject.

    See options for which data to check and ignore.
    """

    if processing_level != "raw":
        raise NotImplementedError("Sorry, only raw data is supported for now.")

    if all([not check_behavior, not check_ephys, not check_videos]):
        raise ValueError("At least one data type must be checked.")

    config = _load_config()

    root_path = config.LOCAL_PATH if check_locally else config.REMOTE_PATH
    subject_path = root_path / processing_level / subject_name

    for session_path in subject_path.iterdir():
        if session_path.is_dir():
            try:
                validate_raw_session(
                    session_path,
                    subject_name,
                    check_behavior,
                    check_ephys,
                    check_videos,
                    config.WHITELISTED_FILES_IN_ROOT,
                    config.EXTENSIONS_TO_RENAME_AND_UPLOAD,
                    rename_videos_first,
                    rename_extra_files_first,
                )
            except Exception as e:
                print(f"[bold red]Problem with {session_path.name}: {e.args[0]}\n")
            else:
                print(f"[bold green]{session_path.name} looking good.\n")


@app.command()
def list_today(
    processing_level: Annotated[
        str,
        typer.Argument(help="Processing level of the session. raw or processed."),
    ] = "raw",
    check_locally: Annotated[
        bool,
        typer.Option("--local/--remote", help="Check local or remote data."),
    ] = True,
) -> list[tuple[str, str]]:
    """
    List all sessions of all subjects that happened today.
    """
    if processing_level not in ["raw", "processed"]:
        raise ValueError("Processing level must be raw or processed.")

    config = _load_config()
    root_path = config.LOCAL_PATH if check_locally else config.REMOTE_PATH

    raw_or_processed_path = root_path / processing_level
    if not raw_or_processed_path.exists():
        raise FileNotFoundError(f"{raw_or_processed_path} found.")

    today = datetime.datetime.today()

    todays_sessions_with_subject = list_all_sessions_on_day(
        raw_or_processed_path, today, config.IGNORED_SUBJECT_LEVEL_DIRS
    )

    for subj, sess in todays_sessions_with_subject:
        print(f"{subj} - {sess}")


@app.command()
def validate_today(
    processing_level: Annotated[
        str,
        typer.Argument(help="Processing level of the session. raw or processed."),
    ] = "raw",
    check_locally: Annotated[
        bool,
        typer.Option("--local/--remote", help="Check local or remote data."),
    ] = True,
    check_behavior: Annotated[
        bool,
        typer.Option(
            "--check-behavior/--ignore-behavior", help="Check behavioral data or not."
        ),
    ] = True,
    check_ephys: Annotated[
        bool,
        typer.Option("--check-ephys/--ignore-ephys", help="Check ephys data or not."),
    ] = True,
    check_videos: Annotated[
        bool,
        typer.Option("--check-videos/--ignore-videos", help="Check videos data or not."),
    ] = True,
):
    """
    Validate all sessions of all subjects that happened today.
    """
    if processing_level != "raw":
        raise NotImplementedError("Sorry, only raw data is supported for now.")

    if all([not check_behavior, not check_ephys, not check_videos]):
        raise ValueError("At least one data type must be checked.")

    config = _load_config()
    root_path = config.LOCAL_PATH if check_locally else config.REMOTE_PATH
    raw_or_processed_path = root_path / processing_level

    today = datetime.datetime.today()

    for subject_path in raw_or_processed_path.iterdir():
        if not subject_path.is_dir():
            continue

        subject_name = subject_path.name
        todays_sessions_with_subject = list_subject_sessions_on_day(subject_path, today)
        for session_name in todays_sessions_with_subject:
            session_path = subject_path / session_name
            try:
                validate_raw_session(
                    session_path,
                    subject_name,
                    check_behavior,
                    check_ephys,
                    check_videos,
                    config.WHITELISTED_FILES_IN_ROOT,
                    config.EXTENSIONS_TO_RENAME_AND_UPLOAD,
                )
            except Exception as e:
                print(f"[bold red]Problem with {session_path.name}: {e.args[0]}\n")
            else:
                print(f"[bold green]{session_path.name} looking good.\n")


@app.command()
def show_config():
    """
    Show the contents of the config file.
    """
    config = _load_config()
    print(f"bnd source code is at {_get_package_path()}", end="\n\n")
    print(config.json(indent=4))


def _check_root(root_path: Path):
    assert root_path.exists(), f"{root_path} does not exist."
    assert root_path.is_dir(), f"{root_path} is not a directory."

    files_in_root = [f.stem for f in root_path.iterdir()]

    assert "raw" in files_in_root, f"No raw folder in {root_path}"
    assert "processed" in files_in_root, f"No processed folder in {root_path}"


@app.command()
def check_config():
    """
    Check that the local and remote root folders have the expected raw and processed folders.
    """
    config = _load_config()

    print(
        "Checking that local and remote root folders have the expected raw and processed folders..."
    )

    _check_root(config.LOCAL_PATH)
    _check_root(config.REMOTE_PATH)

    print("[green]Config looks good.")


@app.command()
def init():
    """
    Create a .env file to store the paths to the local and remote data storage.
    """

    # check if the file exists
    env_path = _get_env_path()

    if env_path.exists():
        print("\n[yellow]Config file already exists.\n")

        check_config()

    else:
        print("\nConfig file doesn't exist. Let's create one.")

        local_path = Path(
            typer.prompt("Enter the absolute path to the root of the local data storage")
        )
        _check_root(local_path)

        remote_path = Path(
            typer.prompt("Enter the absolute path to the root of remote data storage")
        )
        _check_root(remote_path)

        with open(env_path, "w") as f:
            f.write(f"LOCAL_PATH = {local_path}\n")
            f.write(f"REMOTE_PATH = {remote_path}\n")

        # make sure that it works
        config = _load_config()
        _check_root(config.LOCAL_PATH)
        _check_root(config.REMOTE_PATH)

        print("[green]Config file created successfully.")


@app.command()
def check_updates():
    """
    Check if there are any new commits on the repo's main branch.
    """
    check_for_updates()


@app.command()
def self_update(
    verbose: Annotated[
        bool,
        typer.Option(help="Print new commits that were pulled."),
    ] = True,
):
    """
    Update the bnd tool by pulling the latest commits from the repo's main branch.
    """
    update_bnd(print_new_commits=verbose)


if __name__ == "__main__":
    app()
