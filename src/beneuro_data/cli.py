from pathlib import Path
from typing_extensions import Annotated

import typer
from rich import print

from beneuro_data.data_validation import validate_raw_session
from beneuro_data.data_transfer import upload_raw_session
from beneuro_data.config import config

app = typer.Typer()


@app.command()
def validate_sessions(
    subject_name: Annotated[str, typer.Argument(help="Subject name.")],
    processing_level: Annotated[
        str,
        typer.Argument(help="Processing level of the session. raw or processed."),
    ],
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
    Validate (raw) experimental data in all sessions of a given subject.

    See options for which data to check and ignore.
    """

    if processing_level != "raw":
        raise NotImplementedError("Sorry, only raw data is supported for now.")

    if all([not check_behavior, not check_ephys, not check_videos]):
        raise ValueError("At least one data type must be checked.")

    subject_path = config.LOCAL_PATH / processing_level / subject_name

    for session_path in subject_path.iterdir():
        if session_path.is_dir():
            try:
                validate_raw_session(
                    session_path,
                    subject_name,
                    check_behavior,
                    check_ephys,
                    check_videos,
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
    print(config.json(indent=4))


# @app.command()
# def print_path(session_path: Path):
#    print(session_path.absolute())


if __name__ == "__main__":
    app()
