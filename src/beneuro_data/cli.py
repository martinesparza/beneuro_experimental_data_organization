import typer
from rich import print

from beneuro_data.data_validation import validate_raw_session
from beneuro_data.data_transfer import upload_raw_session
from beneuro_data.config import config

app = typer.Typer()


@app.command()
def validate_structure(
    subject_name: str,
    processing_level: str,
    include_behavior: bool = True,
    include_ephys: bool = True,
    include_videos: bool = True,
):
    subject_path = config.LOCAL_PATH / processing_level / subject_name

    for session_path in subject_path.iterdir():
        if session_path.is_dir():
            # typer.echo(f"Checking {session_path}")
            try:
                validate_raw_session(
                    session_path,
                    subject_name,
                    include_behavior,
                    include_ephys,
                    include_videos,
                )
            except Exception as e:
                # typer.echo(f"Problem with {session_path.name}: {e.args[0]}\n")
                print(f"[bold red]Problem with {session_path.name}: {e.args[0]}\n")
            else:
                print(f"[bold green]{session_path.name} looking good.\n")


@app.command()
def show_config():
    typer.echo(config.json(indent=4))


if __name__ == "__main__":
    app()
