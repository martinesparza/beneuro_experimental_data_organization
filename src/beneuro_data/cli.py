import typer

from beneuro_data.data_validation import validate_raw_session
from beneuro_data.data_transfer import upload_raw_session
from beneuro_data.config import config

app = typer.Typer()


@app.command()
def validate_structure(subject_name: str, processing_level: str, include_behavior: bool, include_ephys: bool):
    subject_path = config.LOCAL_PATH / processing_level / subject_name

    for session_path in subject_path.iterdir():
        if session_path.is_dir():
            typer.echo(f"Checking {session_path}")
            validate_raw_session(session_path, subject_name, include_behavior, include_ephys)
            typer.echo(f"{session_path.name} looking good.")


@app.command()
def show_config():
    typer.echo(config.json(indent=4))


if __name__ == "__main__":
    app()
