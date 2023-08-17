import typer

from beneuro_data.data_transfer import Subject
from beneuro_data.config import Config

app = typer.Typer()


@app.command()
def validate_structure(subject_name: str):
    subject = Subject(subject_name)
    if subject.validate_local_session_folders("raw"):
        typer.echo(f"{subject_name} looking good.")


@app.command()
def show_config():
    config = Config()
    typer.echo(config.json(indent=4))
