import typer

from beneuro_data.data_validation import Subject
from beneuro_data.config import config

app = typer.Typer()


@app.command()
def validate_structure(subject_name: str):
    subject = Subject(subject_name)
    if subject.validate_local_session_folders("raw"):
        typer.echo(f"{subject_name} looking good.")


@app.command()
def show_config():
    typer.echo(config.json(indent=4))


if __name__ == "__main__":
    app()
