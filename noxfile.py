import nox


# type annotations don't work with 3.8
# e.g. list[Path] is not supported
@nox.session(python=["3.9", "3.10", "3.11"])
def tests(session):
    session.run("poetry", "install", external=True)
    session.run("pytest")
