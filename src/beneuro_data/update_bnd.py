import subprocess
from pathlib import Path

from rich import print


def _run_git_command(repo_path: Path, command: list[str]) -> str:
    """
    Run a git command in the specified repository and return its output

    Parameters
    ----------
    repo_path : Path
        Path to the git repository to run the command in.
    command : list[str]
        Git command to run, as a list of strings.
        E.g. ["log", "HEAD..origin/main", "--oneline"]

    Returns
    -------
    The output of the git command as a string.
    """
    repo_path = Path(repo_path)

    if not repo_path.is_absolute():
        raise ValueError(f"{repo_path} is not an absolute path")

    if not (repo_path / ".git").exists():
        raise ValueError(f"{repo_path} is not a git repository")

    result = subprocess.run(
        ["git", "-C", repo_path.absolute()] + command, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise Exception(f"Git command failed: {result.stderr}")

    return result.stdout.strip()


def _get_new_commits(repo_path: Path) -> list[str]:
    """
    Check for new commits from origin/main of the specified repository.

    Parameters
    ----------
    repo_path : Path
        Path to the git repository.

    Returns
    -------
    Each new commit as a string in a list.
    """
    repo_path = Path(repo_path)

    # Fetch the latest changes from the remote repository
    _run_git_command(repo_path, ["fetch"])

    # Check if origin/main has new commits compared to the local branch
    new_commits = _run_git_command(repo_path, ["log", "HEAD..origin/main", "--oneline"])

    # filter empty lines and strip whitespaces
    return [commit.strip() for commit in new_commits.split("\n") if commit.strip() != ""]


def check_for_updates() -> bool:
    """
    Check if the package has new commits on the origin/main branch.

    Returns True if new commits are found, False otherwise.
    """
    package_path = Path(__file__).absolute().parent.parent.parent

    new_commits = _get_new_commits(package_path)

    if len(new_commits) > 0:
        print("New commits found, run `bnd self-update` to update the package.")
        for commit in new_commits:
            print(f" - {commit}")

        return True

    print("No new commits found, package is up to date.")

    return False


def update_bnd(print_new_commits: bool = False) -> None:
    """
    Update the package to the latest version from origin/main.

    Parameters
    ----------
    print_new_commits : bool, optional, default: False
        If True, print the new commits that were applied.
    """
    package_path = Path(__file__).absolute().parent.parent.parent

    new_commits = _get_new_commits(package_path)

    if len(new_commits) > 0:
        print("New commits found, pulling changes...")
        print(3 * "\n")

        # pull changes from origin/main
        _run_git_command(package_path, ["pull", "origin", "main"])

        print(
            "NOTE: If the install hangs, running the following then retrying might help:",
            end="\t",
        )
        print("export PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring")

        # install the updated package
        subprocess.run(["poetry", "install"], cwd=package_path)

        print(3 * "\n")
        print("Package updated successfully.")
        print("\n")

        if print_new_commits:
            print("New commits:")
            for commit in new_commits:
                print(f" - {commit}")
    else:
        print("Package appears to be up to date, no new commits found.")
