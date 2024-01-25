import subprocess
from pathlib import Path

from rich import print


def _run_git_command(repo_path: str, command: list[str]) -> str:
    """Run a git command in the specified repository and return its output"""
    result = subprocess.run(
        ["git", "-C", repo_path] + command, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise Exception(f"Git command failed: {result.stderr}")
    return result.stdout.strip()


def _get_new_commits(repo_path: str) -> list[str]:
    """Check for new commits from origin/main"""
    # Fetch the latest changes from the remote repository
    _run_git_command(repo_path, ["fetch"])

    # Check if origin/main has new commits compared to the local branch
    new_commits = _run_git_command(repo_path, ["log", "HEAD..origin/main", "--oneline"])

    return [commit.strip() for commit in new_commits.split("\n") if commit.strip() != ""]


def check_for_updates() -> bool:
    package_path = Path(__file__).absolute().parent.parent.parent

    new_commits = _get_new_commits(package_path)

    if len(new_commits) > 0:
        print("New commits found, run `bnd self-update` to update the package.")
        for commit in new_commits:
            print(f" - {commit}")

        return True

    print("No new commits found, package is up to date.")

    return False


def update_bnd(print_new_commits: bool = False):
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
