import os


def get_file_size_in_kilobytes(file_path: str) -> float:
    """Return the size of a file in kilobytes."""
    return os.path.getsize(file_path) / 1024


def get_folder_size_in_kilobytes(folder_path: str) -> float:
    """Return the size of a folder in kilbytes."""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            total_size += get_file_size_in_kilobytes(file_path)

    return total_size


def get_folder_size_in_megabytes(folder_path: str) -> float:
    """Return the size of a folder in megabytes."""
    return get_folder_size_in_kilobytes(folder_path) / 1024


def get_folder_size_in_gigabytes(folder_path: str) -> float:
    """Return the size of a folder in gigabytes."""
    return get_folder_size_in_megabytes(folder_path) / 1024
