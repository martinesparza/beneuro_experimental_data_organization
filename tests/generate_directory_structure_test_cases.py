import os

from ruamel.yaml import YAML


def generate_dict_from_path(path: str):
    """
    Generate a YAML representation of the directory structure rooted at `path`.
    """
    structure = {
        "type": "folder" if os.path.isdir(path) else "file",
        "name": os.path.basename(path),
    }

    if structure["type"] == "folder":
        structure["children"] = [
            generate_dict_from_path(os.path.join(path, child_name))
            for child_name in os.listdir(path)
        ]

    return structure


def create_directory_structure_from_dict(data: dict, path: str):
    """
    Create a directory structure rooted at `path` from a YAML representation.

    Parameters
    ----------
    data : dict
        A dictionary representing the directory structure.
    path : str
        The path to create the directory structure at.

    Returns
    -------
    None
    """
    if data["type"] == "folder":
        path = os.path.join(path, data["name"])
        os.mkdir(path)
        for child in data.get("children", []):
            create_directory_structure_from_dict(child, path)
    elif data["type"] == "file":
        with open(os.path.join(path, data["name"]), "w") as f:
            # this will create an empty file
            pass


if __name__ == "__main__":
    test_dir_path = os.path.dirname(__file__)
    yaml_folder = os.path.join(test_dir_path, "number_of_valid_sessions_test_yamls")
    # fake_directory_structure_folder = os.path.join(
    #    test_dir_path, "directory_structure_test_cases"
    # )
    data_root_path = "/data/nwb_prototype/raw/"

    # mouse_names = [item.name for item in os.scandir(root_path) if item.is_dir()]
    mouse_names = ["M011", "M015", "M016"]

    os.mkdir(yaml_folder)
    # os.mkdir(fake_directory_structure_folder)

    for mouse_name in mouse_names:
        mouse_source_path = os.path.join(data_root_path, mouse_name)
        mouse_yaml_path = os.path.join(yaml_folder, f"{mouse_name}.yaml")

        mouse_dir_dict = generate_dict_from_path(mouse_source_path)

        # save the YAML representation to a file
        with open(mouse_yaml_path, "w") as f:
            YAML().dump(mouse_dir_dict, f)

        # create_directory_structure_from_dict(
        #    mouse_dir_dict,
        #    fake_directory_structure_folder,
        # )
