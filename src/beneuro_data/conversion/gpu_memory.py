import subprocess as sp


def get_free_gpu_memory() -> list[int]:
    """
    Get the amount of free GPU memory in MiB for each GPU in the computer.
    """
    # adapted from https://stackoverflow.com/questions/59567226/how-to-programmatically-determine-available-gpu-memory-with-tensorflow
    command = "nvidia-smi --query-gpu=memory.free --format=csv"
    memory_free_info = sp.check_output(command.split()).decode("ascii").split("\n")
    # first element is 'memory.free [MiB]', last is ''
    # the rest are '1234 MiB' for each GPU in the computer -> process those
    memory_free_values = [int(x.split()[0]) for x in memory_free_info[1:-1]]

    return memory_free_values
