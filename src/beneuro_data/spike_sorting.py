import logging
import platform
import warnings
from pathlib import Path
from typing import Optional

from spikeinterface.sorters.utils.misc import (
    has_docker,
    has_docker_nvidia_installed,
    has_docker_python,
    has_nvidia
)

try:
    import spikeinterface.extractors as se
    import spikeinterface.preprocessing as sip
    import spikeinterface.sorters as ss
except ImportError as e:
    raise ImportError(
        "Could not import spike sorting functionality. You might want to reinstall bnd with `poetry install --with processing`"
    ) from e

from beneuro_data.data_validation import (
    _find_spikeglx_recording_folders_in_session,
    validate_raw_ephys_data_of_session
)


def run_kilosort_on_stream(
    input_path: Path,
    stream_name: str,
    output_path: Path,
    clean_up_temp_files: bool = False,
    verbose: bool = False,
    sorter_params: Optional[dict] = None,
):
    """
    Run Kilosort 4 on a SpikeGLX recording.

    Parameters
    ----------
    input_path: pathlib.Path
        The path to the folder containing the SpikeGLX data.
    stream_name: str
        The name of the stream to use, e.g. 'imec0.ap'.
        Each probe has its own stream.
    output_path: pathlib.Path
        The path to the output folder where the kilosort results will be saved.
    clean_up_temp_files: bool
        Whether to delete temporary .mat and .dat files left by Kilosort after sorting.
    verbose: bool
        Run Kilosort in verbose mode.
    sorter_params: Optional[dict]
        Optional parameters to pass to the sorter.

    Returns
    -------
    sorting: SortingExtractor
    """

    recording = se.read_spikeglx(str(input_path), stream_name=stream_name)

    if sorter_params is None:
        sorter_params = {}

    sorting_obj = ss.run_sorter(
        "kilosort4",
        recording,
        output_folder=str(output_path),
        docker_image=True,
        verbose=verbose,
        **sorter_params,
    )

    if clean_up_temp_files:
        sorter_output_folder = output_path / "sorter_output"

        temp_files_to_delete = [
            *sorter_output_folder.glob("*.mat"),
            *sorter_output_folder.glob("*.dat"),
        ]

        for f in temp_files_to_delete:
            f.unlink()

    return sorting_obj


def get_ap_stream_names(recording_path: Path) -> list[str]:
    """
    Get the names of the AP streams (e.g. "imec0.ap") in a SpikeGLX recording.
    """
    all_stream_names, _ = se.get_neo_streams("spikeglx", str(recording_path))
    return [stream_name for stream_name in all_stream_names if stream_name.endswith("ap")]


def run_kilosort_on_recording_and_save_in_processed(
    raw_recording_path: Path,
    base_path: Path,
    stream_names_to_process: Optional[list[str]] = None,
    clean_up_temp_files: bool = False,
    verbose: bool = False,
) -> None:
    """
    Run Kilosort on a SpikeGLX recording and save the results in the processed folder.

    Parameters
    ----------
    raw_recording_path: Path
        The path to the folder containing the raw SpikeGLX data.
    base_path: Path
        The path to the base of the data storage (the folder containing the "raw" and
        "processed" folders).
    stream_names_to_process: Optional[tuple[str, ...]]
        A tuple of stream names to process.
        If None, all available AP streams will be processed.
    clean_up_temp_files: bool
        Whether to delete temporary .mat and .dat files left by Kilosort after sorting.
    verbose: bool
        Run Kilosort in verbose mode.

    Returns
    -------
    None, but the results are saved in the processed folder.
    """
    if isinstance(raw_recording_path, str):
        raw_recording_path = Path(raw_recording_path)
    if isinstance(base_path, str):
        base_path = Path(base_path)

    if not raw_recording_path.is_relative_to(base_path / "raw"):
        raise ValueError(f"{raw_recording_path} is not in {base_path / 'raw'}")

    # determine output path
    raw_base_path = base_path / "raw"
    processed_base_path = base_path / "processed"

    raw_session_path = raw_recording_path.parent
    recording_folder_name = raw_recording_path.name
    session_folder_name = raw_session_path.name

    processed_session_path = processed_base_path / raw_session_path.relative_to(
        raw_base_path
    )
    processed_recording_ephys_path = (
        processed_session_path / f"{session_folder_name}_ephys" / recording_folder_name
    )

    # if they are not explicitly given, figure out the AP streams ~ probes, e.g. imec0.ap
    if stream_names_to_process is None:
        stream_names_to_process = get_ap_stream_names(raw_recording_path)

    # make sure that the recording contains those streams
    # can catch typos or missing probes when stream names are explicitly given
    for stream_name in stream_names_to_process:
        if stream_name not in get_ap_stream_names(raw_recording_path):
            raise ValueError(
                f"Probe {stream_name} is not in recording's AP streams. Found {get_ap_stream_names(raw_recording_path)}"
            )

    # run kilosort for all probes in the recording
    for ap_stream_name in stream_names_to_process:
        probe_name = ap_stream_name.split(".")[0]

        probe_folder_name = f"{processed_recording_ephys_path.name}_{probe_name}"
        processed_probe_path = processed_recording_ephys_path / probe_folder_name

        if verbose:
            print(f"Running Kilosort for {ap_stream_name}")

        _ = run_kilosort_on_stream(
            input_path=raw_recording_path,
            stream_name=f"{probe_name}.ap",
            output_path=processed_probe_path,
            clean_up_temp_files=clean_up_temp_files,
            verbose=verbose,
        )


def run_kilosort_on_session_and_save_in_processed(
    raw_session_path: Path,
    subject_name: str,
    base_path: Path,
    allowed_extensions_not_in_root: tuple[str, ...],
    stream_names_to_process: Optional[list[str]] = None,
    clean_up_temp_files: bool = False,
    verbose: bool = False,
) -> None:
    """
    Run Kilosort on all recordings within a session and save the results in the processed folder.

    Parameters
    ----------
    raw_session_path: Path
        The path to the session's raw data.
    subject_name: str
        The name of the experimental subject. (Used for validation.)
    base_path: Path
        The path to the base of the data storage (the folder containing the "raw" and
        "processed" folders).
    allowed_extensions_not_in_root : tuple[str, ...]
        A tuple of file extensions that are allowed in the session directory excluding the root level.
        E.g. (".txt", )
    stream_names_to_process: Optional[tuple[str, ...]]
        A tuple of stream names to process.
        If None, all available AP streams will be processed.
    clean_up_temp_files: bool
        Whether to delete temporary .mat and .dat files left by Kilosort after sorting.
    verbose: bool
        Run Kilosort in verbose mode.

    Returns
    -------
    None, but the results are saved in the processed folder.
    """
    # adapted from spikeinterface
    if platform.system() != "Linux":
        warnings.warn(
            f"Spike sorting might not work on {platform.system()}. Try Linux instead."
        )
    if platform.system() == "Linux" and not has_docker_nvidia_installed():
        warnings.warn(
            "Kilosort is best run on a GPU, otherwise it might take very long, "
            "Your computer seems to not be able to use the GPU in docker images. "
            "Try installing `nvidia-container-toolkit`."
            "\n\n"
            "Some additional info:\n"
            f"NVIDIA GPU working: {has_nvidia()}"
            f"Docker installed: {has_docker()}"
            f"Docker Python package installed: {has_docker_python()}"
        )

    if isinstance(raw_session_path, str):
        raw_session_path = Path(raw_session_path)

    # validate ephys data before spike sorting
    _ = validate_raw_ephys_data_of_session(
        raw_session_path, subject_name, allowed_extensions_not_in_root
    )

    # get the actual folders because validate_raw_ephys_data_of_session returns a list of files
    ephys_recording_folders = _find_spikeglx_recording_folders_in_session(raw_session_path)

    for recording_path in ephys_recording_folders:
        if verbose:
            print(f"Processing {recording_path.name}...")

        run_kilosort_on_recording_and_save_in_processed(
            recording_path,
            base_path,
            stream_names_to_process,
            clean_up_temp_files,
            verbose,
        )


def preprocess_recording(
    recording: se.BaseRecording,
    verbose: bool = True,
):
    """
    Preprocess a recording before spike sorting.
    Based on https://spikeinterface.readthedocs.io/en/latest/how_to/analyse_neuropixels.html#preprocess-the-recording


    Parameters
    ----------
    recording: BaseRecording
        The recording to preprocess.
        E.g. SpikeGLX recording you got by calling `si.read_spikeglx()`.
    verbose: bool
        Whether to print logging messages.

    Returns
    -------
    recording: BaseRecording
        The preprocessed recording.

    Examples
    --------
    >>> recording = si.read_spikeglx(spikeglx_dir, stream_name = 'imec0.ap')
    >>> preprocessed_recording = preprocess_recording(recording)

    Extra notes
    -----------
    - Description of spikeinterface's preprocessing module: https://spikeinterface.readthedocs.io/en/latest/modules/preprocessing.html
    - All possible preprocessing steps: https://spikeinterface.readthedocs.io/en/latest/api.html#module-spikeinterface.preprocessing
    - Alternative common preprocessing pipelines we can implement: https://spikeinterface.readthedocs.io/en/latest/modules/preprocessing.html#how-to-implement-ibl-destriping-or-spikeglx-catgt-in-spikeinterface
    """
    rec1 = sip.highpass_filter(recording, freq_min=400.0)

    bad_channel_ids, channel_labels = sip.detect_bad_channels(rec1)
    if verbose:
        logging.warning(f"bad_channel_ids: {bad_channel_ids}")

    rec2 = rec1.remove_channels(bad_channel_ids)

    rec3 = sip.phase_shift(rec2)

    rec4 = sip.common_reference(rec3, operator="median", reference="global")

    return rec4
