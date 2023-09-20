import os
import logging
from pathlib import Path
from typing import Optional

import spikeinterface.extractors as se
import spikeinterface.sorters as ss
import spikeinterface.preprocessing as sip

from beneuro_data.data_validation import validate_raw_ephys_data_of_session


def run_kilosort_on_stream(
    input_path: Path,
    stream_name: str,
    output_path: Path,
    clean_up_temp_files: bool = False,
    sorter_params: Optional[dict] = None,
):
    """
    Run Kilosort3 on a SpikeGLX recording.

    Parameters
    ----------
    input_path: pathlib.Path
        The path to the folder containing the SpikeGLX data.
    stream_name: str
        The name of the stream to use, e.g. 'imec0.ap'.
        Each probe has its own stream.
    output_path: pathlib.Path
        The path to the output folder where the kilosort results will be saved.

    Returns
    -------
    sorting: SortingExtractor
    """

    recording = se.read_spikeglx(str(input_path), stream_name=stream_name)

    if sorter_params is None:
        sorter_params = {}

    sorting_KS3 = ss.run_kilosort3(
        recording,
        output_path=str(output_path),
        # docker_image = "spikeinterface/kilosort3-compiled-base:latest",
        docker_image=True,
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

    return sorting_KS3


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


def get_ap_stream_names(recording_path: Path):
    all_stream_names, _ = se.get_neo_streams("spikeglx", str(recording_path))
    return [stream_name for stream_name in all_stream_names if stream_name.endswith("ap")]


def run_kilosort_on_probe_and_save_in_processed(
    raw_recording_path: Path,
    probe_name: str,
    base_path: Path,
    clean_up_temp_files: bool = False,
):
    # make the folders where the sorting output will be saved
    raw_base_path = base_path / "raw"
    processed_base_path = base_path / "processed"

    processed_recording_path = processed_base_path / raw_recording_path.relative_to(
        raw_base_path
    )

    # NOTE might want to make sure that the probe is in the ap streams

    probe_folder_name = f"{processed_recording_path.name}_{probe_name}"
    processed_probe_path = processed_recording_path / probe_folder_name

    if not processed_probe_path.exists():
        processed_probe_path.mkdir(parents=True)

    # run sorting
    sorting_KS3 = run_kilosort_on_stream(
        input_path=raw_recording_path,
        stream_name=f"{probe_name}.ap",
        output_path=processed_probe_path,
        clean_up_temp_files=clean_up_temp_files,
    )


def run_kilosort_on_recording_and_save_in_processed(
    raw_recording_path: Path, base_path: Path, clean_up_temp_files: bool = False
):
    for ap_stream_name in get_ap_stream_names(raw_recording_path):
        probe_name = ap_stream_name.split(".")[0]
        run_kilosort_on_probe_and_save_in_processed(
            raw_recording_path, probe_name, base_path, clean_up_temp_files
        )


def run_kilosort_on_session_and_save_in_processed(
    raw_session_path: Path,
    subject_name: str,
    base_path: Path,
    clean_up_temp_files: bool = False,
):
    ephys_recording_folders = validate_raw_ephys_data_of_session(
        raw_session_path, subject_name
    )

    for recording_path in ephys_recording_folders:
        run_kilosort_on_recording_and_save_in_processed(
            recording_path, base_path, clean_up_temp_files
        )
