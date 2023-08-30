import os
import logging
import pathlib
from typing import Optional

import spikeinterface.extractors as se
import spikeinterface.sorters as ss
import spikeinterface.preprocessing as sip

from beneuro_data.data_validation import EphysRecording


def run_kilosort_on_stream(
    input_folder: str,
    stream_name: str,
    output_folder: str,
    clean_up_temp_files: bool = False,
    sorter_params: Optional[dict] = None,
):
    """
    Run Kilosort3 on a SpikeGLX recording.

    Parameters
    ----------
    input_folder: str
        The path to the folder containing the SpikeGLX data.
    stream_name: str
        The name of the stream to use, e.g. 'imec0.ap'.
        Each probe has its own stream.
    output_folder: str
        The path to the output folder where the kilosort results will be saved.

    Returns
    -------
    sorting: SortingExtractor
    """

    recording = se.read_spikeglx(input_folder, stream_name=stream_name)

    if sorter_params is None:
        sorter_params = {}

    sorting_KS3 = ss.run_kilosort3(
        recording,
        output_folder=output_folder,
        # docker_image = "spikeinterface/kilosort3-compiled-base:latest",
        docker_image=True,
        **sorter_params,
    )

    if clean_up_temp_files:
        temp_files_to_delete = list(
            pathlib.Path(output_folder, "sorter_output").glob("*.mat")
        ) + list(pathlib.Path(output_folder, "sorter_output").glob("*.dat"))

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


class SpikeSorter:
    def __init__(self, ephys_recording: EphysRecording):
        self.ephys_recording = ephys_recording

    @property
    def ap_streams(self):
        stream_names, _ = se.get_neo_streams(
            "spikeglx", self.ephys_recording.get_path("local", "raw")
        )
        return [stream_name for stream_name in stream_names if stream_name.endswith("ap")]

    @property
    def number_of_probes(self):
        return len(self.ap_streams)

    def run_kilosort(
        self,
        ap_stream_name: str,
        clean_up_temp_files: bool = False,
        sorter_params: Optional[dict] = None,
    ):
        assert ap_stream_name in self.ap_streams
        assert self.ephys_recording.has_folder("local", "raw")

        if not self.ephys_recording.has_folder("local", "processed"):
            self.ephys_recording.make_folder("local", "processed")
        assert self.ephys_recording.has_folder("local", "processed")

        # each probe should have its own output folder
        output_folder_name = (
            f"{self.ephys_recording.folder_name}_{ap_stream_name.split('.')[0]}"
        )
        output_path = os.path.join(
            self.ephys_recording.get_path("local", "processed"), output_folder_name
        )
        if not os.path.exists(output_path):
            os.mkdir(output_path)

        sorting_KS3 = run_kilosort_on_stream(
            input_folder=self.ephys_recording.get_path("local", "raw"),
            stream_name=ap_stream_name,
            output_folder=output_path,
            clean_up_temp_files=clean_up_temp_files,
            sorter_params=sorter_params,
        )

        return sorting_KS3
