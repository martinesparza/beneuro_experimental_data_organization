import spikeinterface.extractors as se
import spikeinterface.sorters as ss
import spikeinterface.preprocessing as sip

import logging
import pathlib

from typing import Optional

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

    recording = se.read_spikeglx(input_folder, stream_name = stream_name)

    if sorter_params is None:
        sorter_params = {}

    sorting_KS3 = ss.run_kilosort3(recording,
                                   output_folder = output_folder,
                                   #docker_image = "spikeinterface/kilosort3-compiled-base:latest",
                                   docker_image = True,
                                   **sorter_params)

    if clean_up_temp_files:
        temp_files_to_delete = \
            list(pathlib.Path(output_folder, 'sorter_output').glob('*.mat')) + \
            list(pathlib.Path(output_folder, 'sorter_output').glob('*.dat'))

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
    rec1 = sip.highpass_filter(recording, freq_min = 400.)

    bad_channel_ids, channel_labels = sip.detect_bad_channels(rec1)
    if verbose:
        logging.warning(f'bad_channel_ids: {bad_channel_ids}')

    rec2 = rec1.remove_channels(bad_channel_ids)

    rec3 = sip.phase_shift(rec2)

    rec4 = sip.common_reference(rec3, operator = "median", reference = "global")

    return rec4
    
