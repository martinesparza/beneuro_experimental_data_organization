"""
Arrange data from .nwb to trialdata format. Steps:

1. Open nwb file
2. Load events data
3. Load keypoints
4. Load spike data

Arrange
"""
from pathlib import Path

import numpy as np
import pandas as pd
from pynwb import NWBHDF5IO


class ParsedNWBData:
    """
    Class to parse .nwb folders before arrangement into task-specific
    trial-dataformat
    """
    def __init__(self, nwbpath: Path):
        with NWBHDF5IO(nwbpath, mode="r") as io:
            self.nwbfile = io.read()
            self.pycontrol_output = self.parse_nwb_pycontrol_output()

    def parse_nwb_pycontrol_output(self):
        """Parse pycontrol output from behavioural processing module of .nwb file

        Returns:
            behav_dict (Dict): Dictionary containing states, event,
                and prints of pycontrol during execution
        """

        print("Parsing behavioural data")

        behav_interface = self.nwbfile.processing["behavior"].data_interfaces
        behav_dict = {}

        for key in behav_interface.keys():
            if key == 'behavioral_events':
                data_dict = {
                    'event': behav_interface[key].time_series['behavioral events'].data[:],
                    'timestamp': behav_interface[key].time_series['behavioral events'].timestamps[:]}
                df = pd.DataFrame(data_dict)
                behav_dict['events'] = df

            if key == 'behavioral_states':
                data_dict = {
                    col: behav_interface[key][col].data[:]
                    for col in behav_interface[key].colnames
                }
                df = pd.DataFrame(data_dict)
                behav_dict['states'] = df

            if key == 'print_events':
                df = pd.DataFrame()
                for print_event in behav_interface[
                    key].time_series.keys():
                    data_dict = {
                        'print_event': np.full(behav_interface[key].time_series[print_event].data[:].shape[0], print_event),
                        'value': behav_interface[key].time_series[print_event].data[:],
                        'timestamp': behav_interface[key].time_series[print_event].timestamps[:]
                    }
                    tmp_df = pd.DataFrame(data_dict)
                    df = pd.concat([df, tmp_df], axis=0, ignore_index=True)
                behav_dict['prints'] = df.sort_values(
                    by='timestamp',
                    ascending=True
                ).reset_index(drop=True)
        return behav_dict


def format_nwb_into_trialdata(
    nwbfile_path: Path,
    task_format: str,  # Can be 'earthquake', 'bci', etc
):
    """Transform data from .nwb format to task-specific trialdata

    Args:
        nwbfile_path:
        task_format:

    Returns:

    """
    parsed_nwb_data = ParsedNWBData(nwbfile_path)

    return




