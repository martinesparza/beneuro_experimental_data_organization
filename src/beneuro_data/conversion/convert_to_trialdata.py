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


class ParsedNWBFile:
    """
    Class to parse .nwb folders before arrangement into task-specific
    trial-dataformat
    """
    def __init__(self, nwbpath: Path):
        with NWBHDF5IO(nwbpath, mode="r") as io:
            self.nwbfile = io.read()

            # Processing modules
            self.behav_module = self.nwbfile.processing["behavior"].data_interfaces
            self.ephys_module = self.nwbfile.processing["ecephys"].data_interfaces

            # Pycontrol outputs
            self.pycontrol_states = self.parse_nwb_pycontrol_states()
            self.pycontrol_events = self.parse_nwb_pycontrol_events()
            self.ball_position = self.parse_ball_position()

            # Anipose data
            self.anipose_data = self.parse_anipose_output()

            # Spiking data
            self.spike_data = self.parse_ephys_data()

    def parse_nwb_pycontrol_states(self):
        """Parse pycontrol output from behavioural processing module of .nwb file

        Returns:
            behav_dict (Dict): Dictionary containing states, event,
                and prints of pycontrol during execution
        """
        print("Parsing pycontrol states")

        data_dict = {
            col: self.behav_module["behavioral_states"][col].data[:]
            for col in self.behav_module["behavioral_states"].colnames
        }
        df = pd.DataFrame(data_dict)
        return df

    def parse_nwb_pycontrol_events(self):
        print("Parsing pycontrol events")

        behav_events_time_series = self.behav_module['behavioral_events'].time_series['behavioral events']
        print_events_time_series = self.behav_module['print_events'].time_series

        # First make dataframe with behav events
        df_behav_events = pd.DataFrame()

        # Behavioural event dont have values but print events do so we need to
        # stay consistent with dimension
        df_behav_events['event'] = behav_events_time_series.data[:]
        df_behav_events['value'] = np.full(behav_events_time_series.data[:].shape[0], np.nan)
        df_behav_events['timestamp'] = behav_events_time_series.timestamps[:]

        # Then make dataframe with print events, and a df for each print event
        # Sounds a bit convoluted but it is converted to .nwb with a different
        # key for each print event
        df_print_events = pd.DataFrame()
        for print_event in print_events_time_series.keys():
            tmp_df = pd.DataFrame()

            tmp_df['event'] = np.full(print_events_time_series[print_event].data[:].shape[0], print_event)
            tmp_df['value'] = print_events_time_series[print_event].data[:]
            tmp_df['timestamp'] = print_events_time_series[print_event].timestamps[:]

            df_print_events = pd.concat([df_print_events, tmp_df], axis=0, ignore_index=True)

        # Concatenate both dataframes
        df_events = pd.concat([df_behav_events, df_print_events], axis=0, ignore_index=True)
        df_events.sort_values(by='timestamp', ascending=True, inplace=True)
        df_events.reset_index(drop=True, inplace=True)

        return df_events

    def parse_ball_position(self):
        pass

    def parse_anipose_output(self):
        return

    def run_conversion(self):
        pass

    def parse_ephys_data(self):
        pass




def convert_to_trialdata(
    nwbfile_path: Path,
):
    """Transform data from .nwb format to task-specific trialdata

    Args:
        nwbfile_path:
        task_format:

    Returns:

    """
    parsed_nwbfile = ParsedNWBFile(nwbfile_path)
    parsed_nwbfile.run_conversion()

    breakpoint()
    return




