from pathlib import Path

import numpy as np
import pandas as pd
from pynwb import NWBHDF5IO
from pynwb.behavior import SpatialSeries, TimeSeries
from pynwb.misc import Units
from ndx_pose.pose import PoseEstimationSeries


def parse_pynwb_units(units: Units):
    breakpoint()
    return None


def parse_pose_estimation_series(pose_est_series: PoseEstimationSeries) -> pd.DataFrame:
    """Parse pose estimation series data from anipose output

    Args:
        pose_est_series (PoseEstimationSeries): ndx_pose object to parse

    Returns:
        df (pd.DataFrame): contains x, y, z or angle and timestamps.

    """
    if pose_est_series.data[:].shape[1] == 3:
        colnames = ['x', 'y', 'z']
    elif pose_est_series.data[:].shape[1] == 2 and all(pose_est_series.data[:, 1] == 0):
        # If this is true we assume we are dealing with angle data
        colnames = ['angle']
    else:
        raise ValueError(f"Shape {pose_est_series.data[:].shape} is not supported by pynwb."
                         f" Please provide a valid PoseEstimationSeries object")

    df = pd.DataFrame()
    for i, col in enumerate(colnames):
        df[col] = pose_est_series.data[:, i]

    timestamps = np.arange(pose_est_series.data[:].shape[0])
    timestamps = timestamps / pose_est_series.rate + pose_est_series.starting_time
    df['timestamps'] = timestamps

    return df


def parse_spatial_series(spatial_series: SpatialSeries) -> pd.DataFrame:
    """Parse data and timestamps of a SpatialSeries .pynwb object

    Args:
        spatial_series (SpatialSeries): pynwb object to parse

    Returns:
        df (pd.DataFrame): contains x, y, z and timestamps.
    """
    if spatial_series.data[:].shape[1] == 2:
        colnames = ['x', 'y']
    elif spatial_series.data[:].shape[1] == 3:
        colnames = ['x', 'y', 'z']
    else:
        raise ValueError(f"Shape {spatial_series.data[:].shape} is not supported by pynwb. "
                         f"Please provide a valid SpatialSeries object")

    df = pd.DataFrame()
    for i, col in enumerate(colnames):
        df[col] = spatial_series.data[:, i]

    df['timestamps'] = spatial_series.timestamps[:]

    return df


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
            self.pycontrol_motion_sensors = self.parse_motion_sensors()

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

    def parse_nwb_pycontrol_events(self) -> pd.DataFrame:
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

    def parse_motion_sensors(self) -> pd.DataFrame:
        print("Parsing motion sensors")
        ball_position_spatial_series = self.behav_module['Position'].spatial_series['Ball position']
        return parse_spatial_series(ball_position_spatial_series)

    def parse_anipose_output(self) -> dict:
        print("Parsing anipose data")
        anipose_data_dict = self.behav_module['Pose estimation'].pose_estimation_series

        parsed_anipose_data_dict = {}
        for key in anipose_data_dict.keys():
            parsed_anipose_data_dict[key] = parse_pose_estimation_series(anipose_data_dict[key])

        return parsed_anipose_data_dict

    def parse_ephys_data(self):
        print(f"Parsing ephys data. Found probes {list(self.ephys_module.keys())}")
        for probe in self.ephys_module.keys():
            parsed_units = parse_pynwb_units(self.ephys_module[probe])
        return

    def run_conversion(self):
        pass

    def save(self):
        pass


def convert_to_trialdata(
    nwbfile_path: Path,
):
    """Transform data from .nwb format to task-specific trialdata

    Args:
        nwbfile_path:

    Returns:

    """
    parsed_nwbfile = ParsedNWBFile(nwbfile_path)
    parsed_nwbfile.run_conversion()
    parsed_nwbfile.save()

    breakpoint()
    return




