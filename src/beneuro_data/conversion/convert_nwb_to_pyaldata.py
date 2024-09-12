"""
Module for conversion from nwb to pyaldata format
"""
import warnings

import numpy as np
import pandas as pd

from ndx_pose import PoseEstimationSeries

from pynwb import NWBHDF5IO
from pynwb.behavior import SpatialSeries
from pynwb.misc import Units


def _bin_spikes(probe_units: Units, bin_size: float) -> np.array:
    """
    Bin spikes from pynwb from one probe

    Parameters
    ----------
    probe_units :
        PyNWB Units object from which to read spike times
    bin_size :
        Bin size in seconds to use

    Returns
    -------
    Array :
        Matrix of neuron x time with binned spikes

    """

    start_time = 0  # This is hardcoded since its aligned in nwb conversion
    end_time = np.max(probe_units.spike_times[:])
    number_of_bins = int(np.ceil((end_time - start_time) / bin_size))

    # Initialize the binned spikes array
    binned_spikes = np.zeros((len(probe_units.id[:]), number_of_bins), dtype=int)

    # Populate the binned spikes array
    for neuron_id in probe_units.id[:]:
        spike_times = probe_units.get_unit_spike_times(neuron_id)
        for spike_time in spike_times:
            if start_time <= spike_time < end_time:
                bin_index = int((spike_time - start_time) / bin_size)
                binned_spikes[neuron_id, bin_index] += 1

    return binned_spikes


def _parse_pynwb_probe(probe_units: Units, electrode_info, bin_size: float):

    # This returns a neurons x bin array of 0s and 1s
    binned_spikes = _bin_spikes(probe_units, bin_size)

    # This returns a [nTemplates, nTimePoints, nTempChannels] matrix
    templates = probe_units.waveform_mean[:]

    # NOTE: We do not need the templates_ind.npy since in the case of Kilosort templates_ind
    # is just the integers from 0 to nChannels-1, as templates are defined on all channels.

    # TODO: Load channel_map.npy to .nwb to have more thorough mapping between templates and
    #   channels. I am not currently loading the channel_map.npy file since for Neuropixels 1.0
    #   it is simply an array of 0 to 383

    # Get max amplitude channel based on templates
    chan_best = (templates**2).sum(axis=1).argmax(axis=-1)

    # Get brain area channel map for this specific probe
    electrode_info_df = electrode_info.to_dataframe()
    probe_electrode_locations_df = electrode_info_df[electrode_info_df['group_name'] == probe_units.name.split('_')[-1]]
    probe_channel_map = probe_electrode_locations_df['location'].to_dict()

    brain_area_spikes = {}
    brain_areas = {value for value in probe_channel_map.values() if value not in ['out', 'void']}
    for brain_area in brain_areas:
        brain_area_channels = [key for key, value in probe_channel_map.items() if value == brain_area]
        brain_area_neurons = np.where(np.isin(chan_best, brain_area_channels))[0]
        brain_area_spikes[brain_area] = binned_spikes[brain_area_neurons, :]

    return brain_area_spikes


def _parse_pose_estimation_series(pose_est_series: PoseEstimationSeries) -> pd.DataFrame:
    """
    Parse pose estimation series data from anipose output

    Parameters
    ----------
    pose_est_series :
        ndx_pose object to parse from nwb file

    Returns
    -------
    pd.DataFrame :
        Contains x, y, z or angle and timestamps

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


def _parse_spatial_series(spatial_series: SpatialSeries) -> pd.DataFrame:
    """
    Parse data and timestamps of a SpatialSeries .pynwb object

    Parameters
    ----------
    spatial_series :
        pynwb object to parse

    Returns
    -------
    pd.DataFrame :
        Contains x, y, z and timestamp
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


def _add_data_to_trial(df_to_add_to, new_data_column, df_to_add_from, columns_to_read_from, timestamp_column=None):
    for index, row in df_to_add_to.iterrows():
        trial_specific_events = df_to_add_from[
            (df_to_add_from['timestamp_idx'] >= row['idx_trial_start']) &
            (df_to_add_from['timestamp_idx'] <= row['idx_trial_end'])
        ]

        # Add to pyaldata dataframe
        df_to_add_to[new_data_column] = df_to_add_to[new_data_column].astype('object')
        df_to_add_to.at[index, new_data_column] = trial_specific_events[columns_to_read_from].to_numpy()

        if timestamp_column is not None:
            df_to_add_to[f'{timestamp_column}'] = df_to_add_to[f'{timestamp_column}'].astype('object')
            df_to_add_to.at[index, f'{timestamp_column}'] = trial_specific_events['timestamp_idx'].to_numpy() - row['idx_trial_start']

    return df_to_add_to


class ParsedNWBFile:

    def __init__(self, nwbpath):
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
            self.bin_size = 0.01  # 10 ms bins hardcoded for now
            self.spike_data = self.parse_spiking_data()

            # Pyaldata dataframe
            self.pyaldata_df = None

    def parse_nwb_pycontrol_states(self):
        """
        Parse pycontrol output from behavioural processing module of .nwb file

        Returns
        -------
        Dict :
            Dictionary containing states, event, and prints of pycontrol during execution

        """

        print("Parsing pycontrol states")

        data_dict = {col: self.behav_module["behavioral_states"][col].data[:] for col in
            self.behav_module["behavioral_states"].colnames}
        df = pd.DataFrame(data_dict)
        return df

    def parse_nwb_pycontrol_events(self):
        print("Parsing pycontrol events")

        behav_events_time_series = self.behav_module['behavioral_events'].time_series[
            'behavioral events']
        print_events_time_series = self.behav_module['print_events'].time_series

        # First make dataframe with behav events
        df_behav_events = pd.DataFrame()

        # Behavioural event dont have values but print events do so we need to
        # stay consistent with dimension
        df_behav_events['event'] = behav_events_time_series.data[:]
        df_behav_events['value'] = np.full(behav_events_time_series.data[:].shape[0],
                                           np.nan)
        df_behav_events['timestamp'] = behav_events_time_series.timestamps[:]

        # Then make dataframe with print events, and a df for each print event
        # Sounds a bit convoluted but it is converted to .nwb with a different
        # key for each print event
        df_print_events = pd.DataFrame()
        for print_event in print_events_time_series.keys():
            tmp_df = pd.DataFrame()

            tmp_df['event'] = np.full(
                print_events_time_series[print_event].data[:].shape[0], print_event)
            tmp_df['value'] = print_events_time_series[print_event].data[:]
            tmp_df['timestamp'] = print_events_time_series[print_event].timestamps[:]

            df_print_events = pd.concat([df_print_events, tmp_df], axis=0,
                                        ignore_index=True)

        # Concatenate both dataframes
        df_events = pd.concat([df_behav_events, df_print_events], axis=0, ignore_index=True)
        df_events.sort_values(by='timestamp', ascending=True, inplace=True)
        df_events.reset_index(drop=True, inplace=True)

        return df_events

    def parse_motion_sensors(self):
        print("Parsing motion sensors")
        ball_position_spatial_series = self.behav_module['Position'].spatial_series[
            'Ball position']
        return _parse_spatial_series(ball_position_spatial_series)

    def parse_anipose_output(self):
        print("Parsing anipose data")
        anipose_data_dict = self.behav_module['Pose estimation'].pose_estimation_series

        parsed_anipose_data_dict = {}
        for key in anipose_data_dict.keys():
            parsed_anipose_data_dict[key] = _parse_pose_estimation_series(
                anipose_data_dict[key])

        return parsed_anipose_data_dict

    def parse_spiking_data(self):
        print(f"Parsing spiking data. Found probes {list(self.ephys_module.keys())}")
        spike_data_dict = {}

        # TODO: Make custom channel map option in case we dont agree with pinpoint
        for probe_units in self.ephys_module.keys():
            spike_data_dict[probe_units] = _parse_pynwb_probe(
                probe_units=self.ephys_module[probe_units],
                electrode_info=self.nwbfile.electrodes,
                bin_size=self.bin_size
            )

        return spike_data_dict

    def add_pycontrol_states_to_df(self):
        # TODO: Fix time units
        start_time = 0.0
        end_time = self.pycontrol_states.stop_time.values[-1] / 1000  # To seconds
        number_of_bins = int(np.floor((end_time - start_time) / self.bin_size))
        self.pyaldata_df['trial_id'] = self.pycontrol_states.start_time.index
        self.pyaldata_df['bin_size'] = self.bin_size

        # Start and stop times of each state
        self.pyaldata_df['idx_trial_start'] = np.ceil(
            self.pycontrol_states.start_time.values[:] / 1000 / self.bin_size).astype(int)
        self.pyaldata_df['idx_trial_end'] = np.floor(
            self.pycontrol_states.stop_time.values[:] / 1000 / self.bin_size).astype(int)

        self.pyaldata_df['trial_name'] = self.pycontrol_states.state_name[:]

        if self.pyaldata_df.idx_trial_end.values[-1] != number_of_bins:
            warnings.warn(
                f'Extract number of bins: {self.pyaldata_df.idx_trial_end.values[-1]} does not match calculated '
                f'number of bins: {number_of_bins} ')

        return

    def add_pycontrol_events_to_df(self, unique_events):

        # Add timestamp_idx
        self.pycontrol_events['timestamp_idx'] = np.floor(self.pycontrol_events.timestamp.values[:] / 1000 / self.bin_size).astype(int)

        # Iterate over states
        for unique_event in unique_events:
            unique_event_df = self.pycontrol_events[self.pycontrol_events['event'] == unique_event]
            self.pyaldata_df = _add_data_to_trial(
                df_to_add_to=self.pyaldata_df,
                new_data_column=f'{unique_event}_event_values',
                df_to_add_from=unique_event_df,
                columns_to_read_from='value',
                timestamp_column=f'{unique_event}_event_idx'
            )

        return

    def add_motion_sensor_data_to_df(self):
        # Bin timestamps
        self.pycontrol_motion_sensors['timestamp_idx'] = np.floor(self.pycontrol_motion_sensors.timestamps.values[:] / 1000 / self.bin_size).astype(int)

        # Add columns
        self.pyaldata_df['motion_sensor_xy'] = np.nan

        # Add data
        self.pyaldata_df = _add_data_to_trial(
            df_to_add_to=self.pyaldata_df,
            new_data_column='motion_sensor_xy',
            df_to_add_from=self.pycontrol_motion_sensors,
            columns_to_read_from=['x', 'y'],
            timestamp_column=None
        )
        return

    def add_anipose_data_to_df(self):
        for anipose_key, anipose_value in self.anipose_data.items():
            # Bin timestamps
            # TODO: Fix time units, since here we are already in seconds its all good
            anipose_value['timestamp_idx'] = np.floor(anipose_value.timestamps.values[:] / self.bin_size).astype(int)

            # Add columns
            self.pyaldata_df[anipose_key] = np.nan

            # Add data
            self.pyaldata_df = _add_data_to_trial(
                df_to_add_to=self.pyaldata_df,
                new_data_column=anipose_key,
                df_to_add_from=anipose_value,
                columns_to_read_from='angle' if 'angle' in anipose_key else ['x', 'y', 'z'],
                timestamp_column=None
            )

        return

    def add_spiking_data_to_df(self):
        pass

    def run_conversion(self):
        """
        Main run routine for pyaldata conversion

        Returns
        -------

        """

        print("Converting parsed nwb data into pyaldata format")

        # Initialize all the necessary columns
        # state columns
        state_columns = ['mouse', 'date', 'trial_id', 'trial_name', 'trial_length',
                         'bin_size', 'idx_trial_start', 'idx_trial_end']
        # event columns
        unique_events = self.pycontrol_events['event'].unique()
        event_columns = []
        for unique_event in unique_events:
            event_columns.append(f'{unique_event}_event_values')
            event_columns.append(f'{unique_event}_event_idx')
        # All columns
        columns = state_columns + event_columns

        self.pyaldata_df = pd.DataFrame(columns=columns)

        # Add information
        self.add_pycontrol_states_to_df()
        self.add_pycontrol_events_to_df(unique_events)
        self.add_motion_sensor_data_to_df()
        self.add_anipose_data_to_df()  # TODO
        self.add_spiking_data_to_df()  # TODO

        return

    def save_to_csv(self):
        print('Saved the data somewhere i guess')
        pass


def convert_nwb_to_pyaldata(nwbfile_path):

    parsed_nwbfile = ParsedNWBFile(nwbfile_path)
    parsed_nwbfile.run_conversion()
    parsed_nwbfile.save_to_csv()
    breakpoint()


    return