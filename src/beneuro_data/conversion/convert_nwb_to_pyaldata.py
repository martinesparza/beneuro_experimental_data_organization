"""
Module for conversion from nwb to pyaldata format
"""
from pynwb import NWBHDF5IO


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
            self.spike_data = self.parse_ephys_data()

    def run_conversion(self):
        pass

    def save_to_csv(self):
        pass

    def parse_nwb_pycontrol_states(self):
        pass

    def parse_nwb_pycontrol_events(self):
        pass

    def parse_motion_sensors(self):
        pass

    def parse_anipose_output(self):
        pass

    def parse_ephys_data(self):
        pass


def convert_to_pyaldata(nwbfile_path):
    """Transform data from .nwb format to task-specific trialdata

        Args:
            nwbfile_path:

        Returns:

        """
    parsed_nwbfile = ParsedNWBFile(nwbfile_path)
    parsed_nwbfile.run_conversion()
    parsed_nwbfile.save_to_csv()

    return