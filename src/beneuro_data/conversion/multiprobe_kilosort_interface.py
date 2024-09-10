import warnings
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from neuroconv.datainterfaces import KiloSortSortingInterface
from neuroconv.utils import DeepDict, FolderPathType
from pynwb import NWBFile
import probeinterface as pi


def try_loading_trajectory_file(raw_recording_path):

    pinpoint_trajectory_file = list(raw_recording_path.glob('*trajectory.txt'))
    if not pinpoint_trajectory_file:
        warnings.warn(f"No trajectory file from pinpoint found")
        return

    elif len(pinpoint_trajectory_file) > 1:
        warnings.warn(f"Too many trajectory files from pinpoint found")
        return

    with open(pinpoint_trajectory_file[0], "r") as f:
        trajectory_str = [l.strip() for l in f.readlines() if l.strip() != ""]
        probe_trajectory_pairs = list(zip(trajectory_str[::2], trajectory_str[1::2]))
        trajectory_dict = {probe: trajectory for probe, trajectory in probe_trajectory_pairs}

    return trajectory_dict


def extract_channel_map_from_pinpoint_probe(filename, pinpoint_probe_name):
    with open(filename, 'r') as file:
        channel_map_str = file.read().strip()

    channel_map_str = channel_map_str.strip('[]').split('","')
    data_string = ''
    for item in channel_map_str:
        if pinpoint_probe_name in item:
            data_string = item.split(":", 1)[1]
            break

    # Split the string by ";" to separate each entry
    entries = data_string.split(";")
    data = [dict(zip(["id", "area_number", "area_name", "area_color"], entry.split(",")))
            for entry in entries]

    df = pd.DataFrame(data)
    return df


def create_channel_map(location_information, raw_recording_path):

    pinpoint_trajectory_file = list(raw_recording_path.glob('*channel_map.txt'))
    if not pinpoint_trajectory_file:
        warnings.warn(f"No channel map file from pinpoint found")
        return

    elif len(pinpoint_trajectory_file) > 1:
        warnings.warn(f"Too many channel map files from pinpoint found")
        return

    channel_map = {}
    for probe in location_information.keys():
        pinpoint_probe_name = location_information[probe].split(":")[0]
        channel_map[probe] = extract_channel_map_from_pinpoint_probe(
            pinpoint_trajectory_file[0],
            pinpoint_probe_name
        )
    
    return channel_map


class MultiProbeKiloSortInterface(KiloSortSortingInterface):
    def __init__(
        self,
        processed_recording_path: str,
        keep_good_only: bool = False,
        verbose: bool = True,
    ):
        kilosort_folder_paths = list(
            Path(processed_recording_path).glob("**/sorter_output")
        )
        self.probe_names = [
            ks_path.parent.name.split("_")[-1] for ks_path in kilosort_folder_paths
        ]

        self.kilosort_interfaces = [
            KiloSortSortingInterface(folder_path, keep_good_only, verbose)
            for folder_path in kilosort_folder_paths
        ]

        self.processed_recording_path = Path(processed_recording_path)
        # self.raw_recording_path = Path(raw_recording_path)

    def set_aligned_starting_time(self, aligned_starting_time: float):
        for kilosort_interface in self.kilosort_interfaces:
            kilosort_interface.set_aligned_starting_time(aligned_starting_time)

    def try_adding_probe_information_to_nwb(self, nwbfile):
        raw_recording_path = Path(
            str(self.processed_recording_path).replace("processed", "raw"))
        meta_files = list(raw_recording_path.rglob("*/*ap.meta"))
        location_information = try_loading_trajectory_file(raw_recording_path)
        if location_information is not None:
            channel_map = create_channel_map(location_information, raw_recording_path)
        elif location_information is None:
            channel_map = None

        for meta_file, probe_name in zip(meta_files, self.probe_names):
            probe = pi.read_spikeglx(meta_file)  # Load probe object

            if probe.get_shank_count() == 1:  # Set shank ids
                probe.set_shank_ids(np.full((probe.get_contact_count(), ), 1))
            else:
                warnings.warn(
                    "More than one shank. You are probably using Neuropixels 2.0. This is not "
                    "yet implemented in bnd"
                )

            nwbfile.create_device(
                name=probe_name,
                description=probe.annotations["model_name"],  # Neuropixels 1.0
                manufacturer=probe.annotations["manufacturer"],
            )
            nwbfile.create_electrode_group(
                name=probe_name,
                description=f'{probe.annotations["model_name"]}. Location is the output from '
                            f'pinpoint and corresponds to the targeted brain area',
                location=location_information[probe_name] if location_information else None,
                device=nwbfile.devices[probe_name],
            )

            for contact_position, contact_id in zip(probe.contact_positions, probe.contact_ids):
                x, y = contact_position
                z = 0.0
                # breakpoint()
                contact_id = int(contact_id.split('e')[1:][0])
                if channel_map is not None:
                    contact_location = channel_map[probe_name].area_name[contact_id]
                else:
                    contact_location = None
                nwbfile.add_electrode(
                    group=nwbfile.electrode_groups[probe_name],
                    x=float(x),
                    y=float(y),
                    z=z,
                    id=contact_id,
                    location=contact_location,
                    reference=f"Local probe reference: Top of the probe",
                    enforce_unique_id=False
                )

    def add_to_nwbfile(
        self,
        nwbfile: NWBFile,
        metadata: Optional[DeepDict] = None,
        stub_test: bool = False,
        # write_ecephys_metadata: bool = True,
        # write_as: Literal["units", "processing"] = "units",
        # units_name: str = "units",
        # units_description: str = "Autogenerated by neuroconv.",
    ):

        self.try_adding_probe_information_to_nwb(nwbfile)
        # breakpoint()

        # Kilosort output will be saved in processing and not units
        # units is reserved for the units curated by Phy
        for probe_name, kilosort_interface in zip(
            self.probe_names, self.kilosort_interfaces,
        ):


            kilosort_interface.add_to_nwbfile(
                nwbfile,
                metadata,
                stub_test,
                write_ecephys_metadata=True,
                write_as="processing",  # kilosort output is not the final curated version
                units_name=f"units_{probe_name}",
                units_description=f"Kilosorted units on {probe_name}",
            )



            # The following does add the probes to the NWB file but without any useful info,
            # so I'll use probeinterface for that
            # raw_recording_path = Path(str(self.processed_recording_path).replace("processed", "raw"))
            # recording = se.read_spikeglx(
            #    raw_recording_path,
            #    stream_name = f"{probe_name}.ap",
            #    load_sync_channel = True,
            # )
            #
            # from neuroconv.tools.spikeinterface import (
            #    add_devices,
            #    add_electrode_groups,
            #    add_electrodes,
            # )
            #
            # add_devices(nwbfile=nwbfile, metadata=metadata)
            # add_electrode_groups(recording=recording, nwbfile=nwbfile, metadata=metadata)
            # add_electrodes(recording=recording, nwbfile=nwbfile, metadata=metadata)

    def get_metadata(self) -> DeepDict:
        return DeepDict()
