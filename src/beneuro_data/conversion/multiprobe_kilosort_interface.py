from copy import deepcopy
import warnings
from pathlib import Path
from typing import Literal, Optional

import numpy as np
import pandas as pd
from neuroconv.datainterfaces import KiloSortSortingInterface
from neuroconv.tools.spikeinterface import add_sorting_to_nwbfile
from neuroconv.utils import DeepDict
from pydantic import DirectoryPath
from pynwb import NWBFile
import probeinterface as pi


def _try_loading_trajectory_file(raw_recording_path):
    """
    Load trajectory of probes from pinpoint output files

    Parameters
    ----------
    raw_recording_path :
        path to raw session

    Returns
    -------
    Dictionary with trajectory information per probe

    """

    pinpoint_trajectory_file = list(raw_recording_path.glob("*trajectory.txt"))
    if not pinpoint_trajectory_file:
        warnings.warn(
            f"No trajectory file from pinpoint found. If no trajectory file is present"
            f" channel map information cannot be loaded"
        )
        return

    elif len(pinpoint_trajectory_file) > 1:
        raise FileExistsError("Too many files found; expected only one.")

    with open(pinpoint_trajectory_file[0], "r") as f:
        trajectory_str = [l.strip() for l in f.readlines() if l.strip() != ""]
        probe_trajectory_pairs = list(zip(trajectory_str[::2], trajectory_str[1::2]))
        trajectory_dict = {
            probe: trajectory for probe, trajectory in probe_trajectory_pairs
        }

    return trajectory_dict


def _load_channel_map_information_from_pinpoint_probe(filename, pinpoint_probe_name):
    """
    Extract the brain area of each electrode per probe

    Parameters
    ----------
    filename :
        Pinpoint channel_map file to open
    pinpoint_probe_name :
        Key generated in pinpoint to identify the probe

    Returns
    -------
    Dataframe containing bran area of each electrode

    """
    with open(filename, "r") as file:
        channel_map_str = file.read().strip()

    channel_map_str = channel_map_str.strip("[]").split('","')
    data_string = ""
    for item in channel_map_str:
        if pinpoint_probe_name in item:
            data_string = item.split(":", 1)[1]
            break

    # Split the string by ";" to separate each entry
    entries = data_string.split(";")
    data = [
        dict(zip(["id", "area_number", "area_name", "area_color"], entry.split(",")))
        for entry in entries
    ]

    df = pd.DataFrame(data)
    return df


def _create_channel_map(location_information, raw_recording_path):
    """
    Function to create the channel map of each probe

    Parameters
    ----------
    location_information :
        Pinpoint generate trajectory file (*trajectory.txt)
    raw_recording_path :
        Path to raw session

    Returns
    -------
    Dictionary of channel map for each probe

    """

    pinpoint_trajectory_file = list(raw_recording_path.glob("*channel_map.txt"))
    if not pinpoint_trajectory_file:
        warnings.warn(f"No channel map file from pinpoint found")
        return

    elif len(pinpoint_trajectory_file) > 1:
        warnings.warn(f"Too many channel map files from pinpoint found")
        return

    channel_map = {}
    for probe in location_information.keys():
        pinpoint_probe_name = location_information[probe].split(":")[0]
        channel_map[probe] = _load_channel_map_information_from_pinpoint_probe(
            filename=pinpoint_trajectory_file[0], pinpoint_probe_name=pinpoint_probe_name
        )

    return channel_map


class MultiProbeKiloSortInterface(KiloSortSortingInterface):
    def __init__(
        self,
        # folder_paths: tuple[DirectoryPath, ...],
        folder_path: DirectoryPath,
        keep_good_only: bool = False,
        verbose: bool = True,
    ):
        self.kilosort_folder_paths = list(Path(folder_path).glob("**/sorter_output"))
        self.probe_names = [
            ks_path.parent.name.split("_")[-1] for ks_path in self.kilosort_folder_paths
        ]

        self.kilosort_interfaces = [
            KiloSortSortingInterface(folder_path, keep_good_only, verbose)
            for folder_path in self.kilosort_folder_paths
        ]

        self.folder_path = Path(folder_path)

    def set_aligned_starting_time(self, aligned_starting_time: float):
        for kilosort_interface in self.kilosort_interfaces:
            kilosort_interface.set_aligned_starting_time(aligned_starting_time)

    def add_probe_information_to_nwb(self, nwbfile):
        """
        Add probe information stored in SpikeGLX and pinpoint to the nwbfile

        Parameters
        ----------
        nwbfile :
            NWBFile handle

        Returns
        -------
        """

        raw_recording_path = Path(str(self.folder_path).replace("processed", "raw"))
        meta_filepaths = list(raw_recording_path.rglob("*/*ap.meta"))

        # Try loading trajectory information from pinpoint
        pinpoint_trajectories = _try_loading_trajectory_file(raw_recording_path)

        # If pinpoint_trajectories is available, load channel map
        channel_map = (
            _create_channel_map(pinpoint_trajectories, raw_recording_path)
            if pinpoint_trajectories is not None
            else None
        )

        for probe_name in self.probe_names:

            # Get meta_file_path for probe_name
            meta_filepath = next(
                (path for path in meta_filepaths if probe_name in str(path)), None
            )

            # Load probe object
            probe = pi.read_spikeglx(meta_filepath)

            if probe.get_shank_count() == 1:  # Set shank ids
                probe.set_shank_ids(np.full((probe.get_contact_count(),), 1))
            else:
                raise NotImplementedError("Multishank probes not yet implemented")

            nwbfile.create_device(
                name=probe_name,
                description=probe.annotations["model_name"],  # Neuropixels 1.0
                manufacturer=probe.annotations["manufacturer"],
            )
            nwbfile.create_electrode_group(
                name=probe_name,
                description=f'{probe.annotations["model_name"]}. Location is the output from '
                f"pinpoint and corresponds to the targeted brain area",
                location=(
                    pinpoint_trajectories[probe_name] if pinpoint_trajectories else "No pinpoint trajectory"
                ),
                device=nwbfile.devices[probe_name],
            )

            for contact_position, contact_id in zip(
                probe.contact_positions, probe.contact_ids
            ):
                x, y = contact_position
                z = 0.0
                contact_id = int(contact_id.split("e")[1:][0])
                if channel_map is not None:
                    contact_location = channel_map[probe_name].area_name[contact_id]
                else:
                    contact_location = "nan"
                nwbfile.add_electrode(
                    group=nwbfile.electrode_groups[probe_name],
                    x=float(x),
                    y=float(y),
                    z=z,
                    id=contact_id,
                    location=contact_location,
                    reference=f"Local probe reference: Top of the probe",
                    enforce_unique_id=False,
                )

    def add_to_nwbfile(
        self,
        nwbfile: NWBFile,
        metadata: Optional[DeepDict] = None,
    ):

        self.add_probe_information_to_nwb(nwbfile)

        # Kilosort output will be saved in processing and not units
        # units is reserved for the units curated by Phy
        for probe_name, kilosort_interface, kilosort_folder_path in zip(
            self.probe_names, self.kilosort_interfaces, self.kilosort_folder_paths
        ):
            templates = np.load(kilosort_folder_path / "templates.npy")

            # Copied function from nwb to add templates to nwb
            self.add_one_probe_to_nwbfile(
                sorting_interface=kilosort_interface,
                nwbfile=nwbfile,
                metadata=metadata,
                write_as="processing",
                units_name=f"units_{probe_name}",
                units_description=f"Kilosorted units on {probe_name}",
                waveform_means=templates,
            )

    def get_metadata(self) -> DeepDict:
        return DeepDict()

    @staticmethod
    def add_one_probe_to_nwbfile(
        sorting_interface: KiloSortSortingInterface,
        nwbfile: NWBFile,
        metadata: Optional[DeepDict] = None,
        stub_test: bool = False,
        write_ecephys_metadata: bool = False,
        write_as: Literal["units", "processing"] = "units",
        units_name: str = "units",
        units_description: str = "Autogenerated by neuroconv.",
        waveform_means: Optional[np.ndarray] = None,
    ):
        """
        Primary function for converting the data in a SortingExtractor to NWB format.

        This function is copied from neuroconv and modified to add waveform means (templates)
        to the NWB file.

        Original definition of neuroconv's BaseSortingExtractorInterface.add_to_nwbfile:
        https://github.com/catalystneuro/neuroconv/blob/96c8ed4d76bd734e335acd999c015770f9cfd92a/src/neuroconv/datainterfaces/ecephys/basesortingextractorinterface.py#L282


        Parameters
        ----------
        sorting_interface : KiloSortSortingInterface
            KiloSortSortingInterface for one probe.
        nwbfile :
            Fill the relevant fields within the NWBFile object.
        metadata :
            Information for constructing the NWB file (optional) and units table descriptions.
            Should be of the format::

                metadata["Ecephys"]["UnitProperties"] = dict(name=my_name, description=my_description)
        stub_test :
            If True, will truncate the data to run the conversion faster and take up less memory.
        write_ecephys_metadata :
            Write electrode information contained in the metadata.
        write_as :
            How to save the units table in the nwb file. Options:
            - 'units' will save it to the official NWBFile.Units position; recommended only for the final form of the data.
            - 'processing' will save it to the processing module to serve as a historical provenance for the official table.
        units_name :
            The name of the units table. If write_as=='units', then units_name must also be 'units'.
        units_description :
            Description of where the units in this sorting come from.
        waveform_means :
            Mean waveform (=template) of each unit as recorded from each channel.
            Array of shape (n_units, n_samples, n_channels)
        """

        metadata_copy = deepcopy(metadata)
        if write_ecephys_metadata:
            sorting_interface.add_channel_metadata_to_nwb(
                nwbfile=nwbfile, metadata=metadata_copy
            )

        if stub_test:
            sorting_extractor = sorting_interface.subset_sorting()
        else:
            sorting_extractor = sorting_interface.sorting_extractor

        property_descriptions = dict()
        for metadata_column in metadata_copy["Ecephys"].get("UnitProperties", []):
            property_descriptions.update(
                {metadata_column["name"]: metadata_column["description"]}
            )
            for unit_id in sorting_extractor.get_unit_ids():
                # Special condition for wrapping electrode group pointers to actual object ids rather than string names
                if (
                    metadata_column["name"] == "electrode_group"
                    and nwbfile.electrode_groups
                ):
                    value = nwbfile.electrode_groups[
                        sorting_interface.sorting_extractor.get_unit_property(
                            unit_id=unit_id, property_name="electrode_group"
                        )
                    ]
                    sorting_extractor.set_unit_property(
                        unit_id=unit_id,
                        property_name=metadata_column["name"],
                        value=value,
                    )

        add_sorting_to_nwbfile(
            sorting_extractor,
            nwbfile=nwbfile,
            property_descriptions=property_descriptions,
            write_as=write_as,
            units_name=units_name,
            units_description=units_description,
            waveform_means=waveform_means,
        )
