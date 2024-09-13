from pathlib import Path
from typing import Optional, Literal

import numpy as np
from neuroconv.datainterfaces import KiloSortSortingInterface
from neuroconv.tools.spikeinterface import add_sorting_to_nwbfile
from neuroconv.utils import DeepDict
from pydantic import DirectoryPath
from pynwb import NWBFile


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

    def add_to_nwbfile(
        self,
        nwbfile: NWBFile,
        metadata: Optional[DeepDict] = None,
    ):
        # Kilosort output will be saved in processing and not units
        # units is reserved for the units curated by Phy
        for probe_name, kilosort_interface, kilosort_folder_path in zip(
            self.probe_names, self.kilosort_interfaces, self.kilosort_folder_paths
        ):
            templates = np.load(kilosort_folder_path / "templates.npy")

            add_one_probe_to_nwbfile(
                nwbfile=nwbfile,
                metadata=metadata,
                write_as="processing",
                units_name=f"units_{probe_name}",
                units_description=f"Kilosorted units on {probe_name}",
                waveform_means=templates,
            )

            # write_sorting_to_nwbfile(
            #     sorting=kilosort_interface.sorting_extractor,
            #     nwbfile=nwbfile,
            #     metadata=metadata,
            #     write_as="processing",
            #     units_name=f"units_{probe_name}",
            #     units_description=f"Kilosorted units on {probe_name}",
            #     waveform_means=templates,
            # )



    def get_metadata(self) -> DeepDict:
        return DeepDict()


def add_one_probe_to_nwbfile(
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

    Parameters
    ----------
    nwbfile : NWBFile
        Fill the relevant fields within the NWBFile object.
    metadata : DeepDict
        Information for constructing the NWB file (optional) and units table descriptions.
        Should be of the format::

            metadata["Ecephys"]["UnitProperties"] = dict(name=my_name, description=my_description)
    stub_test : bool, default: False
        If True, will truncate the data to run the conversion faster and take up less memory.
    write_ecephys_metadata : bool, default: False
        Write electrode information contained in the metadata.
    write_as : {'units', 'processing'}
        How to save the units table in the nwb file. Options:
        - 'units' will save it to the official NWBFile.Units position; recommended only for the final form of the data.
        - 'processing' will save it to the processing module to serve as a historical provenance for the official table.
    units_name : str, default: 'units'
        The name of the units table. If write_as=='units', then units_name must also be 'units'.
    units_description : str, default: 'Autogenerated by neuroconv.'
    """
    from ...tools.spikeinterface import add_sorting_to_nwbfile

    metadata_copy = deepcopy(metadata)
    if write_ecephys_metadata:
        self.add_channel_metadata_to_nwb(nwbfile=nwbfile, metadata=metadata_copy)

    if stub_test:
        sorting_extractor = self.subset_sorting()
    else:
        sorting_extractor = self.sorting_extractor

    property_descriptions = dict()
    for metadata_column in metadata_copy["Ecephys"].get("UnitProperties", []):
        property_descriptions.update({metadata_column["name"]: metadata_column["description"]})
        for unit_id in sorting_extractor.get_unit_ids():
            # Special condition for wrapping electrode group pointers to actual object ids rather than string names
            if metadata_column["name"] == "electrode_group" and nwbfile.electrode_groups:
                value = nwbfile.electrode_groups[
                    self.sorting_extractor.get_unit_property(unit_id=unit_id, property_name="electrode_group")
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
    )