from pathlib import Path
from typing import Optional

import numpy as np
from neuroconv.datainterfaces import KiloSortSortingInterface
from neuroconv.tools.spikeinterface import write_sorting_to_nwbfile
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

            write_sorting_to_nwbfile(
                sorting=kilosort_interface.sorting_extractor,
                nwbfile=nwbfile,
                metadata=metadata,
                write_as="processing",
                units_name=f"units_{probe_name}",
                units_description=f"Kilosorted units on {probe_name}",
                waveform_means=templates,
            )

    def get_metadata(self) -> DeepDict:
        return DeepDict()
