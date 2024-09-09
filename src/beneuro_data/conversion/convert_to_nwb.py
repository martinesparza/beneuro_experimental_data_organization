import warnings
from pathlib import Path
from typing import Optional

from beneuro_data.conversion.animal_profile_interface import AnimalProfileInterface
from beneuro_data.conversion.anipose_interface import AniposeInterface
from beneuro_data.conversion.beneuro_converter import BeNeuroConverter
from beneuro_data.conversion.gpu_memory import get_free_gpu_memory
from beneuro_data.conversion.multiprobe_kilosort_interface import (
    MultiProbeKiloSortInterface,
)
from beneuro_data.data_validation import (
    _find_spikeglx_recording_folders_in_session,
    validate_raw_session,
)
from beneuro_data.spike_sorting import run_kilosort_on_session_and_save_in_processed


def _try_adding_kilosort_to_source_data(
    source_data: dict,
    processed_session_path: Path,
    raw_session_path: Path,
) -> None:
    if any(processed_session_path.glob("**/spike_times.npy")):
        # if it looks like kilosort has been run
        # then try loading it
        try:
            MultiProbeKiloSortInterface(str(processed_session_path))
        # warn if we can't read it
        except Exception as e:
            warnings.warn(f"Problem loading Kilosort data: {str(e)}")
        # if we can, then add it to the conversion
        else:
            source_data.update(
                Kilosort={
                    "processed_recording_path": str(processed_session_path),
                }
            )

    elif len(_find_spikeglx_recording_folders_in_session(raw_session_path)) > 0:
        # if there's no kilosort output found,
        # check if there could be one because the raw data exists
        warnings.warn(
            "You might want to run Kilosort. Found ephys data but no Kilosort output."
        )


def _try_adding_profile_to_source_data(source_data: dict, raw_session_path: Path) -> None:
    # if loading .profile data works, add it to the interfaces
    try:
        AnimalProfileInterface(raw_session_path)
    except Exception as e:
        warnings.warn(f"Problem loading data from profile: {str(e)}")
    else:
        source_data.update(
            AnimalProfile={
                "session_path": str(raw_session_path),
            }
        )


def _try_adding_anipose_to_source_data(
    source_data: dict, processed_session_path: Path, raw_session_path: Path
):
    csv_paths = list(processed_session_path.glob("**/*3dpts_angles.csv"))

    if len(csv_paths) == 0:
        warnings.warn("No pose estimation data found.")
        return

    if len(csv_paths) > 1:
        raise FileExistsError(
            f"More than one pose estimation HDF file " f"found: {csv_paths}"
        )

    csv_path = csv_paths[0]
    try:
        AniposeInterface(csv_path, raw_session_path)
    except Exception as e:
        warnings.warn(f"Problem loading anipose data: {str(e)}")
    else:
        source_data.update(
            Anipose={
                "csv_path": str(csv_path),
                "raw_session_path": str(raw_session_path),
            }
        )


def convert_to_nwb(
    raw_session_path: Path,
    subject_name: str,
    base_path: Path,
    whitelisted_files_in_root: tuple[str, ...],
    allowed_extensions_not_in_root: tuple[str, ...],
    run_kilosort: bool,
    stream_names_to_process: Optional[tuple[str, ...]] = None,
    clean_up_temp_files: Optional[bool] = True,
    verbose_kilosort: bool = True,
):
    # make sure the kilosort arguments are given
    # if run_kilosort:
    #    assert allowed_extensions_not_in_root is not None
    #    assert stream_names_to_process is not None
    #    assert clean_up_temp_files is not None

    # make sure the paths are Path objects and consistent with each other
    if isinstance(raw_session_path, str):
        raw_session_path = Path(raw_session_path)
    if isinstance(base_path, str):
        base_path = Path(base_path)
    if not raw_session_path.is_relative_to(base_path / "raw"):
        raise ValueError(f"{raw_session_path} is not in {base_path / 'raw'}")

    # validate session before doing any conversion
    _, ephys_files, _ = validate_raw_session(
        raw_session_path,
        subject_name,
        include_behavior=True,
        include_ephys=True,
        include_videos=True,
        whitelisted_files_in_root=whitelisted_files_in_root,
        allowed_extensions_not_in_root=allowed_extensions_not_in_root,
    )

    # determine output path
    raw_base_path = base_path / "raw"
    processed_base_path = base_path / "processed"

    # the base processed path should exist
    # we're not going to create it now
    if not processed_base_path.exists():
        raise FileNotFoundError(
            f"Base processed path does not exist. It should be at {processed_base_path}"
        )

    processed_session_path = processed_base_path / raw_session_path.relative_to(
        raw_base_path
    )
    if not processed_session_path.exists():
        processed_session_path.mkdir(parents=True, exist_ok=False)

    # make sure the NWB file doesn't already exist
    nwb_file_output_path = processed_session_path / f"{raw_session_path.name}.nwb"
    if nwb_file_output_path.exists():
        raise FileExistsError(f"NWB file already exists at {nwb_file_output_path}")

    # for raw_recording_path in _find_spikeglx_recording_folders_in_session(raw_session_path):
    #    processed_recording_ephys_path = (
    #        processed_session_path
    #        / f"{raw_session_path.name}_ephys"
    #        / raw_recording_path.name
    #    )

    #    # make the subject's, session's, and ephys folders if they don't exist
    #    # this creates all of them
    #    if not processed_recording_ephys_path.exists():
    #        processed_recording_ephys_path.mkdir(parents=True, exist_ok=False)

    #    # see if kilosort has already been run
    #    raw_probe_folders = sorted(
    #        [p.name for p in raw_recording_path.iterdir() if p.is_dir()]
    #    )
    #    processed_probe_folders = sorted(
    #        [p.name for p in processed_recording_ephys_path.iterdir() if p.is_dir()]
    #    )
    #    if (not run_kilosort) and (raw_probe_folders != processed_probe_folders):
    #        non_sorted_probe_folders = sorted(
    #            list(set(raw_probe_folders).difference(set(processed_probe_folders)))
    #        )
    #        warnings.warn(
    #            f"Looks like not all probes have been kilosorted. You might want to do it.\nNon-sorted folders:\n{non_sorted_probe_folders}"
    #        )

    if run_kilosort:
        # kilosort needs around 4.5 GB of GPU memory, might fail otherwise
        # so check if we have enough
        if all(free_mem_mb < 4400 for free_mem_mb in get_free_gpu_memory()):
            warnings.warn("Kilosort might fail because of low GPU memory.")

        run_kilosort_on_session_and_save_in_processed(
            raw_session_path,
            subject_name,
            base_path,
            allowed_extensions_not_in_root,
            stream_names_to_process,
            clean_up_temp_files,
            verbose_kilosort,
        )

    # specify where the data should be read from by the converter
    source_data = dict(
        PyControl={
            "file_path": str(raw_session_path),
        },
    )

    _try_adding_kilosort_to_source_data(
        source_data, processed_session_path, raw_session_path
    )

    _try_adding_profile_to_source_data(source_data, raw_session_path)

    _try_adding_anipose_to_source_data(
        source_data, processed_session_path, raw_session_path
    )

    # finally, run the conversion
    converter = BeNeuroConverter(source_data)

    metadata = converter.get_metadata()

    metadata["NWBFile"].deep_update(
        lab="Be.Neuro Lab",
        institution="Imperial College London",
    )

    converter.run_conversion(
        metadata=metadata,
        nwbfile_path=nwb_file_output_path,
    )

    return nwb_file_output_path
