import warnings
from pathlib import Path

from typing import Optional

from beneuro_data.spike_sorting import run_kilosort_on_session_and_save_in_processed

from beneuro_data.conversion.beneuro_converter import BeNeuroConverter
from beneuro_data.conversion.gpu_memory import get_free_gpu_memory

from beneuro_data.data_validation import (
    validate_raw_session,
    _find_spikeglx_recording_folders_in_session,
)


def convert_to_nwb(
    raw_session_path: Path,
    subject_name: str,
    base_path: Path,
    whitelisted_files_in_root: tuple[str, ...],
    allowed_extensions_not_in_root: tuple[str, ...],
    run_kilosort: bool,
    stream_names_to_process: Optional[tuple[str, ...]] = None,
    clean_up_temp_files: Optional[bool] = False,
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
    validate_raw_session(
        raw_session_path,
        subject_name,
        include_behavior=True,
        include_ephys=True,
        include_videos=True,
        whitelisted_files_in_root=whitelisted_files_in_root,
        allowed_extensions_not_in_root=allowed_extensions_not_in_root,
    )

    raw_recording_path = _find_spikeglx_recording_folders_in_session(raw_session_path)[0]

    recording_folder_name = raw_recording_path.name
    session_folder_name = raw_session_path.name

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
    processed_recording_ephys_path = (
        processed_session_path / f"{session_folder_name}_ephys" / recording_folder_name
    )

    # make the subject's, session's, and ephys folders if they don't exist
    # this creates all of them
    if not processed_recording_ephys_path.exists():
        processed_recording_ephys_path.mkdir(parents=True, exist_ok=False)

    # make sure the NWB file doesn't already exist
    nwb_file_output_path = processed_session_path / f"{session_folder_name}.nwb"
    if nwb_file_output_path.exists():
        raise FileExistsError(f"NWB file already exists at {nwb_file_output_path}")

    # see if kilosort has already been run
    raw_probe_folders = sorted([p.name for p in raw_recording_path.iterdir() if p.is_dir()])
    processed_probe_folders = sorted(
        [p.name for p in processed_recording_ephys_path.iterdir() if p.is_dir()]
    )
    if (not run_kilosort) and (raw_probe_folders != processed_probe_folders):
        warnings.warn(
            "Looks like not all probes have been kilosorted. You might want to do it."
        )

    if run_kilosort:
        # kilosort needs around 4.5 GB of GPU memory, might fail otherwise
        # so check if we have enough
        if all(free_mem_mb < 4400 for free_mem_mb in get_free_gpu_memory()):
            warnings.warn("KiloSort might fail because of low GPU memory.")

        run_kilosort_on_session_and_save_in_processed(
            raw_session_path,
            subject_name,
            base_path,
            allowed_extensions_not_in_root,
            stream_names_to_process,
            clean_up_temp_files,
        )

    # finally, run the conversion
    source_data = dict(
        PyControl={
            "file_path": str(raw_session_path),
        },
        KiloSort={
            "processed_recording_path": str(processed_session_path),
        },
    )

    converter = BeNeuroConverter(source_data)

    converter.run_conversion(
        metadata=converter.get_metadata(),
        nwbfile_path=nwb_file_output_path,
    )

    return nwb_file_output_path
