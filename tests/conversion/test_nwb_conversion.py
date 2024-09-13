from pathlib import Path

import pytest
from pynwb import NWBHDF5IO

from beneuro_data.config import _load_config
from beneuro_data.conversion import convert_to_nwb

DATA_DIR = Path("/data/nwb_prototype/")

# TODO Add more sessions with different combinations of data available
# TODO Add more checks and tailor them to the data available


@pytest.mark.needs_experimental_data
@pytest.mark.processing
@pytest.mark.parametrize("session_name", ["M027_2024_03_20_11_30"])
def test_nwb_conversion_on_data(session_name):
    # setup
    config = _load_config()
    subject_name = session_name[:4]

    raw_session_folder_path = DATA_DIR / "raw" / subject_name / session_name
    processed_folder_path = DATA_DIR / "processed" / subject_name / session_name

    # delete existing NWB file
    for nwb_file_path in processed_folder_path.glob("*.nwb"):
        nwb_file_path.unlink()

    nwb_file_path = convert_to_nwb(
        raw_session_folder_path,
        subject_name,
        DATA_DIR,
        whitelisted_files_in_root=config.WHITELISTED_FILES_IN_ROOT,
        allowed_extensions_not_in_root=config.EXTENSIONS_TO_RENAME_AND_UPLOAD,
        run_kilosort=False,
        clean_up_temp_files=True,
    )

    # find the created NWB file in the processed folder
    nwb_file_path = list(processed_folder_path.glob("*.nwb"))[0]

    with NWBHDF5IO(nwb_file_path, "r") as io:
        read_nwbfile = io.read()

        assert sorted(read_nwbfile.processing.keys()) == ["behavior", "ecephys"]

        assert {c.name for c in read_nwbfile.processing["behavior"].children} == {
            "behavioral_states",
            "Position",
            "behavioral_events",
            "print_events",
            "Pose estimation",
        }

        assert "spike_times" in read_nwbfile.processing["ecephys"]["units_imec0"].colnames
        assert "waveform_mean" in read_nwbfile.processing["ecephys"]["units_imec0"].colnames
