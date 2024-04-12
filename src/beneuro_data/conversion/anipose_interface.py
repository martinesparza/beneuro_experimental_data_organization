from pathlib import Path
from typing import Optional

import h5py
import numpy as np
import spikeinterface.extractors as se
from ndx_pose import PoseEstimation, PoseEstimationSeries
from neuroconv.basetemporalalignmentinterface import \
    BaseTemporalAlignmentInterface
from neuroconv.tools.signal_processing import get_rising_frames_from_ttl
from neuroconv.utils import DeepDict, FilePathType
from pynwb import NWBFile

from beneuro_data.data_validation import \
    _find_spikeglx_recording_folders_in_session
from beneuro_data.spike_sorting import get_ap_stream_names

DEFAULT_FPS = 100


class AniposeInterface(BaseTemporalAlignmentInterface):
    keypoint_names = [
        "shoulder_center",
        "left_shoulder",
        "left_paw",
        "right_shoulder",
        "right_elbow",
        "right_paw",
        "hip_center",
        "left_knee",
        "left_ankle",
        "left_foot",
        "right_knee",
        "right_ankle",
        "right_foot",
        "tail_base",
        "tail_middle",
        "tail_tip",
        "left_elbow",
        "left_wrist",
        "right_wrist",
    ]

    def __init__(self, h5_path: FilePathType, raw_session_path: FilePathType):
        super().__init__()

        self.h5_path = Path(h5_path)
        self.raw_session_path = Path(raw_session_path)

        self.pose_data = self.load_anipose_from_h5()

    def _add_to_behavior_module(self, beh_obj, nwbfile: NWBFile) -> None:
        behavior_module = nwbfile.processing.get("behavior")

        if behavior_module is None:
            behavior_module = nwbfile.create_processing_module(
                "behavior", "processed behavioral data"
            )

        behavior_module.add(beh_obj)

    def get_original_timestamps(self) -> np.ndarray:
        return self.load_timestamps_from_spikeglx()

    def get_timestamps(self) -> np.ndarray:
        return self.get_original_timestamps()

    def set_aligned_timestamps(self):
        raise NotImplementedError

    def add_to_nwbfile(
        self,
        nwbfile: NWBFile,
        metadata: Optional[DeepDict] = None,
        stub_test: bool = False,
    ):
        try:
            timestamps = self.get_original_timestamps()
            starting_time = None
            rate = None
        except:
            # if we can't get the timestamps from spikeglx for whatever reason
            # then just assume that the frames come with DEFAULT_FPS
            # and that the first one is at t=0 which should be true
            timestamps = None
            starting_time = 0.0
            rate = float(DEFAULT_FPS)

        keypoint_series_objects = []
        for i, keypoint_name in enumerate(self.keypoint_names):
            keypoint_series = PoseEstimationSeries(
                name=keypoint_name,
                description=f"Marker placed at {keypoint_name.replace('_', ' ')}",
                data=self.pose_data[:, i, :],
                unit="a.u.",  # TODO
                reference_frame="(0, 0, 0) is what?",  # TODO
                timestamps=timestamps,
                starting_time=starting_time,
                rate=rate,
                confidence=np.full(self.n_frames, np.nan),
                confidence_definition="Filled with nan because we don't have an estimate.",
            )
            keypoint_series_objects.append(keypoint_series)

        pose_estimation = PoseEstimation(
            name="Pose estimation",
            pose_estimation_series=keypoint_series_objects,
            description="Estimated positions selected parts of the animal's body.",
        )

        self._add_to_behavior_module(pose_estimation, nwbfile)

    def load_anipose_from_h5(self) -> np.ndarray:
        """
        Load the array containing the pose estimation from the HDF5 output of sleap-anipose
        """
        with h5py.File(self.h5_path, "r") as file:
            assert file["tracks"].shape[1] == 1
            pose_data = file["tracks"][:, 0, :, :]

        return pose_data

    @property
    def n_frames(self) -> int:
        return self.pose_data.shape[0]

    def load_timestamps_from_spikeglx(self) -> np.ndarray:
        """
        Load the synchronization channel from
        """
        stream_names = get_ap_stream_names(
            _find_spikeglx_recording_folders_in_session(self.raw_session_path)[0]
        )

        if len(stream_names) == 0:
            raise FileNotFoundError(
                f"Could not find SpikeGLX .ap streams in {self.raw_session_path}"
            )

        print("Setting pose estimation timestamps using pulse signal from SpikeGLX...")

        rising_edges_dict = {}
        for stream_name in stream_names:
            rec_with_sync_channel = se.read_spikeglx(
                self.raw_session_path, stream_name=stream_name, load_sync_channel=True
            )

            last_channel = np.array(rec_with_sync_channel.get_traces()[1:, -1])
            rising_frames = get_rising_frames_from_ttl(last_channel)
            rising_edges_sec = rising_frames / rec_with_sync_channel.sampling_frequency
            rising_edges_dict[stream_name] = rising_edges_sec

        for rising_edges_sec in rising_edges_dict.values():
            assert rising_edges_sec.size == self.n_frames

        mid_timestamps_sec = sum(rising_edges_dict.values()) / len(rising_edges_dict)

        # our synchronization time is the first rising edge, so that has to be at t=0
        mid_timestamps_sec -= mid_timestamps_sec[0]

        return mid_timestamps_sec
