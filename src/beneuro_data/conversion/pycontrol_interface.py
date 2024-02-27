# NOTE currently I need the Session class to read pycontrol files
# what if pycontrol-interface worked by getting a Session object
# or some data that can be parsed and validated with pydantic?
# then I can specify what it expects to get from each experiment

import warnings
from pathlib import Path

import dateutil.tz
import numpy as np
from neuroconv.basetemporalalignmentinterface import BaseTemporalAlignmentInterface
from neuroconv.utils import DeepDict, FilePathType
from pynwb import NWBFile
from pynwb.behavior import BehavioralEvents, Position, SpatialSeries
from pynwb.epoch import TimeIntervals
from pynwb.file import Subject

from .pycontrol_data_import import Session


class PyControlInterface(BaseTemporalAlignmentInterface):
    def __init__(self, file_path: FilePathType):
        super().__init__(file_path=file_path)
        self.reload_session()

    def reload_session(self) -> None:
        sessions = []
        for txt_file in Path(self.source_data["file_path"]).glob("*.txt"):
            try:
                sessions.append(Session(txt_file, int_subject_IDs=False))
            except StopIteration:
                pass
        if len(sessions) != 1:
            raise ValueError(f"Expected 1 PyControl .txt file, found {len(sessions)}")

        self.session = sessions[0]

    def get_original_timestamps(self) -> np.ndarray:
        self.reload_session()
        return self.get_timestamps()

    def get_timestamps(self) -> np.ndarray:
        time_x = self.session.analog_data["MotSen1-X"][:, 0]
        time_y = self.session.analog_data["MotSen1-Y"][:, 0]

        assert len(time_x) == len(time_y)
        assert np.all(time_x == time_y)

        return time_x

    def _add_to_behavior_module(self, beh_obj, nwbfile: NWBFile) -> None:
        # behavior_module = nwbfile.processing.get(
        #    "behavior",
        #    nwbfile.create_processing_module("behavior", "processed behavioral data")
        # )
        behavior_module = nwbfile.processing.get("behavior")

        if behavior_module is None:
            behavior_module = nwbfile.create_processing_module(
                "behavior", "processed behavioral data"
            )

        behavior_module.add(beh_obj)

    def add_position(self, nwbfile: NWBFile) -> None:
        data_x = self.session.analog_data["MotSen1-X"][:, 1]
        data_y = self.session.analog_data["MotSen1-Y"][:, 1]

        pos_data = np.stack([data_x, data_y]).T

        spatial_series_obj = SpatialSeries(
            name="Ball position",
            description="(x,y) position as measured by PyControl",
            data=pos_data,
            timestamps=self.get_timestamps(),
            reference_frame="(0,0) is what?",  # TODO
        )

        self._add_to_behavior_module(Position(spatial_series=spatial_series_obj), nwbfile)

    def add_print_events(self, nwbfile: NWBFile):
        print_events = BehavioralEvents(name="print_events")

        for print_item in self.session.print_items:
            print_events.create_timeseries(
                name=print_item,
                data=[it.value for it in self.session.print_data if it.name == print_item],
                timestamps=[
                    it.time for it in self.session.print_data if it.name == print_item
                ],
                unit="",
            )

        self._add_to_behavior_module(print_events, nwbfile)

    def add_behavioral_events(self, nwbfile: NWBFile) -> None:
        behavioral_events = BehavioralEvents(name="behavioral_events")

        behavioral_events.create_timeseries(
            name="behavioral events",
            data=[e.name for e in self.session.events],
            timestamps=[e.time for e in self.session.events],
            unit="",
        )

        self._add_to_behavior_module(behavioral_events, nwbfile)

    def add_behavioral_states(self, nwbfile: NWBFile) -> None:
        behavioral_states = TimeIntervals(
            name="behavioral_states",
            description="intervals when each PyControl state was active",
        )

        behavioral_states.add_column(
            name="state_name", description="Name of the state as defined in PyControl"
        )

        try:
            session_length = self.session.times["session_timer"][0]
        except:
            warnings.warn("Could not find explicit session length, using latest timestamp.")

            session_length = max(
                [
                    e.time
                    for coll in [
                        self.session.events,
                        self.session.states,
                        self.session.print_data,
                    ]
                    for e in coll
                ]
            )

        for state in self.session.states:
            duration = (
                state.duration
                if state.duration is not None
                else session_length - state.time
            )

            behavioral_states.add_row(
                start_time=state.time,
                stop_time=state.time + duration,
                state_name=state.name,
            )

        self._add_to_behavior_module(behavioral_states, nwbfile)

    def add_subject(self, nwbfile: NWBFile) -> None:
        assert nwbfile.subject is None

        # TODO
        nwbfile.subject = Subject(
            subject_id=str(self.session.subject_ID),
            age="age??",
            description="??",
            species="??",
            sex="F",
        )

    def set_aligned_timestamps(self):
        raise NotImplementedError()

    def add_to_nwbfile(self, nwbfile: NWBFile, metadata: DeepDict) -> None:
        self.add_subject(nwbfile)
        self.add_behavioral_states(nwbfile)
        self.add_behavioral_events(nwbfile)
        self.add_print_events(nwbfile)
        self.add_position(nwbfile)

    def get_metadata(self) -> DeepDict:
        metadata = super().get_metadata()

        metadata["NWBFile"].deep_update(
            session_description=self.session.task_name,
            session_start_time=self.session.datetime.astimezone(
                dateutil.tz.gettz("Europe/London")
            ),
        )

        # TODO These should go into the NWBConverter
        metadata["NWBFile"].deep_update(
            experimenter=[
                "Baggins, Bilbo",  # TODO
            ],
            lab="Be.Neuro Lab",
            institution="Imperial College London",
        )

        return metadata
