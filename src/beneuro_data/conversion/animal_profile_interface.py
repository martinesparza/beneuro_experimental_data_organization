from datetime import datetime
from pathlib import Path

import dateutil.tz
import pandas as pd
from neuroconv.basedatainterface import BaseDataInterface
from neuroconv.utils import DeepDict, FilePathType
from pynwb import NWBFile
from pynwb.file import Subject

from beneuro_data.data_validation import EXPECTED_DATE_FORMAT, validate_session_path


class AnimalProfileInterface(BaseDataInterface):
    def __init__(self, session_path: FilePathType):
        # file_path is the session path
        super().__init__(file_path=session_path)

        self.session_name = Path(session_path).name
        self.subject_dir = Path(session_path).parent
        self.subject_name = self.subject_dir.name

        validate_session_path(Path(session_path), self.subject_name)

        # profile file should be in the subject's directory
        self.profile_file_path = self.subject_dir / f"{self.subject_name}.profile"
        if not self.profile_file_path.exists():
            raise FileNotFoundError(
                f"{self.profile_file_path.name} doesn't exists at {self.profile_file_path}"
            )

        # if it exists, read the contents
        self.profile_lines = self.profile_file_path.read_text().splitlines()

        # extract the session's date
        self.session_date = datetime.strptime(
            self.session_name.replace(f"{self.subject_name}_", ""),
            EXPECTED_DATE_FORMAT,
        ).date()

        # if it's in there, load session info from the profile file into a pd.Series
        try:
            self.session_info = self.load_session_info()
        except ValueError:
            self.session_info = None

    def load_session_info(self) -> pd.Series:
        """
        Loads the line from the .profile file corresponding to the session as a pd.Series
        """
        sessions_table = pd.read_csv(
            self.profile_file_path,
            delim_whitespace=True,
            comment="#",
        )
        sessions_table.replace(to_replace={"Sessions": {"%": ""}}, regex=True, inplace=True)

        if self.session_name not in sessions_table.Sessions.values:
            raise ValueError(f"{self.session_name} not found in {self.profile_file_path}")

        if sum(sessions_table.Sessions == self.session_name) > 1:
            raise ValueError(
                f"Multiple rows for {self.session_name} found in profile file."
            )

        return sessions_table[sessions_table.Sessions == self.session_name].iloc[0]

    def extract_data_from_header(self, field: str) -> str:
        """
        Get the value (as a string) corresponding to `field` from the profile file's header
        """
        field_lines = [line for line in self.profile_lines if line.startswith(f"#{field}")]
        assert len(field_lines) == 1
        return field_lines[0].split(":")[-1]

    def add_subject(self, nwbfile: NWBFile) -> None:
        assert nwbfile.subject is None

        SPECIES = "Mus musculus"

        date_of_birth = datetime.strptime(self.extract_data_from_header("DoB"), "%Y_%m_%d")

        age = self.session_date - date_of_birth.date()

        try:
            weight = float(self.session_info.weight)
        except (ValueError, AttributeError):
            weight = None

        nwbfile.subject = Subject(
            subject_id=self.subject_name,
            age=f"P{age.days}",
            age__reference="birth",
            date_of_birth=date_of_birth.astimezone(dateutil.tz.gettz("Europe/London")),
            sex=self.extract_data_from_header("sex"),
            strain=self.extract_data_from_header("strain"),
            species=SPECIES,
            weight=weight,
        )

        # TODO where to add extra subject info?
        # we will probably need to make an extension for LabMetadata

    def add_to_nwbfile(self, nwbfile: NWBFile, metadata: DeepDict) -> None:
        self.add_subject(nwbfile)

        # NOTE other stuff is added in get_metadata which I think is weird,
        # but it doesn't work if I modify the metadata here

    def get_metadata(self) -> DeepDict:
        metadata = DeepDict()

        if self.session_info is not None:
            metadata["NWBFile"]["experimenter"] = [self.session_info.experimenter]

        return metadata
