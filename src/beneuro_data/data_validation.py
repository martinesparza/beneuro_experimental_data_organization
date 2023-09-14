import os
import shutil
import re
import warnings
from datetime import datetime
from typing import Optional
from abc import ABC, abstractmethod

from beneuro_data.config import config

HIDDEN_FILE_PATTERN = r"^\."
# Regex pattern to match folder names ending with "_gx" where x is any integer.
SPIKEGLX_RECORDING_PATTERN = re.compile(r"_g(\d+)$")


def list_files_with_extension(folder_path: str, ext: str) -> list[str]:
    files = []
    for filename in os.listdir(folder_path):
        if os.path.splitext(filename)[1] == ext:
            files.append(filename)
    return files


def list_subfolders(path: str) -> list[str]:
    subfolders = []
    for filename in os.listdir(path):
        if os.path.isdir(os.path.join(path, filename)):
            subfolders.append(filename)

    return subfolders


class ValidatorNode(ABC):
    # @abstractmethod
    # def validate(self):
    #    raise NotImplementedError

    @abstractmethod
    def get_path(self) -> str:
        raise NotImplementedError

    def get_parent_path(self) -> str:
        raise NotImplementedError

    def has_folder(self) -> bool:
        return os.path.exists(self.get_path())

    def has_parent_folder(self) -> bool:
        return os.path.exists(self.get_parent_path())

    def list_files_with_extension(self, ext: str) -> list[str]:
        return list_files_with_extension(self.get_path(), ext)


class Subject:
    def __init__(
        self,
        name: str,
        local_base_path: Optional[str] = None,
        remote_base_path: Optional[str] = None,
    ):
        self.name = name

        if local_base_path is None:
            local_base_path = config.LOCAL_PATH
        if remote_base_path is None:
            remote_base_path = config.REMOTE_PATH

        self.local_store = SubjectStore(name, local_base_path)
        self.remote_store = SubjectStore(name, remote_base_path)

    def _select_store(self, local_or_remote: str) -> "SubjectStore":
        if local_or_remote == "local":
            return self.local_store
        elif local_or_remote == "remote":
            return self.remote_store
        else:
            raise ValueError("local_or_remote has to be either 'local' or 'remote'")

    def upload_whole_session(self, processing_level: str, folder_name: str):
        # this fails if the local session is in an invalid format
        local_session = self.local_store.load_session(processing_level, folder_name)

        if not self.remote_store.has_folder(processing_level):
            # raise FileNotFoundError(f"Subject {self.name} has no remote folder")
            self.remote_store.create_folder(processing_level)

        source_location = local_session.get_path()
        target_location = source_location.replace(
            self.local_store.base_path,
            self.remote_store.base_path,
        )

        if os.path.exists(target_location):
            raise FileExistsError(f"Target location already exists: {target_location}")

        shutil.copytree(source_location, target_location)

    def upload_behavioral_data(self, processing_level: str, folder_name: str):
        raise NotImplementedError

        local_session = self.local_store.load_session(processing_level, folder_name)
        remote_session = self.remote_store.load_session(processing_level, folder_name)

        if not remote_session.has_folder():
            self.remote_store.create_folder()

        if remote_session.behavioral_data is not None:
            raise FileExistsError(
                f"Remote behavioral data already exists for {remote_session.get_path()}"
            )

        if local_session.behavioral_data is None:
            raise FileNotFoundError(
                "No behavioral data found in local session {local_session.get_path()}"
            )

        source_files = local_session.behavioral_data.list_children_paths()
        target_files = [path.replace()]


class SubjectStore:
    def __init__(
        self,
        name: str,
        base_path: str,
    ):
        self.name = name
        self.base_path = base_path

        if self.has_folder("raw"):
            self.check_only_subject_subfolders("raw")

        if self.has_folder("processed"):
            self.check_only_subject_subfolders("processed")

        # self.raw_sessions = self.load_sessions("raw")
        # self.processed_sessions = self.load_sessions("processed")

    def relative_path(self, processing_level: str) -> str:
        return os.path.join(processing_level, self.name)

    def absolute_path(self, processing_level: str) -> str:
        return os.path.join(self.base_path, processing_level, self.name)

    def get_parent_path(self) -> str:
        return self.base_path

    def get_path(self, processing_level: str) -> str:
        return os.path.join(self.get_parent_path(), processing_level, self.name)

    def has_folder(self, processing_level: str) -> bool:
        return os.path.exists(self.get_path(processing_level))

    def create_folder(self, relative_path: str):
        os.mkdir(os.path.join(self.base_path, relative_path))

    def list_subfolders(self, processing_level: str) -> list[str]:
        return list_subfolders(self.get_path(processing_level))

    def load_session(self, processing_level: str, folder_name: str):
        if processing_level == "raw":
            return self._load_raw_session(folder_name)
        else:
            return self._load_processed_session(folder_name)

    def _load_raw_session(self, folder_name: str) -> "RawSession":
        return RawSession.from_disk(self, folder_name)

    def _load_processed_session(self, folder_name: str):
        raise NotImplementedError

    def get_valid_sessions(self, processing_level: str) -> list["Session"]:
        valid_sessions = []
        for foldername in self.list_subfolders("raw"):
            try:
                valid_sessions.append(self.load_session(processing_level, foldername))
            except:
                pass

        return valid_sessions

    def load_sessions(self, processing_level: str):
        return [
            self.load_session(processing_level, folder_name)
            for folder_name in self.list_subfolders(processing_level)
        ]

    def check_only_subject_subfolders(self, processing_level: str) -> bool:
        subject_path = self.get_path(processing_level)

        for folder_name in os.listdir(subject_path):
            # allow hidden files
            if re.match(HIDDEN_FILE_PATTERN, folder_name):
                continue

            if not os.path.isdir(os.path.join(subject_path, folder_name)):
                raise ValueError(
                    f"Only subfolders are allowed in a subject's folder.\
                    Found {folder_name} in {subject_path}"
                )

            if not folder_name.startswith(self.name):
                raise ValueError(
                    f"Folder name has to start with subject name. Got {folder_name} with subject name {self.name}"
                )

        return True


class Session(ValidatorNode):
    date_format: str = "%Y_%m_%d_%H_%M"

    #    def __init__(self, subject_store: SubjectStore, folder_name: str):
    #        self.subject_store = subject_store
    #        self.subject_name = subject_store.name
    #
    #        self._prescreen_folder_name(folder_name)
    #
    #        self.date = self.extract_date(folder_name)
    #        self.folder_name = folder_name
    #
    #        self.ephys_recordings = self.load_ephys_recordings()
    #        self.behavioral_data = self.load_behavioral_data()
    #

    def __init__(self, subject_store: SubjectStore, folder_name: str, date: datetime):
        self.subject_store = subject_store
        self.folder_name = folder_name
        self.date = date

        self.ephys_recordings: Optional[list[EphysRecording]] = None
        self.behavioral_data: Optional[BehavioralData] = None

    @classmethod
    def from_disk(cls, subject_store: SubjectStore, folder_name: str):
        cls.prescreen_folder_name(folder_name, subject_store.name)

        date = cls.extract_date(folder_name, subject_store.name)

        session = cls(subject_store, folder_name, date)

        session.ephys_recordings = session.load_ephys_recordings()
        session.behavioral_data = session.load_behavioral_data()

        return session

    @property
    def subject_name(self) -> str:
        return self.subject_store.name

    def get_parent_path(self):
        # has to be overwritten in RawSession and ProcessedSession
        raise NotImplementedError

    def get_path(self):
        return os.path.join(self.get_parent_path(), self.folder_name)

    def get_expected_folder_name(self) -> str:
        return f"{self.subject_name}_{self.get_date_str()}"

    def get_date_str(self) -> str:
        return self.date.strftime(self.date_format)

    @classmethod
    def extract_date(cls, folder_name: str, subject_name: str) -> datetime:
        date_str = cls._extract_date_str(folder_name, subject_name)
        return datetime.strptime(date_str, cls.date_format)

    @classmethod
    def prescreen_folder_name(cls, folder_name: str, subject_name: str) -> bool:
        if not folder_name.startswith(subject_name):
            raise ValueError(
                f"Folder name has to start with subject name. Got {folder_name} with subject name {subject_name}"
            )

        if folder_name[len(subject_name)] != "_":
            raise ValueError(
                f"Folder name has to have an underscore after subject name. Got {folder_name}."
            )

        # fails if the ending after the subject + underscore is not
        # a date in the expected format
        Session._validate_time_format(cls._extract_date_str(folder_name, subject_name))

        return True

    @staticmethod
    def _extract_date_str(folder_name: str, subject_name: str) -> str:
        # assumes the folder_name is correct format
        return folder_name[len(subject_name) + 1 :]

    @staticmethod
    def _validate_time_format(time_str: str) -> bool:
        # parsing the string to datetime, then generating the correctly formatted string to compare to
        try:
            correct_str = datetime.strptime(time_str, Session.date_format).strftime(
                Session.date_format
            )
        except ValueError:
            raise ValueError(
                f"{time_str} doesn't match expected format of {Session.date_format}"
            )

        if time_str != correct_str:
            raise ValueError(f"{time_str} doesn't match expected format of {correct_str}")

        return True

    def list_ephys_recording_folders(self) -> list[str]:
        base_path = self.get_ephys_folder_path()

        if not os.path.exists(base_path):
            return []

        return [
            fname
            for fname in os.listdir(base_path)
            if self._ephys_recording_folder_criteria(fname)
        ]

    def _ephys_recording_folder_criteria(self, filename: str) -> bool:
        if not os.path.isdir(os.path.join(self.get_path(), filename)):
            return False

        return SPIKEGLX_RECORDING_PATTERN.search(filename) is not None

    @abstractmethod
    def get_ephys_folder_path(self):
        raise NotImplementedError

    @abstractmethod
    def get_behavior_folder_path(self):
        raise NotImplementedError

    @abstractmethod
    def load_ephys_recordings(self):
        raise NotImplementedError

    @abstractmethod
    def load_behavioral_data(self):
        raise NotImplementedError


class RawSession(Session):
    def get_parent_path(self):
        return self.subject_store.get_path("raw")

    def get_ephys_folder_path(self):
        return self.get_path()

    def get_behavior_folder_path(self):
        return self.get_path()

    def load_ephys_recordings(self):
        ephys_recording_folders = self.list_ephys_recording_folders()

        # if len(ephys_recording_folders) == 0:
        #    warnings.warn("No raw ephys data found.")
        if len(ephys_recording_folders) > 1:
            warnings.warn(f"More than one raw ephys recordings found in {self.get_path()}.")

        return [
            RawEphysRecording(self, foldername) for foldername in ephys_recording_folders
        ]

    def load_behavioral_data(self):
        beh_data = RawBehavioralData(self)
        if beh_data.has_pycontrol_files():
            beh_data.validate()
            return beh_data
        else:
            return None


class ProcessedSession(Session):
    def get_parent_path(self):
        return self.subject_store.get_path("processed")

    def get_ephys_folder_path(self):
        return os.path.join(self.get_path(), f"{self.folder_name}_ephys")

    def get_behavior_folder_path(self):
        return os.path.join(self.get_path(), f"{self.folder_name}_behavior")

    def load_ephys_recordings(self):
        raise NotImplementedError


class EphysRecording(ValidatorNode):
    pass


class RawEphysRecording(EphysRecording):
    def __init__(self, session: RawSession, folder_name: str):
        self.session = session

        # self.gid is g0 or g1 etc.
        self.gid = self._extract_gid(folder_name)
        self.validate_folder_name(folder_name)
        self.validate_probe_folder_names()

    @staticmethod
    def _extract_gid(folder_name: str) -> str:
        return SPIKEGLX_RECORDING_PATTERN.search(folder_name).group(0)[1:]

    def validate_folder_name(self, folder_name: str) -> bool:
        expected_folder_name = self.get_expected_folder_name()

        if folder_name != expected_folder_name:
            raise ValueError(
                f"Folder name {folder_name} does not match expected format {expected_folder_name}"
            )

        return True

    def get_expected_folder_name(self) -> str:
        return f"{self.session.get_expected_folder_name()}_{self.gid}"

    def validate_probe_folder_names(self) -> bool:
        # make sure only folders corresponding to the different probes
        recording_path = self.get_path()

        for filename in os.listdir(recording_path):
            filepath = os.path.join(recording_path, filename)

            # allow hidden files
            if re.match(HIDDEN_FILE_PATTERN, filename):
                continue

            if not os.path.isdir(filepath):
                raise ValueError("Only folders are allowed in the ephys recordings folder")

            if not re.match(self.get_expected_probe_folder_pattern(), filename):
                raise ValueError(
                    f"The following folder name doesn't match the expected format for probes: {filename}"
                )

        return True

    def get_expected_probe_folder_pattern(self) -> str:
        return rf"{self.get_expected_folder_name()}_imec\d"

    def get_path(self) -> str:
        return os.path.join(
            self.session.get_ephys_folder_path(),
            self.get_expected_folder_name(),
        )

    def get_parent_path(self) -> str:
        return self.session.get_ephys_folder_path()


class BehavioralData(ValidatorNode):
    pass


class RawBehavioralData(BehavioralData):
    pycontrol_ending_pattern_per_extension = {
        ".pca": rf"_MotSen\d-(X|Y)\.pca",
        ".txt": r"\.txt",
    }

    expected_number_of_pycontrol_files_per_extension = {
        ".pca": 2,
        ".txt": 1,
    }

    pycontrol_py_foldername = "run_task-task_files"

    def __init__(
        self,
        session: RawSession,
    ):
        self.session = session
        self.validate()

    def get_parent_path(self) -> str:
        return self.session.get_path()

    def get_path(self) -> str:
        return self.get_parent_path()

    def relative_path(self):
        pass

    def list_pycontrol_extensions(self) -> list[str]:
        return list(self.pycontrol_ending_pattern_per_extension.keys())

    def validate(self) -> bool:
        self._validate_pycontrol_filenames()
        self._validate_number_of_pycontrol_files()

        if self._pycontrol_py_folder_exists():
            self._validate_only_one_py_file()
        else:
            warnings.warn(f"No PyControl task folder found in {self.get_path()}\n")

        return True

    def _validate_pycontrol_filenames(self) -> bool:
        # validate that pycontrol files are named correctly
        for ext in self.list_pycontrol_extensions():
            for filename in self.list_files_with_extension(ext):
                self._validate_pycontrol_filename(ext, filename)

        return True

    def _validate_pycontrol_filename(self, extension: str, filename: str) -> bool:
        pattern = self.pycontrol_filename_pattern(extension)

        if re.match(pattern, filename) is None:
            raise ValueError(
                f"Filename {filename} does not match expected pattern for PyControl {extension} files"
            )

        return True

    def has_pycontrol_files(self) -> bool:
        return len(self._list_files_with_pycontrol_extensions()) > 0

    def _list_files_with_pycontrol_extensions(self):
        matching_filenames = []
        for extension in self.list_pycontrol_extensions():
            matching_filenames += self.list_files_with_extension(extension)

        return matching_filenames

    def pycontrol_filename_pattern(self, extension: str) -> str:
        start_pattern = (
            rf"^{self.session.subject_name}_{re.escape(self.session.get_date_str())}"
        )

        # middle_pattern = rf'{self.session.date.strftime("%Y-%m-%d")}-\d{{6}}'
        middle_pattern = r".*"

        end_pattern = self.pycontrol_ending_pattern_per_extension[extension]

        return start_pattern + middle_pattern + end_pattern

    def _validate_number_of_pycontrol_files(self) -> bool:
        for ext in self.list_pycontrol_extensions():
            self._validate_number_of_pycontrol_files_with_extension(ext)

        return True

    def _validate_number_of_pycontrol_files_with_extension(self, extension: str) -> bool:
        expected_number_of_files = self.expected_number_of_pycontrol_files_per_extension[
            extension
        ]

        matching_filenames = self.list_files_with_extension(extension)

        if len(matching_filenames) != expected_number_of_files:
            raise ValueError(
                f"Expected {expected_number_of_files} files with extension {extension}, but found {len(matching_filenames)}"
            )

        return True

    def _pycontrol_py_folder_exists(self) -> bool:
        return os.path.exists(self._expected_pyfolder_path())

    def _expected_pyfolder_path(self) -> str:
        return os.path.join(self.get_path(), self.pycontrol_py_foldername)

    def _validate_only_one_py_file(self) -> bool:
        if not self._pycontrol_py_folder_exists():
            raise FileNotFoundError(
                "Trying to validate .py files but PyControl task folder doesn't exist in {self.get_path()}"
            )

        pyfolder_path = self._expected_pyfolder_path()

        python_filenames = list_files_with_extension(pyfolder_path, ".py")

        if len(python_filenames) == 0:
            raise FileNotFoundError(f"Could not find any .py files in {pyfolder_path}")

        if len(python_filenames) > 1:
            raise ValueError(f"Found more than one .py files in {pyfolder_path}")

        return True
