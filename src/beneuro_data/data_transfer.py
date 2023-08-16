import os
import re
from datetime import datetime
from typing import Optional

from fabric import Connection
import spikeinterface.extractors as se

from beneuro_data.config import Config
from beneuro_data.folder_size import get_file_size_in_kilobytes, get_folder_size_in_gigabytes
from beneuro_data.folder_io import folder_exists, make_folder
from beneuro_data.spike_sorting import run_kilosort_on_stream

config = Config()

# Regex pattern to match folder names ending with "_gx" where x is any integer.
SPIKEGLX_RECORDING_PATTERN = re.compile(r"_g(\d+)$")

class Subject:
    def __init__(self, name: str, load_sessions: bool = True):
        self.name = name

        # NOTE treat local raw sessions as the source of truth
        # if it's not on the local machine, it can't be uploaded
        if load_sessions:
            self.local_sessions = [Session(self, foldername) for foldername in self.list_local_session_folders('raw')]

    def get_path(self, local_or_remote: str, processing_level: str):
        if local_or_remote not in {'local', 'remote'}:
            raise ValueError(f'Invalid local_or_remote {local_or_remote}')

        if processing_level not in {'raw', 'processed'}:
            raise ValueError(f'Invalid processing_level {processing_level}')

        #base_path = getattr(config, f'{local_or_remote.upper()}_PATH')
        base_path = config.LOCAL_PATH if local_or_remote == 'local' else config.REMOTE_PATH

        return os.path.join(base_path, processing_level, self.name)

    def has_folder(self, local_or_remote: str, processing_level: str):
        return folder_exists(local_or_remote, self.get_path(local_or_remote, processing_level))

    def make_folder(self, local_or_remote: str, processing_level: str):
        return make_folder(local_or_remote, self.get_path(local_or_remote, processing_level))

    def validate_local_session_folders(self, processing_level: str):
        for filename in os.listdir(self.get_path('local', processing_level)):
            # .profile file is fine to have
            if os.path.splitext(filename)[1] == '.profile':
                continue
            
            Session._validate_folder_name(filename, self.name)

        return True

    def list_local_session_folders(self, processing_level: str) -> list[str]:
        base_path = self.get_path('local', processing_level)

        session_folders = []
        for filename in os.listdir(base_path):
            if os.path.isdir(os.path.join(base_path, filename)) and filename.startswith(self.name):
                session_folders.append(filename)

        return session_folders

    def list_remote_session_folders(self, processing_level: str) -> list[str]:
        base_path = self.get_path('remote', processing_level)

        session_folders = []
        with Connection(host=config.REMOTE_SERVER_ADDRESS, user=config.USERNAME, connect_kwargs={'password': config.PASSWORD.get_secret_value()}) as c:
            with c.sftp() as sftp:
                for filename in sftp.listdir(base_path):
                    if filename.startswith(self.name):
                        try:
                            sftp.listdir(os.path.join(base_path, filename))
                            session_folders.append(filename)
                        except FileNotFoundError:
                            pass

        return session_folders


class Session:
    date_format: str = '%Y_%m_%d_%H_%M'

    def __init__(self, subject: Subject, folder_name: str, load_ephys: bool = True, load_behavior: bool = True):
        self.subject = subject
        date_str = Session.extract_date_str(folder_name, subject.name)
        self.date = datetime.strptime(date_str, Session.date_format)

        self.validate_folder_name(folder_name)

        # again, local and raw are the source of truth
        # can't upload what's not on the local machine
        if load_ephys:
            self.ephys_recordings = [
                EphysRecording(self, foldername)
                for foldername in self.list_local_ephys_folders('raw')
            ]

        if load_behavior:
            self.behavioral_data = BehavioralData(self)

    @property
    def date_str(self):
        return self.date.strftime(Session.date_format)

    @property
    def folder_name(self):
        return f'{self.subject.name}_{self.date_str}'

    def validate_folder_name(self, folder_name: str) -> bool:
        if folder_name != self.folder_name:
            raise ValueError(f'Folder name {folder_name} does not match expected format {self.folder_name}')

        return True

    @staticmethod
    def extract_date_str(folder_name: str, subject_name: str) -> str:
        # validate folder name first
        Session._validate_folder_name(folder_name, subject_name)

        date_str = folder_name[len(subject_name)+1:]

        return date_str

    @staticmethod
    def _validate_folder_name(folder_name: str, subject_name: str) -> bool:
        if not folder_name.startswith(subject_name):
            raise ValueError(f"Folder name has to start with subject name. Got {folder_name} with subject name {subject_name}")

        if folder_name[len(subject_name)] != '_':
            raise ValueError(f"Folder name has to have an underscore after subject name. Got {folder_name}.")

        date_str = folder_name[len(subject_name)+1:]
        Session._validate_time_format(date_str)

        return True

    @staticmethod
    def _validate_time_format(time_str: str) -> bool:
        # parsing the string to datetime, then generating the correctly formatted string to compare to
        try:
            correct_str = datetime.strptime(time_str, Session.date_format).strftime(Session.date_format)
        except ValueError:
            raise ValueError(f"{time_str} doesn't match expected format of {Session.date_format}")

        if time_str != correct_str:
            raise ValueError(f"{time_str} doesn't match expected format of {correct_str}")

        return True


    def get_path(self, local_or_remote: str, processing_level: str) -> str:
        return os.path.join(self.subject.get_path(local_or_remote, processing_level), self.folder_name)

    def has_folder(self, local_or_remote: str, processing_level: str):
        return folder_exists(local_or_remote, self.get_path(local_or_remote, processing_level))

    def make_folder(self, local_or_remote: str, processing_level: str):
        return make_folder(local_or_remote, self.get_path(local_or_remote, processing_level))

    def get_elphys_folder_path(self, local_or_remote: str, processing_level: str) -> str:
        base_path = self.get_path(local_or_remote, processing_level)

        if processing_level == 'processed':
            return os.path.join(base_path, f"{self.folder_name}_epyhs")
        else:
            return base_path

    def get_behavior_folder_path(self, local_or_remote: str, processing_level: str) -> str:
        base_path = self.get_path(local_or_remote, processing_level)

        if processing_level == 'processed':
            base_path += '_behavior'

        return base_path

    def list_local_ephys_folders(self, processing_level: str) -> list[str]:
        """
        Find and list folders ending with '_gx' where x is any integer.
        """
        base_path = self.get_elphys_folder_path('local', processing_level)

        if not os.path.exists(base_path):
            return []

        matching_folders = [
            fname
            for fname in os.listdir(base_path)
            if os.path.isdir(os.path.join(base_path, fname)) and SPIKEGLX_RECORDING_PATTERN.search(fname)
        ]
            
        return matching_folders



class EphysRecording:
    def __init__(self, session: Session, folder_name: str):
        self.session = session

        # self.gid is g0 or g1 etc.
        self.gid = SPIKEGLX_RECORDING_PATTERN.search(folder_name).group(0)[1:]
        
        self.validate_folder_name(folder_name)

    def __repr__(self) -> str:
        return f'EphysRecording(session={self.session.folder_name}, gid={self.gid})'

    @property
    def folder_name(self):
        return f'{self.session.subject.name}_{self.session.date_str}_{self.gid}'

    def validate_folder_name(self, folder_name: str) -> bool:
        if folder_name != self.folder_name:
            raise ValueError(f'Folder name {folder_name} does not match expected format {self.folder_name}')

        return True

    def get_path(self, local_or_remote: str, processing_level: str) -> str:
        parent_path = self.session.get_elphys_folder_path(local_or_remote, processing_level)
            
        return os.path.join(parent_path, self.folder_name)

    def has_folder(self, local_or_remote: str, processing_level: str):
        return folder_exists(local_or_remote, self.get_path(local_or_remote, processing_level))

    def make_folder(self, local_or_remote: str, processing_level: str):
        return make_folder(local_or_remote, self.get_path(local_or_remote, processing_level))

    def get_raw_size_in_gigabytes(self) -> float:
        return get_folder_size_in_gigabytes(self.get_path('local', 'raw'))

    @property
    def ap_streams(self):
        stream_names, _ = se.get_neo_streams('spikeglx', self.get_path('local', 'raw'))
        return [stream_name for stream_name in stream_names if stream_name.endswith('ap')]

    @property
    def number_of_probes(self):
        return len(self.ap_streams)

    def run_kilosort(
        self,
        ap_stream_name: str,
        clean_up_temp_files: bool = False,
        sorter_params: Optional[dict] = None,
    ):

        assert ap_stream_name in self.ap_streams
        assert self.has_folder('local', 'raw')

        if not self.has_folder('local', 'processed'):
            self.make_folder('local', 'processed')
        assert self.has_folder('local', 'processed')

        # each probe should have its own output folder
        output_folder_name = f"{self.folder_name}_{ap_stream_name.split('.')[0]}"
        output_path = os.path.join(self.get_path('local', 'processed'), output_folder_name)
        if not os.path.exists(output_path):
            make_folder('local', output_path)

        sorting_KS3 = run_kilosort_on_stream(
            input_folder = self.get_path('local', 'raw'),
            stream_name = ap_stream_name,
            output_folder = output_path,
            clean_up_temp_files = clean_up_temp_files,
            sorter_params = sorter_params,
        )

        return sorting_KS3


class BehavioralData:
    def __init__(self, session: Session):
       self.session = session 

       self.pycontrol_pattern = rf'^{session.subject.name}_{re.escape(session.date_str)}-'
       #self.pycontrol_pattern += rf'{session.date.strftime("%Y-%m-%d")}-\d{{6}}' # this or the next one
       self.pycontrol_pattern += r'.*' # this or the previous one. this if we don't care about the middle

       self.pycontrol_pca_pattern = self.pycontrol_pattern + rf'_MotSen\d-(X|Y)\.pca'
       self.pycontrol_txt_pattern = self.pycontrol_pattern + rf'\.txt'

       # validate that txt and pca files are named correctly
       for filename in os.listdir(self.get_path('local', 'raw')):
           if os.path.splitext(filename)[1] == '.pca':
               if re.match(self.pycontrol_pca_pattern, filename) is None:
                   raise ValueError(f'Filename {filename} does not match expected pattern for PyControl .pca files')

           if os.path.splitext(filename)[1] == '.txt':
               if re.match(self.pycontrol_txt_pattern, filename) is None:
                   raise ValueError(f'Filename {filename} does not match expected pattern for PyControl .txt files')

           # don't care about any other files
           if os.path.splitext(filename)[1] not in {'.pca', '.txt'}:
               continue

    def get_path(self, local_or_remote: str, processing_level: str) -> str:
        return self.session.get_behavior_folder_path(local_or_remote, processing_level)

    def get_pycontrol_py_file(self):
        raise NotImplementedError
