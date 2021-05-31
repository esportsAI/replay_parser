import os
from time import time
from src.db import DB
from src.replay import Replay
from src.evaluation import Match


class File(object):
    def __init__(self, file_name):
        self.name = file_name
        self.processed = False
        self.last_processed = None

    def mark_processed(self):
        self.processed = True
        self.last_processed = time()


class ReplayFile(File):
    def __init__(self, file_name):
        super().__init__(file_name=file_name)

        # Extract information from file name
        strp_file_name = file_name.split('-')
        strp_match_info = strp_file_name[3].split(' ')
        strp_team_info = strp_file_name[4].split(' ')

        # get match & map info
        self.match_id = int(strp_match_info[2])
        self.round_id = int(strp_match_info[-2])

        # get team info
        team_1 = strp_team_info[1]
        team_2 = strp_team_info[-2]
        self.teams = [team_1, team_2]


class DirectoryWatchDog(object):
    def __init__(self, working_dir, config_dir):
        self._working_dir = working_dir
        self._last_state_file = os.path.join(config_dir, 'files.csv')

        self.dir_content = {}
        self.update()

    def update(self):
        current_state = os.listdir(self._working_dir)
        previous_state = list(self.dir_content.keys())

        added_files = [
            file_name for file_name in current_state
            if file_name not in previous_state
        ]

        removed_files = [
            file_name for file_name in previous_state
            if file_name not in current_state
        ]

        if len(added_files) > 0:
            self.add_files(file_names=added_files)

        if len(removed_files) > 0:
            self.remove_files(file_names=removed_files)

    def add_file(self, file_name):
        self.dir_content[file_name] = File(file_name=file_name)

    def add_files(self, file_names):
        for file_name in file_names:
            self.add_file(file_name=file_name)

    def remove_file(self, file_name):
        del self.dir_content[file_name]

    def remove_files(self, file_names):
        for file_name in file_names:
            self.remove_file(file_name=file_name)

    def mark_processed(self, file_name):
        self.dir_content[file_name].mark_processed()


class ReplayDirectoryWatchDog(DirectoryWatchDog):
    def add_file(self, file_name):
        self.dir_content[file_name] = ReplayFile(file_name=file_name)


class DataBaseUpdater(object):
    def __init__(self, watch_dog, db_path, db_framework='sqlite'):
        self._db = DB(path=db_path, framework=db_framework)
        self._watchdog = watch_dog

        if not os.path.exists(db_path):
            self._db.create_db()

    def update(self):
        for file_name in self._watchdog.dir_content:
            pass
