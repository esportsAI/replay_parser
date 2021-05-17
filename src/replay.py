import mpyq
import pandas as pd
import heroprotocol.versions as protocol_versions


class ReplayParser(object):
    def __init__(self, replay_path):
        self.archive = mpyq.MPQArchive(replay_path)

        # Read Header
        contents = self.archive.header['user_data_header']['content']
        self.header = protocol_versions.latest().decode_replay_header(contents)

        # Determine protocol version
        base_build = self.header['m_version']['m_baseBuild']
        self.protocol = protocol_versions.build(base_build)

    def get_header(self):
        return self.header

    def get_details(self):
        contents = self.archive.read_file('replay.details')

        return self.protocol.decode_replay_details(contents)

    def get_init_data(self):
        contents = self.archive.read_file('replay.initData')

        return self.protocol.decode_replay_initdata(contents)

    def get_events(self,
                   event_types=['game', 'message', 'tracker', 'attributes']):
        events = {}

        for event_type in event_types:
            event_attr = f'decode_replay_{event_type}_events'
            if not hasattr(self.protocol, event_attr):
                continue

            events[event_type] = []

            contents = self.archive.read_file(f'replay.{event_type}.events')
            result = getattr(self.protocol, event_attr)

            for event in result(contents):
                events[event_type].append(event)

        return events


class Replay(object):
    def __init__(self,
                 replay_path,
                 league=None,
                 season=None,
                 match_id=None,
                 round_id=None):
        self.league = league
        self.season = season
        self.match_id = match_id
        self.round_id = round_id

        replay = ReplayParser(replay_path=replay_path)

        self._details = replay.get_details()

        events = replay.get_events(event_types=['tracker'])
        self._tracker_events = events['tracker']

        # Get Replay Information
        self.map_name = self._details['m_title'].decode('utf-8')
        self.utc_time = self.__get_utc_time__()
        self.duration = self.__get_duration__()

    def __get_utc_time__(self):
        return int(self._details['m_timeUTC'] / 10**7 - 1164447360)

    def __get_duration__(self):
        df = pd.DataFrame(self._tracker_events)

        start_gameloop = df.query(
            'm_eventName == b"GatesOpen"').iloc[0]['_gameloop']
        final_gameloop = df.query(
            '_event == "NNet.Replay.Tracker.SUnitDiedEvent"'
        ).iloc[-1]['_gameloop']

        game_loop_duration = final_gameloop - start_gameloop

        return int((game_loop_duration % 2**32) / 16)

    def get_duration_secs(self):
        return self.duration

    def get_duration_mins(self):
        return self.duration / 60

    def __get_player_list_df__(self):
        df = pd.DataFrame(self._details['m_playerList'])
        df['m_name'] = df['m_name'].str.decode('utf-8')
        df['m_hero'] = df['m_hero'].str.decode('utf-8')

        return df

    def __get_player_stats_df__(self, player_slot_ids, metrics_list):
        metrics_dict = {'m_workingSetSlotId': player_slot_ids}

        for metric_dataset in metrics_list:
            metric_key = metric_dataset['m_name'].decode('utf-8')

            metric_values = [
                metric_dataset['m_values'][slot_id][0]['m_value']
                for slot_id in player_slot_ids
            ]

            metrics_dict[metric_key] = metric_values

        return pd.DataFrame(metrics_dict)

    def get_metrics(self):
        players_df = self.__get_player_list_df__()
        stats_df = self.__get_player_stats_df__(
            players_df['m_workingSetSlotId'].to_list(),
            self._tracker_events[-1]['m_instanceList'])

        metrics_df = players_df.merge(stats_df, on='m_workingSetSlotId')

        # Rename Columns
        conserve_columns = [
            'm_name', 'm_hero', 'm_teamId', 'm_result', 'SoloKill', 'Deaths',
            'Assists', 'ExperienceContribution', 'Healing', 'DamageSoaked'
        ]

        colum_names = [
            'player_name', 'hero', 'team_id', 'winner_team', 'kills', 'deaths',
            'assists', 'exp_contrib', 'healing', 'damage_soaked'
        ]

        metrics_df = metrics_df[conserve_columns]
        metrics_df.columns = colum_names

        # Change data types
        metrics_df['winner_team'] -= 1

        conversion_dict = dict(winner_team=bool)
        metrics_df = metrics_df.astype(conversion_dict)

        return metrics_df
