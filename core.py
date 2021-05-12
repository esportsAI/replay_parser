import heroprotocol.versions as protocol_versions
import mpyq
import pandas as pd


class ReplayParser(object):
    def __init__(self, replay_path):
        self.archive = mpyq.MPQArchive(replay_path)

        # Read Header
        contents = self.archive.header['user_data_header']['content']
        self.header = protocol_versions.latest().decode_replay_header(contents)

        # Determine protocol version
        base_build = self.header['m_version']['m_baseBuild']
        self.protocol = protocol_versions.build(base_build)

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


class Map(object):
    def __init__(self, replay_path):
        # Load Replay
        replay = ReplayParser(replay_path=replay_path)

        self.details = replay.get_details()

        self.events = replay.get_events(event_types=['tracker'])
        self.tracker_events = pd.DataFrame(self.events['tracker'])

        # Get Game Information
        self.name = self.details['m_title'].decode('utf-8')
        self.start = self.details['m_timeUTC']

        start_gameloop = self.tracker_events.query(
            'm_eventName == b"GatesOpen"').iloc[0]['_gameloop']
        final_gameloop = self.tracker_events.query(
            '_event == "NNet.Replay.Tracker.SUnitDiedEvent"'
        ).iloc[-1]['_gameloop']
        game_loop_duration = final_gameloop - start_gameloop
        self.duration = int((game_loop_duration % 2**32) / 16)

    def __get_player_info_df__(self):
        df = pd.DataFrame(self.details['m_playerList'])
        df['m_name'] = df['m_name'].str.decode('utf-8')
        df['m_hero'] = df['m_hero'].str.decode('utf-8')

        return df

    def __get_player_stats_df__(self, working_slot_ids):
        player_metrics = []

        for player_id in working_slot_ids:
            player_dict = {'m_workingSetSlotId': player_id}

            for metric in self.events['tracker'][-1]['m_instanceList']:
                player_dict[metric['m_name'].decode(
                    'utf-8')] = metric['m_values'][player_id][0]['m_value']

            player_metrics.append(player_dict)

        return pd.DataFrame(player_metrics)

    def get_metrics(self):
        player_info_df = self.__get_player_info_df__()
        player_stats_df = self.__get_player_stats_df__(
            player_info_df['m_workingSetSlotId'])

        metrics_df = player_info_df.merge(player_stats_df,
                                          on='m_workingSetSlotId')

        # Rename Columns
        conserve_columns = [
            'm_name', 'm_hero', 'm_teamId', 'm_result', 'SoloKill', 'Deaths',
            'Assists', 'ExperienceContribution', 'Healing', 'DamageSoaked'
        ]

        colum_names = [
            'player_name', 'hero', 'team_id', 'winner_team', 'kills', 'deaths',
            'assists', 'experience_contribution', 'healing', 'damage_soaked'
        ]

        metrics_df = metrics_df[conserve_columns]
        metrics_df.columns = colum_names

        # Change data types
        metrics_df['winner_team'] -= 1

        conversion_dict = dict(winner_team=bool)

        metrics_df = metrics_df.astype(conversion_dict)

        return metrics_df


class Player(object):
    def __init__(self, name, team):
        self.name = name
        self.team = team

        self.statistics = []

    def update_statistics(self, stats):
        self.statistics.append(stats)

    def flush_statistics(self):
        del self.statistics
        self.statistics = []

    def get_score(self):
        df = pd.DataFrame(self.statistics)

        kill_score = 3 * df['kills'].astype(int)
        death_score = -1 * df['deaths'].astype(int)
        assists_score = 1.5 * df['assists'].astype(int)
        contrib_score = 0.0075 * df['experience_contribution'] / df[
            'duration'].astype(int)
        healing_score = 0.0001 * df['healing'].astype(int)
        dmg_soaked_score = 0.0001 * df['damage_soaked'].astype(int)
        win_score = 2 * df['winner_team'].astype(int)
        u_15_score = 2 * df['under_15mins'].astype(int)
        u_10_score = 5 * df['under_10mins'].astype(int)

        print(kill_score)
        print(death_score)
        print(assists_score)
        print(df['experience_contribution'] / df['duration'])
        print(contrib_score)
        print(healing_score)
        print(dmg_soaked_score)
        print(win_score)
        print(u_15_score)
        print(u_10_score)

        df['score'] = kill_score + death_score + assists_score + contrib_score + healing_score + dmg_soaked_score + win_score + +u_10_score + u_15_score

        return df


class Team(object):
    def __init__(self, team_id, team_name):
        self.id = team_id
        self.name = team_name


class Match(object):
    def __init__(self, maps, team_names=['red', 'blue']):
        self.maps = maps

        self.teams = [Team(i, team_name=team_names[i]) for i in range(2)]

        self.players = []

        for player_entry in self.maps[0].details['m_playerList']:
            player = Player(name=player_entry['m_name'].decode('utf-8'),
                            team=self.teams[player_entry['m_teamId']])

            self.players.append(player)

    def serve_player_statistics(self):
        for player in self.players:
            player.flush_statistics()

        for c_map in self.maps:
            map_df = c_map.get_metrics().set_index('player_name')

            duration_mins = c_map.duration / 60

            map_df['duration'] = duration_mins
            map_df['under_10mins'] = duration_mins < 10
            map_df['under_15mins'] = 10 <= duration_mins < 15
            map_df.drop(['team_id'], axis=1, inplace=True)

            for player in self.players:
                stats = map_df.loc[player.name]
                player.update_statistics(stats.to_dict())
