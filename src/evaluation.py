import numpy as np
import pandas as pd
from src.db import Player as PlayerDB
from src.db import Match as MatchDB
from src.db import Round as RoundDB
from src.db import PlayerStats as PlayerStatsDB


class Player(object):
    def __init__(self, name, db):
        self.name = name

        # create queries
        query = db.session.query(PlayerStatsDB).join(
            PlayerStatsDB.player).join(PlayerStatsDB.round).join(RoundDB.match)
        query = query.filter(PlayerDB.name == self.name)
        query = query.add_columns(MatchDB.season, MatchDB.match_in_season,
                                  RoundDB.round_in_match, RoundDB.duration,
                                  MatchDB.date)

        # create df
        self.df = pd.read_sql(query.statement, query.session.bind)
        self.df = self.df[[
            'season', 'match_in_season', 'date', 'round_in_match', 'duration',
            'kills', 'deaths', 'assists', 'exp_contrib', 'healing',
            'damage_soaked', 'winner_team'
        ]]

        new_column_names = {
            'season': 'season',
            'match_in_season': 'match',
            'round_in_match': 'round'
        }

        self.df.rename(columns=new_column_names, inplace=True)

    @staticmethod
    def __get_individual_scores__(data_series):

        scores_dict = {
            'kills':
            3 * data_series.kills,
            'deaths':
            -1 * data_series.deaths,
            'assists':
            1.5 * data_series.assists,
            'exp_per_min':
            0.0075 * data_series.exp_contrib / data_series.duration,
            'healing':
            0.0001 * data_series.healing,
            'damage_soaked':
            0.0001 * data_series.damage_soaked,
            'winner':
            2 * data_series.winner_team,
            'under_10_mins':
            5 * data_series.winner_team * (data_series.duration < 10),
            'under_15_mins':
            2 * data_series.winner_team * (10 <= data_series.duration < 15),
        }

        return scores_dict

    @staticmethod
    def __get_score__(scores_dict):
        return np.sum(list(scores_dict.values()))

    def get_round_scores(self, season_id, match_id, round_id):
        data = self.df.query(
            f'season == {season_id} & match == {match_id} & round == {round_id}'
        )

        if len(data) == 1:
            data = data.iloc[0]
        elif len(data) > 1:
            raise Exception('Ambigious entries!')
        else:
            raise Exception('No matching data found!')

        score_dict = self.__get_individual_scores__(data_series=data)
        score_dict['total'] = self.__get_score__(scores_dict=score_dict)

        return score_dict

    def get_match_scores(self, season_id, match_id):
        data = self.df.query(f'season == {season_id} & match == {match_id}')

        score_dicts = [
            self.get_round_scores(season_id=season_id,
                                  match_id=match_id,
                                  round_id=round_id)
            for round_id in data['round']
        ]

        df = pd.DataFrame(score_dicts).sort_values(['total'],
                                                   ascending=False).iloc[:3]

        return df.mean().to_dict()

    def get_season_scores(self, season_id):
        data = self.df.query(f'season == {season_id}')

        scores = {}

        for match_id in data['match'].unique():
            match_date = data.query(f'match == {match_id}')['date'].iloc[0]
            calendar_week = match_date.isocalendar().week

            scores[calendar_week] = self.get_match_scores(season_id=season_id,
                                                          match_id=match_id)

        return scores


class ScoreEvaluation(object):
    def __init__(self, season_id, db):
        self.season_id = season_id

        # create queries
        query = db.session.query(PlayerStatsDB).join(
            PlayerStatsDB.player).join(PlayerStatsDB.round).join(RoundDB.match)
        query = query.filter(MatchDB.season == self.season_id)
        query = query.add_columns(PlayerDB.name, MatchDB.date)

        df = pd.read_sql(query.statement, query.session.bind)

        self.players = [
            Player(name=name, db=db) for name in df['name'].unique()
        ]

        self.weeks = df['date']

    def get_scores(self):
        scores = []

        for player in self.players:
            season_scores = player.get_season_scores(season_id=self.season_id)

            for key, value in season_scores.items():
                entry = {'player_name': player.name}
                entry['week'] = key
                entry.update(value)

                scores.append(entry)

        return scores

    def get_summary(self):
        score_board = []
        scores_df = pd.DataFrame(self.get_scores())

        players = scores_df['player_name'].unique()
        weeks = scores_df['week'].unique()

        for player_name in players:
            player_dict = {'Player Name': player_name}
            for week in weeks:
                entry = scores_df.query(
                    f'player_name == "{player_name}" & week == {week}')

                if len(entry) == 0:
                    continue
                elif len(entry) == 1:
                    player_dict[week] = entry.iloc[0]['total']
                else:
                    raise Exception('Ambigious entry found!')

            score_board.append(player_dict)

        score_board = pd.DataFrame(score_board)
        score_board['Avg. Score'] = score_board.mean(axis=1)

        # rename columns
        week_offset = score_board.columns[1] - 1
        cols = {}

        for week in score_board.columns[1:-1]:
            cols[week] = f'Week {week - week_offset}'

        score_board.rename(columns=cols, inplace=True)

        return score_board.round(2)
