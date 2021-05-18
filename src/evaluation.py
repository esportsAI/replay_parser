import numpy as np
import pandas as pd
from sqlalchemy import create_engine
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
                                  RoundDB.round_in_match, RoundDB.duration)

        # create df
        self.df = pd.read_sql(query.statement, query.session.bind)
        self.df = self.df[[
            'season', 'match_in_season', 'round_in_match', 'duration', 'kills',
            'deaths', 'assists', 'exp_contrib', 'healing', 'damage_soaked',
            'winner_team'
        ]]
        self.df.rename(columns={
            'season': 'season',
            'match_in_season': 'match',
            'round_in_match': 'round'
        },
                       inplace=True)

    @staticmethod
    def __calculate_score__(kills, deaths, assists, xp_contrib, duration,
                            healing, dmg_soaked, winner):
        xp_per_min = xp_contrib / duration
        under_10_mins = duration < 10
        under_15_mins = 10 <= duration < 15

        individual_scores = [
            3 * kills, -1 * deaths, 1.5 * assists, 0.0075 * xp_per_min,
            0.0001 * healing, 0.0001 * dmg_soaked, 2 * winner,
            2 * under_15_mins, 5 * under_10_mins
        ]

        return np.sum(individual_scores)

    def get_round_score(self, season_id, match_id, round_id):
        data = self.df.query(
            f'season == {season_id} & match == {match_id} & round == {round_id}'
        )

        if len(data) == 1:
            data = data.iloc[0]
        elif len(data) > 1:
            raise Exception('Ambigious entries!')
        else:
            raise Exception('No matching data found!')

        return self.__calculate_score__(kills=data.kills,
                                        deaths=data.deaths,
                                        assists=data.assists,
                                        xp_contrib=data.exp_contrib,
                                        duration=data.duration,
                                        healing=data.healing,
                                        dmg_soaked=data.damage_soaked,
                                        winner=data.winner_team)

    def get_match_score(self, season_id, match_id):
        data = self.df.query(f'season == {season_id} & match == {match_id}')

        scores = [
            self.get_round_score(season_id=season_id,
                                 match_id=match_id,
                                 round_id=round_id)
            for round_id in data['round']
        ]

        return np.sort(scores)[-3:].mean()

    def get_season_scores(self, season_id):
        data = self.df.query(f'season == {season_id}')

        scores = [
            self.get_match_score(season_id=season_id, match_id=match_id)
            for match_id in data['match'].unique()
        ]

        return scores


class ScoreBoard(object):
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

    def get_scoreboard(self):
        scores_dict = {}

        for player in self.players:
            scores_dict[player.name] = np.round(
                player.get_season_scores(season_id=self.season_id), 2)

        return scores_dict
