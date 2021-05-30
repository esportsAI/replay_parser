import numpy as np
import pandas as pd
from src.db import Player as PlayerDB
from src.db import Match as MatchDB
from src.db import Round as RoundDB
from src.db import PlayerStats as PlayerStatsDB


class Entity(object):
    def __init__(self, db):
        self._db = db

        # set query
        self.query = self._db.session.query(PlayerStatsDB).join(
            PlayerStatsDB.round).join(RoundDB.match)
        self.query = self.query.add_columns(MatchDB.season,
                                            MatchDB.match_in_season,
                                            RoundDB.round_in_match,
                                            RoundDB.duration, MatchDB.date,
                                            MatchDB.league)

    @staticmethod
    def __prettify_stat_df__(df):
        df = df[[
            'player_id', 'date', 'league', 'season', 'match_in_season',
            'round_in_match', 'duration', 'kills', 'deaths', 'assists',
            'exp_contrib', 'healing', 'damage_soaked', 'winner_team'
        ]]

        new_column_names = {
            'season': 'season',
            'match_in_season': 'match',
            'round_in_match': 'round'
        }

        return df.rename(columns=new_column_names)

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

    def __get_score_dict__(self, data_series):
        score_dict = self.__get_individual_scores__(data_series)
        score_dict['total'] = np.sum(list(score_dict.values()))
        score_dict['player_id'] = data_series['player_id']

        return score_dict

    def get_stats(self, filter_query):
        query = self.query.filter(*filter_query)
        df = pd.read_sql(query.statement, query.session.bind)

        return self.__prettify_stat_df__(df)

    def get_scores(self, df):
        scores = []

        for i in range(len(df)):
            score_dict = self.__get_score_dict__(df.iloc[i])
            scores.append(score_dict)

        return pd.DataFrame(scores)


class Player(Entity):
    def __init__(self, db, db_id=None, name=None, blizzard_id=None):
        super().__init__(db=db)

        # get player information
        if db_id is not None:
            query = (PlayerDB.id == db_id, )
        elif blizzard_id is not None:
            query = (PlayerDB.blizzard_id == blizzard_id, )
        elif name is not None:
            query = (PlayerDB.name == name, )
        else:
            raise Exception(
                'Need at least one information to identify player.')

        result = self._db.session.query(PlayerDB).filter(*query).all()

        # check if found exactly one player in DB
        if len(result) < 1:
            raise Exception(
                'No entry found. Please update your search parameters!')
        elif len(result) > 1:
            msg = [(entry.id, entry.blizzard_id, entry.name)
                   for entry in result]
            raise Exception(
                f'Multiple entries found. Parameters to ambigious. The results are {msg}'
            )
        result = result[0]

        # set player information
        self.db_id = result.id
        self.blizzard_id = result.blizzard_id
        self.name = result.name


class Round(Entity):
    def __init__(self, league, season_id, match_id, round_id, db):
        super().__init__(db=db)

        self.league = league
        self.season = season_id
        self.match = match_id
        self.round = round_id

    def get_stats(self):
        return super().get_stats(
            filter_query=(MatchDB.league == self.league,
                          MatchDB.season == self.season,
                          MatchDB.match_in_season == self.match,
                          RoundDB.round_in_match == self.round))

    def get_scores(self):
        stats_df = self.get_stats()
        df = super().get_scores(df=stats_df)

        return df.set_index('player_id')


class Match(Entity):
    def __init__(self, league, season_id, match_id, db):
        super().__init__(db=db)

        self.league = league
        self.season = season_id
        self.match = match_id

        self._filter_query = (MatchDB.league == self.league,
                              MatchDB.season == self.season,
                              MatchDB.match_in_season == self.match)

        result = self._db.session.query(MatchDB).filter(
            *self._filter_query).all()

        # check if found exactly one player in DB
        if len(result) < 1:
            raise Exception(
                'No entry found. Please update your search parameters!')
        elif len(result) > 1:
            msg = [(entry.id, entry.league, entry.season,
                    entry.match_in_season) for entry in result]
            raise Exception(
                f'Multiple entries found. Parameters to ambigious. The results are {msg}'
            )
        result = result[0]

        self.id = result.id

    def get_stats(self):
        return super().get_stats(filter_query=self._filter_query)

    def get_scores(self):
        stats_df = self.get_stats()
        df = super().get_scores(df=stats_df)

        week = stats_df['date'][0].isocalendar().week

        df = df.groupby(
            "player_id",
            group_keys=False).apply(lambda g: g.nlargest(3, "total"))
        df = df.groupby("player_id",
                        group_keys=False).apply(lambda g: g.mean())
        df['player_id'] = df['player_id'].astype(int)
        df.set_index('player_id', inplace=True)
        df['week'] = week

        return df
