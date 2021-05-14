from sqlalchemy import create_engine
from sqlalchemy import Column, ForeignKey, Boolean, Integer, Float, String, Date, Time
from sqlalchemy.ext.declarative import declarative_base
from parser import ReplayParser

Base = declarative_base()


class Player(Base):
    __table_name__ = 'players'

    id = Column(Integer, primary_key=True)
    name = Column(String)


class Match(Base):
    __table_name__ = 'matches'

    id = Column(Integer, primary_key=True)
    league = Column(String),
    season = Column(Integer)
    match_in_season = Column(Integer)
    date = Column(Date)
    time = Column(Time)


class Round(Base):
    __table_name__ = 'rounds'

    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('matches.id'))
    round_in_match = Column(Integer),
    map_name = Column(String),
    duration = Column(Integer)


class PlayerStats(Base):
    __table_name__ = 'player_stats'

    id = Column(Integer, primary_key=True)
    round_id = Column(Integer, ForeignKey('rounds.id'))
    player_id = Column(Integer, ForeignKey('players.id'))
    winner_team = Column(Boolean)
    kills = Column(Float)
    deaths = Column(Float)
    assists = Column(Float)
    exp_contrib = Column(Float)
    healing = Column(Float)
    damage_soaked = Column(Float)


class DB(object):
    def __init__(self, db_path, db_framework='sqlite'):
        self.engine = create_engine(f'{db_framework}:///{db_path}')

    def update_db(self, replay_path):
        replay = ReplayParser(replay_path=replay_path)
