from sqlalchemy import create_engine, select
from sqlalchemy import Column, ForeignKey, Boolean, Integer, Float, String, Date, Time
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from dateutil import tz

Base = declarative_base()


class Player(Base):
    __tablename__ = 'players'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    stats = relationship("PlayerStats", backref="players")


class Match(Base):
    __tablename__ = 'matches'

    id = Column(Integer, primary_key=True)
    league = Column(String)
    season = Column(Integer)
    match_in_season = Column(Integer)
    date = Column(Date)

    rounds = relationship("Round", backref="matches")


class Round(Base):
    __tablename__ = 'rounds'

    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('matches.id'))
    round_in_match = Column(Integer)
    map_name = Column(String)
    duration = Column(Integer)
    time = Column(Time)

    stats = relationship("PlayerStats", backref="rounds")


class PlayerStats(Base):
    __tablename__ = 'player_stats'

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


class DataBaseException(Exception):
    pass


class DB(object):
    def __init__(self, db_path, db_framework='sqlite'):
        self.engine = create_engine(f'{db_framework}:///{db_path}')

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def __get_entry__(self, query, entry):
        with self.engine.connect() as conn:
            query_result = list(conn.execute(query))

        if len(query_result) == 0:
            self.session.add(entry)
            self.session.commit()

            return entry

        elif len(query_result) == 1:
            entry.id = query_result[0][0]

            return entry

        else:
            raise DataBaseException(
                'Ambigious entry. Please contact the developer.')

    def __get_player__(self, name):
        query = select(Player).where(Player.name == name)
        player = Player(name=name)

        return self.__get_entry__(query=query, entry=player)

    def __get_match__(self, league, season, match_in_season, date):
        query = select(Match).where(Match.league == league,
                                    Match.season == season,
                                    Match.match_in_season == match_in_season,
                                    Match.date == date)
        match = Match(league=league,
                      season=season,
                      match_in_season=match_in_season,
                      date=date)

        return self.__get_entry__(query=query, entry=match)

    def __get_round__(self, match, round_in_match, map_name, duration, time):
        query = select(Round).where(Round.match_id == match.id,
                                    Round.round_in_match == round_in_match,
                                    Round.map_name == map_name,
                                    Round.duration == duration,
                                    Round.time == time)
        round = Round(round_in_match=round_in_match,
                      map_name=map_name,
                      duration=duration,
                      time=time)

        match.rounds.append(round)

        return self.__get_entry__(query=query, entry=round)

    def __get_player_stat__(self, round, player, winner_team, kills, deaths,
                            assists, exp_contrib, healing, damage_soaked):
        query = select(PlayerStats).where(
            PlayerStats.round_id == round.id,
            PlayerStats.player_id == player.id,
            PlayerStats.winner_team == winner_team, PlayerStats.kills == kills,
            PlayerStats.deaths == deaths, PlayerStats.assists == assists,
            PlayerStats.exp_contrib == exp_contrib,
            PlayerStats.healing == healing,
            PlayerStats.damage_soaked == damage_soaked)

        player_stat = PlayerStats(winner_team=winner_team,
                                  kills=kills,
                                  deaths=deaths,
                                  assists=assists,
                                  exp_contrib=exp_contrib,
                                  healing=healing,
                                  damage_soaked=damage_soaked)
        player.stats.append(player_stat)
        round.stats.append(player_stat)

        return self.__get_entry__(query=query, entry=player_stat)

    def add_replay(self, replay):
        print(replay.utc_time)
        dt = datetime.fromtimestamp(replay.utc_time, tz.tzutc())

        match = self.__get_match__(league=replay.league,
                                   season=replay.season,
                                   match_in_season=replay.match_id,
                                   date=dt.date())

        round = self.__get_round__(match=match,
                                   round_in_match=replay.round_id,
                                   map_name=replay.map_name,
                                   duration=replay.get_duration_mins(),
                                   time=dt.time())

        df = replay.get_metrics()

        for i in range(len(df)):
            series = df.iloc[i]

            player = self.__get_player__(name=series['player_name'])

            self.__get_player_stat__(round=round,
                                     player=player,
                                     winner_team=series['winner_team'],
                                     kills=series['kills'],
                                     deaths=series['deaths'],
                                     assists=series['assists'],
                                     exp_contrib=series['exp_contrib'],
                                     healing=series['healing'],
                                     damage_soaked='damage_soaked')
