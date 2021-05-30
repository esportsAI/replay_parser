from sqlalchemy import create_engine
from sqlalchemy import Column, ForeignKey, Boolean, Integer, Float, String, Date, Time
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from dateutil import tz

Base = declarative_base()


class Player(Base):
    __tablename__ = 'Player'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    blizzard_id = Column(Integer)

    # first parameter points to class not to table name
    stats = relationship("PlayerStats", back_populates="player")
    scores = relationship("PlayerScores", back_populates="player")


class Match(Base):
    __tablename__ = 'Match'

    id = Column(Integer, primary_key=True)
    league = Column(String)
    season = Column(Integer)
    match_in_season = Column(Integer)
    date = Column(Date)

    rounds = relationship("Round", back_populates="match")
    scores = relationship("PlayerScores", back_populates="match")


class Round(Base):
    __tablename__ = 'Round'

    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('Match.id'))
    round_in_match = Column(Integer)
    map_name = Column(String)
    duration = Column(Integer)
    time = Column(Time)

    match = relationship("Match", back_populates="rounds")
    stats = relationship("PlayerStats", back_populates="round")


class PlayerStats(Base):
    __tablename__ = 'PlayerStatistics'

    id = Column(Integer, primary_key=True)
    round_id = Column(Integer, ForeignKey('Round.id'))
    player_id = Column(Integer, ForeignKey('Player.id'))
    winner_team = Column(Boolean)
    kills = Column(Float)
    deaths = Column(Float)
    assists = Column(Float)
    exp_contrib = Column(Float)
    healing = Column(Float)
    damage_soaked = Column(Float)

    player = relationship("Player", back_populates="stats")
    round = relationship("Round", back_populates="stats")


class PlayerScores(Base):
    __tablename__ = 'PlayerScores'

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('Player.id'))
    match_id = Column(Integer, ForeignKey('Match.id'))
    kills = Column(Float)
    deaths = Column(Float)
    assists = Column(Float)
    exp_per_min = Column(Float)
    healing = Column(Float)
    damage_soaked = Column(Float)
    winner = Column(Float)
    under_10_mins = Column(Float)
    under_15_mins = Column(Float)
    total = Column(Float)

    player = relationship("Player", back_populates="scores")
    match = relationship("Match", back_populates="scores")


class DataBaseException(Exception):
    pass


class DB(object):
    def __init__(self, path, framework='sqlite'):
        self.engine = create_engine(f'{framework}:///{path}')

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def create_db(self):
        Base.metadata.create_all(self.engine)

    def __get_entry__(self, query, entry, entry_class):
        query_result = self.session.query(entry_class).filter(*query).all()

        if len(query_result) == 0:
            return entry, False

        elif len(query_result) == 1:
            return query_result[0], True

        else:
            raise DataBaseException(
                'Ambigious entry. Please contact the developer.')

    def __get_player__(self, name, blizzard_id):
        query = (Player.blizzard_id == blizzard_id, )
        player = Player(name=name, blizzard_id=blizzard_id)

        player, exists = self.__get_entry__(query=query,
                                            entry=player,
                                            entry_class=Player)

        if not exists:
            self.session.add(player)
            self.session.commit()

        return player

    def __get_player_by_id__(self, id):
        query_result = self.session.query(Player).filter(Player.id == id).all()

        return query_result[0]

    def __get_match__(self, league, season, match_in_season, date):
        query = (Match.league == league, Match.season == season,
                 Match.match_in_season == match_in_season)
        match = Match(league=league,
                      season=season,
                      match_in_season=match_in_season,
                      date=date)

        match, exists = self.__get_entry__(query=query,
                                           entry=match,
                                           entry_class=Match)

        if not exists:
            self.session.add(match)
            self.session.commit()

        return match

    def __get_match_by_id__(self, id):
        query_result = self.session.query(Match).filter(Match.id == id).all()

        return query_result[0]

    def __get_round__(self, match, round_in_match, map_name, duration, time):
        query = (Round.match_id == match.id,
                 Round.round_in_match == round_in_match,
                 Round.map_name == map_name, Round.duration == duration,
                 Round.time == time)
        round = Round(round_in_match=round_in_match,
                      map_name=map_name,
                      duration=duration,
                      time=time)

        round, exists = self.__get_entry__(query=query,
                                           entry=round,
                                           entry_class=Round)

        if not exists:
            match.rounds.append(round)

            self.session.add(round)
            self.session.commit()

        return round

    def __get_player_stat__(self, round, player, winner_team, kills, deaths,
                            assists, exp_contrib, healing, damage_soaked):

        query = (PlayerStats.round_id == round.id,
                 PlayerStats.player_id == player.id,
                 PlayerStats.winner_team == winner_team,
                 PlayerStats.kills == kills, PlayerStats.deaths == deaths,
                 PlayerStats.assists == assists,
                 PlayerStats.exp_contrib == exp_contrib,
                 PlayerStats.healing == healing,
                 PlayerStats.damage_soaked == damage_soaked)

        player_stats = PlayerStats(winner_team=winner_team,
                                   kills=kills,
                                   deaths=deaths,
                                   assists=assists,
                                   exp_contrib=exp_contrib,
                                   healing=healing,
                                   damage_soaked=damage_soaked)

        player_stats, exists = self.__get_entry__(query=query,
                                                  entry=player_stats,
                                                  entry_class=PlayerStats)

        if not exists:
            player.stats.append(player_stats)
            round.stats.append(player_stats)

            self.session.add(player_stats)
            self.session.commit()

        return player_stats

    def __get_player_scores__(self, player, match, kills, deaths, assists,
                              exp_per_min, healing, damage_soaked, winner,
                              under_10_mins, under_15_mins, total):

        query = (PlayerScores.player_id == player.id,
                 PlayerScores.match_id == match.id)

        player_scores = PlayerScores(kills=kills,
                                     deaths=deaths,
                                     assists=assists,
                                     exp_per_min=exp_per_min,
                                     healing=healing,
                                     damage_soaked=damage_soaked,
                                     winner=winner,
                                     under_10_mins=under_10_mins,
                                     under_15_mins=under_15_mins,
                                     total=total)

        player_scores, exists = self.__get_entry__(query=query,
                                                   entry=player_scores,
                                                   entry_class=PlayerScores)

        if not exists:
            player.scores.append(player_scores)
            match.scores.append(player_scores)

            self.session.add(player_scores)
            self.session.commit()

        return player_scores

    def add_replay(self, replay):
        # set time
        dt = datetime.fromtimestamp(replay.utc_time, tz.tzutc())
        dt = dt.astimezone(tz.gettz('America/New_York'))

        # add replay data to DB
        match = self.__get_match__(league=replay.league,
                                   season=replay.season,
                                   match_in_season=replay.match_id,
                                   date=dt.date())

        round = self.__get_round__(match=match,
                                   round_in_match=replay.round_id,
                                   map_name=replay.map_name,
                                   duration=replay.get_duration_mins(),
                                   time=dt.time())

        metrics_df = replay.get_metrics()
        player_info_df = replay.get_player_info()

        df = player_info_df.merge(metrics_df, on='player_name')

        for i in range(len(df)):
            series = df.iloc[i]

            player = self.__get_player__(name=series['player_name'],
                                         blizzard_id=int(
                                             series['blizzard_id']))

            self.__get_player_stat__(round=round,
                                     player=player,
                                     winner_team=series['winner_team'],
                                     kills=series['kills'],
                                     deaths=series['deaths'],
                                     assists=series['assists'],
                                     exp_contrib=series['exp_contrib'],
                                     healing=series['healing'],
                                     damage_soaked=series['damage_soaked'])

    def add_match_scores(self, match):
        df = match.get_scores()
        df.reset_index(inplace=True)

        db_match_entry = self.__get_match_by_id__(id=match.id)

        for i in range(len(df)):
            data_series = df.iloc[i]
            player = self.__get_player_by_id__(id=data_series['player_id'])

            self.__get_player_scores__(
                player=player,
                match=db_match_entry,
                kills=data_series['kills'],
                deaths=data_series['deaths'],
                assists=data_series['assists'],
                exp_per_min=data_series['exp_per_min'],
                healing=data_series['healing'],
                damage_soaked=data_series['damage_soaked'],
                winner=data_series['winner'],
                under_10_mins=data_series['under_10_mins'],
                under_15_mins=data_series['under_15_mins'],
                total=data_series['total'])
