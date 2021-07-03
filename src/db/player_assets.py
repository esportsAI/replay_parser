from core import Base


class Player(Base):
    __tablename__ = 'Player'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    blizzard_id = Column(Integer)

    # first parameter points to class not to table name
    stats = relationship("PlayerStats", back_populates="player")
    scores = relationship("PlayerScores", back_populates="player")
    
class PlayerName(Base):
    pass
    
# TODO: Rename PlayerStats to PlayerStatistic
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
    
# TODO: Rename PlayerScores to PlayerScore
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

class Score(Base):
    pass
