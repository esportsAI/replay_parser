from core import Base


class League(Base):
    pass
    
class Season(Base):
    pass
    
class SeasonPhase(Base):
    pass
    
class SeasonStagePhase(Base):
    pass
    
# TODO: Replace Match with Game
class Match(Base):
    __tablename__ = 'Match'

    id = Column(Integer, primary_key=True)
    league = Column(String)
    season = Column(Integer)
    match_in_season = Column(Integer)
    date = Column(Date)

    rounds = relationship("Round", back_populates="match")
    scores = relationship("PlayerScores", back_populates="match")

# TODO: Replace Round with GameRound
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