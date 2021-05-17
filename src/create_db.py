import os
from sqlalchemy import create_engine, MetaData, Table, Column, Boolean, Integer, Float, String, Date, Time


def init_db(path, name, framework='sqlite'):
    db_path = os.path.join(path, f'{name}.db')

    # connect to db, or create if non-existent
    engine = create_engine(f'{framework}:///{db_path}')
    con = engine.connect()

    # create tables
    meta = MetaData()

    players = Table('players', meta, Column('id', Integer, primary_key=True),
                    Column('name', String))

    matches = Table(
        'matches',
        meta,
        Column('id', Integer, primary_key=True),
        Column('league', String),
        Column('season', Integer),
        Column('match_in_season', Integer),
        Column('date', Date),
    )

    rounds = Table('rounds', meta, Column('id', Integer, primary_key=True),
                   Column('match_id', Integer),
                   Column('round_in_match', Integer),
                   Column('map_name', String), Column('duration', Integer),
                   Column('time', Time))

    player_stats = Table('player_stats', meta,
                         Column('id', Integer, primary_key=True),
                         Column('round_id', Integer),
                         Column('player_id', Integer),
                         Column('winner_team',
                                Boolean), Column('kills', Float),
                         Column('deaths', Float), Column('assists', Float),
                         Column('exp_contrib', Float),
                         Column('healing', Float),
                         Column('damage_soaked', Float))

    meta.create_all(engine)
    con.close()


if __name__ == "__main__":
    init_db('.', 'test')
