"""
Database bootstrap: engine, Base, Session.
We keep this small so switching SQLite -> Postgres later is painless.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

_engine = None
_SessionLocal = None

class Base(DeclarativeBase):
    pass

def init_db(url: str = "sqlite:///data/app.db", echo: bool = False):
    """
    Initialize database engine + session factory and create tables.
    """
    global _engine, _SessionLocal
    _engine = create_engine(url, echo=echo, future=True)
    _SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False, future=True)
    # Late import to avoid circulars
    from .database_models import Journey, SubProcess, Step, EventFact  # noqa
    Base.metadata.create_all(_engine)
    return _engine

def get_session():
    if _SessionLocal is None:
        # default to local sqlite if init_db wasn't called explicitly
        init_db()
    return _SessionLocal()
