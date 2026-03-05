from contextlib import contextmanager
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()  # module-level singleton

class Database:
    def __init__(self, db_url: str):
        self.engine = create_engine(
            db_url,
            connect_args={"check_same_thread": False}
        )
        self._register_pragmas()
        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False
        )

    def _register_pragmas(self):
        @event.listens_for(self.engine, "connect")
        def set_pragmas(conn, _):
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")

    def create_tables(self):
        Base.metadata.create_all(bind=self.engine)

    @contextmanager
    def get_session(self):
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
