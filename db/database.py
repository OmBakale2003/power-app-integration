from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

class Database:
    def __init__(self, db_url: str):
        self.engine = create_engine(
            db_url,
            connect_args={"check_same_thread": False}
        )
        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False
        )
        self.Base = declarative_base()

    def create_tables(self):
        self.Base.metadata.create_all(bind=self.engine)

    def get_session(self):
        return self.SessionLocal()