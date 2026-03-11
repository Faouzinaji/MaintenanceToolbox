import os
from datetime import datetime, timezone

from sqlalchemy import create_engine, Integer, String, DateTime
from sqlalchemy.orm import DeclarativeBase, mapped_column, sessionmaker


class Base(DeclarativeBase):
    pass


class Planning(Base):

    __tablename__ = "plannings"

    id = mapped_column(Integer, primary_key=True)
    name = mapped_column(String)
    created_at = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc)
    )


def get_database_url():

    return os.getenv(
        "DATABASE_URL",
        "sqlite:///maintenance_toolbox.db"
    )


engine = create_engine(get_database_url(), future=True)

SessionLocal = sessionmaker(bind=engine)


def init_db():

    Base.metadata.create_all(engine)
