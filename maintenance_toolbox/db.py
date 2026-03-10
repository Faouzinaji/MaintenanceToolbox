import os
from datetime import datetime

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Boolean,
    select,
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

Base = declarative_base()

# ---------------------------------------------------
# DATABASE CONNECTION
# ---------------------------------------------------

def make_engine():

    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise RuntimeError("DATABASE_URL not configured")

    return create_engine(database_url, echo=False, future=True)


engine = make_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


# ---------------------------------------------------
# MODELS
# ---------------------------------------------------

class Organization(Base):

    __tablename__ = "organizations"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    code = Column(String, nullable=True)

    users = relationship("User", back_populates="organization")


class User(Base):

    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True, nullable=False)
    password_hash = Column(String, nullable=False)

    organization_id = Column(Integer, ForeignKey("organizations.id"))

    organization = relationship("Organization", back_populates="users")


class Planning(Base):

    __tablename__ = "plannings"

    id = Column(Integer, primary_key=True)

    name = Column(String)
    sector = Column(String)

    start_date = Column(DateTime)
    end_date = Column(DateTime)

    status = Column(String, default="draft")

    organization_id = Column(Integer, ForeignKey("organizations.id"))

    created_at = Column(DateTime, default=datetime.utcnow)


class FieldMapping(Base):

    __tablename__ = "field_mappings"

    id = Column(Integer, primary_key=True)

    organization_id = Column(Integer, ForeignKey("organizations.id"))

    # champs CSV (configurable plus tard)
    ot_field = Column(String, default="OT")
    equipment_field = Column(String, default="Equipement")
    description_field = Column(String, default="Description OT")
    estimated_hours_field = Column(String, default="Heures estimées")


# ---------------------------------------------------
# INITIALIZATION
# ---------------------------------------------------

def init_db():

    Base.metadata.create_all(bind=engine)


# ---------------------------------------------------
# DEFAULTS
# ---------------------------------------------------

def ensure_org_defaults(session, org):

    mapping = session.scalar(
        select(FieldMapping).where(FieldMapping.organization_id == org.id)
    )

    if mapping:
        return mapping

    mapping = FieldMapping(
        organization_id=org.id
    )

    session.add(mapping)
    session.commit()

    return mapping
