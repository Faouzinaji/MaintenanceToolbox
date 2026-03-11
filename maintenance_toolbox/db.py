from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    Text,
    create_engine,
    select,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker
from werkzeug.security import check_password_hash, generate_password_hash


DEFAULT_ADMIN_EMAIL = "admin@admin.admin"
DEFAULT_ADMIN_PASSWORD = "Admin123!"


DEFAULT_CAUSES = [
    ("Pièce indisponible", "Part unavailable", "Onderdeel niet beschikbaar"),
    ("Manque de temps", "Lack of time", "Tijdgebrek"),
    ("Ressource indisponible", "Resource unavailable", "Resource niet beschikbaar"),
    ("Accès impossible", "No access", "Geen toegang"),
    ("Priorité changée", "Priority changed", "Prioriteit gewijzigd"),
    ("Sécurité", "Safety", "Veiligheid"),
    ("Attente production", "Waiting for production", "Wachten op productie"),
    ("Autre", "Other", "Andere"),
]


class Base(DeclarativeBase):
    pass


class Organization(Base):
    __tablename__ = "organizations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    timezone: Mapped[str] = mapped_column(String(64), default="Europe/Paris")
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    users: Mapped[list["User"]] = relationship(back_populates="organization")
    plannings: Mapped[list["Planning"]] = relationship(
        back_populates="organization", cascade="all, delete-orphan"
    )
    mappings: Mapped[list["FieldMapping"]] = relationship(
        back_populates="organization", cascade="all, delete-orphan"
    )
    rex_causes: Mapped[list["RexCause"]] = relationship(
        back_populates="organization", cascade="all, delete-orphan"
    )


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(512))
    full_name: Mapped[str] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(32), default="admin")
    language: Mapped[str] = mapped_column(String(8), default="fr")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    first_login: Mapped[bool] = mapped_column(Boolean, default=True)
    organization_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("organizations.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    organization: Mapped[Optional[Organization]] = relationship(back_populates="users")

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)


class FieldMapping(Base):
    __tablename__ = "field_mappings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(
        ForeignKey("organizations.id"), unique=True, index=True
    )

    ot_id_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    description_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    status_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    atelier_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    secteur_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    equipment_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    equipment_desc_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_by_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    requested_week_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    condition_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    estimated_hours_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    organization: Mapped[Organization] = relationship(back_populates="mappings")


class RexCause(Base):
    __tablename__ = "rex_causes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(ForeignKey("organizations.id"), index=True)
    label_fr: Mapped[str] = mapped_column(String(255))
    label_en: Mapped[str] = mapped_column(String(255))
    label_nl: Mapped[str] = mapped_column(String(255))
    active: Mapped[bool] = mapped_column(Boolean, default=True)

    organization: Mapped[Organization] = relationship(back_populates="rex_causes")


class Planning(Base):
    __tablename__ = "plannings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(ForeignKey("organizations.id"), index=True)
    created_by_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    name: Mapped[str] = mapped_column(String(255), index=True)
    sectors_csv: Mapped[str] = mapped_column(Text, default="")
    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    daily_open: Mapped[str] = mapped_column(String(5), default="07:00")
    daily_close: Mapped[str] = mapped_column(String(5), default="15:00")
    status: Mapped[str] = mapped_column(String(32), default="draft")
    csv_filename: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    csv_bytes: Mapped[Optional[bytes]] = mapped_column(LargeBinary, nullable=True)
    window_real_start: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    window_real_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
    archived: Mapped[bool] = mapped_column(Boolean, default=False)

    organization: Mapped[Organization] = relationship(back_populates="plannings")
    tasks: Mapped[list["PlanningTask"]] = relationship(
        back_populates="planning", cascade="all, delete-orphan"
    )
    teams: Mapped[list["PlanningTeam"]] = relationship(
        back_populates="planning", cascade="all, delete-orphan"
    )


class PlanningTeam(Base):
    __tablename__ = "planning_teams"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    planning_id: Mapped[int] = mapped_column(ForeignKey("plannings.id"), index=True)
    atelier: Mapped[str] = mapped_column(String(255), index=True)
    code: Mapped[str] = mapped_column(String(255))
    name: Mapped[str] = mapped_column(String(255))
    available_from: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    available_to: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    planning: Mapped[Planning] = relationship(back_populates="teams")


class PlanningTask(Base):
    __tablename__ = "planning_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    planning_id: Mapped[int] = mapped_column(ForeignKey("plannings.id"), index=True)

    external_ot_id: Mapped[str] = mapped_column(String(255), index=True)
    task_type: Mapped[str] = mapped_column(String(32), default="ot")
    description: Mapped[str] = mapped_column(Text)

    equipment_code: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    equipment_desc: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    atelier: Mapped[str] = mapped_column(String(255), index=True)
    secteur: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    source_status: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at_source: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_by_source: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    requested_week_source: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    condition_source: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    estimated_hours: Mapped[float] = mapped_column(Float, default=0.0)
    duration_hours: Mapped[float] = mapped_column(Float, default=0.0)

    selected: Mapped[bool] = mapped_column(Boolean, default=False)
    selected_warning: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    predecessor_ot_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    forced_team_codes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    forced_start_at_text: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    planned_start_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    planned_end_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    planned_team_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    rex_done: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    rex_realized: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    rex_cause_id: Mapped[Optional[int]] = mapped_column(ForeignKey("rex_causes.id"), nullable=True)
    rex_comment: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    rex_recorded_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    rex_recorded_by_user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True)

    planning: Mapped[Planning] = relationship(back_populates="tasks")
    rex_cause: Mapped[Optional[RexCause]] = relationship()


def get_database_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if url:
        return url
    return "sqlite:///maintenance_toolbox.db"


def make_engine(echo: bool = False):
    url = get_database_url()
    connect_args = {}
    if url.startswith("sqlite"):
        connect_args = {"check_same_thread": False}
    return create_engine(url, echo=echo, future=True, connect_args=connect_args)


engine = make_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def ensure_org_defaults(session, org: Organization) -> None:
    mapping = session.scalar(
        select(FieldMapping).where(FieldMapping.organization_id == org.id)
    )
    if mapping is None:
        mapping = FieldMapping(
            organization_id=org.id,
            ot_id_col="OT",
            description_col="Description",
            status_col="Statut",
            atelier_col="Atelier",
            secteur_col="Secteur",
            equipment_col="Equipement",
            equipment_desc_col="Description équipement",
            created_at_col="Créé le",
            created_by_col="Créé par",
            requested_week_col="Sem. souhaitée",
            condition_col="Condition réalisation",
            estimated_hours_col="Durée estimée",
        )
        session.add(mapping)

    existing_causes = session.scalars(
        select(RexCause).where(RexCause.organization_id == org.id)
    ).all()

    if not existing_causes:
        for fr, en, nl in DEFAULT_CAUSES:
            session.add(
                RexCause(
                    organization_id=org.id,
                    label_fr=fr,
                    label_en=en,
                    label_nl=nl,
                    active=True,
                )
            )

    session.commit()


def init_db() -> None:
    Base.metadata.create_all(bind=engine)

    with SessionLocal() as session:
        org = session.scalar(
            select(Organization).where(Organization.name == "Default Organization")
        )
        if not org:
            org = Organization(
                name="Default Organization",
                timezone="Europe/Paris",
                active=True,
            )
            session.add(org)
            session.commit()
            session.refresh(org)

        ensure_org_defaults(session, org)

        admin = session.scalar(select(User).where(User.email == DEFAULT_ADMIN_EMAIL))
        if not admin:
            admin = User(
                email=DEFAULT_ADMIN_EMAIL,
                full_name="Global Admin",
                role="admin",
                language="fr",
                is_active=True,
                first_login=True,
                organization_id=org.id,
            )
            admin.set_password(DEFAULT_ADMIN_PASSWORD)
            session.add(admin)
            session.commit()
