from __future__ import annotations

import json
import os
import random
from datetime import datetime, timedelta, timezone
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
    UniqueConstraint,
    create_engine,
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker
from werkzeug.security import check_password_hash, generate_password_hash


DEFAULT_ADMIN_EMAIL = "admin@admin.admin"
DEFAULT_ADMIN_PASSWORD = "Admin123!"


DEFAULT_CAUSES = [
    ("Piece indisponible", "Part unavailable", "Onderdeel niet beschikbaar"),
    ("Manque de temps", "Lack of time", "Tijdgebrek"),
    ("Ressource indisponible", "Resource unavailable", "Resource niet beschikbaar"),
    ("Acces impossible", "No access", "Geen toegang"),
    ("Priorite changee", "Priority changed", "Prioriteit gewijzigd"),
    ("Securite", "Safety", "Veiligheid"),
    ("Attente production", "Waiting for production", "Wachten op productie"),
    ("Autre", "Other", "Andere"),
]


DEFAULT_MEETING_TYPES = [
    {
        "name": "Pré-scheduling",
        "frequency": "weekly",
        "duration_minutes": 120,
        "active": True,
        "order_index": 1,
        "icon": "📋",
        "module_key": "pre_scheduling",
        "agenda": json.dumps([
            "Revue du backlog OT",
            "Sélection et priorisation des OT pour l'arrêt",
            "Définition des équipes et ressources",
            "Génération du planning",
            "Validation et export du planning",
        ]),
    },
    {
        "name": "Scheduling",
        "frequency": "weekly",
        "duration_minutes": 90,
        "active": True,
        "order_index": 2,
        "icon": "📅",
        "module_key": "scheduling",
        "agenda": json.dumps([
            "Revue du planning précédent (REX)",
            "Analyse des écarts et causes racines",
            "Plan d'action correctif",
            "Validation du planning à venir",
            "Communication équipes et production",
        ]),
    },
    {
        "name": "Comité de maintenance",
        "frequency": "monthly",
        "duration_minutes": 60,
        "active": True,
        "order_index": 3,
        "icon": "📊",
        "module_key": "comite",
        "agenda": json.dumps([
            "KPI tenue des réunions",
            "KPI respect du timing",
            "KPI participation",
            "Suivi et clôture des actions",
            "Points divers",
        ]),
    },
    {
        "name": "Codir",
        "frequency": "monthly",
        "duration_minutes": 45,
        "active": True,
        "order_index": 4,
        "icon": "🎯",
        "module_key": "codir",
        "agenda": json.dumps([
            "Tableau de bord gouvernance maintenance",
            "Taux de tenue global",
            "Respect timing & participation",
            "Statut des actions critiques",
            "Arbitrages et décisions",
        ]),
    },
    {
        "name": "Réunion fiabilité",
        "frequency": "monthly",
        "duration_minutes": 60,
        "active": False,
        "order_index": 5,
        "icon": "🔧",
        "module_key": None,
        "agenda": json.dumps([
            "Analyse des pannes récurrentes",
            "Plans de fiabilisation",
            "Retour d'expérience terrain",
        ]),
    },
    {
        "name": "Magasin",
        "frequency": "weekly",
        "duration_minutes": 30,
        "active": False,
        "order_index": 6,
        "icon": "📦",
        "module_key": None,
        "agenda": json.dumps([
            "Revue des stocks critiques",
            "Commandes en attente",
            "Alertes rupture",
        ]),
    },
    {
        "name": "Hebdomadaire",
        "frequency": "weekly",
        "duration_minutes": 45,
        "active": False,
        "order_index": 7,
        "icon": "📆",
        "module_key": None,
        "agenda": json.dumps([
            "Revue semaine écoulée",
            "Planification semaine suivante",
            "Points blocants",
        ]),
    },
    {
        "name": "Gatekeeping",
        "frequency": "weekly",
        "duration_minutes": 30,
        "active": False,
        "order_index": 8,
        "icon": "🚦",
        "module_key": None,
        "agenda": json.dumps([
            "Validation entrée des OT",
            "Priorisation des demandes",
            "Vérification des prérequis",
        ]),
    },
    {
        "name": "Comex",
        "frequency": "monthly",
        "duration_minutes": 90,
        "active": False,
        "order_index": 9,
        "icon": "🏛️",
        "module_key": None,
        "agenda": json.dumps([
            "Revue direction maintenance",
            "Indicateurs stratégiques",
            "Décisions et arbitrages",
        ]),
    },
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
    mappings: Mapped[list["FieldMapping"]] = relationship(
        back_populates="organization", cascade="all, delete-orphan"
    )
    plannings: Mapped[list["Planning"]] = relationship(
        back_populates="organization", cascade="all, delete-orphan"
    )
    rex_causes: Mapped[list["RexCause"]] = relationship(
        back_populates="organization", cascade="all, delete-orphan"
    )
    meeting_instances: Mapped[list["MeetingInstance"]] = relationship(
        back_populates="organization", cascade="all, delete-orphan"
    )
    actions: Mapped[list["Action"]] = relationship(
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
    __table_args__ = (UniqueConstraint("organization_id", name="uq_mapping_org"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(ForeignKey("organizations.id"), index=True)

    ot_id_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, default="OT")
    description_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, default="Description")
    status_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, default="Statut")
    atelier_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, default="Atelier")
    secteur_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, default="Secteur")
    equipment_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, default="Equipement")
    equipment_desc_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, default="Description equipement")
    created_at_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, default="Cree le")
    created_by_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, default="Cree par")
    requested_week_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, default="Sem. souhaitee")
    condition_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, default="Condition realisation")
    estimated_hours_col: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, default="Duree estimee")
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
    sectors_csv: Mapped[str] = mapped_column(Text)
    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    daily_open: Mapped[str] = mapped_column(String(5), default="07:00")
    daily_close: Mapped[str] = mapped_column(String(5), default="15:00")
    site_open: Mapped[str] = mapped_column(String(5), default="06:00")
    site_close: Mapped[str] = mapped_column(String(5), default="22:00")
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
    selected: Mapped[bool] = mapped_column(Boolean, default=False)
    selected_warning: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    predecessor_ot_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    forced_team_codes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    forced_start_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    operation_mode: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    free_start_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    free_end_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    planned_start_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    planned_end_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    planned_team_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    plan_locked: Mapped[bool] = mapped_column(Boolean, default=False)
    rex_done: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    rex_cause_id: Mapped[Optional[int]] = mapped_column(ForeignKey("rex_causes.id"), nullable=True)
    rex_comment: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    rex_actual_start: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)  # HH:MM
    rex_actual_end: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)    # HH:MM

    planning: Mapped[Planning] = relationship(back_populates="tasks")
    rex_cause: Mapped[Optional[RexCause]] = relationship()


# ─────────────────────────────────────────────────────────
#  MEETING HUB MODELS
# ─────────────────────────────────────────────────────────

class MeetingType(Base):
    __tablename__ = "meeting_types"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True)
    frequency: Mapped[str] = mapped_column(String(32), default="weekly")
    duration_minutes: Mapped[int] = mapped_column(Integer, default=60)
    agenda_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    order_index: Mapped[int] = mapped_column(Integer, default=0)
    icon: Mapped[str] = mapped_column(String(16), default="📅")
    module_key: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    instances: Mapped[list["MeetingInstance"]] = relationship(
        back_populates="meeting_type", cascade="all, delete-orphan"
    )


class MeetingInstance(Base):
    """A meeting configured by admin from a MeetingType template.
    Called 'réunion' in the UI (e.g. 'Pré-scheduling Ligne A').
    """
    __tablename__ = "meeting_instances"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    meeting_type_id: Mapped[int] = mapped_column(ForeignKey("meeting_types.id"), index=True)
    organization_id: Mapped[int] = mapped_column(ForeignKey("organizations.id"), index=True)
    name: Mapped[str] = mapped_column(String(255))
    # Custom agenda overrides the MeetingType default agenda when set
    custom_agenda_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # Custom duration overrides the MeetingType default duration when set
    custom_duration_minutes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    # Legacy field kept for backward compat; new duplicates store emails in MeetingSession
    participants_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    scheduled_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_by_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    meeting_type: Mapped["MeetingType"] = relationship(back_populates="instances")
    organization: Mapped[Organization] = relationship(back_populates="meeting_instances")
    sessions: Mapped[list["MeetingSession"]] = relationship(
        back_populates="instance", cascade="all, delete-orphan"
    )

    def effective_agenda(self) -> list[str]:
        src = self.custom_agenda_json or (self.meeting_type.agenda_json if self.meeting_type else None)
        if src:
            try:
                return json.loads(src)
            except Exception:
                pass
        return []

    def effective_duration(self) -> int:
        if self.custom_duration_minutes is not None:
            return self.custom_duration_minutes
        if self.meeting_type:
            return self.meeting_type.duration_minutes
        return 60


class MeetingSession(Base):
    """A duplicate/run of a MeetingInstance. Called 'duplicate' in the UI.
    Stores its own participant list (emails) set by admin at creation time.
    """
    __tablename__ = "meeting_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    instance_id: Mapped[int] = mapped_column(ForeignKey("meeting_instances.id"), index=True)
    # Name of this duplicate (e.g. "Semaine 12 — Ligne A")
    session_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    # Participant emails set by admin when creating the duplicate
    invited_emails_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_real_minutes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    attendees_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    absents_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="draft")
    summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    mail_subject: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    mail_recipients: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    mail_body: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    instance: Mapped["MeetingInstance"] = relationship(back_populates="sessions")
    actions: Mapped[list["Action"]] = relationship(
        back_populates="session", cascade="all, delete-orphan"
    )

    def get_invited(self) -> list[str]:
        """Return participant list from invited_emails_json or legacy participants_json."""
        if self.invited_emails_json:
            try:
                return json.loads(self.invited_emails_json)
            except Exception:
                pass
        # Fallback to instance-level participants
        if self.instance and self.instance.participants_json:
            try:
                return json.loads(self.instance.participants_json)
            except Exception:
                pass
        return []


class Action(Base):
    __tablename__ = "actions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    description: Mapped[str] = mapped_column(Text)
    owner: Mapped[str] = mapped_column(String(255))
    due_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="Open")
    meeting_session_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("meeting_sessions.id"), nullable=True, index=True
    )
    meeting_type_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("meeting_types.id"), nullable=True, index=True
    )
    organization_id: Mapped[int] = mapped_column(ForeignKey("organizations.id"), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    session: Mapped[Optional["MeetingSession"]] = relationship(back_populates="actions")
    organization: Mapped[Organization] = relationship(back_populates="actions")


# ─────────────────────────────────────────────────────────
#  ENGINE & SESSION
# ─────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────
#  INIT HELPERS
# ─────────────────────────────────────────────────────────

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
            equipment_desc_col="Description equipement",
            created_at_col="Cree le",
            created_by_col="Cree par",
            requested_week_col="Sem. souhaitee",
            condition_col="Condition realisation",
            estimated_hours_col="Duree estimee",
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


def _init_meeting_types(session) -> None:
    for mt_data in DEFAULT_MEETING_TYPES:
        existing = session.scalar(
            select(MeetingType).where(MeetingType.name == mt_data["name"])
        )
        if not existing:
            mt = MeetingType(
                name=mt_data["name"],
                frequency=mt_data["frequency"],
                duration_minutes=mt_data["duration_minutes"],
                active=mt_data["active"],
                order_index=mt_data["order_index"],
                icon=mt_data["icon"],
                module_key=mt_data["module_key"],
                agenda_json=mt_data["agenda"],
            )
            session.add(mt)
    session.commit()


def _seed_demo_data(session, org: Organization, admin_user: "User") -> None:
    """Seed realistic demo data for KPI pages. Runs only if no instances exist for the org."""
    try:
        existing = session.scalar(
            select(MeetingInstance).where(MeetingInstance.organization_id == org.id)
        )
        if existing:
            return

        rng = random.Random(42)
        now = datetime.now(timezone.utc)

        PARTICIPANTS = [
            "Pierre Dupont", "Marie Lambert", "Thomas Bernard",
            "Isabelle Fontaine", "Laurent Morel", "Christine Petit",
            "Nicolas Roux", "Sophie Martin",
        ]

        ACTION_TEMPLATES = [
            ("Commander les pièces pour OT-{n}", "Pierre Dupont"),
            ("Valider le planning avec la production", "Marie Lambert"),
            ("Mettre à jour la GMAO suite au REX", "Thomas Bernard"),
            ("Former l'équipe sur la nouvelle procédure", "Isabelle Fontaine"),
            ("Clôturer les OT en attente depuis >30j", "Laurent Morel"),
            ("Réviser le plan préventif du trimestre", "Christine Petit"),
            ("Audit sécurité zone convoyeur", "Nicolas Roux"),
            ("Rapport mensuel maintenance", "Sophie Martin"),
            ("Relance fournisseur pièce critique", "Pierre Dupont"),
            ("Réunion alignement planning/production", "Marie Lambert"),
            ("Vérifier alignement pompe secteur 4", "Thomas Bernard"),
            ("Mettre à jour le plan de lubrification", "Isabelle Fontaine"),
        ]

        def _pick_attendees():
            n = rng.randint(5, 8)
            present = rng.sample(PARTICIPANTS, n)
            absent = [p for p in PARTICIPANTS if p not in present]
            return present, absent

        def _action_status(weeks_ago: int) -> str:
            if weeks_ago >= 4:
                return rng.choice(["Done", "Done", "Done", "Late"])
            if weeks_ago >= 2:
                return rng.choice(["Done", "In Progress", "Open", "Late"])
            return rng.choice(["Open", "Open", "In Progress"])

        mt_map = {
            mt.module_key: mt
            for mt in session.scalars(select(MeetingType)).all()
            if mt.module_key
        }

        # ── Phase 1: create all instances (no flush per instance) ──────────────
        # Store (instance, weeks_ago, theoretical_min) for phase 2
        instance_specs: list[tuple] = []

        mt = mt_map.get("pre_scheduling")
        if mt:
            for i in range(6, -1, -1):
                sched = now - timedelta(weeks=i)
                week_num = sched.isocalendar()[1]
                inst = MeetingInstance(
                    meeting_type_id=mt.id,
                    organization_id=org.id,
                    name=f"Pré-scheduling S{week_num}",
                    scheduled_date=sched,
                    participants_json=json.dumps(PARTICIPANTS),
                    created_by_user_id=admin_user.id,
                    created_at=sched - timedelta(days=1),
                )
                session.add(inst)
                instance_specs.append((inst, i, mt.duration_minutes))

        mt = mt_map.get("scheduling")
        if mt:
            for i in range(6, -1, -1):
                sched = now - timedelta(weeks=i)
                week_num = sched.isocalendar()[1]
                inst = MeetingInstance(
                    meeting_type_id=mt.id,
                    organization_id=org.id,
                    name=f"Scheduling S{week_num}",
                    scheduled_date=sched,
                    participants_json=json.dumps(PARTICIPANTS),
                    created_by_user_id=admin_user.id,
                    created_at=sched - timedelta(days=1),
                )
                session.add(inst)
                instance_specs.append((inst, i, mt.duration_minutes))

        mt = mt_map.get("comite")
        if mt:
            for i in range(3, -1, -1):
                sched = now - timedelta(weeks=i * 4)
                month_name = sched.strftime("%B %Y")
                inst = MeetingInstance(
                    meeting_type_id=mt.id,
                    organization_id=org.id,
                    name=f"Comité maintenance {month_name}",
                    scheduled_date=sched,
                    participants_json=json.dumps(PARTICIPANTS),
                    created_by_user_id=admin_user.id,
                    created_at=sched - timedelta(days=2),
                )
                session.add(inst)
                instance_specs.append((inst, i * 4, mt.duration_minutes))

        mt = mt_map.get("codir")
        if mt:
            for i in range(2, -1, -1):
                sched = now - timedelta(weeks=i * 4)
                month_name = sched.strftime("%B %Y")
                inst = MeetingInstance(
                    meeting_type_id=mt.id,
                    organization_id=org.id,
                    name=f"Codir {month_name}",
                    scheduled_date=sched,
                    participants_json=json.dumps(PARTICIPANTS),
                    created_by_user_id=admin_user.id,
                    created_at=sched - timedelta(days=3),
                )
                session.add(inst)
                instance_specs.append((inst, i * 4, mt.duration_minutes))

        # Single flush → all instance.id are now populated
        session.flush()

        # ── Phase 2: create all sessions (no flush per session) ────────────────
        # Store (session, weeks_ago, meeting_type_id, org_id) for phase 3
        session_specs: list[tuple] = []

        for inst, weeks_ago, theoretical_min in instance_specs:
            sched_date = now - timedelta(weeks=weeks_ago)
            if weeks_ago == 0:
                status = "draft"
                started = ended = real_min = None
            else:
                status = "closed"
                variance = rng.randint(-20, 30)
                real_min = theoretical_min + variance
                started = sched_date.replace(hour=9, minute=0, second=0, microsecond=0)
                ended = started + timedelta(minutes=real_min)

            present, absent = _pick_attendees()
            ms = MeetingSession(
                instance_id=inst.id,
                started_at=started,
                ended_at=ended,
                duration_real_minutes=real_min,
                attendees_json=json.dumps(present),
                absents_json=json.dumps(absent),
                status=status,
                summary=(
                    f"Réunion {inst.name} — {len(present)} présents. "
                    f"Durée réelle : {real_min} min." if real_min else "Session non démarrée."
                ),
            )
            session.add(ms)
            session_specs.append((ms, weeks_ago, inst.meeting_type_id, org.id))

        # Single flush → all ms.id are now populated
        session.flush()

        # ── Phase 3: create all actions ────────────────────────────────────────
        for ms, weeks_ago, meeting_type_id, organization_id in session_specs:
            if ms.status != "closed":
                continue
            sched_date = now - timedelta(weeks=weeks_ago)
            n_actions = rng.randint(2, 4)
            for _ in range(n_actions):
                tpl = rng.choice(ACTION_TEMPLATES)
                desc = tpl[0].format(n=rng.randint(1000, 9999))
                owner = tpl[1]
                due = sched_date + timedelta(weeks=rng.randint(1, 4))
                a_status = _action_status(weeks_ago)
                session.add(Action(
                    description=desc,
                    owner=owner,
                    due_date=due,
                    status=a_status,
                    meeting_session_id=ms.id,
                    meeting_type_id=meeting_type_id,
                    organization_id=organization_id,
                ))

        session.commit()

    except Exception:
        session.rollback()


def _run_migrations() -> None:
    """Safely add new columns to existing tables (idempotent — catches duplicate-column errors)."""
    new_columns = [
        ("meeting_instances", "custom_agenda_json", "TEXT"),
        ("meeting_instances", "custom_duration_minutes", "INTEGER"),
        ("meeting_sessions", "session_name", "VARCHAR(255)"),
        ("meeting_sessions", "invited_emails_json", "TEXT"),
        ("planning_tasks", "rex_actual_start", "VARCHAR(10)"),
        ("planning_tasks", "rex_actual_end", "VARCHAR(10)"),
        ("plannings", "site_open", "VARCHAR(5)"),
        ("plannings", "site_close", "VARCHAR(5)"),
    ]
    with engine.connect() as conn:
        for table, column, col_type in new_columns:
            try:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"))
                conn.commit()
            except Exception:
                conn.rollback()  # Column already exists — ignore


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _run_migrations()

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
            session.refresh(admin)

        _init_meeting_types(session)
        _seed_demo_data(session, org, admin)
