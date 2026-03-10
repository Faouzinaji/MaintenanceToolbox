from __future__ import annotations

import io
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import func, select

from maintenance_toolbox.db import (
    DEFAULT_ADMIN_EMAIL,
    DEFAULT_ADMIN_PASSWORD,
    AuditLog,
    FieldMapping,
    Organization,
    Planning,
    PlanningTask,
    PlanningTeam,
    RexCause,
    SessionLocal,
    User,
    ensure_org_defaults,
    init_db,
    log_action,
)
from maintenance_toolbox.i18n import tr
from maintenance_toolbox.scheduler import build_schedule

st.set_page_config(page_title="MaintenanceToolbox", layout="wide")

init_db()

# -------------------- Style --------------------
PRIMARY = "#F59C00"
DARK = "#4A4F55"
BG = "#F3F3F3"
BORDER = "#D8D8D8"
LIGHT_ROW = "#F5F5F5"

st.markdown(
    f"""
    <style>
        .stApp {{ background: {BG}; color: {DARK}; }}
        .hero {{ background: linear-gradient(90deg, #FFFFFF, #FFF7E8); border: 1px solid {BORDER}; border-radius: 18px; padding: 18px 22px; margin-bottom: 14px; }}
        .hero-title {{ font-size: 30px; font-weight: 800; color: {DARK}; }}
        .hero-sub {{ font-size: 14px; color: #6B7280; }}
        .card {{ background:white; border:1px solid {BORDER}; border-radius:16px; padding:16px; margin-bottom:10px; }}
        .section-title {{ font-weight:700; font-size:18px; margin-bottom:8px; color:{DARK}; }}
        .warning-box {{ background:#FFF7ED; border:1px solid #FDBA74; color:#9A3412; padding:10px 12px; border-radius:10px; margin:8px 0; }}
        .ok-box {{ background:#ECFDF5; border:1px solid #86EFAC; color:#166534; padding:10px 12px; border-radius:10px; margin:8px 0; }}
        .pill {{ display:inline-block; padding:4px 10px; border-radius:999px; background:#fff; border:1px solid {BORDER}; margin-right:6px; font-size:12px; }}
        .planning-btn button {{ height:90px; font-size:22px; border-radius:16px; }}
        .stButton > button, .stDownloadButton > button {{ background:{PRIMARY}; color:white; border:none; border-radius:10px; font-weight:700; }}
        .stButton > button:hover, .stDownloadButton > button:hover {{ background:#D98800; color:white; }}
        [data-testid="stDataFrame"] * {{ font-size: 11px !important; }}
        [data-testid="stDataEditor"] * {{ font-size: 11px !important; }}
    </style>
    """,
    unsafe_allow_html=True,
)

# -------------------- Helpers --------------------

MAP_FIELDS = {
    "ot_id_col": "OT ID",
    "description_col": "Description",
    "status_col": "Statut",
    "atelier_col": "Atelier",
    "secteur_col": "Secteur",
    "equipment_col": "Equipement",
    "equipment_desc_col": "Description équipement",
    "created_at_col": "Créé le",
    "created_by_col": "Créé par",
    "requested_week_col": "Sem. souhaitée",
    "condition_col": "Condition réalisation",
    "estimated_hours_col": "Durée estimée",
}

STATUS_LABELS = {
    "draft": {"fr": "Brouillon", "en": "Draft", "nl": "Concept"},
    "validated": {"fr": "Validé", "en": "Validated", "nl": "Gevalideerd"},
    "rex_pending": {"fr": "Attente REX", "en": "REX pending", "nl": "Wacht op REX"},
    "closed": {"fr": "Terminé", "en": "Closed", "nl": "Afgesloten"},
    "archived": {"fr": "Archivé", "en": "Archived", "nl": "Gearchiveerd"},
}


def get_lang() -> str:
    return st.session_state.get("lang", "fr")


def T(key: str) -> str:
    return tr(get_lang(), key)


def parse_csv(uploaded_file) -> pd.DataFrame:
    raw = uploaded_file.getvalue()
    attempts = [
        {"sep": ";", "encoding": "utf-8"},
        {"sep": ";", "encoding": "latin-1"},
        {"sep": ",", "encoding": "utf-8"},
        {"sep": ",", "encoding": "latin-1"},
        {"sep": "\t", "encoding": "utf-8"},
        {"sep": "\t", "encoding": "latin-1"},
    ]
    for a in attempts:
        try:
            return pd.read_csv(io.BytesIO(raw), sep=a["sep"], encoding=a["encoding"], low_memory=False)
        except Exception:
            continue
    raise ValueError("Unable to read CSV")


def normalize_text(x: Any) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip().lower()


def guess_hours(description: str, equipment_desc: str) -> float:
    txt = f"{description} {equipment_desc}".lower()
    if any(x in txt for x in ["inspection", "graiss", "controle", "contrôle"]):
        return 1.0
    if any(x in txt for x in ["remplac", "changer", "changé"]):
        return 2.0
    if any(x in txt for x in ["moteur", "convoyeur", "rouleau"]):
        return 3.0
    return 2.0


def current_session_user(session) -> User | None:
    user_id = st.session_state.get("user_id")
    if not user_id:
        return None
    return session.get(User, user_id)


def login_required() -> bool:
    return "user_id" in st.session_state


def maybe_transition_statuses(session, org_id: int | None = None):
    q = select(Planning)
    if org_id:
        q = q.where(Planning.organization_id == org_id)
    plannings = session.scalars(q).all()
    now = datetime.now(timezone.utc)
    changed = False
    for p in plannings:
        if p.csv_bytes is not None and now >= p.start_at:
            p.csv_bytes = None
            p.csv_filename = None
            changed = True
        if p.status == "validated" and now >= p.start_at:
            p.status = "rex_pending"
            changed = True
    if changed:
        session.commit()


def get_status_label(status: str, lang: str) -> str:
    return STATUS_LABELS.get(status, {}).get(lang, status)


def load_mapping(session, org_id: int) -> FieldMapping:
    mapping = session.scalar(select(FieldMapping).where(FieldMapping.organization_id == org_id))
    if not mapping:
        org = session.get(Organization, org_id)
        ensure_org_defaults(session, org)
        mapping = session.scalar(select(FieldMapping).where(FieldMapping.organization_id == org_id))
    return mapping


def get_active_causes(session, org_id: int):
    return session.scalars(select(RexCause).where(RexCause.organization_id == org_id, RexCause.active == True)).all()


def cause_label(cause: RexCause, lang: str) -> str:
    return {
        "fr": cause.label_fr,
        "en": cause.label_en,
        "nl": cause.label_nl,
    }.get(lang, cause.label_fr)


def make_default_planning_name(sectors: list[str], start_at: datetime) -> str:
    sector_txt = ", ".join(sectors[:2]) if sectors else "Planning"
    return f"{sector_txt} - {start_at.strftime('%d-%m-%Y')}"


def cleanup_session_planning_state():
    for k in [
        "wizard_csv_df", "wizard_mapping_preview", "wizard_selected_sectors", "wizard_selected_statuses",
        "wizard_selected_ateliers", "wizard_team_df", "wizard_tasks_df", "wizard_manual_df", "wizard_planning_id"
    ]:
        st.session_state.pop(k, None)


def build_task_rows_from_csv(df: pd.DataFrame, mapping: FieldMapping, org_planning_id: int, session, org_id: int) -> pd.DataFrame:
    cols = {field: getattr(mapping, field) for field in MAP_FIELDS.keys()}

    def gc(field: str, default=""):
        col = cols.get(field)
        if col and col in df.columns:
            return df[col]
        return pd.Series([default] * len(df))

    out = pd.DataFrame({
        "external_ot_id": gc("ot_id_col", "").astype(str),
        "description": gc("description_col", "").astype(str),
        "source_status": gc("status_col", "").astype(str),
        "atelier": gc("atelier_col", "").astype(str),
        "secteur": gc("secteur_col", "").astype(str),
        "equipment_code": gc("equipment_col", "").astype(str),
        "equipment_desc": gc("equipment_desc_col", "").astype(str),
        "created_at_source": gc("created_at_col", "").astype(str),
        "created_by_source": gc("created_by_col", "").astype(str),
        "requested_week_source": gc("requested_week_col", "").astype(str),
        "condition_source": gc("condition_col", "").astype(str),
    })

    est_col = cols.get("estimated_hours_col")
    if est_col and est_col in df.columns:
        out["estimated_hours"] = pd.to_numeric(df[est_col], errors="coerce")
    else:
        out["estimated_hours"] = pd.NA

    out["estimated_hours"] = out.apply(
        lambda r: float(r["estimated_hours"]) if pd.notna(r["estimated_hours"]) and float(r["estimated_hours"]) > 0 else guess_hours(r["description"], r["equipment_desc"]),
        axis=1,
    )

    # historical non-realized warning
    prev_unfinished = set(
        session.scalars(
            select(PlanningTask.external_ot_id)
            .join(Planning, Planning.id == PlanningTask.planning_id)
            .where(
                Planning.organization_id == org_id,
                PlanningTask.rex_done == False,
            )
        ).all()
    )
    out["selected_warning"] = out["external_ot_id"].apply(
        lambda x: tr(get_lang(), "warning_unfinished_history") if str(x) in prev_unfinished else ""
    )
    out["priority_score"] = out["selected_warning"].apply(lambda x: 1000 if str(x).strip() else 0)
    out["planning_id"] = org_planning_id
    out["task_type"] = "ot"
    out["selected"] = False
    out["predecessor_ot_id"] = ""
    out["forced_team_codes"] = ""
    out["forced_start_at"] = ""
    return out


def persist_tasks_and_teams(session, planning: Planning, tasks_df: pd.DataFrame, teams_df: pd.DataFrame, csv_bytes: bytes | None = None, csv_filename: str | None = None, set_validated: bool = False):
    planning.tasks.clear()
    planning.teams.clear()

    if csv_bytes is not None:
        planning.csv_bytes = csv_bytes
        planning.csv_filename = csv_filename

    for _, row in teams_df.iterrows():
        planning.teams.append(
            PlanningTeam(
                atelier=str(row["atelier"]),
                code=str(row["code"]),
                name=str(row["name"]),
                available_from=pd.Timestamp(row["available_from"]).to_pydatetime(),
                available_to=pd.Timestamp(row["available_to"]).to_pydatetime(),
            )
        )

    for _, row in tasks_df.iterrows():
        planning.tasks.append(
            PlanningTask(
                external_ot_id=str(row["external_ot_id"]),
                task_type=str(row.get("task_type", "ot")),
                description=str(row.get("description", "")),
                equipment_code=str(row.get("equipment_code", "")),
                equipment_desc=str(row.get("equipment_desc", "")),
                atelier=str(row.get("atelier", "")),
                secteur=str(row.get("secteur", "")),
                source_status=str(row.get("source_status", "")),
                created_at_source=str(row.get("created_at_source", "")),
                created_by_source=str(row.get("created_by_source", "")),
                requested_week_source=str(row.get("requested_week_source", "")),
                condition_source=str(row.get("condition_source", "")),
                estimated_hours=float(row.get("estimated_hours", 0.0) or 0.0),
                selected=bool(row.get("selected", False)),
                selected_warning=str(row.get("selected_warning", "")),
                predecessor_ot_id=str(row.get("predecessor_ot_id", "") or ""),
                forced_team_codes=str(row.get("forced_team_codes", "") or ""),
                forced_start_at=parse_dt(row.get("forced_start_at"), None).to_pydatetime() if parse_dt(row.get("forced_start_at"), None) is not None else None,
                operation_mode=str(row.get("operation_mode", "") or "") if str(row.get("task_type", "ot")) == "manual" else None,
                free_start_at=parse_dt(row.get("free_start_at"), None).to_pydatetime() if parse_dt(row.get("free_start_at"), None) is not None else None,
                free_end_at=parse_dt(row.get("free_end_at"), None).to_pydatetime() if parse_dt(row.get("free_end_at"), None) is not None else None,
                planned_start_at=parse_dt(row.get("planned_start_at"), None).to_pydatetime() if parse_dt(row.get("planned_start_at"), None) is not None else None,
                planned_end_at=parse_dt(row.get("planned_end_at"), None).to_pydatetime() if parse_dt(row.get("planned_end_at"), None) is not None else None,
                planned_team_name=str(row.get("planned_team_name", "") or "") or None,
                plan_locked=bool(row.get("planned_start_at") is not None),
            )
        )
    if set_validated:
        planning.status = "validated"
    session.add(planning)
    session.commit()


def parse_dt(value, fallback=None):
    if value is None or str(value).strip() == "":
        return pd.Timestamp(fallback) if fallback is not None else None
    try:
        return pd.Timestamp(value)
    except Exception:
        return pd.Timestamp(fallback) if fallback is not None else None


def build_print_html(planning: Planning, lang: str) -> str:
    tasks = [t for t in planning.tasks if t.selected and t.planned_start_at]
    if not tasks:
        return "<html><body><h3>No planning</h3></body></html>"
    rows = []
    for t in sorted(tasks, key=lambda x: x.planned_start_at):
        rows.append(
            f"<tr><td>{escape(t.external_ot_id)}</td><td>{escape(t.description)}</td><td>{escape(t.atelier)}</td>"
            f"<td>{escape(t.planned_team_name or '')}</td><td>{t.estimated_hours:.1f}</td>"
            f"<td>{t.planned_start_at.strftime('%d-%m-%Y %H:%M')}</td><td>{t.planned_end_at.strftime('%d-%m-%Y %H:%M')}</td></tr>"
        )
    meta = (
        f"Organisation: {escape(planning.organization.name)}<br>"
        f"Sectors: {escape(planning.sectors_csv)}<br>"
        f"Start: {planning.start_at.strftime('%d-%m-%Y %H:%M')}<br>"
        f"End: {planning.end_at.strftime('%d-%m-%Y %H:%M')}<br>"
        f"Day: {escape(planning.daily_open)} - {escape(planning.daily_close)}"
    )
    return f"""
    <html><head><meta charset='utf-8'><title>Planning</title>
    <style>
    body {{ font-family: Arial, sans-serif; margin: 16px; }}
    .title {{ background:{PRIMARY}; color:white; padding:10px 12px; border-radius:10px; font-weight:700; margin-bottom:12px; }}
    .meta {{ background:#FFF8EC; border:1px solid #F3D7A2; padding:10px 12px; border-radius:10px; margin-bottom:12px; }}
    table {{ width:100%; border-collapse:collapse; font-size:11px; }}
    th, td {{ border:1px solid #ccc; padding:6px; }}
    th {{ background:#efefef; }}
    tr:nth-child(even) td {{ background:#f5f5f5; }}
    </style></head><body>
    <div class='title'>{escape(planning.name)}</div>
    <div class='meta'>{meta}</div>
    <table><thead><tr><th>OT</th><th>Description</th><th>Atelier</th><th>Team</th><th>Hours</th><th>Planned start</th><th>Planned end</th></tr></thead><tbody>
    {''.join(rows)}
    </tbody></table></body></html>
    """

# -------------------- Authentication --------------------

def render_login():
    st.markdown(f"<div class='hero'><div class='hero-title'>{T('app_name')}</div><div class='hero-sub'>Login</div></div>", unsafe_allow_html=True)
    with st.form("login_form"):
        email = st.text_input(T("email"))
        password = st.text_input(T("password"), type="password")
        submitted = st.form_submit_button(T("login"), use_container_width=True)
    if submitted:
        with SessionLocal() as session:
            user = session.scalar(select(User).where(User.email == email.strip()))
            if not user or not user.is_active or not user.check_password(password):
                st.error("Invalid credentials")
                return
            st.session_state["user_id"] = user.id
            st.session_state["lang"] = user.language or "fr"
            log_action(session, user.id, user.organization_id, "login", "")
            st.rerun()


def render_force_password_change(user: User):
    st.warning(T("first_login_change"))
    with st.form("force_pw_change"):
        p1 = st.text_input(T("new_password"), type="password")
        p2 = st.text_input(T("confirm_password"), type="password")
        ok = st.form_submit_button(T("change_password"), use_container_width=True)
    if ok:
        if len(p1) < 8:
            st.error("Password too short")
            return False
        if p1 != p2:
            st.error("Passwords do not match")
            return False
        with SessionLocal() as session:
            dbu = session.get(User, user.id)
            dbu.set_password(p1)
            dbu.first_login = False
            session.commit()
        st.success("Password updated")
        st.rerun()
    return True

# -------------------- Admin --------------------

def render_admin(user: User):
    st.subheader(T("admin"))
    with SessionLocal() as session:
        tab1, tab2, tab3 = st.tabs([T("create_org"), T("create_user"), T("audit_log")])
        with tab1:
            with st.form("create_org_form"):
                org_name = st.text_input("Organization name")
                org_tz = st.text_input("Timezone", value="Europe/Paris")
                go = st.form_submit_button(T("create_org"), use_container_width=True)
            if go and org_name.strip():
                org = Organization(name=org_name.strip(), timezone=org_tz.strip() or "Europe/Paris", active=True)
                session.add(org)
                session.commit()
                ensure_org_defaults(session, org)
                log_action(session, user.id, org.id, "create_organization", org.name)
                st.success("Organization created")
                st.rerun()

        with tab2:
            orgs = session.scalars(select(Organization).order_by(Organization.name)).all()
            if not orgs:
                st.info("Create an organization first.")
            else:
                with st.form("create_user_form"):
                    full_name = st.text_input(T("full_name"))
                    email = st.text_input(T("email"), key="new_user_email")
                    password = st.text_input("Temporary password", type="password", value="Temp1234!")
                    org = st.selectbox(T("organization"), options=orgs, format_func=lambda x: x.name)
                    lang = st.selectbox(T("language"), options=["fr", "en", "nl"])
                    role = st.selectbox("Role", options=["user", "admin"])
                    submitted = st.form_submit_button(T("create_user"), use_container_width=True)
                if submitted:
                    new_user = User(
                        email=email.strip(),
                        full_name=full_name.strip(),
                        role=role,
                        organization_id=org.id,
                        language=lang,
                        is_active=True,
                        first_login=True,
                    )
                    new_user.set_password(password)
                    session.add(new_user)
                    session.commit()
                    log_action(session, user.id, org.id, "create_user", email.strip())
                    st.success("User created")
                    st.rerun()

                st.markdown("### Users")
                users = session.scalars(select(User).order_by(User.email)).all()
                rows = []
                for u in users:
                    org_name = u.organization.name if u.organization else "-"
                    rows.append({
                        "id": u.id,
                        "email": u.email,
                        "name": u.full_name,
                        "role": u.role,
                        "organization": org_name,
                        "language": u.language,
                        "active": u.is_active,
                    })
                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True)
                    target_id = st.number_input("User ID", min_value=1, step=1)
                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button(T("deactivate"), key="deactivate_user"):
                            u = session.get(User, int(target_id))
                            if u:
                                u.is_active = False
                                session.commit()
                                log_action(session, user.id, u.organization_id, "deactivate_user", u.email)
                                st.success("User deactivated")
                                st.rerun()
                    with c2:
                        new_lang = st.selectbox("Admin language change", ["fr", "en", "nl"], key="admin_lang_change")
                        if st.button("Change language", key="chg_lang_user"):
                            u = session.get(User, int(target_id))
                            if u:
                                u.language = new_lang
                                session.commit()
                                log_action(session, user.id, u.organization_id, "change_user_language", u.email)
                                st.success("Language updated")
                                st.rerun()

        with tab3:
            logs = session.scalars(select(AuditLog).order_by(AuditLog.created_at.desc()).limit(200)).all()
            if logs:
                st.dataframe(pd.DataFrame([
                    {
                        "when": l.created_at,
                        "action": l.action,
                        "organization_id": l.organization_id,
                        "user_id": l.user_id,
                        "details": l.details,
                    } for l in logs
                ]), use_container_width=True)

# -------------------- Settings --------------------

def render_settings(user: User):
    st.subheader(T("settings"))
    with SessionLocal() as session:
        dbu = session.get(User, user.id)
        c1, c2 = st.columns(2)
        with c1:
            new_lang = st.selectbox(T("language"), ["fr", "en", "nl"], index=["fr", "en", "nl"].index(dbu.language or "fr"))
            if st.button(T("save"), key="save_lang"):
                dbu.language = new_lang
                session.commit()
                st.session_state["lang"] = new_lang
                st.success("Saved")
                st.rerun()
        with c2:
            with st.form("change_pw_form"):
                p1 = st.text_input(T("new_password"), type="password")
                p2 = st.text_input(T("confirm_password"), type="password")
                ok = st.form_submit_button(T("change_password"))
            if ok:
                if len(p1) < 8:
                    st.error("Password too short")
                elif p1 != p2:
                    st.error("Passwords do not match")
                else:
                    dbu.set_password(p1)
                    session.commit()
                    st.success("Password updated")

        if dbu.organization_id:
            st.markdown("### CSV mapping")
            mapping = load_mapping(session, dbu.organization_id)
            org = session.get(Organization, dbu.organization_id)
            ensure_org_defaults(session, org)
            with st.form("mapping_form"):
                vals = {}
                for field, label in MAP_FIELDS.items():
                    vals[field] = st.text_input(label, value=getattr(mapping, field) or "")
                save_map = st.form_submit_button(T("save"), use_container_width=True)
            if save_map:
                for field, value in vals.items():
                    setattr(mapping, field, value.strip() or None)
                session.commit()
                st.success("Mapping saved")

# -------------------- Scheduling --------------------

def render_my_plannings(user: User):
    with SessionLocal() as session:
        maybe_transition_statuses(session, user.organization_id)
        org = session.get(Organization, user.organization_id)
        plannings = session.scalars(
            select(Planning).where(Planning.organization_id == user.organization_id, Planning.archived == False).order_by(Planning.created_at.desc())
        ).all()

        st.markdown("<div class='card'><div class='section-title'>Scheduling</div></div>", unsafe_allow_html=True)
        c1, c2 = st.columns([1,1])
        with c1:
            if st.button(T("create_new"), use_container_width=True):
                cleanup_session_planning_state()
                st.session_state["page"] = "create_planning"
                st.rerun()
        with c2:
            if st.button(T("dashboard"), use_container_width=True):
                st.session_state["page"] = "dashboard"
                st.rerun()

        q = st.text_input(T("search"))
        sectors_filter = st.text_input(T("filter_sector"))
        rows = []
        for p in plannings:
            if q and q.lower() not in (p.name or "").lower():
                continue
            if sectors_filter and sectors_filter.lower() not in (p.sectors_csv or "").lower():
                continue
            creator = session.get(User, p.created_by_user_id)
            rows.append({
                "id": p.id,
                "name": p.name,
                "sectors": p.sectors_csv,
                "start": p.start_at,
                "end": p.end_at,
                "status": get_status_label(p.status, get_lang()),
                "creator": creator.full_name if creator else "-",
            })
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
            chosen_id = st.number_input("Planning ID", min_value=1, step=1)
            c3, c4, c5 = st.columns(3)
            with c3:
                if st.button("Open planning", use_container_width=True):
                    st.session_state["open_planning_id"] = int(chosen_id)
                    st.session_state["page"] = "planning_detail"
                    st.rerun()
            with c4:
                if st.button("Archive", use_container_width=True):
                    p = session.get(Planning, int(chosen_id))
                    if p and p.organization_id == user.organization_id:
                        p.archived = True
                        session.commit()
                        st.success("Archived")
                        st.rerun()
            with c5:
                if user.role == "admin" and st.button("Delete", use_container_width=True):
                    p = session.get(Planning, int(chosen_id))
                    if p:
                        session.delete(p)
                        session.commit()
                        st.success("Deleted")
                        st.rerun()
        else:
            st.info("No planning yet.")


def render_create_planning(user: User):
    with SessionLocal() as session:
        org = session.get(Organization, user.organization_id)
        mapping = load_mapping(session, user.organization_id)
        causes = get_active_causes(session, user.organization_id)
        st.markdown("<div class='card'><div class='section-title'>Create planning</div></div>", unsafe_allow_html=True)
        step = st.radio("Step", [1,2,3,4,5,6], horizontal=True, format_func=lambda x: {
            1: "1 Parameters", 2: "2 Teams", 3: "3 Import OT", 4: "4 Select OT", 5: "5 Constraints", 6: "6 Generate"
        }[x])

        if "wizard_planning_id" not in st.session_state:
            st.session_state["wizard_planning_id"] = None

        if step == 1:
            with st.form("planning_params"):
                sectors_txt = st.text_input(T("sectors"), value=", ".join(st.session_state.get("wizard_selected_sectors", [])))
                start_at = st.datetime_input(T("start"), value=datetime.now())
                end_at = st.datetime_input(T("end"), value=datetime.now())
                daily_open = st.time_input(T("daily_open"), value=datetime.strptime("07:00", "%H:%M").time())
                daily_close = st.time_input(T("daily_close"), value=datetime.strptime("15:00", "%H:%M").time())
                default_name = make_default_planning_name([x.strip() for x in sectors_txt.split(",") if x.strip()], pd.Timestamp(start_at).to_pydatetime())
                planning_name = st.text_input(T("planning_name"), value=default_name)
                ok = st.form_submit_button(T("save_draft"), use_container_width=True)
            if ok:
                if end_at <= start_at:
                    st.error("End must be after start")
                else:
                    planning = Planning(
                        organization_id=user.organization_id,
                        created_by_user_id=user.id,
                        name=planning_name.strip(),
                        sectors_csv=", ".join([x.strip() for x in sectors_txt.split(",") if x.strip()]),
                        start_at=pd.Timestamp(start_at).to_pydatetime(),
                        end_at=pd.Timestamp(end_at).to_pydatetime(),
                        daily_open=daily_open.strftime("%H:%M"),
                        daily_close=daily_close.strftime("%H:%M"),
                        status="draft",
                    )
                    session.add(planning)
                    session.commit()
                    st.session_state["wizard_planning_id"] = planning.id
                    st.session_state["wizard_selected_sectors"] = [x.strip() for x in sectors_txt.split(",") if x.strip()]
                    st.success("Draft saved")

        elif step == 2:
            if not st.session_state.get("wizard_planning_id"):
                st.info("Save parameters first.")
                return
            uploaded = st.file_uploader(T("upload_csv"), type=["csv"])
            if uploaded is not None:
                df = parse_csv(uploaded)
                st.session_state["wizard_csv_df"] = df.copy()
                st.session_state["wizard_csv_name"] = uploaded.name
                st.session_state["wizard_csv_bytes"] = uploaded.getvalue()
                st.success(f"CSV loaded: {len(df):,} rows")
                st.dataframe(df.head(20), use_container_width=True)
                st.markdown("### Mapping preview")
                map_preview = {}
                cols = list(df.columns)
                for field, label in MAP_FIELDS.items():
                    current = getattr(mapping, field) or (cols[0] if cols else "")
                    idx = cols.index(current) if current in cols else 0
                    map_preview[field] = st.selectbox(label, cols, index=idx, key=f"map_{field}") if cols else ""
                if st.button(T("save"), key="save_mapping_in_wizard"):
                    for field, value in map_preview.items():
                        setattr(mapping, field, value)
                    session.commit()
                    st.success("Mapping updated")

        elif step == 3:
            if not st.session_state.get("wizard_planning_id") or "wizard_csv_df" not in st.session_state:
                st.info("Save parameters and upload a CSV first.")
                return
            planning = session.get(Planning, st.session_state["wizard_planning_id"])
            df = st.session_state["wizard_csv_df"].copy()
            tasks_df = build_task_rows_from_csv(df, mapping, planning.id, session, user.organization_id)
            sectors = sorted([x for x in tasks_df["secteur"].dropna().astype(str).unique().tolist() if x])
            statuses = sorted([x for x in tasks_df["source_status"].dropna().astype(str).unique().tolist() if x])
            ateliers = sorted([x for x in tasks_df["atelier"].dropna().astype(str).unique().tolist() if x])
            selected_sectors = st.multiselect(T("sectors"), sectors, default=st.session_state.get("wizard_selected_sectors", sectors))
            selected_statuses = st.multiselect(T("status"), statuses, default=statuses)
            selected_ateliers = st.multiselect("Ateliers", ateliers, default=ateliers)
            filtered = tasks_df.copy()
            if selected_sectors:
                filtered = filtered[filtered["secteur"].astype(str).isin(selected_sectors)]
            if selected_statuses:
                filtered = filtered[filtered["source_status"].astype(str).isin(selected_statuses)]
            if selected_ateliers:
                filtered = filtered[filtered["atelier"].astype(str).isin(selected_ateliers)]
            st.session_state["wizard_selected_statuses"] = selected_statuses
            st.session_state["wizard_selected_ateliers"] = selected_ateliers
            st.session_state["wizard_tasks_df"] = filtered.copy()
            st.dataframe(filtered.head(50), use_container_width=True)
            if st.button("Create team table", use_container_width=True):
                teams = []
                for at in selected_ateliers:
                    teams.append({
                        "atelier": at,
                        "code": f"{at}-1",
                        "name": f"{at}-1",
                        "available_from": planning.start_at,
                        "available_to": planning.end_at,
                    })
                st.session_state["wizard_team_df"] = pd.DataFrame(teams)
                st.success("Teams initialized")

        elif step == 4:
            if "wizard_team_df" not in st.session_state or "wizard_tasks_df" not in st.session_state:
                st.info("Prepare tasks and teams first.")
                return
            team_df = st.session_state["wizard_team_df"]
            edited_teams = st.data_editor(
                team_df,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                key="teams_editor",
            )
            st.session_state["wizard_team_df"] = edited_teams.copy()

            tasks_df = st.session_state["wizard_tasks_df"].copy()
            tasks_df = tasks_df.sort_values(["selected_warning", "priority_score", "estimated_hours"], ascending=[False, False, True])
            tasks_df["select"] = tasks_df.get("selected", False)
            show = tasks_df[[
                "select", "external_ot_id", "description", "atelier", "secteur", "source_status",
                "estimated_hours", "selected_warning"
            ]].rename(columns={
                "external_ot_id": "OT", "description": "Description", "atelier": "Atelier",
                "secteur": "Secteur", "source_status": "Statut", "estimated_hours": "Hours",
                "selected_warning": "Warning"
            })
            edited = st.data_editor(show, use_container_width=True, hide_index=True, key="tasks_select_editor")
            selected_ot_ids = set(edited[edited["select"] == True]["OT"].astype(str).tolist())
            tasks_df["selected"] = tasks_df["external_ot_id"].astype(str).isin(selected_ot_ids)
            st.session_state["wizard_tasks_df"] = tasks_df.copy()
            flagged = tasks_df[(tasks_df["selected"] == True) & (tasks_df["selected_warning"].astype(str).str.strip() != "")]
            if not flagged.empty:
                st.markdown(f"<div class='warning-box'>{len(flagged)} OT were not completed in previous plannings and are prioritized.</div>", unsafe_allow_html=True)

        elif step == 5:
            if "wizard_tasks_df" not in st.session_state:
                st.info("Select tasks first.")
                return
            tasks_df = st.session_state["wizard_tasks_df"].copy()
            selected = tasks_df[tasks_df["selected"] == True].copy()
            if selected.empty:
                st.info("No selected OT.")
                return

            team_df = st.session_state.get("wizard_team_df", pd.DataFrame())
            team_by_atelier = {}
            if not team_df.empty:
                for at, grp in team_df.groupby("atelier"):
                    team_by_atelier[str(at)] = [(str(r["code"]), str(r["name"])) for _, r in grp.iterrows()]

            selected_ids = selected["external_ot_id"].astype(str).tolist()
            new_rows = []
            st.markdown("### Selected OT constraints")
            for _, r in selected.iterrows():
                ot_id = str(r["external_ot_id"])
                desc = str(r["description"])
                equip = str(r.get("equipment_desc", ""))
                st.markdown(f"#### {ot_id} | {desc[:60]} | {equip[:40]}")
                pred_opts = [""] + [x for x in selected_ids if x != ot_id]
                pred_val = st.selectbox("Predecessor", pred_opts, index=0, key=f"pred_{ot_id}")
                forced_start = st.text_input("Forced start (YYYY-MM-DD HH:MM)", value="", key=f"fs_{ot_id}")
                team_options = team_by_atelier.get(str(r["atelier"]), [])
                team_codes = [c for c, _ in team_options]
                team_map = {c: l for c, l in team_options}
                forced_teams = st.multiselect(
                    "Forced teams",
                    options=team_codes,
                    default=[],
                    format_func=lambda x: team_map.get(x, x),
                    key=f"ft_{ot_id}",
                )
                nr = dict(r)
                nr["predecessor_ot_id"] = pred_val
                nr["forced_start_at"] = forced_start
                nr["forced_team_codes"] = "; ".join(forced_teams)
                new_rows.append(nr)
            unselected = tasks_df[tasks_df["selected"] == False].copy().to_dict("records")
            all_rows = new_rows + unselected
            st.session_state["wizard_tasks_df"] = pd.DataFrame(all_rows)

            st.markdown("### Manual operations")
            manual_df = st.session_state.get("wizard_manual_df", pd.DataFrame(columns=[
                "external_ot_id", "description", "atelier", "estimated_hours", "operation_mode", "free_start_at", "free_end_at", "forced_team_codes"
            ]))
            manual_view = manual_df.copy()
            if manual_view.empty:
                manual_view = pd.DataFrame(columns=[
                    "external_ot_id", "description", "atelier", "estimated_hours", "operation_mode", "free_start_at", "free_end_at", "forced_team_codes"
                ])
            manual_view = st.data_editor(
                manual_view,
                use_container_width=True,
                num_rows="dynamic",
                hide_index=True,
                key="manual_editor",
                column_config={
                    "operation_mode": st.column_config.SelectboxColumn("Mode", options=["start", "end", "free"])
                }
            )
            st.session_state["wizard_manual_df"] = manual_view.copy()

        elif step == 6:
            if "wizard_tasks_df" not in st.session_state or "wizard_team_df" not in st.session_state:
                st.info("Complete previous steps first.")
                return
            planning = session.get(Planning, st.session_state["wizard_planning_id"])
            tasks_df = st.session_state["wizard_tasks_df"].copy()
            team_df = st.session_state["wizard_team_df"].copy()
            manual_df = st.session_state.get("wizard_manual_df", pd.DataFrame())

            # selected tasks + manual tasks
            selected = tasks_df[tasks_df["selected"] == True].copy()
            selected["task_type"] = "ot"
            manual_rows = []
            if manual_df is not None and not manual_df.empty:
                for i, r in manual_df.iterrows():
                    external_ot_id = str(r.get("external_ot_id") or f"MAN-{i+1:03d}")
                    manual_rows.append({
                        "external_ot_id": external_ot_id,
                        "description": str(r.get("description", "")),
                        "atelier": str(r.get("atelier", "")),
                        "secteur": "",
                        "equipment_code": "",
                        "equipment_desc": "",
                        "source_status": "Manual operation",
                        "created_at_source": "",
                        "created_by_source": "",
                        "requested_week_source": "",
                        "condition_source": "",
                        "estimated_hours": float(r.get("estimated_hours", 1.0) or 1.0),
                        "selected": True,
                        "selected_warning": "",
                        "priority_score": 10000,
                        "predecessor_ot_id": "",
                        "forced_team_codes": str(r.get("forced_team_codes", "") or ""),
                        "forced_start_at": r.get("free_start_at", "") if str(r.get("operation_mode", "")) == "free" else "",
                        "task_type": "manual",
                        "operation_mode": str(r.get("operation_mode", "") or "start"),
                        "free_start_at": r.get("free_start_at", ""),
                        "free_end_at": r.get("free_end_at", ""),
                    })
            all_tasks = pd.concat([selected, pd.DataFrame(manual_rows)], ignore_index=True) if manual_rows else selected.copy()
            all_tasks = all_tasks.fillna("")

            # place start/end manual ops
            for idx, row in all_tasks.iterrows():
                if row.get("task_type") == "manual":
                    mode = str(row.get("operation_mode") or "")
                    if mode == "start":
                        all_tasks.at[idx, "forced_start_at"] = planning.start_at
                    elif mode == "end":
                        dur = float(row.get("estimated_hours") or 0.0)
                        all_tasks.at[idx, "forced_start_at"] = planning.end_at - pd.Timedelta(hours=dur)

            teams = team_df.to_dict("records")
            tasks = []
            for _, r in all_tasks.iterrows():
                tasks.append({
                    "external_ot_id": str(r.get("external_ot_id", "")),
                    "description": str(r.get("description", "")),
                    "equipment_code": str(r.get("equipment_code", "")),
                    "equipment_desc": str(r.get("equipment_desc", "")),
                    "atelier": str(r.get("atelier", "")),
                    "secteur": str(r.get("secteur", "")),
                    "source_status": str(r.get("source_status", "")),
                    "created_at_source": str(r.get("created_at_source", "")),
                    "created_by_source": str(r.get("created_by_source", "")),
                    "requested_week_source": str(r.get("requested_week_source", "")),
                    "condition_source": str(r.get("condition_source", "")),
                    "estimated_hours": float(r.get("estimated_hours", 0.0) or 0.0),
                    "selected_warning": str(r.get("selected_warning", "")),
                    "priority_score": float(r.get("priority_score", 0.0) or 0.0),
                    "predecessor_ot_id": str(r.get("predecessor_ot_id", "") or ""),
                    "forced_team_codes": str(r.get("forced_team_codes", "") or ""),
                    "forced_start_at": parse_dt(r.get("forced_start_at"), None),
                })

            planning_rows, unscheduled_rows, fits_window = build_schedule(tasks, teams, planning.start_at, planning.end_at)
            if not fits_window:
                st.error("The planning exceeds the selected shutdown window. Please adjust the load or teams.")
                return

            if unscheduled_rows:
                st.warning(f"{len(unscheduled_rows)} task(s) could not be scheduled.")
                st.dataframe(pd.DataFrame(unscheduled_rows), use_container_width=True)

            if planning_rows:
                out_df = pd.DataFrame(planning_rows)
                st.dataframe(out_df[[
                    "external_ot_id", "description", "atelier", "planned_team_name", "estimated_hours", "planned_start_at", "planned_end_at", "selected_warning"
                ]], use_container_width=True)
                fig = px.timeline(
                    out_df,
                    x_start="planned_start_at",
                    x_end="planned_end_at",
                    y="planned_team_name",
                    color="atelier",
                    hover_data=["external_ot_id", "description", "estimated_hours"],
                    text="external_ot_id",
                )
                fig.update_yaxes(autorange="reversed")
                fig.update_layout(height=max(450, 70 + 35 * out_df["planned_team_name"].nunique()))
                st.plotly_chart(fig, use_container_width=True)

                if st.button(T("generate_plan"), use_container_width=True):
                    csv_bytes = st.session_state.get("wizard_csv_bytes")
                    csv_name = st.session_state.get("wizard_csv_name")
                    persist_tasks_and_teams(session, planning, out_df, team_df, csv_bytes=csv_bytes, csv_filename=csv_name, set_validated=True)
                    log_action(session, user.id, user.organization_id, "generate_planning", planning.name)
                    st.success("Planning saved and validated")
                    cleanup_session_planning_state()
                    st.session_state["page"] = "my_plannings"
                    st.rerun()

# -------------------- Planning detail / REX --------------------

def render_planning_detail(user: User):
    planning_id = st.session_state.get("open_planning_id")
    if not planning_id:
        st.session_state["page"] = "my_plannings"
        st.rerun()

    with SessionLocal() as session:
        maybe_transition_statuses(session, user.organization_id)
        planning = session.get(Planning, planning_id)
        if not planning or planning.organization_id != user.organization_id:
            st.error("Planning not found")
            return

        st.markdown(f"<div class='hero'><div class='hero-title'>{planning.name}</div><div class='hero-sub'>{planning.sectors_csv} | {planning.start_at.strftime('%d-%m-%Y %H:%M')} → {planning.end_at.strftime('%d-%m-%Y %H:%M')}</div></div>", unsafe_allow_html=True)
        st.markdown(f"<span class='pill'>{get_status_label(planning.status, get_lang())}</span>", unsafe_allow_html=True)

        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button(T("my_plannings"), use_container_width=True):
                st.session_state["page"] = "my_plannings"
                st.rerun()
        with c2:
            html = build_print_html(planning, get_lang())
            st.download_button(T("print"), data=html.encode("utf-8"), file_name=f"planning_{planning.id}.html", mime="text/html", use_container_width=True)
        with c3:
            if planning.status == "validated" and datetime.now(timezone.utc) < planning.start_at:
                st.info("Planning editable until start time")

        rows = []
        for t in sorted(planning.tasks, key=lambda x: (x.planned_start_at or planning.start_at)):
            rows.append({
                "OT": t.external_ot_id,
                "Description": t.description,
                "Atelier": t.atelier,
                "Team": t.planned_team_name,
                "Hours": t.estimated_hours,
                "Start": t.planned_start_at,
                "End": t.planned_end_at,
                "Warning": t.selected_warning or "",
            })
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True)

        if planning.status in ["rex_pending", "closed"]:
            st.subheader(T("rex"))
            causes = get_active_causes(session, user.organization_id)
            cause_opts = {str(c.id): cause_label(c, get_lang()) for c in causes}
            with st.form("rex_form"):
                real_start = st.datetime_input("Real shutdown start", value=planning.window_real_start or planning.start_at)
                real_end = st.datetime_input("Real shutdown end", value=planning.window_real_end or planning.end_at)
                rex_rows = []
                for t in [x for x in planning.tasks if x.task_type == "ot" and x.selected]:
                    st.markdown(f"**{t.external_ot_id} | {t.description}**")
                    done = st.selectbox("Status", ["", "done", "not_done"], index=0 if t.rex_done is None else (1 if t.rex_done else 2), key=f"rex_done_{t.id}")
                    cause_sel = ""
                    comment = t.rex_comment or ""
                    if done == "not_done":
                        cause_sel = st.selectbox(T("cause"), options=[""] + list(cause_opts.keys()), format_func=lambda x: cause_opts.get(x, "") if x else "", key=f"cause_{t.id}")
                        comment = st.text_input(T("comment"), value=comment, key=f"comment_{t.id}")
                    rex_rows.append((t.id, done, cause_sel, comment))
                finalize = st.form_submit_button(T("finalize_rex"), use_container_width=True)
            if finalize:
                errors = []
                for task_id, done, cause_sel, comment in rex_rows:
                    if done not in ["done", "not_done"]:
                        errors.append(f"Task {task_id} not filled")
                    if done == "not_done" and not cause_sel:
                        errors.append(f"Cause missing for task {task_id}")
                    if done == "not_done" and cause_sel:
                        cause_obj = next((c for c in causes if str(c.id) == cause_sel), None)
                        if cause_obj and cause_label(cause_obj, "fr") == "Autre" and not str(comment).strip():
                            errors.append(f"Comment required for 'Other' on task {task_id}")
                if errors:
                    st.error(" | ".join(errors))
                else:
                    planning.window_real_start = pd.Timestamp(real_start).to_pydatetime()
                    planning.window_real_end = pd.Timestamp(real_end).to_pydatetime()
                    for task_id, done, cause_sel, comment in rex_rows:
                        task = session.get(PlanningTask, task_id)
                        task.rex_done = True if done == "done" else False
                        task.rex_cause_id = int(cause_sel) if cause_sel else None
                        task.rex_comment = comment if comment else None
                    planning.status = "closed"
                    session.commit()
                    log_action(session, user.id, user.organization_id, "finalize_rex", planning.name)
                    st.success("REX finalized")
                    st.rerun()

# -------------------- Dashboard --------------------

def render_dashboard(user: User):
    with SessionLocal() as session:
        maybe_transition_statuses(session, user.organization_id)
        st.subheader(T("dashboard"))
        plannings = session.scalars(select(Planning).where(Planning.organization_id == user.organization_id, Planning.archived == False)).all()
        if not plannings:
            st.info("No data")
            return

        all_rows = []
        for p in plannings:
            for t in p.tasks:
                if t.task_type != "ot" or not t.selected:
                    continue
                cause = session.get(RexCause, t.rex_cause_id) if t.rex_cause_id else None
                all_rows.append({
                    "planning_id": p.id,
                    "planning_name": p.name,
                    "sector": p.sectors_csv,
                    "planning_start": p.start_at,
                    "week": p.start_at.isocalendar()[1],
                    "month": p.start_at.month,
                    "status": p.status,
                    "rex_done": t.rex_done,
                    "cause": cause_label(cause, get_lang()) if cause else "",
                    "window_respected": (p.window_real_end <= p.end_at) if p.window_real_end else None,
                })
        if not all_rows:
            st.info("No OT data")
            return
        df = pd.DataFrame(all_rows)

        sector_filter = st.text_input(T("filter_sector"))
        time_filter = st.selectbox(T("filter_time"), [T("week"), T("month"), T("custom")])
        if sector_filter:
            df = df[df["sector"].astype(str).str.contains(sector_filter, case=False, na=False)]

        if time_filter == T("week"):
            current_week = datetime.now().isocalendar()[1]
            df = df[df["week"] == current_week]
        elif time_filter == T("month"):
            current_month = datetime.now().month
            df = df[df["month"] == current_month]
        else:
            min_d = st.date_input("Start date", value=df["planning_start"].min().date())
            max_d = st.date_input("End date", value=df["planning_start"].max().date())
            df = df[(df["planning_start"].dt.date >= min_d) & (df["planning_start"].dt.date <= max_d)]

        total_ot = len(df)
        realized = int((df["rex_done"] == True).sum())
        adherence = round((realized / total_ot * 100), 1) if total_ot else 0.0
        window_rate = round((df.dropna(subset=["window_respected"])["window_respected"] == True).mean() * 100, 1) if not df.dropna(subset=["window_respected"]).empty else 0.0

        c1, c2, c3 = st.columns(3)
        c1.metric("OT planned", total_ot)
        c2.metric(T("realized_rate"), f"{adherence}%")
        c3.metric(T("window_respect"), f"{window_rate}%")

        by_week = df.groupby("week", as_index=False).agg(total=("planning_id", "count"), realized=("rex_done", lambda s: int((s == True).sum())))
        by_week["adherence_pct"] = by_week.apply(lambda r: round(r["realized"] / r["total"] * 100, 1) if r["total"] else 0, axis=1)
        fig1 = px.line(by_week, x="week", y="adherence_pct", markers=True, title="Weekly adherence trend")
        st.plotly_chart(fig1, use_container_width=True)

        causes_df = df[df["rex_done"] == False].copy()
        if not causes_df.empty:
            fig2 = px.bar(causes_df.groupby("cause", as_index=False).size(), x="cause", y="size", title="Non-realized causes")
            st.plotly_chart(fig2, use_container_width=True)

# -------------------- Shell --------------------

def render_app():
    with SessionLocal() as session:
        user = current_session_user(session)
        if not user:
            render_login()
            return
        if not user.is_active:
            st.error("Account disabled")
            return
        st.session_state["lang"] = user.language or "fr"

        maybe_transition_statuses(session, user.organization_id)

        st.sidebar.markdown(f"### {T('app_name')}")
        st.sidebar.write(user.full_name)
        st.sidebar.write(user.email)
        if user.organization:
            st.sidebar.write(user.organization.name)
        if st.sidebar.button(T("logout"), use_container_width=True):
            st.session_state.clear()
            st.rerun()

        if user.first_login:
            render_force_password_change(user)
            return

        page = st.session_state.get("page", "home")
        if user.role == "admin":
            nav = st.sidebar.radio("Navigation", ["home", "scheduling", "settings", "admin"], format_func=lambda x: {
                "home": T("home"),
                "scheduling": T("scheduling"),
                "settings": T("settings"),
                "admin": T("admin"),
            }[x], index=["home", "scheduling", "settings", "admin"].index(page if page in ["home","scheduling","settings","admin"] else "home"))
        else:
            nav = st.sidebar.radio("Navigation", ["home", "scheduling", "settings"], format_func=lambda x: {
                "home": T("home"),
                "scheduling": T("scheduling"),
                "settings": T("settings"),
            }[x], index=["home", "scheduling", "settings"].index(page if page in ["home","scheduling","settings"] else "home"))
        if nav != page and page not in ["my_plannings", "create_planning", "planning_detail", "dashboard"]:
            st.session_state["page"] = nav
            page = nav

        if page == "home":
            st.markdown(f"<div class='hero'><div class='hero-title'>{T('app_name')}</div><div class='hero-sub'>Corporate modern planning portal</div></div>", unsafe_allow_html=True)
            st.markdown("<div class='planning-btn'>", unsafe_allow_html=True)
            if st.button("📅 Scheduling", use_container_width=True):
                st.session_state["page"] = "my_plannings"
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)
        elif page in ["scheduling", "my_plannings"]:
            st.session_state["page"] = "my_plannings"
            render_my_plannings(user)
        elif page == "create_planning":
            render_create_planning(user)
        elif page == "planning_detail":
            render_planning_detail(user)
        elif page == "dashboard":
            render_dashboard(user)
        elif page == "settings":
            render_settings(user)
        elif page == "admin" and user.role == "admin":
            render_admin(user)
        else:
            st.session_state["page"] = "home"
            st.rerun()


if __name__ == "__main__":
    render_app()
