"""Cockpit — hub de pilotage des routines de maintenance."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import streamlit as st
from sqlalchemy import select

from maintenance_toolbox.db import (
    Action, MeetingInstance, MeetingSession, MeetingType,
)
from maintenance_toolbox.meetings.common import effective_status, _to_utc, FREQ_LABELS


def render_home(user, db_session) -> None:
    org_id = user.organization_id

    # Load meeting types
    all_types = db_session.scalars(
        select(MeetingType).order_by(MeetingType.order_index)
    ).all()
    active_types = [mt for mt in all_types if mt.active]
    inactive_types = [mt for mt in all_types if not mt.active]

    # Header
    st.markdown(
        f"""<div style="margin-bottom:24px;">
        <h1 style="color:#3f434f;margin-bottom:4px;">🏭 MaintenanceHub</h1>
        <div style="color:#888;font-size:1rem;">Pilotage des routines de maintenance · Bonjour, <strong>{user.full_name}</strong></div>
        </div>""",
        unsafe_allow_html=True,
    )

    # Global KPI strip
    _render_kpi_strip(db_session, org_id)

    st.divider()

    # ── Active meetings ─────────────────────────────────────
    st.markdown(
        """<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;">
        <div style="width:5px;height:28px;background:#f39200;border-radius:3px;"></div>
        <h3 style="margin:0;color:#3f434f;">Réunions actives</h3>
        </div>""",
        unsafe_allow_html=True,
    )

    if active_types:
        cols = st.columns(len(active_types))
        for col, mt in zip(cols, active_types):
            with col:
                _render_active_card(db_session, mt, org_id)
    else:
        st.info("Aucun type de réunion actif configuré.")

    st.divider()

    # ── Inactive meetings ────────────────────────────────────
    st.markdown(
        """<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;">
        <div style="width:5px;height:28px;background:#aaa;border-radius:3px;"></div>
        <h3 style="margin:0;color:#888;">En cours de déploiement</h3>
        </div>""",
        unsafe_allow_html=True,
    )

    if inactive_types:
        # Show up to 5 per row
        cols = st.columns(min(len(inactive_types), 5))
        for col, mt in zip(cols, inactive_types):
            with col:
                _render_inactive_card(mt)


# ─────────────────────────────────────────────────────────
#  KPI STRIP
# ─────────────────────────────────────────────────────────

def _render_kpi_strip(db_session, org_id: int) -> None:
    all_instances = db_session.scalars(
        select(MeetingInstance).where(MeetingInstance.organization_id == org_id)
    ).all()
    all_sessions = db_session.scalars(
        select(MeetingSession).join(MeetingInstance).where(
            MeetingInstance.organization_id == org_id
        )
    ).all()
    all_actions = db_session.scalars(
        select(Action).where(Action.organization_id == org_id)
    ).all()

    closed_sessions = [s for s in all_sessions if s.status == "closed"]
    tenue = (
        round(len(closed_sessions) / len(all_instances) * 100)
        if all_instances else 0
    )

    open_actions = sum(1 for a in all_actions if effective_status(a) in ("Open", "In Progress"))
    late_actions = sum(1 for a in all_actions if effective_status(a) == "Late")
    done_actions = sum(1 for a in all_actions if effective_status(a) == "Done")
    total_actions = len(all_actions)
    closure_rate = round(done_actions / total_actions * 100) if total_actions else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("📅 Réunions", len(all_instances), help="Instances créées au total")
    with c2:
        tenue_delta = f"{tenue}% tenue" if tenue else None
        st.metric("✅ Sessions clôturées", len(closed_sessions))
    with c3:
        st.metric("🎯 Taux de tenue", f"{tenue}%")
    with c4:
        st.metric("⚡ Actions ouvertes", open_actions, delta=f"-{late_actions} en retard" if late_actions else None, delta_color="inverse")
    with c5:
        st.metric("📊 Taux de clôture", f"{closure_rate}%")


# ─────────────────────────────────────────────────────────
#  ACTIVE CARD
# ─────────────────────────────────────────────────────────

def _render_active_card(db_session, mt: MeetingType, org_id: int) -> None:
    instances = db_session.scalars(
        select(MeetingInstance).where(
            MeetingInstance.meeting_type_id == mt.id,
            MeetingInstance.organization_id == org_id,
        ).order_by(MeetingInstance.scheduled_date.desc())
    ).all()

    last_session_date = "—"
    open_actions = 0
    session_status_label = ""

    if instances:
        last_inst = instances[0]
        sessions = db_session.scalars(
            select(MeetingSession).where(MeetingSession.instance_id == last_inst.id)
            .order_by(MeetingSession.created_at.desc())
        ).all()
        if sessions:
            ls = sessions[0]
            d = _to_utc(ls.created_at)
            last_session_date = d.strftime("%d/%m/%Y") if d else "—"
            status_map = {
                "draft": "🟡 En attente",
                "ongoing": "🟢 En cours",
                "closed": "✅ Clôturée",
            }
            session_status_label = status_map.get(ls.status, "")
            open_actions = sum(
                1 for a in ls.actions if effective_status(a) in ("Open", "In Progress", "Late")
            )

    freq = FREQ_LABELS.get(mt.frequency, mt.frequency)

    with st.container(border=True):
        st.markdown(
            f"""<div style="text-align:center;padding:8px 0;">
            <div style="font-size:2rem;">{mt.icon}</div>
            <div style="font-weight:700;font-size:1.05rem;color:#3f434f;">{mt.name}</div>
            <div style="color:#888;font-size:0.82rem;">{freq} · {mt.duration_minutes} min</div>
            </div>""",
            unsafe_allow_html=True,
        )

        c1, c2 = st.columns(2)
        with c1:
            st.metric("Instances", len(instances))
        with c2:
            st.metric("Actions ouvertes", open_actions)

        if session_status_label:
            st.caption(f"Dernière session : {last_session_date} · {session_status_label}")
        else:
            st.caption("Aucune session")

        if st.button(
            f"Ouvrir →",
            key=f"home_open_{mt.id}",
            use_container_width=True,
            type="primary",
        ):
            st.session_state["hub_meeting_type_id"] = mt.id
            st.session_state["hub_view"] = "list"
            st.session_state.pop("hub_instance_id", None)
            st.session_state.pop("hub_session_id", None)
            st.session_state["page"] = "meeting_hub"
            st.rerun()


# ─────────────────────────────────────────────────────────
#  INACTIVE CARD
# ─────────────────────────────────────────────────────────

def _render_inactive_card(mt: MeetingType) -> None:
    freq = FREQ_LABELS.get(mt.frequency, mt.frequency)
    agenda = json.loads(mt.agenda_json or "[]")

    with st.container(border=True):
        st.markdown(
            f"""<div style="text-align:center;padding:8px 0;opacity:0.55;">
            <div style="font-size:1.6rem;filter:grayscale(100%);">{mt.icon}</div>
            <div style="font-weight:600;color:#888;font-size:0.95rem;">{mt.name}</div>
            <div style="color:#aaa;font-size:0.78rem;">{freq} · {mt.duration_minutes} min</div>
            </div>""",
            unsafe_allow_html=True,
        )
        with st.expander("Voir l'agenda type", expanded=False):
            for item in agenda:
                st.caption(f"• {item}")
        st.markdown(
            '<div style="text-align:center;"><span style="background:#e9e9e9;color:#888;'
            'padding:3px 10px;border-radius:12px;font-size:0.75rem;">🔒 Déploiement à venir</span></div>',
            unsafe_allow_html=True,
        )
