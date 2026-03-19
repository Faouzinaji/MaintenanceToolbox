"""Meeting Hub — instance selection, session management and dispatch."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta

import streamlit as st
from sqlalchemy import select

from maintenance_toolbox.db import (
    MeetingType, MeetingInstance, MeetingSession, Action,
)
from maintenance_toolbox.meetings.common import (
    render_timer, render_checkin, render_actions_panel, render_closure,
    FREQ_LABELS, STATUS_ICONS, effective_status, _to_utc,
)


# ─────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────

def render_meeting_hub(db_session, user) -> None:
    mt_id = st.session_state.get("hub_meeting_type_id")
    if not mt_id:
        st.error("Aucun type de réunion sélectionné.")
        return

    meeting_type = db_session.get(MeetingType, mt_id)
    if not meeting_type:
        st.error("Type de réunion introuvable.")
        return

    hub_view = st.session_state.get("hub_view", "list")

    # Back button
    col_back, col_title = st.columns([1, 8])
    with col_back:
        if st.button("← Cockpit", key="hub_back"):
            _clear_hub_state()
            st.session_state["page"] = "home"
            st.rerun()
    with col_title:
        st.markdown(
            f"<h2 style='margin:0;color:#3f434f;'>{meeting_type.icon} {meeting_type.name}</h2>",
            unsafe_allow_html=True,
        )

    # Meeting type info banner
    freq = FREQ_LABELS.get(meeting_type.frequency, meeting_type.frequency)
    agenda = json.loads(meeting_type.agenda_json or "[]")
    with st.container(border=True):
        c1, c2, c3 = st.columns([2, 2, 5])
        with c1:
            st.metric("Fréquence", freq)
        with c2:
            st.metric("Durée théorique", f"{meeting_type.duration_minutes} min")
        with c3:
            if agenda:
                st.markdown("**Agenda type :**")
                for item in agenda:
                    st.markdown(f"• {item}")

    st.divider()

    if hub_view == "list":
        _render_instance_list(db_session, user, meeting_type)
    elif hub_view == "session":
        _render_session_view(db_session, user, meeting_type)


# ─────────────────────────────────────────────────────────
#  INSTANCE LIST
# ─────────────────────────────────────────────────────────

def _render_instance_list(db_session, user, meeting_type: MeetingType) -> None:
    org_id = user.organization_id

    instances = db_session.scalars(
        select(MeetingInstance)
        .where(
            MeetingInstance.meeting_type_id == meeting_type.id,
            MeetingInstance.organization_id == org_id,
        )
        .order_by(MeetingInstance.scheduled_date.desc())
    ).all()

    # Admin: create new instance
    if user.role == "admin":
        with st.expander("➕ Créer une nouvelle instance", expanded=(not instances)):
            _render_create_instance_form(db_session, user, meeting_type, org_id)

    st.markdown(f"### Instances de réunion ({len(instances)})")

    if not instances:
        st.info("Aucune instance créée. Un administrateur peut en créer une ci-dessus.")
        return

    for inst in instances:
        _render_instance_card(db_session, inst, meeting_type)


def _render_create_instance_form(db_session, user, meeting_type: MeetingType, org_id: int) -> None:
    now = datetime.now(timezone.utc)
    week_num = now.isocalendar()[1]
    default_name = f"{meeting_type.name} S{week_num}"

    with st.form(f"create_instance_{meeting_type.id}"):
        name = st.text_input("Nom de l'instance", value=default_name)
        scheduled_date = st.date_input("Date planifiée", value=now.date())
        participants_raw = st.text_area(
            "Participants (un par ligne)",
            placeholder="Pierre Dupont\nMarie Lambert\n...",
            height=120,
        )
        submitted = st.form_submit_button("Créer l'instance", use_container_width=True)

    if submitted:
        if not name.strip():
            st.error("Le nom est obligatoire.")
            return
        participants = [p.strip() for p in participants_raw.splitlines() if p.strip()]
        sched_dt = datetime.combine(scheduled_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        inst = MeetingInstance(
            meeting_type_id=meeting_type.id,
            organization_id=org_id,
            name=name.strip(),
            scheduled_date=sched_dt,
            participants_json=json.dumps(participants),
            created_by_user_id=user.id,
        )
        db_session.add(inst)
        db_session.commit()
        st.success(f"Instance « {name} » créée.")
        st.rerun()


def _render_instance_card(db_session, inst: MeetingInstance, meeting_type: MeetingType) -> None:
    sessions = db_session.scalars(
        select(MeetingSession).where(MeetingSession.instance_id == inst.id)
        .order_by(MeetingSession.created_at.desc())
    ).all()

    last_session = sessions[0] if sessions else None
    session_status = last_session.status if last_session else "none"

    status_badge = {
        "none": ("⬜ Sans session", "#888"),
        "draft": ("🟡 Session créée", "#FFA500"),
        "ongoing": ("🟢 En cours", "#28a745"),
        "closed": ("✅ Clôturée", "#28a745"),
    }.get(session_status, ("?", "#888"))

    date_str = ""
    if inst.scheduled_date:
        d = _to_utc(inst.scheduled_date)
        date_str = d.strftime("%d/%m/%Y") if d else ""

    participants = json.loads(inst.participants_json or "[]")

    with st.container(border=True):
        c1, c2, c3, c4, c5 = st.columns([3, 2, 2, 2, 2])
        with c1:
            st.markdown(f"**{inst.name}**")
            st.caption(f"📅 {date_str} · 👥 {len(participants)} participants")
        with c2:
            st.markdown(
                f'<span style="color:{status_badge[1]};font-weight:bold;">{status_badge[0]}</span>',
                unsafe_allow_html=True,
            )
        with c3:
            n_sessions = len(sessions)
            st.caption(f"{n_sessions} session(s)")
        with c4:
            if last_session:
                actions = last_session.actions
                open_a = sum(1 for a in actions if effective_status(a) in ("Open", "In Progress", "Late"))
                st.caption(f"⚡ {open_a} action(s) ouvertes")
        with c5:
            if st.button("Ouvrir →", key=f"open_inst_{inst.id}", use_container_width=True):
                _open_instance(db_session, inst)
                st.rerun()


def _open_instance(db_session, inst: MeetingInstance) -> None:
    sessions = db_session.scalars(
        select(MeetingSession).where(MeetingSession.instance_id == inst.id)
        .order_by(MeetingSession.created_at.desc())
    ).all()

    # Reuse existing draft/ongoing session or create new one
    active = next((s for s in sessions if s.status in ("draft", "ongoing")), None)

    if active is None:
        participants = json.loads(inst.participants_json or "[]")
        active = MeetingSession(
            instance_id=inst.id,
            attendees_json=json.dumps(participants),
            absents_json=json.dumps([]),
            status="draft",
        )
        db_session.add(active)
        db_session.commit()

    st.session_state["hub_instance_id"] = inst.id
    st.session_state["hub_session_id"] = active.id
    st.session_state["hub_view"] = "session"


# ─────────────────────────────────────────────────────────
#  SESSION VIEW
# ─────────────────────────────────────────────────────────

def _render_session_view(db_session, user, meeting_type: MeetingType) -> None:
    inst_id = st.session_state.get("hub_instance_id")
    sess_id = st.session_state.get("hub_session_id")

    instance = db_session.get(MeetingInstance, inst_id)
    session_record = db_session.get(MeetingSession, sess_id)

    if not instance or not session_record:
        st.error("Session introuvable.")
        if st.button("← Retour à la liste"):
            st.session_state["hub_view"] = "list"
            st.rerun()
        return

    # Back to list
    c_back, c_name, c_status = st.columns([1, 5, 2])
    with c_back:
        if st.button("← Liste", key="sess_back_list"):
            st.session_state["hub_view"] = "list"
            st.rerun()
    with c_name:
        st.markdown(f"**{instance.name}**")
    with c_status:
        status_map = {
            "draft": "🟡 Non démarrée",
            "ongoing": "🟢 En cours",
            "closed": "✅ Clôturée",
        }
        st.markdown(
            f'<span style="font-weight:bold;">{status_map.get(session_record.status, session_record.status)}</span>',
            unsafe_allow_html=True,
        )

    # Start button if draft
    if session_record.status == "draft":
        st.info("La réunion n'a pas encore démarré.")
        if st.button("▶️ Lancer la réunion", type="primary", key="start_session"):
            session_record.started_at = datetime.now(timezone.utc)
            session_record.status = "ongoing"
            db_session.commit()
            st.rerun()

    # Timer
    render_timer(session_record, meeting_type.duration_minutes)

    # Session tabs
    tab_names = ["👥 Participants", "📋 Contenu", "⚡ Actions", "🔒 Clôture"]
    tabs = st.tabs(tab_names)

    with tabs[0]:
        render_checkin(session_record, instance, db_session)

    with tabs[1]:
        _render_content_tab(db_session, user, meeting_type, session_record)

    with tabs[2]:
        render_actions_panel(
            session_record, db_session, user.organization_id, meeting_type.id
        )

    with tabs[3]:
        render_closure(session_record, instance, meeting_type, db_session)

    # History expander
    all_sessions = db_session.scalars(
        select(MeetingSession).where(MeetingSession.instance_id == inst_id)
        .order_by(MeetingSession.created_at.desc())
    ).all()
    past = [s for s in all_sessions if s.id != sess_id and s.status == "closed"]
    if past:
        with st.expander(f"📚 Historique ({len(past)} session(s) précédente(s))"):
            for ps in past:
                attendees = json.loads(ps.attendees_json or "[]")
                real = ps.duration_real_minutes
                date_str = ps.ended_at.strftime("%d/%m/%Y") if ps.ended_at else "—"
                st.markdown(
                    f"**{date_str}** — {len(attendees)} présents — {real} min — "
                    f"{len(ps.actions)} action(s)"
                )
                if ps.summary:
                    st.caption(ps.summary[:200] + ("..." if len(ps.summary) > 200 else ""))
                st.divider()


def _render_content_tab(db_session, user, meeting_type: MeetingType, session_record: MeetingSession) -> None:
    module_key = meeting_type.module_key

    if module_key == "pre_scheduling":
        from maintenance_toolbox.meetings.pre_scheduling import render_pre_scheduling_content
        render_pre_scheduling_content(db_session, user)

    elif module_key == "scheduling":
        from maintenance_toolbox.meetings.scheduling_meeting import render_scheduling_content
        render_scheduling_content(db_session, user)

    elif module_key == "comite":
        from maintenance_toolbox.meetings.comite import render_comite_content
        render_comite_content(db_session, user)

    elif module_key == "codir":
        from maintenance_toolbox.meetings.codir import render_codir_content
        render_codir_content(db_session, user)

    else:
        agenda = json.loads(meeting_type.agenda_json or "[]")
        st.markdown("### Agenda")
        for i, item in enumerate(agenda, 1):
            st.checkbox(f"{i}. {item}", key=f"agenda_item_{session_record.id}_{i}")
        st.info("Ce module est en cours de déploiement.")


# ─────────────────────────────────────────────────────────
#  UTILS
# ─────────────────────────────────────────────────────────

def _clear_hub_state() -> None:
    for key in ("hub_meeting_type_id", "hub_instance_id", "hub_session_id", "hub_view"):
        st.session_state.pop(key, None)
