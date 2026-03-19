"""Comité de maintenance — KPI dashboard filtrables par type et instance."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy import select

from maintenance_toolbox.db import (
    Action, MeetingInstance, MeetingSession, MeetingType,
)
from maintenance_toolbox.meetings.common import (
    STATUS_COLORS, STATUS_ICONS, FREQ_LABELS, effective_status, _to_utc,
)


def render_comite_content(db_session, user) -> None:
    st.markdown("## 📊 Comité de maintenance — Tableau de bord")

    org_id = user.organization_id

    # ── Load data ──────────────────────────────────────────
    all_types = db_session.scalars(
        select(MeetingType).where(MeetingType.active == True).order_by(MeetingType.order_index)
    ).all()

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

    if not all_instances:
        _render_demo_comite()
        return

    # ── Filters ────────────────────────────────────────────
    type_names = ["Toutes"] + [mt.name for mt in all_types]
    with st.container(border=True):
        fc1, fc2 = st.columns(2)
        with fc1:
            selected_type = st.selectbox("Filtrer par type de réunion", type_names, key="comite_filter_type")
        with fc2:
            period_options = ["4 dernières semaines", "8 dernières semaines", "3 derniers mois", "Tout"]
            selected_period = st.selectbox("Période", period_options, key="comite_filter_period")

    now = datetime.now(timezone.utc)
    period_map = {
        "4 dernières semaines": timedelta(weeks=4),
        "8 dernières semaines": timedelta(weeks=8),
        "3 derniers mois": timedelta(weeks=13),
        "Tout": None,
    }
    delta = period_map[selected_period]

    # Filter instances
    filtered_instances = []
    for inst in all_instances:
        mt = next((m for m in all_types if m.id == inst.meeting_type_id), None)
        if selected_type != "Toutes" and (mt is None or mt.name != selected_type):
            continue
        if delta and inst.scheduled_date:
            sched = _to_utc(inst.scheduled_date)
            if sched and sched < now - delta:
                continue
        filtered_instances.append(inst)

    inst_ids = {i.id for i in filtered_instances}
    filtered_sessions = [s for s in all_sessions if s.instance_id in inst_ids]
    filtered_actions = [a for a in all_actions if a.meeting_session_id in {s.id for s in filtered_sessions}]

    # ── KPI cards ──────────────────────────────────────────
    st.divider()
    st.markdown("### Indicateurs clés")

    closed_sessions = [s for s in filtered_sessions if s.status == "closed"]
    total_instances = len(filtered_instances)
    tenue_pct = (len(closed_sessions) / total_instances * 100) if total_instances > 0 else 0

    on_time_sessions = [
        s for s in closed_sessions
        if s.duration_real_minutes is not None
        and s.instance_id in inst_ids
        and s.duration_real_minutes <= _get_theoretical(s, all_instances, all_types)
    ]
    timing_pct = (len(on_time_sessions) / len(closed_sessions) * 100) if closed_sessions else 0

    avg_attendance = 0.0
    if closed_sessions:
        attendee_counts = []
        for s in closed_sessions:
            inst = next((i for i in all_instances if i.id == s.instance_id), None)
            if inst:
                total_p = len(json.loads(inst.participants_json or "[]"))
                present = len(json.loads(s.attendees_json or "[]"))
                if total_p > 0:
                    attendee_counts.append(present / total_p * 100)
        avg_attendance = sum(attendee_counts) / len(attendee_counts) if attendee_counts else 0

    open_actions = [a for a in filtered_actions if effective_status(a) in ("Open", "In Progress")]
    late_actions = [a for a in filtered_actions if effective_status(a) == "Late"]
    done_actions = [a for a in filtered_actions if effective_status(a) == "Done"]
    closure_rate = (len(done_actions) / len(filtered_actions) * 100) if filtered_actions else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _kpi_card("Taux de tenue", f"{tenue_pct:.0f}%", f"{len(closed_sessions)}/{total_instances} réunions", tenue_pct >= 80)
    with c2:
        _kpi_card("Respect du timing", f"{timing_pct:.0f}%", f"{len(on_time_sessions)}/{len(closed_sessions)} dans les temps", timing_pct >= 70)
    with c3:
        _kpi_card("Participation moyenne", f"{avg_attendance:.0f}%", "des participants convoqués", avg_attendance >= 75)
    with c4:
        _kpi_card("Taux de clôture actions", f"{closure_rate:.0f}%", f"{len(done_actions)}/{len(filtered_actions)} actions closes", closure_rate >= 60)

    # ── Charts ─────────────────────────────────────────────
    st.divider()
    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("#### Tenue des réunions par type")
        _chart_tenue_by_type(filtered_instances, filtered_sessions, all_types)

    with col_right:
        st.markdown("#### Durée réelle vs théorique (dernières sessions)")
        _chart_timing(filtered_sessions, filtered_instances, all_types)

    st.divider()
    col_left2, col_right2 = st.columns(2)

    with col_left2:
        st.markdown("#### Participation par type")
        _chart_participation(filtered_sessions, filtered_instances, all_types)

    with col_right2:
        st.markdown("#### Statut des actions")
        _chart_action_status(filtered_actions)

    # ── Action table ───────────────────────────────────────
    if filtered_actions:
        st.divider()
        st.markdown("#### 📋 Détail des actions")

        rows = []
        for a in filtered_actions:
            eff = effective_status(a)
            rows.append({
                "Description": a.description,
                "Responsable": a.owner,
                "Échéance": a.due_date.strftime("%d/%m/%Y") if a.due_date else "—",
                "Statut": f"{STATUS_ICONS.get(eff, '')} {eff}",
                "Créée le": a.created_at.strftime("%d/%m/%Y") if a.created_at else "—",
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────

def _get_theoretical(session: MeetingSession, all_instances, all_types) -> int:
    inst = next((i for i in all_instances if i.id == session.instance_id), None)
    if not inst:
        return 60
    mt = next((m for m in all_types if m.id == inst.meeting_type_id), None)
    return mt.duration_minutes if mt else 60


def _kpi_card(label: str, value: str, subtitle: str, good: bool) -> None:
    color = "#28a745" if good else "#dc3545"
    st.markdown(
        f"""<div style="border:2px solid {color};border-radius:10px;padding:16px;text-align:center;">
        <div style="font-size:2rem;font-weight:bold;color:{color};">{value}</div>
        <div style="font-weight:600;color:#3f434f;">{label}</div>
        <div style="font-size:0.8rem;color:#888;">{subtitle}</div>
        </div>""",
        unsafe_allow_html=True,
    )


def _chart_tenue_by_type(instances, sessions, all_types):
    rows = []
    inst_ids_by_type = {}
    for inst in instances:
        inst_ids_by_type.setdefault(inst.meeting_type_id, []).append(inst.id)

    for mt in all_types:
        inst_ids = inst_ids_by_type.get(mt.id, [])
        if not inst_ids:
            continue
        closed = sum(1 for s in sessions if s.instance_id in inst_ids and s.status == "closed")
        total = len(inst_ids)
        rows.append({"Type": mt.name, "Tenue (%)": round(closed / total * 100) if total else 0})

    if rows:
        df = pd.DataFrame(rows).set_index("Type")
        st.bar_chart(df, color="#f39200")
    else:
        st.info("Pas de données disponibles.")


def _chart_timing(sessions, instances, all_types):
    rows = []
    closed = sorted(
        [s for s in sessions if s.status == "closed" and s.duration_real_minutes],
        key=lambda s: s.created_at or datetime.min.replace(tzinfo=timezone.utc),
    )[-8:]

    for s in closed:
        inst = next((i for i in instances if i.id == s.instance_id), None)
        if not inst:
            continue
        label = inst.name[:20]
        theo = _get_theoretical(s, instances, all_types)
        rows.append({
            "Réunion": label,
            "Réelle": s.duration_real_minutes,
            "Théorique": theo,
        })

    if rows:
        df = pd.DataFrame(rows).set_index("Réunion")
        st.bar_chart(df)
    else:
        st.info("Pas de sessions clôturées disponibles.")


def _chart_participation(sessions, instances, all_types):
    rows = []
    inst_ids_by_type = {}
    for inst in instances:
        inst_ids_by_type.setdefault(inst.meeting_type_id, []).append(inst.id)

    for mt in all_types:
        inst_ids = set(inst_ids_by_type.get(mt.id, []))
        type_sessions = [s for s in sessions if s.instance_id in inst_ids and s.status == "closed"]
        if not type_sessions:
            continue
        rates = []
        for s in type_sessions:
            inst = next((i for i in instances if i.id == s.instance_id), None)
            if inst:
                total = len(json.loads(inst.participants_json or "[]"))
                present = len(json.loads(s.attendees_json or "[]"))
                if total > 0:
                    rates.append(present / total * 100)
        if rates:
            rows.append({"Type": mt.name, "Participation (%)": round(sum(rates) / len(rates))})

    if rows:
        df = pd.DataFrame(rows).set_index("Type")
        st.bar_chart(df, color="#1E90FF")
    else:
        st.info("Pas de données de participation.")


def _chart_action_status(actions):
    if not actions:
        st.info("Aucune action dans la période sélectionnée.")
        return
    counts = {}
    for a in actions:
        eff = effective_status(a)
        counts[eff] = counts.get(eff, 0) + 1
    df = pd.DataFrame(
        [{"Statut": k, "Nombre": v} for k, v in counts.items()]
    ).set_index("Statut")
    st.bar_chart(df)


def _render_demo_comite():
    st.info("📊 Aucune donnée réelle trouvée — affichage de la vue démonstration.")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _kpi_card("Taux de tenue", "83%", "5/6 réunions", True)
    with c2:
        _kpi_card("Respect du timing", "67%", "4/6 dans les temps", False)
    with c3:
        _kpi_card("Participation moyenne", "81%", "des participants convoqués", True)
    with c4:
        _kpi_card("Taux de clôture actions", "58%", "14/24 actions closes", False)

    st.divider()
    demo_data = pd.DataFrame({
        "Type": ["Pré-scheduling", "Scheduling", "Comité", "Codir"],
        "Tenue (%)": [86, 100, 75, 67],
    }).set_index("Type")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Tenue par type (démo)")
        st.bar_chart(demo_data, color="#f39200")
    with col2:
        st.markdown("#### Statut actions (démo)")
        demo_actions = pd.DataFrame({
            "Statut": ["Done", "In Progress", "Open", "Late"],
            "Nombre": [14, 5, 3, 2],
        }).set_index("Statut")
        st.bar_chart(demo_actions)
