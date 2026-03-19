"""Codir — KPI gouvernance globale."""
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
    STATUS_ICONS, effective_status, _to_utc,
)


def render_codir_content(db_session, user) -> None:
    st.markdown("## 🎯 Codir — Tableau de bord gouvernance")

    org_id = user.organization_id

    all_types = db_session.scalars(
        select(MeetingType).where(MeetingType.active == True)
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
        _render_demo_codir()
        return

    closed_sessions = [s for s in all_sessions if s.status == "closed"]
    total_inst = len(all_instances)
    tenue_global = (len(closed_sessions) / total_inst * 100) if total_inst > 0 else 0

    on_time = 0
    for s in closed_sessions:
        if s.duration_real_minutes is None:
            continue
        inst = next((i for i in all_instances if i.id == s.instance_id), None)
        if not inst:
            continue
        mt = next((m for m in all_types if m.id == inst.meeting_type_id), None)
        theo = mt.duration_minutes if mt else 60
        if s.duration_real_minutes <= theo:
            on_time += 1
    timing_global = (on_time / len(closed_sessions) * 100) if closed_sessions else 0

    rates = []
    for s in closed_sessions:
        inst = next((i for i in all_instances if i.id == s.instance_id), None)
        if inst:
            total_p = len(json.loads(inst.participants_json or "[]"))
            present = len(json.loads(s.attendees_json or "[]"))
            if total_p > 0:
                rates.append(present / total_p * 100)
    participation_global = sum(rates) / len(rates) if rates else 0

    action_statuses = {
        eff: sum(1 for a in all_actions if effective_status(a) == eff)
        for eff in ["Open", "In Progress", "Done", "Late"]
    }
    total_actions = len(all_actions)
    closure_rate = (action_statuses["Done"] / total_actions * 100) if total_actions > 0 else 0
    late_count = action_statuses["Late"]

    # ── KPI cards ──────────────────────────────────────────
    st.markdown("### Indicateurs gouvernance — Vue globale")
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        _kpi_card_big("Taux de tenue", f"{tenue_global:.0f}%", tenue_global >= 80)
    with c2:
        _kpi_card_big("Respect timing", f"{timing_global:.0f}%", timing_global >= 70)
    with c3:
        _kpi_card_big("Participation", f"{participation_global:.0f}%", participation_global >= 75)
    with c4:
        _kpi_card_big("Clôture actions", f"{closure_rate:.0f}%", closure_rate >= 60)

    # Late actions alert
    if late_count > 0:
        st.error(
            f"🔴 **{late_count} action(s) en retard** — Arbitrage requis. "
            f"Voir le détail ci-dessous."
        )

    # ── Per-type breakdown ─────────────────────────────────
    st.divider()
    st.markdown("### Détail par type de réunion")

    rows = []
    for mt in all_types:
        type_instances = [i for i in all_instances if i.meeting_type_id == mt.id]
        if not type_instances:
            continue
        type_inst_ids = {i.id for i in type_instances}
        type_sessions = [s for s in all_sessions if s.instance_id in type_inst_ids]
        type_closed = [s for s in type_sessions if s.status == "closed"]

        tenue = len(type_closed) / len(type_instances) * 100 if type_instances else 0

        ot = sum(
            1 for s in type_closed
            if s.duration_real_minutes is not None
            and s.duration_real_minutes <= mt.duration_minutes
        )
        timing = ot / len(type_closed) * 100 if type_closed else 0

        r = []
        for s in type_closed:
            inst = next((i for i in type_instances if i.id == s.instance_id), None)
            if inst:
                tp = len(json.loads(inst.participants_json or "[]"))
                pr = len(json.loads(s.attendees_json or "[]"))
                if tp > 0:
                    r.append(pr / tp * 100)
        partic = sum(r) / len(r) if r else 0

        type_session_ids = {s.id for s in type_sessions}
        type_actions = [a for a in all_actions if a.meeting_session_id in type_session_ids]
        done_a = sum(1 for a in type_actions if effective_status(a) == "Done")
        late_a = sum(1 for a in type_actions if effective_status(a) == "Late")
        clos = done_a / len(type_actions) * 100 if type_actions else 0

        rows.append({
            "Type": f"{mt.icon} {mt.name}",
            "Instances": len(type_instances),
            "Tenue": f"{tenue:.0f}%",
            "Timing": f"{timing:.0f}%",
            "Participation": f"{partic:.0f}%",
            "Actions closes": f"{clos:.0f}%",
            "En retard": late_a,
        })

    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)

    # ── Trend chart ────────────────────────────────────────
    st.divider()
    st.markdown("### Évolution de la tenue — 8 dernières semaines")
    _chart_tenue_trend(all_instances, all_sessions, all_types)

    # ── Critical actions ───────────────────────────────────
    st.divider()
    st.markdown("### ⚡ Actions critiques (ouvertes ou en retard)")
    critical = [a for a in all_actions if effective_status(a) in ("Open", "In Progress", "Late")]
    if critical:
        crit_rows = []
        for a in sorted(critical, key=lambda x: (0 if effective_status(x) == "Late" else 1, x.due_date or datetime.max.replace(tzinfo=timezone.utc))):
            eff = effective_status(a)
            mt = next((m for m in all_types if m.id == a.meeting_type_id), None)
            crit_rows.append({
                "Réunion": mt.name if mt else "—",
                "Action": a.description,
                "Responsable": a.owner,
                "Échéance": a.due_date.strftime("%d/%m/%Y") if a.due_date else "—",
                "Statut": f"{STATUS_ICONS.get(eff, '')} {eff}",
            })
        df_crit = pd.DataFrame(crit_rows)
        st.dataframe(df_crit, use_container_width=True, hide_index=True)
    else:
        st.success("✅ Aucune action critique en suspens.")


def _chart_tenue_trend(all_instances, all_sessions, all_types):
    now = datetime.now(timezone.utc)
    weeks = []
    tenue_vals = []
    for w in range(7, -1, -1):
        week_start = now - timedelta(weeks=w + 1)
        week_end = now - timedelta(weeks=w)
        week_instances = [
            i for i in all_instances
            if i.scheduled_date and week_start <= _to_utc(i.scheduled_date) < week_end
        ]
        if not week_instances:
            continue
        inst_ids = {i.id for i in week_instances}
        closed = sum(1 for s in all_sessions if s.instance_id in inst_ids and s.status == "closed")
        tenue = closed / len(week_instances) * 100
        week_label = week_start.strftime("S%W")
        weeks.append(week_label)
        tenue_vals.append(tenue)

    if weeks:
        df = pd.DataFrame({"Semaine": weeks, "Tenue (%)": tenue_vals}).set_index("Semaine")
        st.line_chart(df)
    else:
        st.info("Pas assez de données pour afficher la tendance.")


def _kpi_card_big(label: str, value: str, good: bool) -> None:
    color = "#28a745" if good else "#dc3545"
    st.markdown(
        f"""<div style="border:3px solid {color};border-radius:12px;padding:20px;
        text-align:center;margin-bottom:8px;">
        <div style="font-size:2.5rem;font-weight:bold;color:{color};">{value}</div>
        <div style="font-weight:600;color:#3f434f;font-size:1.1rem;">{label}</div>
        </div>""",
        unsafe_allow_html=True,
    )


def _render_demo_codir():
    st.info("🎯 Aucune donnée réelle trouvée — affichage de la vue démonstration.")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _kpi_card_big("Taux de tenue", "85%", True)
    with c2:
        _kpi_card_big("Respect timing", "62%", False)
    with c3:
        _kpi_card_big("Participation", "79%", True)
    with c4:
        _kpi_card_big("Clôture actions", "56%", False)

    st.warning("🔴 3 actions en retard — Arbitrage requis.")

    st.divider()
    demo_trend = pd.DataFrame({
        "Semaine": [f"S{i}" for i in range(1, 9)],
        "Tenue (%)": [100, 75, 100, 50, 100, 100, 75, 100],
    }).set_index("Semaine")
    st.markdown("### Évolution de la tenue (démo)")
    st.line_chart(demo_trend)

    st.divider()
    demo_rows = [
        {"Réunion": "Scheduling", "Action": "Clôturer OT en attente >30j", "Responsable": "Laurent Morel", "Échéance": "10/03/2026", "Statut": "🔴 Late"},
        {"Réunion": "Pré-scheduling", "Action": "Valider planning avec production", "Responsable": "Marie Lambert", "Échéance": "15/03/2026", "Statut": "🟠 In Progress"},
        {"Réunion": "Comité", "Action": "Rapport mensuel maintenance", "Responsable": "Sophie Martin", "Échéance": "20/03/2026", "Statut": "🔵 Open"},
    ]
    st.markdown("### Actions critiques (démo)")
    st.dataframe(pd.DataFrame(demo_rows), use_container_width=True, hide_index=True)
