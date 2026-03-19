"""Shared components for all meeting sessions: timer, check-in, actions panel, closure."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Optional

import streamlit as st
from sqlalchemy import select

from maintenance_toolbox.db import Action, MeetingSession, MeetingInstance, MeetingType


# ─────────────────────────────────────────────────────────
#  UTILS
# ─────────────────────────────────────────────────────────

def _to_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _elapsed_minutes(started_at: Optional[datetime]) -> int:
    if not started_at:
        return 0
    now = datetime.now(timezone.utc)
    started = _to_utc(started_at)
    return max(0, int((now - started).total_seconds() / 60))


STATUS_COLORS = {
    "Open": "#1E90FF",
    "In Progress": "#FFA500",
    "Done": "#28a745",
    "Late": "#dc3545",
}

STATUS_ICONS = {
    "Open": "🔵",
    "In Progress": "🟠",
    "Done": "✅",
    "Late": "🔴",
}

FREQ_LABELS = {
    "daily": "Quotidienne",
    "weekly": "Hebdomadaire",
    "biweekly": "Bimensuelle",
    "monthly": "Mensuelle",
}


def effective_status(action: Action) -> str:
    """Return 'Late' if overdue and not done, else action.status."""
    if action.status == "Done":
        return "Done"
    if action.due_date:
        due = _to_utc(action.due_date)
        if due and due < datetime.now(timezone.utc):
            return "Late"
    return action.status


# ─────────────────────────────────────────────────────────
#  TIMER BANNER
# ─────────────────────────────────────────────────────────

def render_timer(session_record: MeetingSession, theoretical_min: int) -> None:
    elapsed = _elapsed_minutes(session_record.started_at)
    pct = (elapsed / theoretical_min * 100) if theoretical_min > 0 else 0
    over = elapsed - theoretical_min

    if session_record.status == "closed":
        real = session_record.duration_real_minutes or elapsed
        delta = real - theoretical_min
        sign = "+" if delta >= 0 else ""
        color = "#dc3545" if delta > 0 else "#28a745"
        st.markdown(
            f"""<div style="background:#f8f3e8;border-left:5px solid {color};
            padding:10px 16px;border-radius:8px;margin-bottom:8px;">
            ⏱️ <strong>Durée réelle : {real} min</strong> &nbsp;|&nbsp;
            Théorique : {theoretical_min} min &nbsp;|&nbsp;
            <span style="color:{color};font-weight:bold;">{sign}{delta} min</span>
            &nbsp; — Session clôturée
            </div>""",
            unsafe_allow_html=True,
        )
        return

    if session_record.started_at is None:
        st.info("⏱️ Session non démarrée — cliquez sur **Lancer la réunion** pour démarrer le chrono.")
        return

    remaining = theoretical_min - elapsed
    alert_mode = 0 < remaining <= 5 or elapsed > theoretical_min

    color = "#dc3545" if elapsed > theoretical_min else "#f39200"
    if elapsed <= theoretical_min * 0.8:
        color = "#28a745"
    if alert_mode:
        color = "#dc3545"

    over_txt = ""
    if over > 0:
        over_txt = f"&nbsp;⚠️ +{over} min"

    pulse_class = "mn-timer-alert" if alert_mode else ""
    alert_bg = "#fff5f5" if alert_mode else "white"
    border_color = "#dc3545" if alert_mode else "#f0f0f0"

    bar_pct = min(pct, 100)

    st.markdown(
        f"""<div class="mn-timer-sticky {pulse_class}"
            style="background:{alert_bg};border:2px solid {border_color};
                   border-radius:10px;padding:10px 16px;">
          <div style="display:flex;align-items:center;gap:24px;flex-wrap:wrap;">
            <div>
              <div style="font-size:0.75rem;color:#888;">⏱️ Temps écoulé</div>
              <div style="font-size:1.4rem;font-weight:700;color:{color};">{elapsed} min</div>
            </div>
            <div>
              <div style="font-size:0.75rem;color:#888;">🎯 Durée théorique</div>
              <div style="font-size:1.4rem;font-weight:700;color:#3f434f;">{theoretical_min} min</div>
            </div>
            <div style="flex:1;min-width:180px;">
              <div style="background:#e9e9e9;border-radius:6px;height:16px;width:100%;margin-top:4px;">
                <div style="background:{color};width:{bar_pct:.0f}%;height:16px;
                     border-radius:6px;transition:width 0.5s;"></div>
              </div>
              <small style="color:{color};">{pct:.0f}% de la durée théorique{over_txt}</small>
            </div>
          </div>
        </div>""",
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────
#  CHECK-IN
# ─────────────────────────────────────────────────────────

def render_checkin(session_record: MeetingSession, instance: MeetingInstance, db_session) -> None:
    st.subheader("👥 Participants")

    participants = json.loads(instance.participants_json or "[]")
    if not participants:
        st.warning("Aucun participant défini pour cette instance. Ajoutez-en dans l'administration.")
        return

    current_attendees = set(json.loads(session_record.attendees_json or json.dumps(participants)))

    st.caption(f"{len(current_attendees)} / {len(participants)} présents")

    updated = list(current_attendees)
    changed = False

    cols = st.columns(2)
    for idx, participant in enumerate(participants):
        present = participant in current_attendees
        col = cols[idx % 2]
        with col:
            new_val = st.checkbox(
                participant,
                value=present,
                key=f"checkin_{session_record.id}_{idx}",
                disabled=(session_record.status == "closed"),
            )
            if new_val != present:
                changed = True
                if new_val:
                    updated.append(participant)
                else:
                    updated = [p for p in updated if p != participant]

    if changed and session_record.status != "closed":
        absent = [p for p in participants if p not in updated]
        session_record.attendees_json = json.dumps(updated)
        session_record.absents_json = json.dumps(absent)
        db_session.commit()
        st.rerun()


# ─────────────────────────────────────────────────────────
#  ACTIONS PANEL
# ─────────────────────────────────────────────────────────

def render_actions_panel(
    session_record: MeetingSession,
    db_session,
    org_id: int,
    meeting_type_id: Optional[int] = None,
) -> None:
    st.subheader("⚡ Actions")

    actions = session_record.actions

    # Update late statuses on render
    for a in actions:
        eff = effective_status(a)
        if eff == "Late" and a.status != "Done":
            a.status = "Late"
    if actions:
        db_session.commit()

    # Display existing actions
    if actions:
        for a in actions:
            eff = effective_status(a)
            icon = STATUS_ICONS.get(eff, "")
            color = STATUS_COLORS.get(eff, "#888")
            due_str = a.due_date.strftime("%d/%m/%Y") if a.due_date else "—"
            with st.container(border=True):
                c1, c2, c3, c4 = st.columns([4, 2, 2, 2])
                with c1:
                    st.markdown(f"**{a.description}**")
                with c2:
                    st.caption(f"👤 {a.owner}")
                with c3:
                    st.caption(f"📅 {due_str}")
                with c4:
                    st.markdown(
                        f'<span style="color:{color};font-weight:bold;">{icon} {eff}</span>',
                        unsafe_allow_html=True,
                    )
                if session_record.status != "closed":
                    new_status = st.selectbox(
                        "Statut",
                        ["Open", "In Progress", "Done", "Late"],
                        index=["Open", "In Progress", "Done", "Late"].index(a.status),
                        key=f"action_status_{a.id}",
                        label_visibility="collapsed",
                    )
                    if new_status != a.status:
                        a.status = new_status
                        db_session.commit()
                        st.rerun()
    else:
        st.info("Aucune action créée pour cette session.")

    # Add new action form (only if session not closed)
    if session_record.status != "closed":
        st.divider()
        st.markdown("**Créer une action**")
        with st.form(f"new_action_form_{session_record.id}", clear_on_submit=True):
            desc = st.text_area("Description", placeholder="Décrire l'action...", height=80)
            c1, c2, c3 = st.columns(3)
            with c1:
                owner = st.text_input("Responsable")
            with c2:
                due = st.date_input("Échéance")
            with c3:
                status_new = st.selectbox("Statut", ["Open", "In Progress"])
            submitted = st.form_submit_button("➕ Ajouter l'action", use_container_width=True)

        if submitted:
            if not desc.strip():
                st.error("La description est obligatoire.")
            elif not owner.strip():
                st.error("Le responsable est obligatoire.")
            else:
                due_dt = datetime.combine(due, datetime.min.time()).replace(tzinfo=timezone.utc)
                new_action = Action(
                    description=desc.strip(),
                    owner=owner.strip(),
                    due_date=due_dt,
                    status=status_new,
                    meeting_session_id=session_record.id,
                    meeting_type_id=meeting_type_id,
                    organization_id=org_id,
                )
                db_session.add(new_action)
                db_session.commit()
                st.success("Action ajoutée.")
                st.rerun()


# ─────────────────────────────────────────────────────────
#  CLOSURE
# ─────────────────────────────────────────────────────────

def render_closure(
    session_record: MeetingSession,
    instance: MeetingInstance,
    meeting_type: MeetingType,
    db_session,
) -> None:
    st.subheader("🔒 Clôture de session")

    if session_record.status == "closed":
        _render_closed_summary(session_record, instance, meeting_type)
        return

    if session_record.started_at is None:
        st.warning("La session n'a pas encore été démarrée.")
        return

    elapsed = _elapsed_minutes(session_record.started_at)
    attendees = json.loads(session_record.attendees_json or "[]")
    absents = json.loads(session_record.absents_json or "[]")
    actions = session_record.actions

    st.markdown("### Résumé avant clôture")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("⏱️ Durée réelle", f"{elapsed} min")
    c2.metric("👥 Présents", len(attendees))
    c3.metric("🚫 Absents", len(absents))
    c4.metric("⚡ Actions créées", len(actions))

    summary = _generate_summary(session_record, instance, meeting_type, elapsed, attendees, absents, actions)
    subject, recipients, body = _generate_mail(summary, instance, meeting_type, attendees, actions)

    with st.expander("📝 Résumé auto-généré", expanded=True):
        st.text_area("Résumé", value=summary, height=200, key="closure_summary_preview", disabled=True)

    with st.expander("📧 Structure mail de sortie"):
        st.text_input("Objet", value=subject, disabled=True)
        st.text_input("Destinataires", value=recipients, disabled=True)
        st.text_area("Corps", value=body, height=300, disabled=True)
        st.caption("Copiez ce contenu dans votre messagerie.")

    st.divider()
    st.error("⚠️ La clôture est irréversible. Toutes les données seront enregistrées.")
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("🔒 Clôturer la session", type="primary", use_container_width=True):
            session_record.ended_at = datetime.now(timezone.utc)
            session_record.duration_real_minutes = elapsed
            session_record.status = "closed"
            session_record.summary = summary
            session_record.mail_subject = subject
            session_record.mail_recipients = recipients
            session_record.mail_body = body
            db_session.commit()
            st.success("Session clôturée avec succès.")
            st.rerun()


def _render_closed_summary(session_record: MeetingSession, instance: MeetingInstance, meeting_type: MeetingType) -> None:
    attendees = json.loads(session_record.attendees_json or "[]")
    absents = json.loads(session_record.absents_json or "[]")

    st.success("✅ Cette session a été clôturée.")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("⏱️ Durée réelle", f"{session_record.duration_real_minutes or 0} min")
    c2.metric("🎯 Durée théorique", f"{meeting_type.duration_minutes} min")
    c3.metric("👥 Présents", len(attendees))
    c4.metric("⚡ Actions", len(session_record.actions))

    if session_record.summary:
        with st.expander("📝 Résumé", expanded=True):
            st.markdown(session_record.summary)

    if session_record.mail_body:
        with st.expander("📧 Mail de sortie"):
            st.text_input("Objet", value=session_record.mail_subject or "", disabled=True)
            st.text_input("Destinataires", value=session_record.mail_recipients or "", disabled=True)
            st.text_area("Corps", value=session_record.mail_body or "", height=300, disabled=True)


def _generate_summary(
    session_record, instance, meeting_type, elapsed, attendees, absents, actions
) -> str:
    date_str = ""
    if instance.scheduled_date:
        d = _to_utc(instance.scheduled_date)
        date_str = d.strftime("%d/%m/%Y") if d else ""

    lines = [
        f"## Compte-rendu — {meeting_type.name}",
        f"**Instance :** {instance.name}",
        f"**Date :** {date_str}",
        f"**Durée réelle :** {elapsed} min (théorique : {meeting_type.duration_minutes} min)",
        "",
        f"**Présents ({len(attendees)}) :** {', '.join(attendees) if attendees else '—'}",
        f"**Absents ({len(absents)}) :** {', '.join(absents) if absents else 'Aucun'}",
        "",
    ]

    if actions:
        lines.append(f"**Actions créées ({len(actions)}) :**")
        for a in actions:
            due = a.due_date.strftime("%d/%m/%Y") if a.due_date else "—"
            lines.append(f"- {a.description} → {a.owner} | échéance : {due} | statut : {a.status}")
    else:
        lines.append("**Aucune action créée lors de cette session.**")

    return "\n".join(lines)


def _generate_mail(summary, instance, meeting_type, attendees, actions) -> tuple[str, str, str]:
    subject = f"[CR] {meeting_type.name} — {instance.name}"
    recipients = ", ".join(attendees)

    open_actions = [a for a in actions if a.status in ("Open", "In Progress", "Late")]
    action_lines = ""
    if open_actions:
        action_lines = "\n\nACTIONS OUVERTES :\n" + "\n".join(
            f"  • {a.description} — {a.owner} ({a.due_date.strftime('%d/%m/%Y') if a.due_date else '?'})"
            for a in open_actions
        )

    body = (
        f"Bonjour,\n\n"
        f"Veuillez trouver ci-dessous le compte-rendu de la réunion {meeting_type.name} "
        f"({instance.name}).\n\n"
        f"--- RÉSUMÉ ---\n{summary}\n"
        f"{action_lines}\n\n"
        f"Cordialement,\nL'équipe MaintenOps"
    )
    return subject, recipients, body
