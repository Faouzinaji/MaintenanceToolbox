from __future__ import annotations

import json
from datetime import datetime, timezone

import streamlit as st
from sqlalchemy import select

from maintenance_toolbox.db import (
    User, Organization, MeetingType, MeetingInstance, MeetingSession,
    ensure_org_defaults,
)


def render_admin(session, current_user) -> None:
    if current_user.role != "admin":
        st.error("Accès refusé")
        return

    st.title("⚙️ Administration")

    tab1, tab2, tab3 = st.tabs(["Utilisateurs", "Organisations", "Réunions"])

    # ── TAB 1: Users ────────────────────────────────────────
    with tab1:
        st.subheader("Créer un utilisateur")

        organizations = session.scalars(
            select(Organization).order_by(Organization.name)
        ).all()
        org_options = {org.name: org.id for org in organizations}

        with st.form("create_user_form"):
            full_name = st.text_input("Nom complet")
            email = st.text_input("Email")
            password = st.text_input("Mot de passe temporaire", type="password")
            role = st.selectbox("Rôle", ["user", "admin"])
            org_name = st.selectbox(
                "Organisation",
                list(org_options.keys()) if org_options else []
            )
            is_active = st.checkbox("Actif", value=True)
            submitted = st.form_submit_button("Créer l'utilisateur", use_container_width=True)

        if submitted:
            existing = session.scalar(select(User).where(User.email == email))
            if existing:
                st.error("Un utilisateur avec cet email existe déjà")
            elif not org_options:
                st.error("Aucune organisation disponible")
            else:
                user = User(
                    full_name=full_name,
                    email=email,
                    role=role,
                    language="fr",
                    is_active=is_active,
                    first_login=True,
                    organization_id=org_options[org_name],
                )
                user.set_password(password)
                session.add(user)
                session.commit()
                st.success("Utilisateur créé")

        st.divider()
        st.subheader("Liste des utilisateurs")
        users = session.scalars(select(User).order_by(User.created_at.desc())).all()
        for user in users:
            st.write(
                f"**{user.full_name}** — {user.email} — rôle: `{user.role}` — "
                f"{'actif' if user.is_active else 'désactivé'}"
            )

    # ── TAB 2: Organizations ────────────────────────────────
    with tab2:
        st.subheader("Créer une organisation")

        with st.form("create_org_form"):
            org_name_input = st.text_input("Nom organisation")
            timezone_val = st.text_input("Timezone", value="Europe/Paris")
            st.markdown("**Administrateur de l'organisation**")
            admin_full_name = st.text_input("Nom complet de l'admin")
            admin_email = st.text_input("Email de l'admin")
            admin_password = st.text_input("Mot de passe temporaire", type="password", key="org_admin_pw")
            submitted_org = st.form_submit_button("Créer l'organisation", use_container_width=True)

        if submitted_org:
            if not org_name_input.strip():
                st.error("Le nom de l'organisation est obligatoire.")
            elif not admin_email.strip():
                st.error("L'email de l'administrateur est obligatoire.")
            else:
                existing_org = session.scalar(
                    select(Organization).where(Organization.name == org_name_input)
                )
                if existing_org:
                    st.error("Cette organisation existe déjà")
                else:
                    existing_admin = session.scalar(
                        select(User).where(User.email == admin_email)
                    )
                    if existing_admin:
                        st.error(f"L'email {admin_email} est déjà utilisé.")
                    else:
                        org = Organization(name=org_name_input, timezone=timezone_val, active=True)
                        session.add(org)
                        session.flush()
                        ensure_org_defaults(session, org)

                        org_admin = User(
                            full_name=admin_full_name or f"Admin {org_name_input}",
                            email=admin_email,
                            role="admin",
                            language="fr",
                            is_active=True,
                            first_login=True,
                            organization_id=org.id,
                        )
                        org_admin.set_password(admin_password or "changeme")
                        session.add(org_admin)
                        session.commit()
                        st.success(f"Organisation « {org_name_input} » créée avec son administrateur.")

        st.divider()
        st.subheader("Liste des organisations")
        orgs = session.scalars(select(Organization).order_by(Organization.name)).all()
        for org in orgs:
            st.write(f"**{org.name}** — timezone: {org.timezone}")

    # ── TAB 3: Meetings ─────────────────────────────────────
    with tab3:
        _render_meetings_tab(session, current_user)


def _render_meetings_tab(session, current_user) -> None:
    all_types = session.scalars(
        select(MeetingType).order_by(MeetingType.order_index)
    ).all()
    active_types = [mt for mt in all_types if mt.active]

    organizations = session.scalars(
        select(Organization).order_by(Organization.name)
    ).all()

    # ── Create a meeting (MeetingInstance) ──────────────────
    st.subheader("Créer une réunion")

    if not active_types:
        st.info("Aucun type de réunion actif.")
    elif not organizations:
        st.info("Aucune organisation disponible.")
    else:
        with st.form("create_meeting_form"):
            type_options = {f"{mt.icon} {mt.name}": mt for mt in active_types}
            selected_type_label = st.selectbox("Modèle de réunion", list(type_options.keys()))
            selected_mt = type_options[selected_type_label]

            org_options = {o.name: o.id for o in organizations}
            selected_org_name = st.selectbox("Organisation", list(org_options.keys()))

            meeting_name = st.text_input(
                "Nom de la réunion",
                placeholder=f"{selected_mt.name} — Équipe A",
            )

            # Show template agenda as editable default
            default_agenda = "\n".join(
                selected_mt.effective_agenda() if hasattr(selected_mt, "effective_agenda")
                else (json.loads(selected_mt.agenda_json) if selected_mt.agenda_json else [])
            )
            custom_agenda_raw = st.text_area(
                "Agenda (un point par ligne)",
                value=default_agenda,
                height=150,
                help="Modifiez l'agenda pour personnaliser cette réunion.",
            )

            default_duration = selected_mt.duration_minutes or 60
            custom_duration = st.number_input(
                "Durée (minutes)",
                min_value=15,
                max_value=480,
                value=default_duration,
                step=15,
            )

            submitted_meeting = st.form_submit_button("Créer la réunion", use_container_width=True, type="primary")

        if submitted_meeting:
            if not meeting_name.strip():
                st.error("Le nom de la réunion est obligatoire.")
            else:
                custom_agenda = [line.strip() for line in custom_agenda_raw.splitlines() if line.strip()]
                template_agenda = json.loads(selected_mt.agenda_json) if selected_mt.agenda_json else []
                # Only store custom agenda if it differs from the template
                agenda_to_store = json.dumps(custom_agenda) if custom_agenda != template_agenda else None
                duration_to_store = custom_duration if custom_duration != selected_mt.duration_minutes else None

                inst = MeetingInstance(
                    meeting_type_id=selected_mt.id,
                    organization_id=org_options[selected_org_name],
                    name=meeting_name.strip(),
                    scheduled_date=None,
                    participants_json=json.dumps([]),
                    created_by_user_id=current_user.id,
                    custom_agenda_json=agenda_to_store,
                    custom_duration_minutes=duration_to_store,
                )
                session.add(inst)
                session.commit()
                st.success(f"Réunion « {meeting_name} » créée.")
                st.rerun()

    st.divider()

    # ── List meetings + manage duplicates (MeetingSession) ──
    st.subheader("Réunions existantes")

    all_instances = session.scalars(
        select(MeetingInstance).order_by(MeetingInstance.created_at.desc())
    ).all()
    all_sessions = session.scalars(select(MeetingSession)).all()
    sessions_by_instance: dict[int, list[MeetingSession]] = {}
    for ms in all_sessions:
        sessions_by_instance.setdefault(ms.instance_id, []).append(ms)

    mt_by_id = {mt.id: mt for mt in all_types}
    org_by_id = {o.id: o for o in organizations}

    if not all_instances:
        st.info("Aucune réunion créée.")
        return

    for inst in all_instances:
        mt = mt_by_id.get(inst.meeting_type_id)
        org = org_by_id.get(inst.organization_id)
        mt_label = f"{mt.icon} {mt.name}" if mt else "—"
        inst_sessions = sessions_by_instance.get(inst.id, [])

        with st.expander(f"**{inst.name}** · {mt_label} · {org.name if org else '—'} · {len(inst_sessions)} duplicate(s)"):
            c1, c2 = st.columns([6, 1])
            with c1:
                duration = inst.effective_duration() if hasattr(inst, "effective_duration") else (inst.custom_duration_minutes or (mt.duration_minutes if mt else 60))
                st.caption(f"Durée : {duration} min")

            with c2:
                if st.button("🗑️ Supprimer", key=f"del_inst_{inst.id}", help="Supprimer cette réunion et ses duplicates"):
                    session.delete(inst)
                    session.commit()
                    st.success("Réunion supprimée.")
                    st.rerun()

            # Existing duplicates
            if inst_sessions:
                st.markdown("**Duplicates :**")
                for ms in inst_sessions:
                    dc1, dc2 = st.columns([5, 1])
                    with dc1:
                        name_display = ms.session_name or f"Duplicate #{ms.id}"
                        emails = ms.get_invited() if hasattr(ms, "get_invited") else []
                        st.write(f"• **{name_display}** — {len(emails)} invité(s) — `{ms.status}`")
                    with dc2:
                        if st.button("🗑️", key=f"del_ms_{ms.id}", help="Supprimer ce duplicate"):
                            session.delete(ms)
                            session.commit()
                            st.rerun()

            # Form to add a duplicate
            with st.form(f"add_duplicate_{inst.id}"):
                st.markdown("**Ajouter un duplicate**")
                dup_name = st.text_input("Nom du duplicate", placeholder="Semaine 12 — Équipe B", key=f"dn_{inst.id}")
                dup_emails = st.text_area(
                    "Emails des participants (un par ligne)",
                    placeholder="pierre.dupont@example.com\nmarie.lambert@example.com",
                    height=80,
                    key=f"de_{inst.id}",
                )
                submitted_dup = st.form_submit_button("Créer le duplicate", use_container_width=True)

            if submitted_dup:
                if not dup_name.strip():
                    st.error("Le nom du duplicate est obligatoire.")
                else:
                    emails = [e.strip() for e in dup_emails.splitlines() if e.strip()]
                    ms = MeetingSession(
                        instance_id=inst.id,
                        status="draft",
                        session_name=dup_name.strip(),
                        invited_emails_json=json.dumps(emails),
                        attendees_json=json.dumps([]),
                        absents_json=json.dumps([]),
                    )
                    session.add(ms)
                    session.commit()
                    st.success(f"Duplicate « {dup_name} » créé.")
                    st.rerun()
