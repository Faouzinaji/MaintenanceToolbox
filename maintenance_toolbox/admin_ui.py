from __future__ import annotations

import json
from datetime import datetime, timezone

import streamlit as st
from sqlalchemy import select

from maintenance_toolbox.db import (
    User, Organization, MeetingType, MeetingInstance,
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
            org_name = st.text_input("Nom organisation")
            timezone_val = st.text_input("Timezone", value="Europe/Paris")
            submitted_org = st.form_submit_button("Créer l'organisation", use_container_width=True)

        if submitted_org:
            existing_org = session.scalar(
                select(Organization).where(Organization.name == org_name)
            )
            if existing_org:
                st.error("Cette organisation existe déjà")
            else:
                org = Organization(name=org_name, timezone=timezone_val, active=True)
                session.add(org)
                session.commit()
                session.refresh(org)
                ensure_org_defaults(session, org)
                st.success("Organisation créée")

        st.divider()
        st.subheader("Liste des organisations")
        orgs = session.scalars(select(Organization).order_by(Organization.name)).all()
        for org in orgs:
            st.write(f"**{org.name}** — timezone: {org.timezone}")

    # ── TAB 3: Meeting instances ────────────────────────────
    with tab3:
        st.subheader("Gérer les instances de réunion")

        all_types = session.scalars(
            select(MeetingType).order_by(MeetingType.order_index)
        ).all()
        active_types = [mt for mt in all_types if mt.active]

        organizations = session.scalars(
            select(Organization).order_by(Organization.name)
        ).all()

        if not active_types:
            st.info("Aucun type de réunion actif.")
        else:
            with st.form("create_instance_admin_form"):
                st.markdown("**Créer une instance**")
                type_options = {mt.name: mt.id for mt in active_types}
                selected_type_name = st.selectbox("Type de réunion", list(type_options.keys()))
                org_options_admin = {o.name: o.id for o in organizations}
                selected_org_name = st.selectbox(
                    "Organisation",
                    list(org_options_admin.keys()) if org_options_admin else [],
                )
                instance_name = st.text_input("Nom de l'instance", placeholder="Pré-scheduling S12")
                scheduled_date = st.date_input("Date planifiée")
                participants_raw = st.text_area(
                    "Participants (un par ligne)",
                    placeholder="Pierre Dupont\nMarie Lambert\n...",
                    height=100,
                )
                submitted_inst = st.form_submit_button("Créer l'instance", use_container_width=True)

            if submitted_inst:
                if not instance_name.strip():
                    st.error("Le nom est obligatoire.")
                elif not org_options_admin:
                    st.error("Aucune organisation disponible.")
                else:
                    participants = [p.strip() for p in participants_raw.splitlines() if p.strip()]
                    sched_dt = datetime.combine(
                        scheduled_date, datetime.min.time()
                    ).replace(tzinfo=timezone.utc)
                    inst = MeetingInstance(
                        meeting_type_id=type_options[selected_type_name],
                        organization_id=org_options_admin[selected_org_name],
                        name=instance_name.strip(),
                        scheduled_date=sched_dt,
                        participants_json=json.dumps(participants),
                        created_by_user_id=current_user.id,
                    )
                    session.add(inst)
                    session.commit()
                    st.success(f"Instance « {instance_name} » créée.")
                    st.rerun()

        st.divider()
        st.subheader("Instances existantes")

        all_instances = session.scalars(
            select(MeetingInstance).order_by(MeetingInstance.scheduled_date.desc())
        ).all()

        if not all_instances:
            st.info("Aucune instance créée.")
        else:
            for inst in all_instances:
                mt = next((t for t in all_types if t.id == inst.meeting_type_id), None)
                org = next((o for o in organizations if o.id == inst.organization_id), None)
                participants = json.loads(inst.participants_json or "[]")
                date_str = inst.scheduled_date.strftime("%d/%m/%Y") if inst.scheduled_date else "—"

                with st.container(border=True):
                    c1, c2, c3, c4 = st.columns([3, 2, 2, 1])
                    with c1:
                        mt_label = f"{mt.icon} {mt.name}" if mt else "—"
                        st.markdown(f"**{inst.name}** · {mt_label}")
                    with c2:
                        st.caption(f"📅 {date_str}")
                    with c3:
                        st.caption(f"🏢 {org.name if org else '—'} · 👥 {len(participants)}")
                    with c4:
                        if st.button("🗑️", key=f"del_inst_{inst.id}", help="Supprimer cette instance"):
                            session.delete(inst)
                            session.commit()
                            st.success("Instance supprimée.")
                            st.rerun()
