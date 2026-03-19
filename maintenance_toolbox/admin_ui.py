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

    st.title("⚙️ Administration — MaintenOps")

    tab1, tab2, tab3 = st.tabs(["Utilisateurs", "Organisations", "Réunions"])

    with tab1:
        _render_users_tab(session, current_user)

    with tab2:
        _render_orgs_tab(session, current_user)

    with tab3:
        _render_meetings_tab(session, current_user)


# ─────────────────────────────────────────────────────────
#  TAB 1 — USERS
# ─────────────────────────────────────────────────────────

def _render_users_tab(session, current_user) -> None:
    # ── Create user ──────────────────────────────────────
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
        org_name_sel = st.selectbox(
            "Organisation",
            list(org_options.keys()) if org_options else []
        )
        is_active = st.checkbox("Actif", value=True)
        submitted = st.form_submit_button("Créer l'utilisateur", use_container_width=True, type="primary")

    if submitted:
        if not email.strip():
            st.error("L'email est obligatoire.")
        elif not org_options:
            st.error("Aucune organisation disponible.")
        else:
            existing = session.scalar(select(User).where(User.email == email))
            if existing:
                st.error("Un utilisateur avec cet email existe déjà.")
            else:
                user = User(
                    full_name=full_name,
                    email=email,
                    role=role,
                    language="fr",
                    is_active=is_active,
                    first_login=True,
                    organization_id=org_options[org_name_sel],
                )
                user.set_password(password)
                session.add(user)
                session.commit()
                st.success("Utilisateur créé.")
                st.rerun()

    st.divider()

    # ── Edit/Delete inline ────────────────────────────────
    st.subheader("Liste des utilisateurs")

    # Edit state
    edit_user_id = st.session_state.get("_admin_edit_user_id")

    users = session.scalars(select(User).order_by(User.created_at.desc())).all()
    for u in users:
        org_label = next((o.name for o in organizations if o.id == u.organization_id), "—")
        is_editing = edit_user_id == u.id

        with st.container(border=True):
            if is_editing:
                # Inline edit form
                with st.form(f"edit_user_form_{u.id}"):
                    new_name = st.text_input("Nom complet", value=u.full_name)
                    new_email = st.text_input("Email", value=u.email)
                    new_role = st.selectbox("Rôle", ["user", "admin"],
                                            index=0 if u.role == "user" else 1)
                    new_org = st.selectbox(
                        "Organisation",
                        list(org_options.keys()),
                        index=list(org_options.keys()).index(org_label)
                        if org_label in org_options else 0,
                    )
                    new_active = st.checkbox("Actif", value=u.is_active)
                    new_pwd = st.text_input("Nouveau mot de passe (laisser vide = inchangé)", type="password")
                    c_save, c_cancel = st.columns(2)
                    with c_save:
                        do_save = st.form_submit_button("💾 Enregistrer", use_container_width=True, type="primary")
                    with c_cancel:
                        do_cancel = st.form_submit_button("Annuler", use_container_width=True)

                if do_save:
                    u.full_name = new_name
                    u.email = new_email
                    u.role = new_role
                    u.is_active = new_active
                    u.organization_id = org_options[new_org]
                    if new_pwd.strip():
                        u.set_password(new_pwd.strip())
                    session.commit()
                    st.session_state.pop("_admin_edit_user_id", None)
                    st.success("Utilisateur mis à jour.")
                    st.rerun()

                if do_cancel:
                    st.session_state.pop("_admin_edit_user_id", None)
                    st.rerun()

            else:
                c1, c2, c3 = st.columns([6, 1, 1])
                with c1:
                    st.markdown(
                        f"**{u.full_name}** &nbsp;·&nbsp; {u.email} &nbsp;·&nbsp; "
                        f"`{u.role}` &nbsp;·&nbsp; {org_label} &nbsp;·&nbsp; "
                        f"{'✅ actif' if u.is_active else '🔴 désactivé'}"
                    )
                with c2:
                    if st.button("✏️", key=f"edit_u_{u.id}", help="Modifier", use_container_width=True):
                        st.session_state["_admin_edit_user_id"] = u.id
                        st.rerun()
                with c3:
                    if u.id != current_user.id:  # Can't delete yourself
                        confirm_key = f"_confirm_del_user_{u.id}"
                        if st.session_state.get(confirm_key):
                            if st.button("🗑️ Confirmer", key=f"confirm_u_{u.id}",
                                         use_container_width=True, type="primary"):
                                session.delete(u)
                                session.commit()
                                st.session_state.pop(confirm_key, None)
                                st.success("Utilisateur supprimé.")
                                st.rerun()
                        else:
                            if st.button("🗑️", key=f"del_u_{u.id}", help="Supprimer",
                                         use_container_width=True):
                                st.session_state[confirm_key] = True
                                st.rerun()


# ─────────────────────────────────────────────────────────
#  TAB 2 — ORGANISATIONS
# ─────────────────────────────────────────────────────────

def _render_orgs_tab(session, current_user) -> None:
    st.subheader("Créer une organisation")

    with st.form("create_org_form"):
        org_name_input = st.text_input("Nom organisation")
        timezone_val = st.text_input("Timezone", value="Europe/Paris")
        st.markdown("**Administrateur de l'organisation**")
        admin_full_name = st.text_input("Nom complet de l'admin")
        admin_email = st.text_input("Email de l'admin")
        admin_password = st.text_input("Mot de passe temporaire", type="password", key="org_admin_pw")
        submitted_org = st.form_submit_button("Créer l'organisation", use_container_width=True, type="primary")

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
                st.error("Cette organisation existe déjà.")
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
                    st.rerun()

    st.divider()
    st.subheader("Liste des organisations")

    edit_org_id = st.session_state.get("_admin_edit_org_id")

    orgs = session.scalars(select(Organization).order_by(Organization.name)).all()
    for org in orgs:
        is_editing = edit_org_id == org.id

        with st.container(border=True):
            if is_editing:
                with st.form(f"edit_org_form_{org.id}"):
                    new_org_name = st.text_input("Nom", value=org.name)
                    new_tz = st.text_input("Timezone", value=org.timezone or "Europe/Paris")
                    new_active = st.checkbox("Active", value=org.active)
                    c_save, c_cancel = st.columns(2)
                    with c_save:
                        do_save = st.form_submit_button("💾 Enregistrer", use_container_width=True, type="primary")
                    with c_cancel:
                        do_cancel = st.form_submit_button("Annuler", use_container_width=True)

                if do_save:
                    org.name = new_org_name
                    org.timezone = new_tz
                    org.active = new_active
                    session.commit()
                    st.session_state.pop("_admin_edit_org_id", None)
                    st.success("Organisation mise à jour.")
                    st.rerun()

                if do_cancel:
                    st.session_state.pop("_admin_edit_org_id", None)
                    st.rerun()

            else:
                c1, c2, c3 = st.columns([6, 1, 1])
                with c1:
                    st.markdown(f"**{org.name}** &nbsp;·&nbsp; {org.timezone}")
                with c2:
                    if st.button("✏️", key=f"edit_org_{org.id}", help="Modifier", use_container_width=True):
                        st.session_state["_admin_edit_org_id"] = org.id
                        st.rerun()
                with c3:
                    confirm_key = f"_confirm_del_org_{org.id}"
                    if st.session_state.get(confirm_key):
                        st.warning("Supprime toutes les données liées !")
                        if st.button("🗑️ Confirmer", key=f"confirm_org_{org.id}",
                                     use_container_width=True, type="primary"):
                            session.delete(org)
                            session.commit()
                            st.session_state.pop(confirm_key, None)
                            st.success("Organisation supprimée.")
                            st.rerun()
                    else:
                        if st.button("🗑️", key=f"del_org_{org.id}", help="Supprimer",
                                     use_container_width=True):
                            st.session_state[confirm_key] = True
                            st.rerun()


# ─────────────────────────────────────────────────────────
#  TAB 3 — MEETINGS
# ─────────────────────────────────────────────────────────

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

            default_agenda = "\n".join(
                json.loads(selected_mt.agenda_json) if selected_mt.agenda_json else []
            )
            custom_agenda_raw = st.text_area(
                "Agenda (un point par ligne)",
                value=default_agenda,
                height=150,
                help="Modifiez l'agenda pour personnaliser cette réunion.",
            )

            default_duration = selected_mt.duration_minutes or 60
            custom_duration = st.number_input(
                "Durée (minutes)", min_value=15, max_value=480,
                value=default_duration, step=15,
            )

            submitted_meeting = st.form_submit_button(
                "Créer la réunion", use_container_width=True, type="primary"
            )

        if submitted_meeting:
            if not meeting_name.strip():
                st.error("Le nom de la réunion est obligatoire.")
            else:
                custom_agenda = [l.strip() for l in custom_agenda_raw.splitlines() if l.strip()]
                template_agenda = json.loads(selected_mt.agenda_json) if selected_mt.agenda_json else []
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

    # ── List meetings + manage duplicates ──
    st.subheader("Réunions existantes")

    all_instances = session.scalars(
        select(MeetingInstance).order_by(MeetingInstance.created_at.desc())
    ).all()
    all_sessions_list = session.scalars(select(MeetingSession)).all()
    sessions_by_instance: dict[int, list[MeetingSession]] = {}
    for ms in all_sessions_list:
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

        with st.expander(
            f"**{inst.name}** · {mt_label} · {org.name if org else '—'} · {len(inst_sessions)} duplicate(s)"
        ):
            c1, c2 = st.columns([6, 1])
            with c1:
                duration = inst.effective_duration() if hasattr(inst, "effective_duration") else 60
                st.caption(f"Durée : {duration} min")
            with c2:
                confirm_key = f"_confirm_del_inst_{inst.id}"
                if st.session_state.get(confirm_key):
                    if st.button("🗑️ Confirmer", key=f"confirm_inst_{inst.id}",
                                 use_container_width=True, type="primary"):
                        session.delete(inst)
                        session.commit()
                        st.session_state.pop(confirm_key, None)
                        st.success("Réunion supprimée.")
                        st.rerun()
                else:
                    if st.button("🗑️ Supprimer", key=f"del_inst_{inst.id}", use_container_width=True):
                        st.session_state[confirm_key] = True
                        st.rerun()

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

            with st.form(f"add_duplicate_{inst.id}"):
                st.markdown("**Ajouter un duplicate**")
                dup_name = st.text_input(
                    "Nom du duplicate",
                    placeholder="Semaine 12 — Équipe B",
                    key=f"dn_{inst.id}",
                )
                dup_emails = st.text_area(
                    "Emails des participants (un par ligne)",
                    placeholder="pierre@example.com\nmarie@example.com",
                    height=80,
                    key=f"de_{inst.id}",
                )
                submitted_dup = st.form_submit_button("Créer le duplicate", use_container_width=True)

            if submitted_dup:
                if not dup_name.strip():
                    st.error("Le nom du duplicate est obligatoire.")
                else:
                    emails = [e.strip() for e in dup_emails.splitlines() if e.strip()]
                    ms_new = MeetingSession(
                        instance_id=inst.id,
                        status="draft",
                        session_name=dup_name.strip(),
                        invited_emails_json=json.dumps(emails),
                        attendees_json=json.dumps([]),
                        absents_json=json.dumps([]),
                    )
                    session.add(ms_new)
                    session.commit()
                    st.success(f"Duplicate « {dup_name} » créé.")
                    st.rerun()
