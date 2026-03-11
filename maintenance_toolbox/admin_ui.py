import streamlit as st
from sqlalchemy import select
from maintenance_toolbox.db import User, Organization


def render_admin(session, current_user):
    if current_user.role != "admin":
        st.error("Accès refusé")
        return

    st.title("Administration")

    tab1, tab2 = st.tabs(["Utilisateurs", "Organisations"])

    with tab1:
        st.subheader("Créer un utilisateur")

        organizations = session.scalars(select(Organization).order_by(Organization.name)).all()
        org_options = {org.name: org.id for org in organizations}

        with st.form("create_user_form"):
            full_name = st.text_input("Nom complet")
            email = st.text_input("Email")
            password = st.text_input("Mot de passe temporaire", type="password")
            role = st.selectbox("Rôle", ["user", "admin"])
            org_name = st.selectbox("Organisation", list(org_options.keys()) if org_options else [])
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

    with tab2:
        st.subheader("Créer une organisation")

        with st.form("create_org_form"):
            org_name = st.text_input("Nom organisation")
            timezone = st.text_input("Timezone", value="Europe/Paris")
            submitted_org = st.form_submit_button("Créer l'organisation", use_container_width=True)

        if submitted_org:
            existing_org = session.scalar(select(Organization).where(Organization.name == org_name))
            if existing_org:
                st.error("Cette organisation existe déjà")
            else:
                org = Organization(name=org_name, timezone=timezone, active=True)
                session.add(org)
                session.commit()
                st.success("Organisation créée")

        st.divider()
        st.subheader("Liste des organisations")

        orgs = session.scalars(select(Organization).order_by(Organization.name)).all()
        for org in orgs:
            st.write(f"**{org.name}** — timezone: {org.timezone}")
