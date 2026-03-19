import streamlit as st
from sqlalchemy import select

from maintenance_toolbox.db import User


def get_current_user(session):
    user_id = st.session_state.get("user_id")
    if not user_id:
        return None
    return session.get(User, user_id)


def login_user(user):
    st.session_state["user_id"] = user.id
    st.session_state["user_role"] = user.role
    st.session_state["user_name"] = user.full_name
    st.session_state["user_org_id"] = user.organization_id


def logout_user():
    for key in list(st.session_state.keys()):
        del st.session_state[key]


def render_login(session):
    st.title("🔧 MaintenOps")
    st.subheader("Connexion")

    with st.form("login_form"):
        email = st.text_input("Email")
        password = st.text_input("Mot de passe", type="password")
        submitted = st.form_submit_button("Se connecter", use_container_width=True)

    if submitted:
        user = session.scalar(select(User).where(User.email == email))

        if user is None:
            st.error("Utilisateur introuvable")
            return

        if not user.is_active:
            st.error("Utilisateur désactivé")
            return

        if not user.check_password(password):
            st.error("Mot de passe incorrect")
            return

        login_user(user)
        st.success("Connexion réussie")
        st.rerun()
