import streamlit as st

from maintenance_toolbox.db import init_db, SessionLocal
from maintenance_toolbox.auth import render_login, get_current_user, logout_user
from maintenance_toolbox.home import render_home
from maintenance_toolbox.admin_ui import render_admin
from maintenance_toolbox.scheduling.ui import render_scheduling_module


st.set_page_config(page_title="MaintenanceToolbox", layout="wide")

try:
    init_db()
except Exception as e:
    st.error(f"Erreur base de données : {e}")
    st.stop()

if "page" not in st.session_state:
    st.session_state["page"] = "home"

with SessionLocal() as session:
    user = get_current_user(session)

    if not user:
        render_login(session)
        st.stop()

    st.sidebar.title("MaintenanceToolbox")
    st.sidebar.write(f"Connecté : **{user.full_name}**")
    st.sidebar.caption(user.email)

    if st.sidebar.button("Accueil", use_container_width=True):
        st.session_state["page"] = "home"

    if st.sidebar.button("Scheduling", use_container_width=True):
        st.session_state["page"] = "scheduling"

    if user.role == "admin":
        if st.sidebar.button("Admin", use_container_width=True):
            st.session_state["page"] = "admin"

    if st.sidebar.button("Logout", use_container_width=True):
        logout_user()
        st.rerun()

    page = st.session_state["page"]

    if page == "home":
        render_home(user)
    elif page == "scheduling":
        render_scheduling_module(session, user)
    elif page == "admin":
        render_admin(session, user)
