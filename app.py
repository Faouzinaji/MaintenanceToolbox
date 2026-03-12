import streamlit as st

from maintenance_toolbox.db import init_db, SessionLocal
from maintenance_toolbox.auth import render_login, get_current_user, logout_user
from maintenance_toolbox.home import render_home
from maintenance_toolbox.admin_ui import render_admin
from maintenance_toolbox.scheduling.ui import render_scheduling_module
from maintenance_toolbox.settings_ui import render_settings


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

    c1, c2, c3, c4, c5 = st.columns([1.2, 1.4, 1.2, 1.2, 8])

    with c1:
        if st.button("🏠 Accueil", key="top_home", use_container_width=True):
            st.session_state["page"] = "home"
            st.rerun()

    with c2:
        if st.button("📅 Scheduling", key="top_scheduling", use_container_width=True):
            st.session_state["page"] = "scheduling"
            st.rerun()

    with c3:
        if st.button("⚙️ Settings", key="top_settings", use_container_width=True):
            st.session_state["page"] = "settings"
            st.rerun()

    with c4:
        if st.button("🚪 Logout", key="top_logout", use_container_width=True):
            logout_user()
            st.rerun()

    st.divider()

    page = st.session_state["page"]

    if page == "home":
        render_home(user)
    elif page == "scheduling":
        render_scheduling_module(session, user)
    elif page == "settings":
        render_settings(session, user)
    elif page == "admin":
        render_admin(session, user)
