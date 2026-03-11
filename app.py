import streamlit as st
from maintenance_toolbox.db import init_db, SessionLocal
from maintenance_toolbox.home import render_home
from maintenance_toolbox.scheduling.ui import render_scheduling_module

st.set_page_config(page_title="MaintenanceToolbox", layout="wide")

init_db()

if "page" not in st.session_state:
    st.session_state["page"] = "home"

with SessionLocal() as session:

    st.sidebar.title("MaintenanceToolbox")

    if st.sidebar.button("Accueil", use_container_width=True):
        st.session_state["page"] = "home"

    if st.sidebar.button("Scheduling", use_container_width=True):
        st.session_state["page"] = "scheduling"

    if st.session_state["page"] == "home":
        render_home()

    if st.session_state["page"] == "scheduling":
        render_scheduling_module(session)
