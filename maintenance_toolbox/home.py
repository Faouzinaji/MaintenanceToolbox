import streamlit as st


def render_home(user):
    st.title("MaintenanceToolbox")
    st.subheader("Industrial Maintenance Platform")
    st.write(f"Bienvenue **{user.full_name}**.")
