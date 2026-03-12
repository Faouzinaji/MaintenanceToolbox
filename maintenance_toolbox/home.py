import streamlit as st


def render_home(user):
    st.title("MaintenanceToolbox")
    st.subheader("Industrial Maintenance Platform")
    st.write(f"Bienvenue **{user.full_name}**.")

    st.divider()

    col1, col2, col3 = st.columns([2, 3, 2])
    with col2:
        if st.button("📅 Scheduling", key="home_scheduling", use_container_width=True):
            st.session_state["page"] = "scheduling"
            st.rerun()
