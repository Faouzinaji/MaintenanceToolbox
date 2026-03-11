import streamlit as st

def render_home():
    st.title("MaintenanceToolbox")
    st.subheader("Industrial Maintenance Platform")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("📅 Scheduling", use_container_width=True):
            st.session_state["page"] = "scheduling"

    with col2:
        st.button("🧠 Reliability / RCA (coming soon)", disabled=True, use_container_width=True)

    with col3:
        st.button("📦 Stock / ABC (coming soon)", disabled=True, use_container_width=True)
