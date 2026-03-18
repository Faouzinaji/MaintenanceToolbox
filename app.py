import streamlit as st

from maintenance_toolbox.db import init_db, SessionLocal
from maintenance_toolbox.auth import render_login, get_current_user, logout_user
from maintenance_toolbox.home import render_home
from maintenance_toolbox.admin_ui import render_admin
from maintenance_toolbox.meetings.hub import render_meeting_hub
from maintenance_toolbox.settings_ui import render_settings


st.set_page_config(page_title="MaintenanceHub", layout="wide")

st.markdown(
    """
    <style>
    :root {
        --mn-orange: #f39200;
        --mn-dark: #3f434f;
        --mn-light: #f7f7f7;
        --mn-soft: #ead7b0;
    }

    .stApp { background-color: white; }

    h1, h2, h3 { color: var(--mn-dark); }

    .stButton > button {
        border-radius: 10px;
        border: 1px solid #d9d9d9;
    }

    .stButton > button[kind="primary"] {
        background-color: var(--mn-orange);
        color: white;
        border: 1px solid var(--mn-orange);
    }

    div[data-testid="stMetricValue"] { color: var(--mn-dark); }

    div[data-testid="stExpander"] details summary {
        color: var(--mn-dark);
        font-weight: 600;
    }

    /* Top nav active state */
    .nav-active > button {
        background: #fff3e0 !important;
        border-color: var(--mn-orange) !important;
        color: var(--mn-orange) !important;
        font-weight: 700 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

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

    # ── Top navigation ─────────────────────────────────────
    page = st.session_state["page"]

    nav_cols = st.columns([1.4, 1.2, 1.2, 1.2, 1.2, 8])

    with nav_cols[0]:
        active_cls = "nav-active" if page == "home" else ""
        st.markdown(f'<div class="{active_cls}">', unsafe_allow_html=True)
        if st.button("🏭 Cockpit", key="top_home", use_container_width=True):
            st.session_state["page"] = "home"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    with nav_cols[1]:
        if user.role == "admin":
            active_cls = "nav-active" if page == "admin" else ""
            st.markdown(f'<div class="{active_cls}">', unsafe_allow_html=True)
            if st.button("🛠️ Admin", key="top_admin", use_container_width=True):
                st.session_state["page"] = "admin"
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

    with nav_cols[2]:
        active_cls = "nav-active" if page == "settings" else ""
        st.markdown(f'<div class="{active_cls}">', unsafe_allow_html=True)
        if st.button("⚙️ Paramètres", key="top_settings", use_container_width=True):
            st.session_state["page"] = "settings"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    with nav_cols[3]:
        if st.button("🚪 Déconnexion", key="top_logout", use_container_width=True):
            logout_user()
            st.rerun()

    st.divider()

    # ── Page routing ────────────────────────────────────────
    if page == "home":
        render_home(user, session)

    elif page == "meeting_hub":
        render_meeting_hub(session, user)

    elif page == "settings":
        render_settings(session, user)

    elif page == "admin":
        render_admin(session, user)
