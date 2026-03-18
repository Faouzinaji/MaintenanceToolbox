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

with st.spinner("Initialisation…"):
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

    n_nav = 4 if user.role == "admin" else 3
    nav_cols = st.columns([1.4] * n_nav + [8 - 1.4 * (n_nav - 3)])

    col_idx = 0
    with nav_cols[col_idx]:
        col_idx += 1
        if st.button("🏭 Cockpit", key="top_home", use_container_width=True,
                     type="primary" if page == "home" else "secondary"):
            st.session_state["page"] = "home"
            st.rerun()

    if user.role == "admin":
        with nav_cols[col_idx]:
            col_idx += 1
            if st.button("🛠️ Admin", key="top_admin", use_container_width=True,
                         type="primary" if page == "admin" else "secondary"):
                st.session_state["page"] = "admin"
                st.rerun()

    with nav_cols[col_idx]:
        col_idx += 1
        if st.button("⚙️ Paramètres", key="top_settings", use_container_width=True,
                     type="primary" if page == "settings" else "secondary"):
            st.session_state["page"] = "settings"
            st.rerun()

    with nav_cols[col_idx]:
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
