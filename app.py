import streamlit as st

from maintenance_toolbox.db import init_db, SessionLocal
from maintenance_toolbox.auth import render_login, get_current_user, logout_user
from maintenance_toolbox.home import render_home
from maintenance_toolbox.admin_ui import render_admin
from maintenance_toolbox.meetings.hub import render_meeting_hub
from maintenance_toolbox.settings_ui import render_settings


st.set_page_config(
    page_title="MaintenOps",
    page_icon="🔧",
    layout="wide",
)

# ── Global CSS ───────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    /* ── Mainnovation Brand Palette ──────────────────────────────────────── */
    :root {
        --mn-orange:  #F5A623;
        --mn-grey:    #6B6B6B;
        --mn-white:   #FFFFFF;
        --mn-light:   #F2F2F2;
        --mn-dark:    #3f434f;
        --mn-soft:    #fde8c0;
    }

    .stApp { background-color: var(--mn-light); }

    h1, h2, h3 { color: var(--mn-grey); font-weight: 700; }

    /* ── Sidebar ─────────────────────────────────────────────────────────── */
    section[data-testid="stSidebar"] {
        background-color: var(--mn-grey) !important;
    }
    section[data-testid="stSidebar"] * { color: var(--mn-white) !important; }

    /* ── Cards / containers ──────────────────────────────────────────────── */
    div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlockBorderWrapper"] {
        background-color: var(--mn-white);
        border-radius: 10px;
        border: 1px solid #e0e0e0;
        padding: 12px;
    }

    /* ── Nav / buttons ───────────────────────────────────────────────────── */
    .stButton > button {
        border-radius: 8px;
        border: 1px solid #d9d9d9;
        font-size: 0.9rem;
        padding: 6px 14px;
        background-color: var(--mn-white);
        color: var(--mn-grey);
        transition: all 0.15s ease;
    }

    /* Primary = orange */
    .stButton > button[kind="primary"] {
        background-color: var(--mn-orange) !important;
        color: var(--mn-white) !important;
        border: 1px solid var(--mn-orange) !important;
        font-weight: 700;
    }
    .stButton > button[kind="primary"]:hover {
        background-color: #e09410 !important;
        border-color: #e09410 !important;
    }

    /* Secondary = ghost */
    .stButton > button[kind="secondary"] {
        background-color: var(--mn-white);
        color: var(--mn-grey);
        border: 1px solid #d0d0d0;
    }
    .stButton > button[kind="secondary"]:hover {
        border-color: var(--mn-orange);
        color: var(--mn-orange);
    }

    /* ── Download button ─────────────────────────────────────────────────── */
    .stDownloadButton > button {
        background-color: var(--mn-orange) !important;
        color: var(--mn-white) !important;
        border-color: var(--mn-orange) !important;
        font-weight: 700;
        border-radius: 8px;
    }
    .stDownloadButton > button:hover {
        background-color: #e09410 !important;
    }

    /* ── Metrics ─────────────────────────────────────────────────────────── */
    div[data-testid="stMetricValue"] {
        color: var(--mn-orange);
        font-weight: 700;
    }
    div[data-testid="stMetricLabel"] { color: var(--mn-grey); }

    /* ── Expanders ───────────────────────────────────────────────────────── */
    div[data-testid="stExpander"] details {
        background-color: var(--mn-white);
        border: 1px solid #e8e8e8;
        border-radius: 8px;
        margin-bottom: 6px;
    }
    div[data-testid="stExpander"] details summary {
        color: var(--mn-grey);
        font-weight: 600;
        padding: 10px 14px;
    }
    div[data-testid="stExpander"] details summary:hover {
        color: var(--mn-orange);
    }

    /* ── Status badges ───────────────────────────────────────────────────── */
    .mn-badge-success {
        display: inline-block;
        background: #28a745;
        color: white;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.82rem;
        font-weight: 700;
    }
    .mn-badge-orange {
        display: inline-block;
        background: var(--mn-orange);
        color: white;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.82rem;
        font-weight: 700;
    }

    /* ── Header banner ───────────────────────────────────────────────────── */
    .mn-page-header {
        background: linear-gradient(90deg, var(--mn-grey) 0%, #4a4e5a 100%);
        color: var(--mn-white);
        padding: 14px 20px;
        border-radius: 10px;
        margin-bottom: 16px;
    }
    .mn-page-header h2 { color: var(--mn-white) !important; margin: 0; }

    /* ── Meeting cards ───────────────────────────────────────────────────── */
    .mn-meeting-card {
        background: var(--mn-white);
        border-left: 4px solid var(--mn-orange);
        padding: 12px 16px;
        border-radius: 6px;
        margin-bottom: 8px;
    }

    /* ── Sticky timer banner ─────────────────────────────────────────────── */
    .mn-timer-sticky {
        position: sticky;
        top: 0;
        z-index: 990;
        background: var(--mn-white);
        padding: 6px 0 10px 0;
        border-bottom: 2px solid var(--mn-light);
        margin-bottom: 12px;
    }

    /* ── Timer alert pulse ───────────────────────────────────────────────── */
    @keyframes mn-pulse {
        0%   { box-shadow: 0 0 0 0 rgba(220,53,69,0.6); }
        70%  { box-shadow: 0 0 0 12px rgba(220,53,69,0); }
        100% { box-shadow: 0 0 0 0 rgba(220,53,69,0); }
    }
    .mn-timer-alert {
        animation: mn-pulse 1.4s ease-in-out infinite;
        border-radius: 8px;
    }

    /* ── Radio tabs ──────────────────────────────────────────────────────── */
    div[data-testid="stRadio"] label {
        font-weight: 600;
        color: var(--mn-grey);
    }

    /* ── Dataframes ──────────────────────────────────────────────────────── */
    div[data-testid="stDataFrame"] {
        border: 1px solid #e0e0e0;
        border-radius: 6px;
        overflow: hidden;
    }

    /* ── Loading overlay ─────────────────────────────────────────────────── */
    .mn-loading-overlay {
        position: fixed;
        inset: 0;
        background: rgba(107,107,107,0.18);
        z-index: 9999;
        display: none;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── DB init ──────────────────────────────────────────────────────────────────
with st.spinner("Chargement…"):
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

    # ── Top navigation ───────────────────────────────────────────────────────
    page = st.session_state["page"]

    # Build nav items dynamically
    nav_items = [("🏭 Cockpit", "home"), ("⚙️ Paramètres", "settings")]
    if user.role == "admin":
        nav_items.insert(1, ("🛠️ Admin", "admin"))
    nav_items.append(("🚪 Déconnexion", "_logout"))

    # Narrow columns for nav buttons + spacer on the right
    nav_widths = [1.5] * len(nav_items) + [max(1, 10 - 1.5 * len(nav_items))]
    nav_cols = st.columns(nav_widths)

    for i, (label, target) in enumerate(nav_items):
        with nav_cols[i]:
            is_active = page == target
            if target == "_logout":
                if st.button(label, key="top_logout", use_container_width=True):
                    logout_user()
                    st.rerun()
            else:
                btn_type = "primary" if is_active else "secondary"
                if st.button(label, key=f"top_{target}", use_container_width=True, type=btn_type):
                    st.session_state["page"] = target
                    st.rerun()

    st.divider()

    # ── Page routing ─────────────────────────────────────────────────────────
    if page == "home":
        render_home(user, session)

    elif page == "meeting_hub":
        render_meeting_hub(session, user)

    elif page == "settings":
        render_settings(session, user)

    elif page == "admin":
        render_admin(session, user)
