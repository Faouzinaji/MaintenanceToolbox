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
    :root {
        --mn-orange: #f39200;
        --mn-dark:   #3f434f;
        --mn-light:  #f7f7f7;
        --mn-soft:   #ead7b0;
    }

    .stApp { background-color: white; }

    h1, h2, h3 { color: var(--mn-dark); }

    /* ── Unified nav / tab button style ──────────────────────────────────── */
    .stButton > button {
        border-radius: 8px;
        border: 1px solid #d9d9d9;
        font-size: 0.9rem;
        padding: 6px 12px;
    }

    /* Primary = orange (active page or primary action) */
    .stButton > button[kind="primary"] {
        background-color: var(--mn-orange);
        color: white !important;
        border: 1px solid var(--mn-orange);
        font-weight: 600;
    }
    .stButton > button[kind="primary"]:hover {
        background-color: #e08700;
        border-color: #e08700;
    }

    /* Secondary = ghost with dark border */
    .stButton > button[kind="secondary"] {
        background-color: white;
        color: var(--mn-dark);
        border: 1px solid #d9d9d9;
    }
    .stButton > button[kind="secondary"]:hover {
        border-color: var(--mn-orange);
        color: var(--mn-orange);
    }

    div[data-testid="stMetricValue"] { color: var(--mn-dark); }

    div[data-testid="stExpander"] details summary {
        color: var(--mn-dark);
        font-weight: 600;
    }

    /* ── Sticky timer banner ─────────────────────────────────────────────── */
    .mn-timer-sticky {
        position: sticky;
        top: 0;
        z-index: 990;
        background: white;
        padding: 6px 0 10px 0;
        border-bottom: 2px solid #f0f0f0;
        margin-bottom: 12px;
    }

    /* ── Red pulsing animation for timer in alert ────────────────────────── */
    @keyframes mn-pulse {
        0%   { box-shadow: 0 0 0 0 rgba(220,53,69,0.6); }
        70%  { box-shadow: 0 0 0 12px rgba(220,53,69,0); }
        100% { box-shadow: 0 0 0 0 rgba(220,53,69,0); }
    }
    .mn-timer-alert {
        animation: mn-pulse 1.4s ease-in-out infinite;
        border-radius: 8px;
    }

    /* ── Page-dim overlay during loading ────────────────────────────────── */
    /* Triggered by adding .mn-loading class to body via JS — lightweight */
    .mn-loading-overlay {
        position: fixed;
        inset: 0;
        background: rgba(63,67,79,0.18);
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
