"""Scheduling meeting content — focuses on REX and plan d'action."""
from __future__ import annotations

import streamlit as st

from maintenance_toolbox.scheduling.ui import render_scheduling_module


def render_scheduling_content(db_session, user) -> None:
    st.markdown(
        """<div style="background:#fff9f0;border-left:5px solid #f39200;
        padding:10px 16px;border-radius:8px;margin-bottom:16px;">
        📅 <strong>Scheduling</strong> — Naviguez vers l'onglet <strong>REX</strong>
        pour analyser les écarts du planning précédent, documenter les causes
        et construire le plan d'action correctif.
        </div>""",
        unsafe_allow_html=True,
    )
    render_scheduling_module(db_session, user)
