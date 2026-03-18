"""Pré-scheduling meeting content — embeds the scheduling wizard."""
from __future__ import annotations

import streamlit as st

from maintenance_toolbox.scheduling.ui import render_scheduling_module


def render_pre_scheduling_content(db_session, user) -> None:
    st.markdown(
        """<div style="background:#fff9f0;border-left:5px solid #f39200;
        padding:10px 16px;border-radius:8px;margin-bottom:16px;">
        📋 <strong>Pré-scheduling</strong> — Utilisez le wizard ci-dessous pour sélectionner
        les OT, configurer les équipes et générer le planning de l'arrêt.
        Exportez le planning validé en fin de séance.
        </div>""",
        unsafe_allow_html=True,
    )
    render_scheduling_module(db_session, user)
