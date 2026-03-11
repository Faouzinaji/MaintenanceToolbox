from datetime import datetime, time

import pandas as pd
import streamlit as st
from sqlalchemy import select

from maintenance_toolbox.db import Planning


def _combine_date_time(d, hhmm: str):
    h, m = hhmm.split(":")
    return datetime.combine(d, time(int(h), int(m)))


def render_scheduling_module(session, user):
    st.title("Scheduling")

    tab1, tab2 = st.tabs(["Mes plannings", "Créer un planning"])

    with tab1:
        st.subheader("Mes plannings")

        stmt = (
            select(Planning)
            .where(Planning.organization_id == user.organization_id)
            .order_by(Planning.created_at.desc())
        )
        plannings = session.scalars(stmt).all()

        if not plannings:
            st.info("Aucun planning pour le moment.")
        else:
            rows = []
            for p in plannings:
                rows.append(
                    {
                        "ID": p.id,
                        "Nom": p.name,
                        "Statut": p.status,
                        "Début": p.start_at,
                        "Fin": p.end_at,
                        "Ouverture": p.daily_open,
                        "Fermeture": p.daily_close,
                        "Secteurs": p.sectors_csv,
                        "Créé le": p.created_at,
                    }
                )

            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True, hide_index=True)

    with tab2:
        st.subheader("Créer un planning")

        with st.form("create_planning_form"):
            name = st.text_input("Nom du planning")
            sectors_txt = st.text_input("Secteurs (séparés par des virgules)")
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Date de début")
                daily_open = st.text_input("Heure d'ouverture", value="07:00")
            with col2:
                end_date = st.date_input("Date de fin")
                daily_close = st.text_input("Heure de fermeture", value="15:00")

            submitted = st.form_submit_button("Créer le planning", use_container_width=True)

        if submitted:
            if not name.strip():
                st.error("Le nom du planning est obligatoire.")
                return

            if end_date < start_date:
                st.error("La date de fin doit être postérieure ou égale à la date de début.")
                return

            try:
                start_at = _combine_date_time(start_date, daily_open)
                end_at = _combine_date_time(end_date, daily_close)

                planning = Planning(
                    organization_id=user.organization_id,
                    created_by_user_id=user.id,
                    name=name.strip(),
                    sectors_csv=sectors_txt.strip(),
                    start_at=start_at,
                    end_at=end_at,
                    daily_open=daily_open,
                    daily_close=daily_close,
                    status="draft",
                )

                session.add(planning)
                session.commit()

                st.success(f"Planning créé avec succès : {planning.name}")
                st.rerun()

            except Exception as e:
                session.rollback()
                st.error(f"Erreur lors de la création du planning : {e}")
