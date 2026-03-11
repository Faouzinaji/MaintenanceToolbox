from datetime import datetime, time
import io

import pandas as pd
import streamlit as st
from sqlalchemy import select

from maintenance_toolbox.db import Planning


def _combine_date_time(d, hhmm: str):
    h, m = hhmm.split(":")
    return datetime.combine(d, time(int(h), int(m)))


def _read_csv_safely(file_bytes: bytes) -> pd.DataFrame:
    candidates = [
        {"sep": ";", "encoding": "utf-8"},
        {"sep": ";", "encoding": "latin-1"},
        {"sep": ",", "encoding": "utf-8"},
        {"sep": ",", "encoding": "latin-1"},
        {"sep": "\t", "encoding": "utf-8"},
        {"sep": "\t", "encoding": "latin-1"},
    ]

    last_error = None
    for c in candidates:
        try:
            return pd.read_csv(
                io.BytesIO(file_bytes),
                sep=c["sep"],
                encoding=c["encoding"]
            )
        except Exception as e:
            last_error = e

    raise ValueError(f"Impossible de lire le CSV : {last_error}")


def render_scheduling_module(session, user):
    st.title("Scheduling")

    if not user.organization_id:
        st.warning("Aucune organisation n'est associée à cet utilisateur.")
        return

    tab1, tab2 = st.tabs(["Mes plannings", "Créer un planning"])

    with tab1:
        st.subheader("Mes plannings")

        try:
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
                            "CSV": "Oui" if p.csv_filename else "Non",
                            "Créé le": p.created_at,
                        }
                    )

                df = pd.DataFrame(rows)
                st.dataframe(df, use_container_width=True, hide_index=True)

                st.divider()
                st.subheader("Importer un CSV dans un planning")

                planning_options = {
                    f"{p.id} - {p.name}": p.id for p in plannings
                }

                selected_label = st.selectbox(
                    "Choisir un planning",
                    list(planning_options.keys())
                )
                selected_planning_id = planning_options[selected_label]
                selected_planning = next(p for p in plannings if p.id == selected_planning_id)

                uploaded_file = st.file_uploader(
                    "Charger un fichier CSV",
                    type=["csv"],
                    key=f"csv_upload_{selected_planning_id}"
                )

                if uploaded_file is not None:
                    file_bytes = uploaded_file.getvalue()

                    col1, col2 = st.columns([1, 1])

                    with col1:
                        if st.button("Enregistrer le CSV", use_container_width=True):
                            try:
                                selected_planning.csv_filename = uploaded_file.name
                                selected_planning.csv_bytes = file_bytes
                                selected_planning.updated_at = datetime.utcnow()
                                session.commit()
                                st.success(f"CSV enregistré dans le planning : {selected_planning.name}")
                                st.rerun()
                            except Exception as e:
                                session.rollback()
                                st.error(f"Erreur lors de l'enregistrement du CSV : {e}")

                    with col2:
                        if st.button("Aperçu du CSV", use_container_width=True):
                            try:
                                preview_df = _read_csv_safely(file_bytes)
                                st.write("Colonnes détectées :")
                                st.write(list(preview_df.columns))
                                st.dataframe(preview_df.head(20), use_container_width=True)
                            except Exception as e:
                                st.error(f"Erreur de lecture du CSV : {e}")

                if selected_planning.csv_filename and selected_planning.csv_bytes:
                    st.info(f"CSV déjà enregistré : {selected_planning.csv_filename}")
                    if st.button("Voir l'aperçu du CSV enregistré", use_container_width=True):
                        try:
                            preview_df = _read_csv_safely(selected_planning.csv_bytes)
                            st.write("Colonnes détectées :")
                            st.write(list(preview_df.columns))
                            st.dataframe(preview_df.head(20), use_container_width=True)
                        except Exception as e:
                            st.error(f"Erreur de lecture du CSV enregistré : {e}")

        except Exception as e:
            st.error(f"Erreur lors du chargement des plannings : {e}")

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
