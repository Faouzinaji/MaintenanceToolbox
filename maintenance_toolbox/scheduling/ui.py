from datetime import datetime, time, timezone
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
                encoding=c["encoding"],
                low_memory=False,
            )
        except Exception as e:
            last_error = e

    raise ValueError(f"Impossible de lire le CSV : {last_error}")


def _get_current_planning(session, planning_id):
    if not planning_id:
        return None
    return session.get(Planning, planning_id)


def _init_wizard_state():
    defaults = {
        "wizard_planning_id": None,
        "wizard_active_section": 1,
        "wizard_csv_columns": [],
        "wizard_mapping": {},
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _reset_wizard():
    st.session_state["wizard_planning_id"] = None
    st.session_state["wizard_active_section"] = 1
    st.session_state["wizard_csv_columns"] = []
    st.session_state["wizard_mapping"] = {}


def render_scheduling_module(session, user):
    st.title("Scheduling")

    if not user.organization_id:
        st.warning("Aucune organisation n'est associée à cet utilisateur.")
        return

    _init_wizard_state()

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

        except Exception as e:
            st.error(f"Erreur lors du chargement des plannings : {e}")

    with tab2:
        planning = _get_current_planning(session, st.session_state["wizard_planning_id"])

        top_col1, top_col2 = st.columns([3, 1])
        with top_col1:
            st.subheader("Wizard de création de planning")
        with top_col2:
            if st.button("Nouveau planning", use_container_width=True):
                _reset_wizard()
                st.rerun()

        if planning:
            st.info(
                f"Planning en cours : **{planning.name}** | "
                f"Statut : **{planning.status}** | "
                f"Période : **{planning.start_at} → {planning.end_at}**"
            )

        # =========================================================
        # 1. PARAMÈTRES
        # =========================================================
        with st.expander(
            "1. Paramètres de l'arrêt",
            expanded=st.session_state["wizard_active_section"] == 1,
        ):
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

                submitted = st.form_submit_button("Valider les paramètres", use_container_width=True)

            if submitted:
                if not name.strip():
                    st.error("Le nom du planning est obligatoire.")
                    st.stop()

                if end_date < start_date:
                    st.error("La date de fin doit être postérieure ou égale à la date de début.")
                    st.stop()

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
                    session.refresh(planning)

                    st.session_state["wizard_planning_id"] = planning.id
                    st.session_state["wizard_active_section"] = 2

                    st.success(f"Planning créé avec succès : {planning.name}")
                    st.rerun()

                except Exception as e:
                    session.rollback()
                    st.error(f"Erreur lors de la création du planning : {e}")

        planning = _get_current_planning(session, st.session_state["wizard_planning_id"])
        if not planning:
            return

        # =========================================================
        # 2. IMPORT CSV
        # =========================================================
        with st.expander(
            "2. Import CSV",
            expanded=st.session_state["wizard_active_section"] == 2,
        ):
            st.write("Charge le fichier CSV directement dans ce planning.")

            uploaded_file = st.file_uploader(
                "Charger un fichier CSV",
                type=["csv"],
                key=f"csv_upload_{planning.id}"
            )

            if planning.csv_filename:
                st.success(f"CSV déjà enregistré : {planning.csv_filename}")

            if uploaded_file is not None:
                file_bytes = uploaded_file.getvalue()

                col1, col2 = st.columns(2)

                with col1:
                    if st.button("Enregistrer le CSV", use_container_width=True):
                        try:
                            planning.csv_filename = uploaded_file.name
                            planning.csv_bytes = file_bytes
                            planning.updated_at = datetime.now(timezone.utc)
                            session.commit()

                            preview_df = _read_csv_safely(file_bytes)
                            st.session_state["wizard_csv_columns"] = list(preview_df.columns)
                            st.session_state["wizard_active_section"] = 3

                            st.success("CSV enregistré avec succès.")
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

            if planning.csv_bytes:
                try:
                    preview_df = _read_csv_safely(planning.csv_bytes)
                    st.write("Colonnes détectées dans le CSV enregistré :")
                    st.write(list(preview_df.columns))
                    st.dataframe(preview_df.head(10), use_container_width=True)
                except Exception as e:
                    st.error(f"Erreur de lecture du CSV enregistré : {e}")

        # =========================================================
        # 3. MAPPING
        # =========================================================
        with st.expander(
            "3. Mapping colonnes",
            expanded=st.session_state["wizard_active_section"] == 3,
        ):
            if not planning.csv_bytes:
                st.info("Charge d'abord un CSV pour passer au mapping.")
            else:
                try:
                    df = _read_csv_safely(planning.csv_bytes)
                    columns = list(df.columns)

                    st.write("Associe les colonnes du fichier aux champs standard.")

                    current_mapping = st.session_state.get("wizard_mapping", {})

                    with st.form("mapping_form"):
                        mapping = {}

                        targets = [
                            ("ot_id", "OT"),
                            ("description", "Description"),
                            ("status", "Statut"),
                            ("atelier", "Atelier"),
                            ("secteur", "Secteur"),
                            ("equipment", "Equipement"),
                            ("equipment_desc", "Description équipement"),
                            ("created_at", "Créé le"),
                            ("created_by", "Créé par"),
                            ("requested_week", "Sem. souhaitée"),
                            ("condition", "Condition réalisation"),
                            ("estimated_hours", "Durée estimée"),
                        ]

                        options = [""] + columns

                        for key, label in targets:
                            default_val = current_mapping.get(key, "")
                            default_index = options.index(default_val) if default_val in options else 0
                            mapping[key] = st.selectbox(
                                label,
                                options,
                                index=default_index,
                                key=f"map_{key}"
                            )

                        submitted_mapping = st.form_submit_button(
                            "Valider le mapping",
                            use_container_width=True
                        )

                    if submitted_mapping:
                        if not mapping["ot_id"] or not mapping["description"] or not mapping["atelier"]:
                            st.error("Au minimum, mappe OT, Description et Atelier.")
                        else:
                            st.session_state["wizard_mapping"] = mapping
                            st.session_state["wizard_active_section"] = 4
                            st.success("Mapping validé.")
                            st.rerun()

                except Exception as e:
                    st.error(f"Erreur lors du mapping : {e}")

        # =========================================================
        # 4. SÉLECTION OT
        # =========================================================
        with st.expander(
            "4. Sélection des OT",
            expanded=st.session_state["wizard_active_section"] == 4,
        ):
            st.info("Étape suivante prête : on branchera ici la sélection OT à partir du mapping validé.")
            if st.session_state.get("wizard_mapping"):
                st.write("Mapping actuel :")
                st.json(st.session_state["wizard_mapping"])
