from datetime import datetime, time, timezone
import csv
import io

import pandas as pd
import streamlit as st
from sqlalchemy import select

from maintenance_toolbox.db import Planning, FieldMapping


def _combine_date_time(d, hhmm: str):
    h, m = hhmm.split(":")
    return datetime.combine(d, time(int(h), int(m)))


def _read_csv_safely(file_bytes: bytes) -> pd.DataFrame:
    raw_text = None
    last_error = None

    for encoding in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
        try:
            raw_text = file_bytes.decode(encoding)
            break
        except Exception as e:
            last_error = e

    if raw_text is None:
        raise ValueError(f"Impossible de décoder le fichier : {last_error}")

    sample = raw_text[:5000]
    delimiters = [",", ";", "\t", "|"]
    detected_sep = None

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=delimiters)
        detected_sep = dialect.delimiter
    except Exception:
        pass

    candidates = []
    if detected_sep:
        candidates.append(detected_sep)

    for sep in delimiters:
        if sep not in candidates:
            candidates.append(sep)

    best_df = None
    best_score = -1
    best_error = None

    for sep in candidates:
        try:
            df = pd.read_csv(
                io.StringIO(raw_text),
                sep=sep,
                engine="python",
            )

            score = len(df.columns)

            if len(df.columns) == 1:
                first_col = str(df.columns[0])
                if "," in first_col or ";" in first_col or "\t" in first_col or "|" in first_col:
                    score = 0

            if score > best_score:
                best_score = score
                best_df = df

        except Exception as e:
            best_error = e

    if best_df is None:
        raise ValueError(f"Impossible de lire le CSV : {best_error}")

    if best_score <= 0:
        raise ValueError(
            "Le fichier a été lu sur une seule colonne. Le séparateur n'a pas été correctement détecté."
        )

    return best_df


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
        "wizard_tasks_df": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _reset_wizard():
    st.session_state["wizard_planning_id"] = None
    st.session_state["wizard_active_section"] = 1
    st.session_state["wizard_csv_columns"] = []
    st.session_state["wizard_mapping"] = {}
    st.session_state["wizard_tasks_df"] = None


def _load_saved_mapping(session, organization_id):
    fm = session.scalar(
        select(FieldMapping).where(FieldMapping.organization_id == organization_id)
    )
    if not fm:
        return {}

    return {
        "ot_id": fm.ot_id_col or "",
        "description": fm.description_col or "",
        "status": fm.status_col or "",
        "atelier": fm.atelier_col or "",
        "secteur": fm.secteur_col or "",
        "equipment": fm.equipment_col or "",
        "equipment_desc": fm.equipment_desc_col or "",
        "created_at": fm.created_at_col or "",
        "created_by": fm.created_by_col or "",
        "requested_week": fm.requested_week_col or "",
        "condition": fm.condition_col or "",
        "estimated_hours": fm.estimated_hours_col or "",
    }


def _save_mapping(session, organization_id, mapping):
    fm = session.scalar(
        select(FieldMapping).where(FieldMapping.organization_id == organization_id)
    )

    if not fm:
        fm = FieldMapping(organization_id=organization_id)
        session.add(fm)

    fm.ot_id_col = mapping.get("ot_id") or None
    fm.description_col = mapping.get("description") or None
    fm.status_col = mapping.get("status") or None
    fm.atelier_col = mapping.get("atelier") or None
    fm.secteur_col = mapping.get("secteur") or None
    fm.equipment_col = mapping.get("equipment") or None
    fm.equipment_desc_col = mapping.get("equipment_desc") or None
    fm.created_at_col = mapping.get("created_at") or None
    fm.created_by_col = mapping.get("created_by") or None
    fm.requested_week_col = mapping.get("requested_week") or None
    fm.condition_col = mapping.get("condition") or None
    fm.estimated_hours_col = mapping.get("estimated_hours") or None

    session.commit()


def _safe_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def _build_tasks_df_from_mapping(df, mapping):
    def col(key):
        c = mapping.get(key, "")
        return c if c in df.columns else None

    out = pd.DataFrame()

    out["ot_id"] = df[col("ot_id")].apply(_safe_text) if col("ot_id") else ""
    out["description"] = df[col("description")].apply(_safe_text) if col("description") else ""
    out["status"] = df[col("status")].apply(_safe_text) if col("status") else ""
    out["atelier"] = df[col("atelier")].apply(_safe_text) if col("atelier") else ""
    out["secteur"] = df[col("secteur")].apply(_safe_text) if col("secteur") else ""
    out["equipment"] = df[col("equipment")].apply(_safe_text) if col("equipment") else ""
    out["equipment_desc"] = df[col("equipment_desc")].apply(_safe_text) if col("equipment_desc") else ""
    out["created_at"] = df[col("created_at")].apply(_safe_text) if col("created_at") else ""
    out["created_by"] = df[col("created_by")].apply(_safe_text) if col("created_by") else ""
    out["requested_week"] = df[col("requested_week")].apply(_safe_text) if col("requested_week") else ""
    out["condition"] = df[col("condition")].apply(_safe_text) if col("condition") else ""

    if col("estimated_hours"):
        out["estimated_hours"] = pd.to_numeric(df[col("estimated_hours")], errors="coerce").fillna(0.0)
    else:
        out["estimated_hours"] = 0.0

    out = out[
        (out["ot_id"] != "") |
        (out["description"] != "") |
        (out["atelier"] != "")
    ].copy()

    out = out.reset_index(drop=True)
    out["selected"] = True
    out["forced_team"] = ""
    out["predecessor_ot"] = ""
    out["forced_start"] = ""
    out["duration_hours"] = out["estimated_hours"]

    return out


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
                    st.session_state["wizard_tasks_df"] = None

                    st.success(f"Planning créé avec succès : {planning.name}")
                    st.rerun()

                except Exception as e:
                    session.rollback()
                    st.error(f"Erreur lors de la création du planning : {e}")

        planning = _get_current_planning(session, st.session_state["wizard_planning_id"])
        if not planning:
            return

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
                            st.session_state["wizard_tasks_df"] = None
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
                            st.success(f"{len(preview_df.columns)} colonnes détectées")
                            st.write(preview_df.columns.tolist())
                            st.dataframe(preview_df.head(20), use_container_width=True)
                        except Exception as e:
                            st.error(f"Erreur de lecture du CSV : {e}")

            if planning.csv_bytes:
                try:
                    preview_df = _read_csv_safely(planning.csv_bytes)
                    st.success(f"{len(preview_df.columns)} colonnes détectées dans le CSV enregistré")
                    st.write(preview_df.columns.tolist())
                    st.dataframe(preview_df.head(10), use_container_width=True)
                except Exception as e:
                    st.error(f"Erreur de lecture du CSV enregistré : {e}")

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

                    saved_mapping = _load_saved_mapping(session, user.organization_id)
                    current_mapping = st.session_state.get("wizard_mapping", {}) or saved_mapping

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

                    with st.form("mapping_form"):
                        mapping = {}
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
                            _save_mapping(session, user.organization_id, mapping)

                            tasks_df = _build_tasks_df_from_mapping(df, mapping)
                            st.session_state["wizard_tasks_df"] = tasks_df
                            st.session_state["wizard_active_section"] = 4

                            st.success("Mapping validé et sauvegardé.")
                            st.rerun()

                except Exception as e:
                    st.error(f"Erreur lors du mapping : {e}")

        with st.expander(
            "4. Sélection des OT",
            expanded=st.session_state["wizard_active_section"] == 4,
        ):
            tasks_df = st.session_state.get("wizard_tasks_df")

            if tasks_df is None:
                if not planning.csv_bytes or not st.session_state.get("wizard_mapping"):
                    st.info("Valide d'abord le mapping.")
                else:
                    try:
                        source_df = _read_csv_safely(planning.csv_bytes)
                        tasks_df = _build_tasks_df_from_mapping(
                            source_df,
                            st.session_state["wizard_mapping"]
                        )
                        st.session_state["wizard_tasks_df"] = tasks_df
                    except Exception as e:
                        st.error(f"Erreur lors de la préparation des OT : {e}")
                        tasks_df = None

            if tasks_df is not None:
                st.write("Filtre et sélectionne les OT à planifier.")

                ateliers = sorted([x for x in tasks_df["atelier"].dropna().astype(str).unique().tolist() if x])
                secteurs = sorted([x for x in tasks_df["secteur"].dropna().astype(str).unique().tolist() if x])
                statuts = sorted([x for x in tasks_df["status"].dropna().astype(str).unique().tolist() if x])

                c1, c2, c3 = st.columns(3)

                with c1:
                    selected_ateliers = st.multiselect(
                        "Ateliers",
                        ateliers,
                        default=ateliers,
                        key="filter_ateliers"
                    )

                with c2:
                    selected_secteurs = st.multiselect(
                        "Secteurs",
                        secteurs,
                        default=secteurs,
                        key="filter_secteurs"
                    )

                with c3:
                    selected_statuts = st.multiselect(
                        "Statuts",
                        statuts,
                        default=statuts,
                        key="filter_statuts"
                    )

                filtered = tasks_df.copy()

                if ateliers:
                    filtered = filtered[filtered["atelier"].isin(selected_ateliers)]
                if secteurs:
                    filtered = filtered[filtered["secteur"].isin(selected_secteurs)]
                if statuts:
                    filtered = filtered[filtered["status"].isin(selected_statuts)]

                st.caption(f"{len(filtered)} OT affichés")

                display_df = filtered[
                    [
                        "selected",
                        "ot_id",
                        "description",
                        "atelier",
                        "secteur",
                        "status",
                        "duration_hours",
                        "forced_team",
                        "predecessor_ot",
                        "forced_start",
                    ]
                ].copy()

                edited_df = st.data_editor(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    num_rows="fixed",
                    column_config={
                        "selected": st.column_config.CheckboxColumn("Sélection"),
                        "ot_id": st.column_config.TextColumn("OT", disabled=True),
                        "description": st.column_config.TextColumn("Description", disabled=True),
                        "atelier": st.column_config.TextColumn("Atelier", disabled=True),
                        "secteur": st.column_config.TextColumn("Secteur", disabled=True),
                        "status": st.column_config.TextColumn("Statut", disabled=True),
                        "duration_hours": st.column_config.NumberColumn("Durée (h)", min_value=0.0, step=0.5),
                        "forced_team": st.column_config.TextColumn("Équipe forcée"),
                        "predecessor_ot": st.column_config.TextColumn("Prédécesseur"),
                        "forced_start": st.column_config.TextColumn("Début forcé"),
                    },
                    key="tasks_editor",
                )

                if st.button("Valider la sélection des OT", use_container_width=True):
                    updated = tasks_df.copy()

                    for _, row in edited_df.iterrows():
                        ot_id = row["ot_id"]
                        mask = updated["ot_id"] == ot_id
                        updated.loc[mask, "selected"] = bool(row["selected"])
                        updated.loc[mask, "duration_hours"] = row["duration_hours"]
                        updated.loc[mask, "forced_team"] = _safe_text(row["forced_team"])
                        updated.loc[mask, "predecessor_ot"] = _safe_text(row["predecessor_ot"])
                        updated.loc[mask, "forced_start"] = _safe_text(row["forced_start"])

                    st.session_state["wizard_tasks_df"] = updated
                    st.session_state["wizard_active_section"] = 5
                    st.success("Sélection OT enregistrée.")

        with st.expander(
            "5. Teams",
            expanded=st.session_state["wizard_active_section"] == 5,
        ):
            tasks_df = st.session_state.get("wizard_tasks_df")
            if tasks_df is None:
                st.info("Valide d'abord la sélection des OT.")
            else:
                selected_count = int(tasks_df["selected"].sum())
                st.success(f"{selected_count} OT sélectionnés pour la suite.")
                st.info("Étape suivante : on va construire ici les équipes par atelier, dans la même logique que ton app locale.")
