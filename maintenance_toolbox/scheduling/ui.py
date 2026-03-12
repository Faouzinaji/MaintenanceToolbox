from datetime import datetime, time, timezone, timedelta
import csv
import io

import pandas as pd
import streamlit as st
from sqlalchemy import select

from maintenance_toolbox.db import (
    Planning,
    PlanningTask,
    PlanningTeam,
    FieldMapping,
    RexCause,
)


def _combine_date_time(d, hhmm: str):
    h, m = hhmm.split(":")
    return datetime.combine(d, time(int(h), int(m)))


def _safe_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


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


def _parse_dt_any(value):
    txt = _safe_text(value)
    if not txt:
        return None
    try:
        dt = pd.to_datetime(txt, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None


def _init_wizard_state():
    defaults = {
        "wizard_planning_id": None,
        "wizard_active_section": 1,
        "wizard_csv_columns": [],
        "wizard_mapping": {},
        "wizard_tasks_df": None,
        "wizard_teams_df": None,
        "rex_planning_id": None,
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
    st.session_state["wizard_teams_df"] = None


def _get_current_planning(session, planning_id):
    if not planning_id:
        return None
    return session.get(Planning, planning_id)


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


def _delete_planning(session, planning_id):
    planning = session.get(Planning, planning_id)
    if planning:
        session.delete(planning)
        session.commit()


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
    out["duration_hours"] = out["estimated_hours"].replace(0, 1.0)
    out["selected_warning"] = ""
    out["planned_start_at"] = ""
    out["planned_end_at"] = ""
    out["planned_team_name"] = ""

    return out


def _load_tasks_df_from_db(planning):
    rows = []
    for t in planning.tasks:
        rows.append(
            {
                "ot_id": t.external_ot_id or "",
                "description": t.description or "",
                "status": t.source_status or "",
                "atelier": t.atelier or "",
                "secteur": t.secteur or "",
                "equipment": t.equipment_code or "",
                "equipment_desc": t.equipment_desc or "",
                "created_at": t.created_at_source or "",
                "created_by": t.created_by_source or "",
                "requested_week": t.requested_week_source or "",
                "condition": t.condition_source or "",
                "estimated_hours": t.estimated_hours or 0.0,
                "selected": bool(t.selected),
                "forced_team": t.forced_team_codes or "",
                "predecessor_ot": t.predecessor_ot_id or "",
                "forced_start": str(t.forced_start_at) if t.forced_start_at else "",
                "duration_hours": t.estimated_hours or 0.0,
                "selected_warning": t.selected_warning or "",
                "planned_start_at": str(t.planned_start_at) if t.planned_start_at else "",
                "planned_end_at": str(t.planned_end_at) if t.planned_end_at else "",
                "planned_team_name": t.planned_team_name or "",
            }
        )
    return pd.DataFrame(rows)


def _build_default_teams_df(tasks_df, planning):
    ateliers = sorted(
        [x for x in tasks_df["atelier"].dropna().astype(str).unique().tolist() if x]
    )
    rows = []
    for at in ateliers:
        rows.append(
            {
                "atelier": at,
                "code": f"{at}-1",
                "name": f"{at}-1",
                "available_from": planning.start_at.strftime("%Y-%m-%d %H:%M"),
                "available_to": planning.end_at.strftime("%Y-%m-%d %H:%M"),
            }
        )
    return pd.DataFrame(rows)


def _load_teams_df_from_db(planning):
    rows = []
    for t in planning.teams:
        rows.append(
            {
                "atelier": t.atelier,
                "code": t.code,
                "name": t.name,
                "available_from": t.available_from.strftime("%Y-%m-%d %H:%M") if t.available_from else "",
                "available_to": t.available_to.strftime("%Y-%m-%d %H:%M") if t.available_to else "",
            }
        )
    return pd.DataFrame(rows)


def _generate_schedule(tasks_df, teams_df, planning):
    result = tasks_df.copy()

    result["selected_warning"] = result["selected_warning"].fillna("").astype(str)
    result["planned_start_at"] = ""
    result["planned_end_at"] = ""
    result["planned_team_name"] = ""

    if teams_df is None or teams_df.empty:
        raise ValueError("Aucune équipe définie.")

    teams = []
    for _, row in teams_df.iterrows():
        av_from = _parse_dt_any(row["available_from"])
        av_to = _parse_dt_any(row["available_to"])
        if av_from is None or av_to is None:
            raise ValueError("Une équipe a une date de disponibilité invalide.")
        teams.append(
            {
                "atelier": _safe_text(row["atelier"]),
                "code": _safe_text(row["code"]),
                "name": _safe_text(row["name"]),
                "available_from": av_from,
                "available_to": av_to,
                "next_available": av_from,
            }
        )

    selected_df = result[result["selected"]].copy()
    pending = selected_df.to_dict("records")
    scheduled_end_by_ot = {}

    safety_counter = 0
    while pending and safety_counter < 10000:
        safety_counter += 1
        next_pending = []
        progress = False

        for task in pending:
            ot_id = _safe_text(task["ot_id"])
            atelier = _safe_text(task["atelier"])
            forced_team = _safe_text(task["forced_team"])
            predecessor_ot = _safe_text(task["predecessor_ot"])
            forced_start = _safe_text(task["forced_start"])

            duration = float(task["duration_hours"]) if str(task["duration_hours"]).strip() else 0.0
            if duration <= 0:
                duration = 1.0

            if predecessor_ot and predecessor_ot not in scheduled_end_by_ot:
                next_pending.append(task)
                continue

            eligible = [t for t in teams if t["atelier"] == atelier]

            if forced_team:
                eligible_forced = [
                    t for t in eligible
                    if forced_team == t["code"] or forced_team == t["name"]
                ]
                if eligible_forced:
                    eligible = eligible_forced

            if not eligible:
                idx = result["ot_id"] == ot_id
                result.loc[idx, "selected_warning"] = "Aucune équipe disponible pour cet atelier."
                continue

            candidate_start_floor = planning.start_at
            if predecessor_ot in scheduled_end_by_ot:
                candidate_start_floor = max(candidate_start_floor, scheduled_end_by_ot[predecessor_ot])

            forced_start_dt = _parse_dt_any(forced_start)
            if forced_start_dt:
                candidate_start_floor = max(candidate_start_floor, forced_start_dt)

            best_team = None
            best_start = None
            best_end = None

            for team in eligible:
                start_dt = max(team["next_available"], team["available_from"], candidate_start_floor)
                end_dt = start_dt + timedelta(hours=duration)

                if end_dt <= team["available_to"] and end_dt <= planning.end_at:
                    if best_start is None or start_dt < best_start:
                        best_team = team
                        best_start = start_dt
                        best_end = end_dt

            if best_team is None:
                idx = result["ot_id"] == ot_id
                result.loc[idx, "selected_warning"] = "Impossible à positionner dans la fenêtre d'arrêt."
                continue

            best_team["next_available"] = best_end
            scheduled_end_by_ot[ot_id] = best_end

            idx = result["ot_id"] == ot_id
            result.loc[idx, "planned_start_at"] = best_start.strftime("%Y-%m-%d %H:%M")
            result.loc[idx, "planned_end_at"] = best_end.strftime("%Y-%m-%d %H:%M")
            result.loc[idx, "planned_team_name"] = best_team["name"]
            result.loc[idx, "selected_warning"] = ""
            progress = True

        if not progress and next_pending:
            for task in next_pending:
                ot_id = _safe_text(task["ot_id"])
                idx = result["ot_id"] == ot_id
                result.loc[idx, "selected_warning"] = "Prédécesseur non planifié."
            break

        pending = next_pending

    return result


def _persist_generation(session, planning, tasks_df, teams_df):
    for task in list(planning.tasks):
        session.delete(task)

    for team in list(planning.teams):
        session.delete(team)

    session.flush()

    for _, row in teams_df.iterrows():
        session.add(
            PlanningTeam(
                planning_id=planning.id,
                atelier=_safe_text(row["atelier"]),
                code=_safe_text(row["code"]),
                name=_safe_text(row["name"]),
                available_from=_parse_dt_any(row["available_from"]),
                available_to=_parse_dt_any(row["available_to"]),
            )
        )

    for _, row in tasks_df.iterrows():
        session.add(
            PlanningTask(
                planning_id=planning.id,
                external_ot_id=_safe_text(row["ot_id"]),
                task_type="ot",
                description=_safe_text(row["description"]),
                equipment_code=_safe_text(row["equipment"]),
                equipment_desc=_safe_text(row["equipment_desc"]),
                atelier=_safe_text(row["atelier"]),
                secteur=_safe_text(row["secteur"]),
                source_status=_safe_text(row["status"]),
                created_at_source=_safe_text(row["created_at"]),
                created_by_source=_safe_text(row["created_by"]),
                requested_week_source=_safe_text(row["requested_week"]),
                condition_source=_safe_text(row["condition"]),
                estimated_hours=float(row["duration_hours"]) if str(row["duration_hours"]).strip() else 0.0,
                selected=bool(row["selected"]),
                selected_warning=_safe_text(row["selected_warning"]),
                predecessor_ot_id=_safe_text(row["predecessor_ot"]),
                forced_team_codes=_safe_text(row["forced_team"]),
                forced_start_at=_parse_dt_any(row["forced_start"]),
                planned_start_at=_parse_dt_any(row["planned_start_at"]),
                planned_end_at=_parse_dt_any(row["planned_end_at"]),
                planned_team_name=_safe_text(row["planned_team_name"]),
            )
        )

    planning.status = "generated"
    planning.updated_at = datetime.now(timezone.utc)

    session.commit()
    session.refresh(planning)


def _load_planning_into_wizard(session, user, planning):
    st.session_state["wizard_planning_id"] = planning.id
    st.session_state["wizard_mapping"] = _load_saved_mapping(session, user.organization_id)

    if planning.csv_bytes:
        try:
            df = _read_csv_safely(planning.csv_bytes)
            st.session_state["wizard_csv_columns"] = list(df.columns)
        except Exception:
            st.session_state["wizard_csv_columns"] = []
    else:
        st.session_state["wizard_csv_columns"] = []

    if planning.tasks:
        st.session_state["wizard_tasks_df"] = _load_tasks_df_from_db(planning)
    else:
        st.session_state["wizard_tasks_df"] = None

    if planning.teams:
        st.session_state["wizard_teams_df"] = _load_teams_df_from_db(planning)
    else:
        st.session_state["wizard_teams_df"] = None

    if planning.teams:
        st.session_state["wizard_active_section"] = 6
    elif planning.tasks:
        st.session_state["wizard_active_section"] = 5
    elif planning.csv_bytes:
        st.session_state["wizard_active_section"] = 3
    else:
        st.session_state["wizard_active_section"] = 1


def _render_rex_panel(session, user, planning_id):
    planning = session.get(Planning, planning_id)
    if not planning:
        st.warning("Planning introuvable.")
        return

    st.divider()
    st.subheader(f"Retour d'expérience — {planning.name}")

    causes = session.scalars(
        select(RexCause)
        .where(RexCause.organization_id == user.organization_id)
        .where(RexCause.active == True)
        .order_by(RexCause.label_fr)
    ).all()

    cause_label_to_id = {c.label_fr: c.id for c in causes}
    cause_options = [""] + list(cause_label_to_id.keys())

    selected_tasks = [t for t in planning.tasks if t.selected]

    if not selected_tasks:
        st.info("Aucun OT sélectionné dans ce planning.")
        return

    rows = []
    for t in selected_tasks:
        current_cause = t.rex_cause.label_fr if t.rex_cause else ""
        rows.append(
            {
                "ot_id": t.external_ot_id,
                "description": t.description,
                "atelier": t.atelier,
                "planned_start_at": str(t.planned_start_at) if t.planned_start_at else "",
                "planned_team_name": t.planned_team_name or "",
                "rex_done": bool(t.rex_done) if t.rex_done is not None else False,
                "rex_cause_label": current_cause,
                "rex_comment": t.rex_comment or "",
            }
        )

    rex_df = pd.DataFrame(rows)

    edited = st.data_editor(
        rex_df,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        column_config={
            "ot_id": st.column_config.TextColumn("OT", disabled=True),
            "description": st.column_config.TextColumn("Description", disabled=True),
            "atelier": st.column_config.TextColumn("Atelier", disabled=True),
            "planned_start_at": st.column_config.TextColumn("Début prévu", disabled=True),
            "planned_team_name": st.column_config.TextColumn("Équipe", disabled=True),
            "rex_done": st.column_config.CheckboxColumn("Réalisé"),
            "rex_cause_label": st.column_config.SelectboxColumn("Cause", options=cause_options),
            "rex_comment": st.column_config.TextColumn("Commentaire"),
        },
        key=f"rex_editor_{planning_id}",
    )

    if st.button("Enregistrer le REX", use_container_width=True, key=f"save_rex_{planning_id}"):
        try:
            for _, row in edited.iterrows():
                ot_id = _safe_text(row["ot_id"])
                task = next((x for x in selected_tasks if x.external_ot_id == ot_id), None)
                if not task:
                    continue

                task.rex_done = bool(row["rex_done"])
                label = _safe_text(row["rex_cause_label"])
                task.rex_cause_id = cause_label_to_id.get(label) if label else None
                task.rex_comment = _safe_text(row["rex_comment"])

            session.commit()
            st.success("REX enregistré.")
        except Exception as e:
            session.rollback()
            st.error(f"Erreur lors de l'enregistrement du REX : {e}")


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
            plannings = session.scalars(
                select(Planning)
                .where(Planning.organization_id == user.organization_id)
                .where(Planning.archived == False)
                .order_by(Planning.created_at.desc())
            ).all()

            if not plannings:
                st.info("Aucun planning pour le moment.")
            else:
                for p in plannings:
                    c1, c2, c3, c4 = st.columns([8, 1, 1, 1])

                    with c1:
                        st.markdown(
                            f"**{p.name}**  \n"
                            f"Statut : `{p.status}`  \n"
                            f"Période : {p.start_at} → {p.end_at}  \n"
                            f"CSV : {'Oui' if p.csv_filename else 'Non'}"
                        )

                    with c2:
                        if st.button("✏️", key=f"edit_{p.id}", use_container_width=True):
                            _load_planning_into_wizard(session, user, p)
                            st.session_state["page"] = "scheduling"
                            st.rerun()

                    with c3:
                        if st.button("🗑️", key=f"delete_{p.id}", use_container_width=True):
                            _delete_planning(session, p.id)
                            if st.session_state.get("rex_planning_id") == p.id:
                                st.session_state["rex_planning_id"] = None
                            if st.session_state.get("wizard_planning_id") == p.id:
                                _reset_wizard()
                            st.success("Planning supprimé.")
                            st.rerun()

                    with c4:
                        if st.button("📝", key=f"rex_{p.id}", use_container_width=True):
                            st.session_state["rex_planning_id"] = p.id
                            st.rerun()

                    st.divider()

            if st.session_state.get("rex_planning_id"):
                _render_rex_panel(session, user, st.session_state["rex_planning_id"])

        except Exception as e:
            st.error(f"Erreur lors du chargement des plannings : {e}")

    with tab2:
        planning = _get_current_planning(session, st.session_state["wizard_planning_id"])

        top_col1, top_col2 = st.columns([3, 1])
        with top_col1:
            st.subheader("Wizard de création / modification de planning")
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

        default_name = planning.name if planning else ""
        default_sectors = planning.sectors_csv if planning else ""
        default_start_date = planning.start_at.date() if planning else datetime.now().date()
        default_end_date = planning.end_at.date() if planning else datetime.now().date()
        default_daily_open = planning.daily_open if planning else "07:00"
        default_daily_close = planning.daily_close if planning else "15:00"

        with st.expander(
            "1. Paramètres de l'arrêt",
            expanded=st.session_state["wizard_active_section"] == 1,
        ):
            with st.form("create_planning_form"):
                name = st.text_input("Nom du planning", value=default_name)
                sectors_txt = st.text_input(
                    "Secteurs (séparés par des virgules)",
                    value=default_sectors
                )

                col1, col2 = st.columns(2)

                with col1:
                    start_date = st.date_input("Date de début", value=default_start_date)
                    daily_open = st.text_input("Heure d'ouverture", value=default_daily_open)

                with col2:
                    end_date = st.date_input("Date de fin", value=default_end_date)
                    daily_close = st.text_input("Heure de fermeture", value=default_daily_close)

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

                    if planning:
                        planning.name = name.strip()
                        planning.sectors_csv = sectors_txt.strip()
                        planning.start_at = start_at
                        planning.end_at = end_at
                        planning.daily_open = daily_open
                        planning.daily_close = daily_close
                        planning.updated_at = datetime.now(timezone.utc)
                        session.commit()
                        st.success("Paramètres du planning mis à jour.")
                    else:
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
                        st.session_state["wizard_tasks_df"] = None
                        st.session_state["wizard_teams_df"] = None
                        st.success(f"Planning créé avec succès : {planning.name}")

                    st.session_state["wizard_active_section"] = 2
                    st.rerun()

                except Exception as e:
                    session.rollback()
                    st.error(f"Erreur lors de l'enregistrement des paramètres : {e}")

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
                            st.session_state["wizard_teams_df"] = None
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

            if tasks_df is None and len(planning.tasks) > 0:
                tasks_df = _load_tasks_df_from_db(planning)
                st.session_state["wizard_tasks_df"] = tasks_df

            if tasks_df is None:
                st.info("Valide d'abord le mapping.")
            else:
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
                    key=f"tasks_editor_{planning.id}",
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

                    teams_df = _build_default_teams_df(updated[updated["selected"]], planning)
                    st.session_state["wizard_teams_df"] = teams_df
                    st.session_state["wizard_active_section"] = 5
                    st.success("Sélection OT enregistrée.")
                    st.rerun()

        with st.expander(
            "5. Teams",
            expanded=st.session_state["wizard_active_section"] == 5,
        ):
            tasks_df = st.session_state.get("wizard_tasks_df")

            if tasks_df is None:
                st.info("Valide d'abord la sélection des OT.")
            else:
                teams_df = st.session_state.get("wizard_teams_df")

                if (teams_df is None or teams_df.empty) and len(planning.teams) > 0:
                    teams_df = _load_teams_df_from_db(planning)
                    st.session_state["wizard_teams_df"] = teams_df

                if teams_df is None or teams_df.empty:
                    teams_df = _build_default_teams_df(tasks_df[tasks_df["selected"]], planning)
                    st.session_state["wizard_teams_df"] = teams_df

                st.write("Définis les équipes disponibles pour l'arrêt.")

                edited_teams = st.data_editor(
                    teams_df,
                    use_container_width=True,
                    hide_index=True,
                    num_rows="dynamic",
                    column_config={
                        "atelier": st.column_config.TextColumn("Atelier"),
                        "code": st.column_config.TextColumn("Code équipe"),
                        "name": st.column_config.TextColumn("Nom équipe"),
                        "available_from": st.column_config.TextColumn("Disponible à partir de"),
                        "available_to": st.column_config.TextColumn("Disponible jusqu'à"),
                    },
                    key=f"teams_editor_{planning.id}",
                )

                if st.button("Valider les équipes", use_container_width=True):
                    st.session_state["wizard_teams_df"] = edited_teams.copy()
                    st.session_state["wizard_active_section"] = 6
                    st.success("Équipes enregistrées.")
                    st.rerun()

        with st.expander(
            "6. Génération du planning",
            expanded=st.session_state["wizard_active_section"] == 6,
        ):
            tasks_df = st.session_state.get("wizard_tasks_df")
            teams_df = st.session_state.get("wizard_teams_df")

            if tasks_df is None or teams_df is None:
                st.info("Valide d'abord la sélection OT et les équipes.")
            else:
                st.write("Génère et enregistre le planning.")

                if st.button("Générer et enregistrer le planning", use_container_width=True):
                    try:
                        generated_df = _generate_schedule(tasks_df, teams_df, planning)
                        st.session_state["wizard_tasks_df"] = generated_df

                        _persist_generation(session, planning, generated_df, teams_df)

                        st.success("Planning généré et enregistré.")
                        st.rerun()

                    except Exception as e:
                        session.rollback()
                        st.error(f"Erreur lors de la génération : {e}")

                generated = st.session_state.get("wizard_tasks_df")
                if generated is not None:
                    preview = generated[
                        [
                            "selected",
                            "ot_id",
                            "description",
                            "atelier",
                            "duration_hours",
                            "planned_start_at",
                            "planned_end_at",
                            "planned_team_name",
                            "selected_warning",
                        ]
                    ].copy()

                    st.dataframe(preview, use_container_width=True, hide_index=True)
