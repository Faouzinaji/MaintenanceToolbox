from datetime import datetime, time, timezone, timedelta
import csv
import io
import math
import re

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


# =========================================================
# HELPERS
# =========================================================

def _combine_date_time(d, hhmm: str):
    h, m = hhmm.split(":")
    return datetime.combine(d, time(int(h), int(m)))


def _safe_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def _safe_key(text):
    return re.sub(r"[^a-zA-Z0-9_]", "_", str(text))


def _shorten(text, n=42):
    t = "" if pd.isna(text) else str(text)
    return t if len(t) <= n else t[: n - 3] + "..."


def _normalize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip().lower()


def _parse_dt_any(value, fallback=None):
    txt = _safe_text(value)
    if not txt:
        return pd.Timestamp(fallback) if fallback is not None else None
    try:
        return pd.Timestamp(txt)
    except Exception:
        return pd.Timestamp(fallback) if fallback is not None else None


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
            df = pd.read_csv(io.StringIO(raw_text), sep=sep, engine="python")

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
        raise ValueError("Le séparateur du CSV n'a pas été correctement détecté.")

    return best_df


def _status_score(status):
    s = _normalize_text(status)
    if s in ["panne à exécuter", "panne a executer"]:
        return 100
    if "planifi" in s:
        return 80
    if "approuv" in s:
        return 70
    if "prépar" in s or "prepar" in s:
        return 60
    if "attente" in s:
        return 30
    if "demand" in s:
        return 20
    return 0


def _default_status_included(status):
    s = _normalize_text(status)
    excluded = {
        "exécuté", "execute",
        "exécuté correctif", "execute correctif",
        "exécuté panne", "execute panne",
    }
    return s not in excluded


def _guess_duration_from_text(description, equipment_desc=""):
    txt = f"{description} {equipment_desc}".lower()

    if any(x in txt for x in ["nettoy", "inspection", "graiss", "contrôle", "controle", "visite"]):
        return 1.0
    if any(x in txt for x in ["remplac", "changé", "change", "dépose", "depose"]):
        return 2.0
    if any(x in txt for x in ["moteur", "rouleau", "convoyeur", "tambour", "chaîne", "chaine", "courroie"]):
        return 3.0
    if any(x in txt for x in ["répar", "repar", "dépann", "depann", "panne"]):
        return 2.5
    return 2.0


def _build_comment(row):
    atelier = _safe_text(row.get("atelier", "")).upper()
    probable = []
    checks = []
    prep = []

    if atelier == "289.CEMOMC":
        probable += ["usure mecanique", "desalignement", "encrassement"]
        checks += ["controle visuel des organes tournants", "verifier alignement et jeu"]
        prep += ["outillage mecanique", "acces securise"]
    elif atelier == "289.CEMOEL":
        probable += ["defaut capteur", "connexion degradee", "anomalie alimentation"]
        checks += ["controle alimentation", "verifier capteurs"]
        prep += ["multimetre", "consignation electrique"]
    elif atelier == "289.CEMOMT":
        probable += ["derive de mesure", "capteur en defaut"]
        checks += ["controle du capteur", "verification du signal"]
        prep += ["materiel de calibrage"]
    elif atelier == "289.CEMOCH":
        probable += ["usure structurelle", "deformation"]
        checks += ["controle visuel structure", "verifier fixations"]
        prep += ["outillage chaudronnerie"]
    else:
        probable += ["cause a confirmer"]
        checks += ["controle visuel et fonctionnel"]
        prep += ["outillage standard"]

    return (
        f"Causes probables : {', '.join(probable[:3])}. "
        f"A verifier : {'; '.join(checks[:3])}. "
        f"Preparation : {'; '.join(prep[:3])}."
    )


# =========================================================
# SESSION STATE
# =========================================================

def _init_wizard_state():
    defaults = {
        "wizard_planning_id": None,
        "wizard_active_section": 1,
        "wizard_csv_columns": [],
        "wizard_mapping": {},
        "wizard_tasks_df": None,
        "wizard_teams_df": None,
        "wizard_selected_ateliers": [],
        "wizard_generated_df": None,
        "wizard_unscheduled_df": None,
        "wizard_team_counts": {},
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
    st.session_state["wizard_selected_ateliers"] = []
    st.session_state["wizard_generated_df"] = None
    st.session_state["wizard_unscheduled_df"] = None
    st.session_state["wizard_team_counts"] = {}


# =========================================================
# DB HELPERS
# =========================================================

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


# =========================================================
# TASKS / TEAMS
# =========================================================

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
        est = pd.to_numeric(df[col("estimated_hours")], errors="coerce").fillna(0.0)
        out["estimated_hours"] = est
    else:
        out["estimated_hours"] = 0.0

    out = out[
        (out["ot_id"] != "") |
        (out["description"] != "") |
        (out["atelier"] != "")
    ].copy()

    out = out.reset_index(drop=True)

    out["priority_score"] = out["status"].apply(_status_score)
    out["selected"] = out["status"].apply(_default_status_included)
    out["forced_team"] = ""
    out["predecessor_ot"] = ""
    out["forced_start"] = ""
    out["duration_hours"] = out["estimated_hours"]

    out["duration_hours"] = out.apply(
        lambda r: r["duration_hours"] if float(r["duration_hours"]) > 0 else _guess_duration_from_text(r["description"], r["equipment_desc"]),
        axis=1
    )

    out["selected_warning"] = ""
    out["planned_start_at"] = ""
    out["planned_end_at"] = ""
    out["planned_team_name"] = ""
    out["commentaire"] = ""

    return out


def _initialize_team_roster(atelier, n_teams, start_dt, end_dt):
    rows = []
    for i in range(1, n_teams + 1):
        code = f"{atelier}-{i}"
        rows.append(
            {
                "atelier": atelier,
                "code": code,
                "name": code,
                "available_from": start_dt.strftime("%Y-%m-%d %H:%M"),
                "available_to": end_dt.strftime("%Y-%m-%d %H:%M"),
            }
        )
    return pd.DataFrame(rows)


def _build_resources_from_team_rosters(selected_ateliers, team_rosters, default_start, default_end):
    resources_by_atelier = {}

    for atelier in selected_ateliers:
        roster = team_rosters.get(atelier, pd.DataFrame())
        rows = []

        if roster is not None and not roster.empty:
            for _, r in roster.iterrows():
                code = _safe_text(r.get("code", ""))
                name = _safe_text(r.get("name", "")) or code
                start = _parse_dt_any(r.get("available_from", ""), default_start)
                end = _parse_dt_any(r.get("available_to", ""), default_end)

                if code and start is not None and end is not None and end > start:
                    rows.append(
                        {
                            "code": code,
                            "name": name,
                            "available_from": start,
                            "available_to": end,
                        }
                    )

        resources_by_atelier[atelier] = rows

    return resources_by_atelier


def _choose_candidate_resources(candidates, forced_teams):
    if not forced_teams:
        return candidates

    forced_norm = {str(x).strip().lower() for x in forced_teams if str(x).strip()}
    if not forced_norm:
        return candidates

    return [
        c for c in candidates
        if c["code"].lower() in forced_norm or c["name"].lower() in forced_norm
    ]


def _prepare_tasks_for_planning(selected_df):
    tasks = []

    if selected_df is not None and not selected_df.empty:
        for _, r in selected_df.iterrows():
            forced_teams = [
                x.strip()
                for x in re.split(r"[;,]", str(r.get("forced_team", "")))
                if x.strip()
            ]

            tasks.append(
                {
                    "task_id": str(r["ot_id"]),
                    "ot_id": str(r["ot_id"]),
                    "description": str(r.get("description", "")),
                    "equipment_desc": str(r.get("equipment_desc", "")),
                    "atelier": str(r.get("atelier", "")),
                    "duration_h": float(r.get("duration_hours", 0)),
                    "predecessor_ot": str(r.get("predecessor_ot", "") or "").strip(),
                    "forced_start": _parse_dt_any(r.get("forced_start", ""), None),
                    "forced_teams": forced_teams,
                    "priority_score": float(r.get("priority_score", 0)),
                    "status": str(r.get("status", "")),
                }
            )

    return pd.DataFrame(tasks)


def _schedule_standard_tasks(tasks_df, resources_by_atelier, window_start, window_end, allow_overrun=False):
    planning_rows = []
    unscheduled_rows = []
    scheduled_map = {}

    if tasks_df.empty:
        return planning_rows, unscheduled_rows, scheduled_map

    resource_states = {}
    for atelier, res_list in resources_by_atelier.items():
        resource_states[atelier] = []
        for res in res_list:
            start = max(pd.Timestamp(window_start), pd.Timestamp(res["available_from"]))
            end = pd.Timestamp(res["available_to"])
            if not allow_overrun:
                end = min(end, pd.Timestamp(window_end))

            resource_states[atelier].append(
                {
                    "code": res["code"],
                    "name": res["name"],
                    "current": start,
                    "available_from": start,
                    "available_to": end,
                }
            )

    tasks = {r["task_id"]: r for _, r in tasks_df.iterrows()}
    unscheduled = set(tasks.keys())

    for task_id in list(unscheduled):
        pred = str(tasks[task_id].get("predecessor_ot", "")).strip()
        if pred and pred not in tasks:
            rr = dict(tasks[task_id])
            rr["reason"] = f"Predecesseur non present : {pred}"
            unscheduled_rows.append(rr)
            unscheduled.remove(task_id)

    progress = True
    while unscheduled and progress:
        progress = False
        ready = []

        for task_id in unscheduled:
            pred = str(tasks[task_id].get("predecessor_ot", "")).strip()
            if not pred or pred in scheduled_map:
                ready.append(task_id)

        ready = sorted(
            ready,
            key=lambda x: (-float(tasks[x].get("priority_score", 0)), float(tasks[x].get("duration_h", 0))),
        )

        for task_id in ready:
            t = tasks[task_id]
            atelier = str(t.get("atelier", ""))
            duration = float(t.get("duration_h", 0))
            forced_start = t.get("forced_start", None)
            forced_teams = t.get("forced_teams", [])

            if atelier not in resource_states or len(resource_states[atelier]) == 0:
                rr = dict(t)
                rr["reason"] = "Aucune equipe disponible sur cet atelier"
                unscheduled_rows.append(rr)
                unscheduled.remove(task_id)
                progress = True
                continue

            pred = str(t.get("predecessor_ot", "")).strip()
            pred_end = pd.Timestamp(window_start)
            if pred and pred in scheduled_map:
                pred_end = scheduled_map[pred]["planned_end_at"]

            candidates = _choose_candidate_resources(resource_states[atelier], forced_teams)
            if not candidates:
                rr = dict(t)
                rr["reason"] = "Aucune equipe forcee correspondante disponible"
                unscheduled_rows.append(rr)
                unscheduled.remove(task_id)
                progress = True
                continue

            best = None
            best_start = None
            best_end = None

            for c in candidates:
                start_candidate = max(
                    pd.Timestamp(c["current"]),
                    pred_end,
                    pd.Timestamp(c["available_from"]),
                    pd.Timestamp(window_start),
                )

                if forced_start is not None:
                    start_candidate = max(start_candidate, pd.Timestamp(forced_start))

                if start_candidate >= pd.Timestamp(c["available_to"]):
                    continue

                end_candidate = start_candidate + pd.Timedelta(hours=duration)

                if not allow_overrun and end_candidate > min(pd.Timestamp(window_end), pd.Timestamp(c["available_to"])):
                    continue
                if allow_overrun and end_candidate > pd.Timestamp(c["available_to"]):
                    continue

                if best_end is None or end_candidate < best_end:
                    best = c
                    best_start = start_candidate
                    best_end = end_candidate

            if best is None:
                rr = dict(t)
                rr["reason"] = "Aucune capacite compatible dans la fenetre"
                unscheduled_rows.append(rr)
                unscheduled.remove(task_id)
                progress = True
                continue

            rr = dict(t)
            rr["planned_team_name"] = best["name"]
            rr["planned_start_at"] = best_start
            rr["planned_end_at"] = best_end

            planning_rows.append(rr)
            scheduled_map[task_id] = rr
            best["current"] = best_end
            unscheduled.remove(task_id)
            progress = True

    return planning_rows, unscheduled_rows, scheduled_map


def _generate_schedule(tasks_df, teams_df, planning):
    selected_df = tasks_df[tasks_df["selected"]].copy()
    selected_ateliers = sorted([x for x in selected_df["atelier"].dropna().astype(str).unique().tolist() if x])

    team_rosters = {}
    if teams_df is not None and not teams_df.empty:
        for atelier in selected_ateliers:
            team_rosters[atelier] = teams_df[teams_df["atelier"] == atelier].copy()
    else:
        team_rosters = {atelier: pd.DataFrame() for atelier in selected_ateliers}

    resources_by_atelier = _build_resources_from_team_rosters(
        selected_ateliers=selected_ateliers,
        team_rosters=team_rosters,
        default_start=planning.start_at,
        default_end=planning.end_at,
    )

    all_tasks = _prepare_tasks_for_planning(selected_df)

    planning_rows, unscheduled_rows, _ = _schedule_standard_tasks(
        all_tasks,
        resources_by_atelier,
        planning.start_at,
        planning.end_at,
        allow_overrun=False,
    )

    planning_df = pd.DataFrame(planning_rows)
    unscheduled_df = pd.DataFrame(unscheduled_rows)

    result = tasks_df.copy()
    result["selected_warning"] = ""
    result["planned_start_at"] = ""
    result["planned_end_at"] = ""
    result["planned_team_name"] = ""
    result["commentaire"] = ""

    if not planning_df.empty:
        planning_df["commentaire"] = planning_df.apply(lambda r: _build_comment({"atelier": r.get("atelier", "")}), axis=1)

        for _, row in planning_df.iterrows():
            mask = result["ot_id"] == str(row["ot_id"])
            result.loc[mask, "planned_start_at"] = pd.Timestamp(row["planned_start_at"]).strftime("%Y-%m-%d %H:%M")
            result.loc[mask, "planned_end_at"] = pd.Timestamp(row["planned_end_at"]).strftime("%Y-%m-%d %H:%M")
            result.loc[mask, "planned_team_name"] = row["planned_team_name"]
            result.loc[mask, "commentaire"] = row["commentaire"]

    if not unscheduled_df.empty:
        for _, row in unscheduled_df.iterrows():
            mask = result["ot_id"] == str(row["ot_id"])
            result.loc[mask, "selected_warning"] = row.get("reason", "Non planifie")

    return result, unscheduled_df


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
                "priority_score": _status_score(t.source_status or ""),
                "selected": bool(t.selected),
                "forced_team": t.forced_team_codes or "",
                "predecessor_ot": t.predecessor_ot_id or "",
                "forced_start": str(t.forced_start_at) if t.forced_start_at else "",
                "duration_hours": t.estimated_hours or 0.0,
                "selected_warning": t.selected_warning or "",
                "planned_start_at": str(t.planned_start_at) if t.planned_start_at else "",
                "planned_end_at": str(t.planned_end_at) if t.planned_end_at else "",
                "planned_team_name": t.planned_team_name or "",
                "commentaire": "",
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

    if len(planning.tasks) > 0:
        tasks_df = _load_tasks_df_from_db(planning)
        st.session_state["wizard_tasks_df"] = tasks_df
        st.session_state["wizard_selected_ateliers"] = sorted(
            [x for x in tasks_df[tasks_df["selected"]]["atelier"].dropna().astype(str).unique().tolist() if x]
        )
    else:
        st.session_state["wizard_tasks_df"] = None
        st.session_state["wizard_selected_ateliers"] = []

    if len(planning.teams) > 0:
        st.session_state["wizard_teams_df"] = _load_teams_df_from_db(planning)
    else:
        st.session_state["wizard_teams_df"] = None

    if len(planning.teams) > 0:
        st.session_state["wizard_active_section"] = 6
    elif len(planning.tasks) > 0:
        st.session_state["wizard_active_section"] = 5
    elif planning.csv_bytes:
        st.session_state["wizard_active_section"] = 3
    else:
        st.session_state["wizard_active_section"] = 1


# =========================================================
# REX
# =========================================================

def _render_rex_panel(session, user, planning_id):
    planning = session.get(Planning, planning_id)
    if not planning:
        st.warning("Planning introuvable.")
        return

    st.divider()
    st.subheader(f"Retour d'experience — {planning.name}")

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
        st.info("Aucun OT selectionne dans ce planning.")
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
            "planned_start_at": st.column_config.TextColumn("Debut prevu", disabled=True),
            "planned_team_name": st.column_config.TextColumn("Equipe", disabled=True),
            "rex_done": st.column_config.CheckboxColumn("Realise"),
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
            st.success("REX enregistre.")
        except Exception as e:
            session.rollback()
            st.error(f"Erreur lors de l'enregistrement du REX : {e}")


# =========================================================
# MAIN UI
# =========================================================

def render_scheduling_module(session, user):
    st.title("Scheduling")

    if not user.organization_id:
        st.warning("Aucune organisation n'est associee a cet utilisateur.")
        return

    _init_wizard_state()

    tab1, tab2 = st.tabs(["Mes plannings", "Creer un planning"])

    # =====================================================
    # MES PLANNINGS
    # =====================================================
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
                            f"Periode : {p.start_at} → {p.end_at}  \n"
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
                            st.success("Planning supprime.")
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

    # =====================================================
    # WIZARD
    # =====================================================
    with tab2:
        planning = _get_current_planning(session, st.session_state["wizard_planning_id"])

        top_col1, top_col2 = st.columns([3, 1])
        with top_col1:
            st.subheader("Wizard de creation / modification de planning")
        with top_col2:
            if st.button("Nouveau planning", use_container_width=True):
                _reset_wizard()
                st.rerun()

        if planning:
            st.info(
                f"Planning en cours : **{planning.name}** | "
                f"Statut : **{planning.status}** | "
                f"Periode : **{planning.start_at} → {planning.end_at}**"
            )

        default_name = planning.name if planning else ""
        default_sectors = planning.sectors_csv if planning else ""
        default_start_date = planning.start_at.date() if planning else datetime.now().date()
        default_end_date = planning.end_at.date() if planning else datetime.now().date()
        default_daily_open = planning.daily_open if planning else "07:00"
        default_daily_close = planning.daily_close if planning else "15:00"

        # =================================================
        # 1. PARAMETRES
        # =================================================
        with st.expander(
            "1. Parametres de l'arret",
            expanded=st.session_state["wizard_active_section"] == 1,
        ):
            with st.form("create_planning_form"):
                name = st.text_input("Nom du planning", value=default_name)
                sectors_txt = st.text_input("Secteurs (separes par des virgules)", value=default_sectors)

                col1, col2 = st.columns(2)

                with col1:
                    start_date = st.date_input("Date de debut", value=default_start_date)
                    daily_open = st.text_input("Heure d'ouverture", value=default_daily_open)

                with col2:
                    end_date = st.date_input("Date de fin", value=default_end_date)
                    daily_close = st.text_input("Heure de fermeture", value=default_daily_close)

                submitted = st.form_submit_button("Valider les parametres", use_container_width=True)

            if submitted:
                if not name.strip():
                    st.error("Le nom du planning est obligatoire.")
                    st.stop()

                if end_date < start_date:
                    st.error("La date de fin doit etre posterieure ou egale a la date de debut.")
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
                        st.success("Parametres du planning mis a jour.")
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
                        st.success(f"Planning cree avec succes : {planning.name}")

                    st.session_state["wizard_active_section"] = 2
                    st.rerun()

                except Exception as e:
                    session.rollback()
                    st.error(f"Erreur lors de l'enregistrement des parametres : {e}")

        planning = _get_current_planning(session, st.session_state["wizard_planning_id"])
        if not planning:
            return

        # =================================================
        # 2. IMPORT CSV
        # =================================================
        with st.expander(
            "2. Import CSV",
            expanded=st.session_state["wizard_active_section"] == 2,
        ):
            uploaded_file = st.file_uploader(
                "Charger un fichier CSV",
                type=["csv"],
                key=f"csv_upload_{planning.id}"
            )

            if planning.csv_filename:
                st.success(f"CSV deja enregistre : {planning.csv_filename}")

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

                            st.session_state["wizard_tasks_df"] = None
                            st.session_state["wizard_teams_df"] = None
                            st.session_state["wizard_generated_df"] = None
                            st.session_state["wizard_unscheduled_df"] = None
                            st.session_state["wizard_active_section"] = 3

                            st.success("CSV enregistre avec succes.")
                            st.rerun()

                        except Exception as e:
                            session.rollback()
                            st.error(f"Erreur lors de l'enregistrement du CSV : {e}")

                with col2:
                    if st.button("Afficher le CSV", use_container_width=True):
                        try:
                            preview_df = _read_csv_safely(file_bytes)
                            st.dataframe(preview_df, use_container_width=True, hide_index=True)
                        except Exception as e:
                            st.error(f"Erreur de lecture du CSV : {e}")

            if planning.csv_bytes:
                if st.button("Afficher le CSV enregistre", use_container_width=True):
                    try:
                        preview_df = _read_csv_safely(planning.csv_bytes)
                        st.dataframe(preview_df, use_container_width=True, hide_index=True)
                    except Exception as e:
                        st.error(f"Erreur de lecture du CSV enregistre : {e}")

        # =================================================
        # 3. MAPPING
        # =================================================
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

                    saved_mapping = _load_saved_mapping(session, user.organization_id)
                    current_mapping = st.session_state.get("wizard_mapping", {}) or saved_mapping

                    targets = [
                        ("ot_id", "OT"),
                        ("description", "Description"),
                        ("status", "Statut"),
                        ("atelier", "Atelier"),
                        ("secteur", "Secteur"),
                        ("equipment", "Equipement"),
                        ("equipment_desc", "Description equipement"),
                        ("created_at", "Cree le"),
                        ("created_by", "Cree par"),
                        ("requested_week", "Sem. souhaitee"),
                        ("condition", "Condition realisation"),
                        ("estimated_hours", "Duree estimee"),
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

                            st.success("Mapping valide et sauvegarde.")
                            st.rerun()

                except Exception as e:
                    st.error(f"Erreur lors du mapping : {e}")

        # =================================================
        # 4. SELECTION OT
        # =================================================
        with st.expander(
            "4. Selection des OT",
            expanded=st.session_state["wizard_active_section"] == 4,
        ):
            tasks_df = st.session_state.get("wizard_tasks_df")

            if tasks_df is None and len(planning.tasks) > 0:
                tasks_df = _load_tasks_df_from_db(planning)
                st.session_state["wizard_tasks_df"] = tasks_df

            if tasks_df is None:
                st.info("Valide d'abord le mapping.")
            else:
                ateliers = sorted([x for x in tasks_df["atelier"].dropna().astype(str).unique().tolist() if x])
                secteurs = sorted([x for x in tasks_df["secteur"].dropna().astype(str).unique().tolist() if x])
                statuts = sorted([x for x in tasks_df["status"].dropna().astype(str).unique().tolist() if x])

                c1, c2, c3 = st.columns(3)

                with c1:
                    selected_ateliers = st.multiselect("Ateliers", ateliers, default=ateliers, key="filter_ateliers")

                with c2:
                    selected_secteurs = st.multiselect("Secteurs", secteurs, default=secteurs, key="filter_secteurs")

                with c3:
                    selected_statuts = st.multiselect("Statuts", statuts, default=statuts, key="filter_statuts")

                filtered = tasks_df.copy()

                if ateliers:
                    filtered = filtered[filtered["atelier"].isin(selected_ateliers)]
                if secteurs:
                    filtered = filtered[filtered["secteur"].isin(selected_secteurs)]
                if statuts:
                    filtered = filtered[filtered["status"].isin(selected_statuts)]

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
                        "selected": st.column_config.CheckboxColumn("Selection"),
                        "ot_id": st.column_config.TextColumn("OT", disabled=True),
                        "description": st.column_config.TextColumn("Description", disabled=True),
                        "atelier": st.column_config.TextColumn("Atelier", disabled=True),
                        "secteur": st.column_config.TextColumn("Secteur", disabled=True),
                        "status": st.column_config.TextColumn("Statut", disabled=True),
                        "duration_hours": st.column_config.NumberColumn("Duree (h)", min_value=0.0, step=0.5),
                        "forced_team": st.column_config.TextColumn("Equipe forcee"),
                        "predecessor_ot": st.column_config.TextColumn("Predecesseur"),
                        "forced_start": st.column_config.TextColumn("Debut force"),
                    },
                    key=f"tasks_editor_{planning.id}",
                )

                if st.button("Valider la selection des OT", use_container_width=True):
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
                    st.session_state["wizard_selected_ateliers"] = sorted(
                        [x for x in updated[updated["selected"]]["atelier"].dropna().astype(str).unique().tolist() if x]
                    )
                    st.session_state["wizard_active_section"] = 5
                    st.success("Selection OT enregistree.")
                    st.rerun()

        # =================================================
        # 5. TEAMS
        # =================================================
        with st.expander(
            "5. Teams",
            expanded=st.session_state["wizard_active_section"] == 5,
        ):
            tasks_df = st.session_state.get("wizard_tasks_df")
            selected_ateliers = st.session_state.get("wizard_selected_ateliers", [])

            if tasks_df is None or not selected_ateliers:
                st.info("Valide d'abord la selection des OT.")
            else:
                stop_hours = max(
                    1.0,
                    (pd.Timestamp(planning.end_at) - pd.Timestamp(planning.start_at)).total_seconds() / 3600
                )

                selected_df = tasks_df[tasks_df["selected"]].copy()
                atelier_load = (
                    selected_df.groupby("atelier", as_index=False)["duration_hours"]
                    .sum()
                    .rename(columns={"duration_hours": "Charge selectionnee (h)"})
                )
                atelier_load = atelier_load[atelier_load["atelier"].astype(str).isin(selected_ateliers)].copy()
                atelier_load["Besoin theorique (equipes)"] = atelier_load["Charge selectionnee (h)"].apply(
                    lambda x: max(1, math.ceil(x / stop_hours)) if x > 0 else 1
                )

                all_rosters = []

                for atelier in selected_ateliers:
                    row = atelier_load[atelier_load["atelier"].astype(str) == atelier]
                    theoretical = int(row["Besoin theorique (equipes)"].iloc[0]) if not row.empty else 1
                    default_nb = int(st.session_state["wizard_team_counts"].get(atelier, theoretical))

                    nb_equipes = st.number_input(
                        f"{atelier} - nombre d'equipes",
                        min_value=0,
                        max_value=max(20, theoretical + 10),
                        value=default_nb,
                        step=1,
                        key=f"teams_{_safe_key(atelier)}"
                    )
                    st.session_state["wizard_team_counts"][atelier] = nb_equipes

                    existing_df = st.session_state.get("wizard_teams_df")
                    if existing_df is not None and not existing_df.empty:
                        atelier_existing = existing_df[existing_df["atelier"] == atelier].copy()
                    else:
                        atelier_existing = pd.DataFrame()

                    if atelier_existing.empty or len(atelier_existing) != nb_equipes:
                        atelier_existing = _initialize_team_roster(
                            atelier=atelier,
                            n_teams=nb_equipes,
                            start_dt=pd.Timestamp(planning.start_at),
                            end_dt=pd.Timestamp(planning.end_at),
                        )

                    st.caption(f"{atelier} — charge selectionnee : {float(row['Charge selectionnee (h)'].iloc[0]) if not row.empty else 0:.1f} h")

                    edited_roster = st.data_editor(
                        atelier_existing,
                        use_container_width=True,
                        hide_index=True,
                        num_rows="fixed",
                        key=f"roster_editor_{_safe_key(atelier)}",
                        column_config={
                            "atelier": st.column_config.TextColumn("Atelier", disabled=True),
                            "code": st.column_config.TextColumn("Code equipe"),
                            "name": st.column_config.TextColumn("Nom equipe"),
                            "available_from": st.column_config.TextColumn("Debut disponibilite"),
                            "available_to": st.column_config.TextColumn("Fin disponibilite"),
                        },
                    )

                    all_rosters.append(edited_roster.copy())
                    st.divider()

                if st.button("Valider les equipes", use_container_width=True):
                    if all_rosters:
                        teams_df = pd.concat(all_rosters, ignore_index=True)
                    else:
                        teams_df = pd.DataFrame(columns=["atelier", "code", "name", "available_from", "available_to"])

                    st.session_state["wizard_teams_df"] = teams_df
                    st.session_state["wizard_active_section"] = 6
                    st.success("Equipes enregistrees.")
                    st.rerun()

        # =================================================
        # 6. GENERATION
        # =================================================
        with st.expander(
            "6. Generation du planning",
            expanded=st.session_state["wizard_active_section"] == 6,
        ):
            tasks_df = st.session_state.get("wizard_tasks_df")
            teams_df = st.session_state.get("wizard_teams_df")

            if tasks_df is None or teams_df is None:
                st.info("Valide d'abord la selection OT et les equipes.")
            else:
                selected_df = tasks_df[tasks_df["selected"]].copy()
                total_selected_hours = float(selected_df["duration_hours"].sum()) if not selected_df.empty else 0.0
                total_capacity_hours = 0.0

                if teams_df is not None and not teams_df.empty:
                    for _, r in teams_df.iterrows():
                        start = _parse_dt_any(r.get("available_from", ""))
                        end = _parse_dt_any(r.get("available_to", ""))
                        if start is not None and end is not None and end > start:
                            total_capacity_hours += (end - start).total_seconds() / 3600

                c1, c2, c3 = st.columns(3)
                c1.metric("OT selectionnes", len(selected_df))
                c2.metric("Charge selectionnee (h)", round(total_selected_hours, 1))
                c3.metric("Capacite theorique totale (h)", round(total_capacity_hours, 1))

                if st.button("Generer et enregistrer le planning", type="primary", use_container_width=True):
                    try:
                        generated_df, unscheduled_df = _generate_schedule(tasks_df, teams_df, planning)
                        st.session_state["wizard_tasks_df"] = generated_df
                        st.session_state["wizard_generated_df"] = generated_df.copy()
                        st.session_state["wizard_unscheduled_df"] = unscheduled_df.copy()

                        _persist_generation(session, planning, generated_df, teams_df)

                        st.success("Planning genere et enregistre.")
                        st.rerun()

                    except Exception as e:
                        session.rollback()
                        st.error(f"Erreur lors de la generation : {e}")

                generated_df = st.session_state.get("wizard_generated_df")
                unscheduled_df = st.session_state.get("wizard_unscheduled_df")

                if generated_df is not None:
                    planned_preview = generated_df[
                        generated_df["selected"] == True
                    ][
                        [
                            "ot_id",
                            "description",
                            "atelier",
                            "duration_hours",
                            "planned_start_at",
                            "planned_end_at",
                            "planned_team_name",
                            "selected_warning",
                            "commentaire",
                        ]
                    ].copy()

                    st.subheader("Planning genere")
                    st.dataframe(planned_preview, use_container_width=True, hide_index=True)

                if unscheduled_df is not None and not unscheduled_df.empty:
                    st.subheader("OT non planifies")
                    st.dataframe(unscheduled_df, use_container_width=True, hide_index=True)
