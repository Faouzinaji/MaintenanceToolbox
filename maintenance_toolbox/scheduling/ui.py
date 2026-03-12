from datetime import datetime, time, timezone
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

@st.cache_data(show_spinner=False)
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
        raise ValueError(f"Impossible de decoder le fichier : {last_error}")

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
        raise ValueError("Le separateur du CSV n'a pas ete correctement detecte.")

    return best_df


def _combine_date_time(d, hhmm: str):
    h, m = hhmm.split(":")
    return datetime.combine(d, time(int(h), int(m)))


def _safe_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def _safe_key(text):
    return re.sub(r"[^a-zA-Z0-9_]", "_", str(text))


def _normalize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip().lower()


def _shorten(text, n=48):
    t = "" if pd.isna(text) else str(text)
    return t if len(t) <= n else t[: n - 3] + "..."


def _parse_dt_any(value, fallback=None):
    txt = _safe_text(value)
    if not txt:
        if fallback is None:
            return None
        return pd.Timestamp(fallback)

    try:
        ts = pd.Timestamp(txt)
    except Exception:
        if fallback is None:
            return None
        ts = pd.Timestamp(fallback)

    return _normalize_ts(ts)


def _normalize_ts(ts):
    if ts is None:
        return None
    ts = pd.Timestamp(ts)
    if ts.tzinfo is not None:
        return ts.tz_localize(None)
    return ts


def _status_score(status):
    s = _normalize_text(status)
    if s in ["panne a executer", "panne à exécuter"]:
        return 100
    if "planifi" in s:
        return 80
    if "approuv" in s:
        return 70
    if "prepar" in s:
        return 60
    if "attente" in s:
        return 30
    if "demand" in s:
        return 20
    return 0


def _guess_duration_from_text(description, equipment_desc=""):
    txt = f"{description} {equipment_desc}".lower()

    if any(x in txt for x in ["nettoy", "inspection", "graiss", "controle", "contrôle", "visite"]):
        return 1.0
    if any(x in txt for x in ["remplac", "change", "changement", "depose", "dep"]):
        return 2.0
    if any(x in txt for x in ["moteur", "rouleau", "convoyeur", "tambour", "chaine", "chaîne", "courroie"]):
        return 3.0
    if any(x in txt for x in ["repar", "reparation", "depann", "panne"]):
        return 2.5
    return 2.0


def _build_comment(row):
    atelier = _safe_text(row.get("atelier", "")).upper()
    probable = []
    checks = []
    prep = []

    if atelier == "289.CEMOMC":
        probable += ["usure mecanique", "desalignement", "encrassement"]
        checks += ["controle visuel organes tournants", "verifier alignement et jeu"]
        prep += ["outillage mecanique", "acces securise"]
    elif atelier == "289.CEMOEL":
        probable += ["defaut capteur", "connexion degradee", "anomalie alimentation"]
        checks += ["controle alimentation", "verification capteurs"]
        prep += ["multimetre", "consignation electrique"]
    elif atelier == "289.CEMOMT":
        probable += ["derive de mesure", "capteur en defaut"]
        checks += ["controle du capteur", "verification du signal"]
        prep += ["materiel de calibrage"]
    elif atelier == "289.CEMOCH":
        probable += ["usure structurelle", "deformation"]
        checks += ["controle visuel structure", "verification fixations"]
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
# STATE
# =========================================================

def _init_wizard_state():
    defaults = {
        "scheduling_view": "Mes plannings",
        "wizard_planning_id": None,
        "wizard_active_section": 1,
        "wizard_mapping": {},
        "wizard_tasks_df": None,
        "wizard_filtered_tasks_df": None,
        "wizard_selected_df": None,
        "wizard_teams_df": None,
        "wizard_team_counts": {},
        "wizard_selected_ateliers": [],
        "wizard_selected_secteurs": [],
        "wizard_generated_df": None,
        "wizard_unscheduled_df": None,
        "wizard_current_atelier_idx": 0,
        "wizard_ot_expanded_atelier": None,
        "wizard_slot_actions_df": None,
        "rex_planning_id": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _reset_wizard():
    st.session_state["wizard_planning_id"] = None
    st.session_state["wizard_active_section"] = 1
    st.session_state["wizard_mapping"] = {}
    st.session_state["wizard_tasks_df"] = None
    st.session_state["wizard_filtered_tasks_df"] = None
    st.session_state["wizard_selected_df"] = None
    st.session_state["wizard_teams_df"] = None
    st.session_state["wizard_team_counts"] = {}
    st.session_state["wizard_selected_ateliers"] = []
    st.session_state["wizard_selected_secteurs"] = []
    st.session_state["wizard_generated_df"] = None
    st.session_state["wizard_unscheduled_df"] = None
    st.session_state["wizard_current_atelier_idx"] = 0
    st.session_state["wizard_ot_expanded_atelier"] = None
    st.session_state["wizard_slot_actions_df"] = None


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
# BUILDERS
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
        out["estimated_hours"] = pd.to_numeric(df[col("estimated_hours")], errors="coerce").fillna(0.0)
    else:
        out["estimated_hours"] = 0.0

    out = out[
        (out["ot_id"] != "") |
        (out["description"] != "") |
        (out["atelier"] != "")
    ].copy()

    out = out.reset_index(drop=True)
    out["priority_score"] = out["status"].apply(_status_score)
    out["selected"] = False
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


def _build_filtered_tasks_df(tasks_df, selected_ateliers, selected_secteurs):
    df = tasks_df.copy()

    if selected_ateliers:
        df = df[df["atelier"].isin(selected_ateliers)]

    if selected_secteurs:
        df = df[df["secteur"].isin(selected_secteurs)]

    return df.copy()


def _initialize_team_roster(atelier, n_teams, start_dt, end_dt):
    rows = []
    for i in range(1, n_teams + 1):
        code = f"{atelier}-{i}"
        rows.append(
            {
                "atelier": atelier,
                "code": code,
                "name": code,
                "available_from": _normalize_ts(start_dt).strftime("%Y-%m-%d %H:%M"),
                "available_to": _normalize_ts(end_dt).strftime("%Y-%m-%d %H:%M"),
            }
        )
    return pd.DataFrame(rows)


def _build_resources_from_team_rosters(selected_ateliers, teams_df, default_start, default_end):
    resources_by_atelier = {}

    for atelier in selected_ateliers:
        roster = teams_df[teams_df["atelier"] == atelier].copy() if teams_df is not None and not teams_df.empty else pd.DataFrame()
        rows = []

        if not roster.empty:
            for _, rr in roster.iterrows():
                code = _safe_text(rr.get("code", ""))
                name = _safe_text(rr.get("name", "")) or code
                start = _normalize_ts(_parse_dt_any(rr.get("available_from", ""), default_start))
                end = _normalize_ts(_parse_dt_any(rr.get("available_to", ""), default_end))

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


def _prepare_tasks_for_planning(selected_df):
    if selected_df is None or selected_df.empty:
        return pd.DataFrame()

    df = selected_df.copy()
    df["task_id"] = df["ot_id"].astype(str)
    df["duration_h"] = pd.to_numeric(df["duration_hours"], errors="coerce").fillna(0.0)
    df["forced_start_dt"] = df["forced_start"].apply(lambda x: _normalize_ts(_parse_dt_any(x, None)))
    df["forced_teams_list"] = df["forced_team"].apply(
        lambda x: [t.strip() for t in re.split(r"[;,]", str(x)) if t.strip()]
    )
    df["slot_type"] = "DURING"

    return df[
        [
            "task_id",
            "ot_id",
            "description",
            "equipment_desc",
            "equipment",
            "atelier",
            "duration_h",
            "predecessor_ot",
            "forced_start_dt",
            "forced_teams_list",
            "priority_score",
            "status",
            "slot_type",
        ]
    ].copy()


def _prepare_manual_actions_df(actions_df):
    if actions_df is None or actions_df.empty:
        return pd.DataFrame(columns=["task_id", "ot_id", "description", "equipment_desc", "equipment", "atelier", "duration_h", "predecessor_ot", "forced_start_dt", "forced_teams_list", "priority_score", "status", "slot_type"])

    df = actions_df.copy()
    df["task_id"] = df["action_id"].astype(str)
    df["ot_id"] = df["action_id"].astype(str)
    df["equipment_desc"] = ""
    df["equipment"] = ""
    df["predecessor_ot"] = ""
    df["forced_teams_list"] = df["forced_team"].apply(lambda x: [t.strip() for t in re.split(r"[;,]", str(x)) if t.strip()])
    df["forced_start_dt"] = df["forced_start"].apply(lambda x: _normalize_ts(_parse_dt_any(x, None)))
    df["duration_h"] = pd.to_numeric(df["duration_hours"], errors="coerce").fillna(0.0)
    df["priority_score"] = 999
    df["status"] = "manual"

    return df[
        [
            "task_id",
            "ot_id",
            "description",
            "equipment_desc",
            "equipment",
            "atelier",
            "duration_h",
            "predecessor_ot",
            "forced_start_dt",
            "forced_teams_list",
            "priority_score",
            "status",
            "slot_type",
        ]
    ].copy()


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


def _schedule_standard_tasks(tasks_df, resources_by_atelier, window_start, window_end, allow_overrun=False):
    planning_rows = []
    unscheduled_rows = []
    scheduled_map = {}

    window_start = _normalize_ts(window_start)
    window_end = _normalize_ts(window_end)

    if tasks_df.empty:
        return planning_rows, unscheduled_rows, scheduled_map

    resource_states = {}
    for atelier, res_list in resources_by_atelier.items():
        resource_states[atelier] = []
        for res in res_list:
            start = max(window_start, _normalize_ts(res["available_from"]))
            end = _normalize_ts(res["available_to"])
            if not allow_overrun:
                end = min(end, window_end)

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
            forced_start = _normalize_ts(t.get("forced_start_dt", None))
            forced_teams = t.get("forced_teams_list", [])

            if duration <= 0:
                duration = 1.0

            if atelier not in resource_states or len(resource_states[atelier]) == 0:
                rr = dict(t)
                rr["reason"] = "Aucune equipe disponible sur cet atelier"
                unscheduled_rows.append(rr)
                unscheduled.remove(task_id)
                progress = True
                continue

            pred = str(t.get("predecessor_ot", "")).strip()
            pred_end = window_start
            if pred and pred in scheduled_map:
                pred_end = _normalize_ts(scheduled_map[pred]["planned_end_at"])

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
                    _normalize_ts(c["current"]),
                    pred_end,
                    _normalize_ts(c["available_from"]),
                    window_start,
                )

                if forced_start is not None:
                    start_candidate = max(start_candidate, forced_start)

                if start_candidate >= _normalize_ts(c["available_to"]):
                    continue

                end_candidate = start_candidate + pd.Timedelta(hours=duration)

                if not allow_overrun and end_candidate > min(window_end, _normalize_ts(c["available_to"])):
                    continue
                if allow_overrun and end_candidate > _normalize_ts(c["available_to"]):
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


def _build_coactivity_df(selected_df):
    if selected_df is None or selected_df.empty:
        return pd.DataFrame(columns=["Equipement", "OT selectionnes"])

    tmp = selected_df.copy()
    tmp["equipment"] = tmp["equipment"].fillna("").astype(str)
    tmp = tmp[tmp["equipment"] != ""].copy()

    if tmp.empty:
        return pd.DataFrame(columns=["Equipement", "OT selectionnes"])

    tmp["label"] = tmp.apply(lambda r: f"{r['ot_id']} | {r['description']}", axis=1)
    grp = tmp.groupby("equipment")["label"].apply(list).reset_index()
    grp["count"] = grp["label"].apply(len)
    grp = grp[grp["count"] > 1].copy()

    if grp.empty:
        return pd.DataFrame(columns=["Equipement", "OT selectionnes"])

    grp["OT selectionnes"] = grp["label"].apply(lambda x: " ; ".join(x))
    grp = grp.rename(columns={"equipment": "Equipement"})
    return grp[["Equipement", "OT selectionnes"]]


def _generate_schedule(selected_df, teams_df, planning, manual_actions_df=None):
    selected_ateliers = sorted([x for x in selected_df["atelier"].dropna().astype(str).unique().tolist() if x])

    resources_by_atelier = _build_resources_from_team_rosters(
        selected_ateliers=selected_ateliers,
        teams_df=teams_df,
        default_start=planning.start_at,
        default_end=planning.end_at,
    )

    all_tasks = _prepare_tasks_for_planning(selected_df)

    manual_df = _prepare_manual_actions_df(manual_actions_df)
    if not manual_df.empty:
        all_tasks = pd.concat([manual_df, all_tasks], ignore_index=True)

    planning_rows = []
    unscheduled_rows = []

    start_df = all_tasks[all_tasks["slot_type"] == "START"].copy()
    during_df = all_tasks[all_tasks["slot_type"] == "DURING"].copy()
    end_df = all_tasks[all_tasks["slot_type"] == "END"].copy()

    window_start = _normalize_ts(planning.start_at)
    window_end = _normalize_ts(planning.end_at)

    latest_end = max(
        [r["available_to"] for res in resources_by_atelier.values() for r in res],
        default=window_end,
    )
    latest_end = _normalize_ts(latest_end)

    if not start_df.empty:
        pr, ur, _ = _schedule_standard_tasks(start_df, resources_by_atelier, window_start, window_end, allow_overrun=False)
        planning_rows += pr
        unscheduled_rows += ur

    if not during_df.empty:
        pr, ur, _ = _schedule_standard_tasks(during_df, resources_by_atelier, window_start, latest_end, allow_overrun=True)
        planning_rows += pr
        unscheduled_rows += ur

    if not end_df.empty:
        pr, ur, _ = _schedule_standard_tasks(end_df, resources_by_atelier, window_end, latest_end, allow_overrun=True)
        planning_rows += pr
        unscheduled_rows += ur

    planning_df = pd.DataFrame(planning_rows)
    unscheduled_df = pd.DataFrame(unscheduled_rows)

    result = selected_df.copy()
    result["selected_warning"] = ""
    result["planned_start_at"] = ""
    result["planned_end_at"] = ""
    result["planned_team_name"] = ""
    result["commentaire"] = ""

    if not planning_df.empty:
        planning_df["commentaire"] = planning_df.apply(
            lambda r: _build_comment({"atelier": r.get("atelier", "")}),
            axis=1
        )

        only_ot = planning_df[planning_df["task_id"].astype(str).str.startswith("ACT_") == False].copy()

        for _, row in only_ot.iterrows():
            mask = result["ot_id"] == str(row["ot_id"])
            result.loc[mask, "planned_start_at"] = _normalize_ts(row["planned_start_at"]).strftime("%Y-%m-%d %H:%M")
            result.loc[mask, "planned_end_at"] = _normalize_ts(row["planned_end_at"]).strftime("%Y-%m-%d %H:%M")
            result.loc[mask, "planned_team_name"] = row["planned_team_name"]
            result.loc[mask, "commentaire"] = row["commentaire"]

    if not unscheduled_df.empty:
        unsched_ot = unscheduled_df[unscheduled_df["task_id"].astype(str).str.startswith("ACT_") == False].copy()
        for _, row in unsched_ot.iterrows():
            mask = result["ot_id"] == str(row["ot_id"])
            result.loc[mask, "selected_warning"] = row.get("reason", "Non planifie")

    manual_result = planning_df[planning_df["task_id"].astype(str).str.startswith("ACT_")].copy() if not planning_df.empty else pd.DataFrame()

    return result, unscheduled_df, manual_result


def _persist_generation(session, planning, all_tasks_df, teams_df):
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
                available_from=_normalize_ts(_parse_dt_any(row["available_from"])),
                available_to=_normalize_ts(_parse_dt_any(row["available_to"])),
            )
        )

    for _, row in all_tasks_df.iterrows():
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
                forced_start_at=_normalize_ts(_parse_dt_any(row["forced_start"])),
                planned_start_at=_normalize_ts(_parse_dt_any(row["planned_start_at"])),
                planned_end_at=_normalize_ts(_parse_dt_any(row["planned_end_at"])),
                planned_team_name=_safe_text(row["planned_team_name"]),
            )
        )

    planning.status = "generated"
    planning.updated_at = datetime.now(timezone.utc)
    session.commit()
    session.refresh(planning)


def _load_planning_into_wizard(session, user, planning):
    st.session_state["scheduling_view"] = "Creer un planning"
    st.session_state["wizard_planning_id"] = planning.id
    st.session_state["wizard_mapping"] = _load_saved_mapping(session, user.organization_id)
    st.session_state["wizard_tasks_df"] = None
    st.session_state["wizard_filtered_tasks_df"] = None
    st.session_state["wizard_selected_df"] = None
    st.session_state["wizard_teams_df"] = None
    st.session_state["wizard_team_counts"] = {}
    st.session_state["wizard_selected_ateliers"] = []
    st.session_state["wizard_selected_secteurs"] = []
    st.session_state["wizard_generated_df"] = None
    st.session_state["wizard_unscheduled_df"] = None
    st.session_state["wizard_current_atelier_idx"] = 0
    st.session_state["wizard_slot_actions_df"] = None

    if planning.csv_bytes:
        st.session_state["wizard_active_section"] = 2
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
# UI
# =========================================================

def render_scheduling_module(session, user):
    st.title("Scheduling")

    if not user.organization_id:
        st.warning("Aucune organisation n'est associee a cet utilisateur.")
        return

    _init_wizard_state()

    st.radio(
        "Navigation Scheduling",
        options=["Mes plannings", "Creer un planning"],
        horizontal=True,
        label_visibility="collapsed",
        key="scheduling_view",
    )

    if st.session_state["scheduling_view"] == "Mes plannings":
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
                            st.rerun()

                    with c3:
                        if st.button("🗑️", key=f"delete_{p.id}", use_container_width=True):
                            _delete_planning(session, p.id)
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

        return

    planning = _get_current_planning(session, st.session_state["wizard_planning_id"])

    top_col1, top_col2 = st.columns([3, 1])
    with top_col1:
        st.subheader("Wizard de creation / modification de planning")
    with top_col2:
        if st.button("Nouveau planning", key="new_planning_btn", use_container_width=True):
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

    with st.expander("1. Parametres de l'arret", expanded=st.session_state["wizard_active_section"] == 1):
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

                st.session_state["wizard_active_section"] = 2
                st.rerun()

            except Exception as e:
                session.rollback()
                st.error(f"Erreur lors de l'enregistrement des parametres : {e}")

    planning = _get_current_planning(session, st.session_state["wizard_planning_id"])
    if not planning:
        return

    with st.expander("2. Import CSV + Mapping", expanded=st.session_state["wizard_active_section"] == 2):
        uploaded_file = st.file_uploader(
            "Charger un fichier CSV",
            type=["csv"],
            key=f"csv_upload_{planning.id}"
        )

        local_df = None

        if uploaded_file is not None:
            try:
                planning.csv_filename = uploaded_file.name
                planning.csv_bytes = uploaded_file.getvalue()
                planning.updated_at = datetime.now(timezone.utc)
                session.commit()
                local_df = _read_csv_safely(planning.csv_bytes)
            except Exception as e:
                session.rollback()
                st.error(f"Erreur lors du chargement du CSV : {e}")

        elif planning.csv_bytes:
            try:
                local_df = _read_csv_safely(planning.csv_bytes)
            except Exception as e:
                st.error(f"Erreur de lecture du CSV enregistre : {e}")

        if local_df is not None:
            columns = list(local_df.columns)
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
                    mapping[key] = st.selectbox(label, options, index=default_index, key=f"map_{key}")

                submitted_mapping = st.form_submit_button("Valider le mapping", use_container_width=True)

            if submitted_mapping:
                if not mapping["ot_id"] or not mapping["description"] or not mapping["atelier"]:
                    st.error("Au minimum, mappe OT, Description et Atelier.")
                else:
                    st.session_state["wizard_mapping"] = mapping
                    _save_mapping(session, user.organization_id, mapping)
                    st.session_state["wizard_tasks_df"] = _build_tasks_df_from_mapping(local_df, mapping)
                    st.session_state["wizard_active_section"] = 3
                    st.rerun()

    with st.expander("3. Selection ateliers / secteurs", expanded=st.session_state["wizard_active_section"] == 3):
        tasks_df = st.session_state.get("wizard_tasks_df")

        if tasks_df is None:
            st.info("Valide d'abord le mapping.")
        else:
            ateliers = sorted([x for x in tasks_df["atelier"].dropna().astype(str).unique().tolist() if x])
            secteurs = sorted([x for x in tasks_df["secteur"].dropna().astype(str).unique().tolist() if x])

            with st.form("scope_form"):
                selected_ateliers = st.multiselect(
                    "Ateliers selectionnes",
                    options=ateliers,
                    default=st.session_state["wizard_selected_ateliers"] or ateliers,
                )
                selected_secteurs = st.multiselect(
                    "Secteurs selectionnes",
                    options=secteurs,
                    default=st.session_state["wizard_selected_secteurs"] or secteurs,
                )

                submit_scope = st.form_submit_button("Valider le scope", use_container_width=True)

            if submit_scope:
                st.session_state["wizard_selected_ateliers"] = selected_ateliers
                st.session_state["wizard_selected_secteurs"] = selected_secteurs
                st.session_state["wizard_filtered_tasks_df"] = _build_filtered_tasks_df(tasks_df, selected_ateliers, selected_secteurs)
                st.session_state["wizard_active_section"] = 4
                st.rerun()

    with st.expander("4. Teams", expanded=st.session_state["wizard_active_section"] == 4):
        filtered_df = st.session_state.get("wizard_filtered_tasks_df")
        selected_ateliers = st.session_state.get("wizard_selected_ateliers", [])

        if filtered_df is None or not selected_ateliers:
            st.info("Valide d'abord le scope.")
        else:
            stop_hours = max(
                1.0,
                (_normalize_ts(planning.end_at) - _normalize_ts(planning.start_at)).total_seconds() / 3600
            )

            all_rosters = []

            for atelier in selected_ateliers:
                atelier_df = filtered_df[filtered_df["atelier"] == atelier].copy()
                total_hours = float(atelier_df["duration_hours"].sum()) if not atelier_df.empty else 0.0
                theoretical = max(1, math.ceil(total_hours / stop_hours)) if total_hours > 0 else 1

                st.markdown(f"**{atelier}** — charge calculee : {round(total_hours,1)} h")

                nb_equipes = st.number_input(
                    f"{atelier} - nombre d'equipes disponibles",
                    min_value=0,
                    max_value=max(20, theoretical + 10),
                    value=int(st.session_state["wizard_team_counts"].get(atelier, theoretical)),
                    step=1,
                    key=f"teams_{_safe_key(atelier)}",
                )
                st.session_state["wizard_team_counts"][atelier] = nb_equipes

                with st.expander(f"Equipes {atelier}", expanded=False):
                    if nb_equipes > 0:
                        roster_df = _initialize_team_roster(
                            atelier=atelier,
                            n_teams=nb_equipes,
                            start_dt=_normalize_ts(planning.start_at),
                            end_dt=_normalize_ts(planning.end_at),
                        )
                        roster_df["label"] = roster_df["code"] + " | " + roster_df["name"]
                        edited_roster = st.data_editor(
                            roster_df[["atelier", "code", "name", "available_from", "available_to"]],
                            use_container_width=True,
                            hide_index=True,
                            num_rows="fixed",
                            key=f"roster_{_safe_key(atelier)}",
                            column_config={
                                "atelier": st.column_config.TextColumn("Atelier", disabled=True),
                                "code": st.column_config.TextColumn("Code equipe"),
                                "name": st.column_config.TextColumn("Nom equipe"),
                                "available_from": st.column_config.TextColumn("Debut disponibilite"),
                                "available_to": st.column_config.TextColumn("Fin disponibilite"),
                            },
                        )
                        all_rosters.append(edited_roster.copy())
                    else:
                        st.info("Aucune equipe pour cet atelier.")

            if st.button("Valider les equipes", key="validate_teams_btn", use_container_width=True):
                st.session_state["wizard_teams_df"] = (
                    pd.concat(all_rosters, ignore_index=True)
                    if all_rosters
                    else pd.DataFrame(columns=["atelier", "code", "name", "available_from", "available_to"])
                )
                st.session_state["wizard_current_atelier_idx"] = 0
                st.session_state["wizard_active_section"] = 5
                st.rerun()

    with st.expander("5. Selection des OT par atelier", expanded=st.session_state["wizard_active_section"] == 5):
        filtered_df = st.session_state.get("wizard_filtered_tasks_df")
        teams_df = st.session_state.get("wizard_teams_df")
        selected_ateliers = st.session_state.get("wizard_selected_ateliers", [])

        if filtered_df is None or teams_df is None or not selected_ateliers:
            st.info("Valide d'abord les equipes.")
        else:
            for idx, atelier in enumerate(selected_ateliers):
                store_key = f"atelier_selection_{_safe_key(atelier)}"

                atelier_df = filtered_df[filtered_df["atelier"] == atelier].copy()
                if store_key not in st.session_state:
                    init_df = atelier_df.copy()
                    init_df["selected"] = False
                    st.session_state[store_key] = init_df

                work_df = st.session_state[store_key].copy()
                selected_hours = float(work_df.loc[work_df["selected"] == True, "duration_hours"].sum()) if not work_df.empty else 0.0

                expanded = idx == st.session_state.get("wizard_current_atelier_idx", 0)
                with st.expander(f"{atelier} — charge selectionnee : {round(selected_hours,1)} h", expanded=expanded):
                    with st.form(f"atelier_main_form_{_safe_key(atelier)}"):
                        display_df = work_df[
                            ["selected", "ot_id", "description", "equipment_desc", "status", "duration_hours"]
                        ].copy()
                        display_df["description"] = display_df["description"].apply(lambda x: _shorten(x, 42))
                        display_df["equipment_desc"] = display_df["equipment_desc"].apply(lambda x: _shorten(x, 30))

                        edited_main = st.data_editor(
                            display_df,
                            use_container_width=True,
                            hide_index=True,
                            num_rows="fixed",
                            key=f"main_editor_{_safe_key(atelier)}",
                            column_config={
                                "selected": st.column_config.CheckboxColumn(""),
                                "ot_id": st.column_config.TextColumn("OT", disabled=True),
                                "description": st.column_config.TextColumn("Description", disabled=True),
                                "equipment_desc": st.column_config.TextColumn("Equipement", disabled=True),
                                "status": st.column_config.TextColumn("Statut", disabled=True),
                                "duration_hours": st.column_config.NumberColumn("Duree retenue (h)", min_value=0.0, step=0.5),
                            },
                        )

                        validate_selection = st.form_submit_button("Valider la selection", use_container_width=True)

                    if validate_selection:
                        tmp = work_df.copy()
                        tmp["OT_STR"] = tmp["ot_id"].astype(str)
                        edited = edited_main.copy()
                        edited["OT_STR"] = edited["ot_id"].astype(str)

                        tmp["selected"] = tmp["OT_STR"].map(edited.set_index("OT_STR")["selected"].to_dict()).fillna(False)
                        tmp["duration_hours"] = tmp["OT_STR"].map(edited.set_index("OT_STR")["duration_hours"].to_dict()).fillna(tmp["duration_hours"])
                        tmp.drop(columns=["OT_STR"], inplace=True)
                        st.session_state[store_key] = tmp
                        st.rerun()

                    selected_rows = st.session_state[store_key][st.session_state[store_key]["selected"] == True].copy()

                    if not selected_rows.empty:
                        st.markdown("#### Contraintes des OT selectionnes")

                        roster = teams_df[teams_df["atelier"] == atelier].copy()
                        team_options = []
                        if not roster.empty:
                            team_options = [f"{r['code']} | {r['name']}" for _, r in roster.iterrows()]
                        team_map = {f"{r['code']} | {r['name']}": r["code"] for _, r in roster.iterrows()} if not roster.empty else {}

                        global_selected = []
                        for at in selected_ateliers:
                            sk = f"atelier_selection_{_safe_key(at)}"
                            if sk in st.session_state:
                                sdf = st.session_state[sk]
                                sdf = sdf[sdf["selected"] == True].copy()
                                if not sdf.empty:
                                    global_selected.append(sdf[["ot_id", "description", "equipment_desc"]])

                        pred_df = pd.concat(global_selected, ignore_index=True).drop_duplicates(subset=["ot_id"]) if global_selected else pd.DataFrame(columns=["ot_id", "description", "equipment_desc"])
                        pred_labels = {
                            str(r["ot_id"]): f"{r['ot_id']} | {_shorten(r['description'], 40)} | {_shorten(r['equipment_desc'], 30)}"
                            for _, r in pred_df.iterrows()
                        }

                        with st.form(f"constraints_form_{_safe_key(atelier)}"):
                            updated_pred = {}
                            updated_fstart_date = {}
                            updated_fstart_time = {}
                            updated_teams = {}

                            for _, r in selected_rows.iterrows():
                                ot_id = str(r["ot_id"])

                                with st.expander(f"{ot_id} — {_shorten(r['description'], 70)}", expanded=False):
                                    pred_options = [""] + [x for x in pred_labels.keys() if x != ot_id]
                                    current_pred = _safe_text(r.get("predecessor_ot", ""))

                                    pred_val = st.selectbox(
                                        f"Predecesseur pour OT {ot_id}",
                                        options=pred_options,
                                        index=pred_options.index(current_pred) if current_pred in pred_options else 0,
                                        format_func=lambda x: "Aucun" if x == "" else pred_labels.get(x, x),
                                        key=f"pred_{_safe_key(atelier)}_{ot_id}"
                                    )
                                    updated_pred[ot_id] = pred_val

                                    current_forced = _parse_dt_any(r.get("forced_start", ""), _normalize_ts(planning.start_at))
                                    default_date = current_forced.date() if current_forced is not None else _normalize_ts(planning.start_at).date()
                                    default_time = current_forced.time() if current_forced is not None else _normalize_ts(planning.start_at).time()

                                    c1, c2 = st.columns(2)
                                    with c1:
                                        forced_date = st.date_input(
                                            f"Date forcee pour OT {ot_id}",
                                            value=default_date,
                                            key=f"forced_date_{_safe_key(atelier)}_{ot_id}"
                                        )
                                    with c2:
                                        forced_time = st.time_input(
                                            f"Heure forcee pour OT {ot_id}",
                                            value=default_time,
                                            key=f"forced_time_{_safe_key(atelier)}_{ot_id}"
                                        )

                                    updated_fstart_date[ot_id] = forced_date
                                    updated_fstart_time[ot_id] = forced_time

                                    current_teams_raw = _safe_text(r.get("forced_team", ""))
                                    current_teams_codes = [x.strip() for x in re.split(r"[;,]", current_teams_raw) if x.strip()]
                                    current_teams_display = []
                                    for disp, code in team_map.items():
                                        if code in current_teams_codes:
                                            current_teams_display.append(disp)

                                    forced_team_sel = st.multiselect(
                                        f"Equipes forcees pour OT {ot_id}",
                                        options=team_options,
                                        default=current_teams_display,
                                        key=f"fteams_{_safe_key(atelier)}_{ot_id}"
                                    )
                                    updated_teams[ot_id] = [team_map[x] for x in forced_team_sel]

                            save_atelier = st.form_submit_button("Enregistrer atelier", use_container_width=True)

                        if save_atelier:
                            tmp = st.session_state[store_key].copy()

                            for ot_id in tmp["ot_id"].astype(str).tolist():
                                mask = tmp["ot_id"].astype(str) == ot_id
                                if ot_id in updated_pred:
                                    tmp.loc[mask, "predecessor_ot"] = updated_pred[ot_id]
                                if ot_id in updated_fstart_date and ot_id in updated_fstart_time:
                                    dt_val = datetime.combine(updated_fstart_date[ot_id], updated_fstart_time[ot_id])
                                    tmp.loc[mask, "forced_start"] = dt_val.strftime("%Y-%m-%d %H:%M")
                                if ot_id in updated_teams:
                                    tmp.loc[mask, "forced_team"] = "; ".join(updated_teams[ot_id])

                            st.session_state[store_key] = tmp

                            if idx < len(selected_ateliers) - 1:
                                st.session_state["wizard_current_atelier_idx"] = idx + 1
                            st.rerun()

            if st.button("Finaliser la selection des OT", key="finalize_ot_btn", use_container_width=True):
                final_frames = []
                for at in selected_ateliers:
                    sk = f"atelier_selection_{_safe_key(at)}"
                    if sk in st.session_state:
                        sdf = st.session_state[sk]
                        sdf = sdf[sdf["selected"] == True].copy()
                        if not sdf.empty:
                            final_frames.append(sdf)

                st.session_state["wizard_selected_df"] = pd.concat(final_frames, ignore_index=True) if final_frames else pd.DataFrame()
                st.session_state["wizard_active_section"] = 6
                st.rerun()

    with st.expander("6. Generation du planning", expanded=st.session_state["wizard_active_section"] == 6):
        selected_df = st.session_state.get("wizard_selected_df")
        teams_df = st.session_state.get("wizard_teams_df")

        if selected_df is None or teams_df is None:
            st.info("Valide d'abord la selection OT et les equipes.")
        else:
            coactivity_df = _build_coactivity_df(selected_df)
            st.markdown("#### Coactivites detectees")
            if coactivity_df.empty:
                st.info("Aucune coactivite detectee.")
            else:
                st.dataframe(coactivity_df, use_container_width=True, hide_index=True)

            st.markdown("#### Actions manuelles a inserer")
            actions_default = pd.DataFrame(
                columns=["action_id", "slot_type", "description", "atelier", "duration_hours", "forced_start", "forced_team"]
            )
            if st.session_state["wizard_slot_actions_df"] is None:
                st.session_state["wizard_slot_actions_df"] = actions_default

            edited_actions = st.data_editor(
                st.session_state["wizard_slot_actions_df"],
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                key="manual_actions_editor",
                column_config={
                    "action_id": st.column_config.TextColumn("ID action"),
                    "slot_type": st.column_config.SelectboxColumn("Type", options=["START", "END", "DURING"]),
                    "description": st.column_config.TextColumn("Description"),
                    "atelier": st.column_config.SelectboxColumn("Atelier", options=st.session_state.get("wizard_selected_ateliers", [])),
                    "duration_hours": st.column_config.NumberColumn("Duree (h)", min_value=0.0, step=0.5),
                    "forced_start": st.column_config.TextColumn("Debut force (YYYY-MM-DD HH:MM)"),
                    "forced_team": st.column_config.TextColumn("Equipe forcee (code)"),
                },
            )
            st.session_state["wizard_slot_actions_df"] = edited_actions.copy()

            selected_ateliers = st.session_state.get("wizard_selected_ateliers", [])
            stop_hours = max(
                1.0,
                (_normalize_ts(planning.end_at) - _normalize_ts(planning.start_at)).total_seconds() / 3600
            )

            total_selected_hours = float(selected_df["duration_hours"].sum()) if not selected_df.empty else 0.0
            total_capacity_hours = float(sum(len(teams_df[teams_df["atelier"] == a]) * stop_hours for a in selected_ateliers))

            c1, c2, c3 = st.columns(3)
            c1.metric("OT selectionnes", len(selected_df))
            c2.metric("Charge selectionnee (h)", round(total_selected_hours, 1))
            c3.metric("Capacite theorique totale (h)", round(total_capacity_hours, 1))

            if st.button("Generer et enregistrer le planning", key="generate_planning_btn", type="primary", use_container_width=True):
                try:
                    generated_selected_df, unscheduled_df, manual_result = _generate_schedule(
                        selected_df=selected_df,
                        teams_df=teams_df,
                        planning=planning,
                        manual_actions_df=st.session_state.get("wizard_slot_actions_df"),
                    )

                    all_tasks_df = st.session_state.get("wizard_tasks_df").copy()
                    all_tasks_df["selected"] = False
                    all_tasks_df["selected_warning"] = ""
                    all_tasks_df["planned_start_at"] = ""
                    all_tasks_df["planned_end_at"] = ""
                    all_tasks_df["planned_team_name"] = ""
                    all_tasks_df["commentaire"] = ""

                    for _, row in generated_selected_df.iterrows():
                        mask = all_tasks_df["ot_id"] == row["ot_id"]
                        all_tasks_df.loc[mask, "selected"] = True
                        all_tasks_df.loc[mask, "duration_hours"] = row.get("duration_hours", all_tasks_df.loc[mask, "duration_hours"])
                        all_tasks_df.loc[mask, "predecessor_ot"] = row.get("predecessor_ot", "")
                        all_tasks_df.loc[mask, "forced_start"] = row.get("forced_start", "")
                        all_tasks_df.loc[mask, "forced_team"] = row.get("forced_team", "")
                        all_tasks_df.loc[mask, "selected_warning"] = row.get("selected_warning", "")
                        all_tasks_df.loc[mask, "planned_start_at"] = row.get("planned_start_at", "")
                        all_tasks_df.loc[mask, "planned_end_at"] = row.get("planned_end_at", "")
                        all_tasks_df.loc[mask, "planned_team_name"] = row.get("planned_team_name", "")
                        all_tasks_df.loc[mask, "commentaire"] = row.get("commentaire", "")

                    _persist_generation(session, planning, all_tasks_df, teams_df)

                    st.session_state["wizard_generated_df"] = generated_selected_df.copy()
                    st.session_state["wizard_unscheduled_df"] = unscheduled_df.copy() if unscheduled_df is not None else pd.DataFrame()
                    st.session_state["wizard_manual_generated_df"] = manual_result.copy() if manual_result is not None else pd.DataFrame()
                    st.session_state["wizard_tasks_df"] = all_tasks_df.copy()

                    st.success("Planning genere et enregistre.")
                    st.rerun()

                except Exception as e:
                    session.rollback()
                    st.error(f"Erreur lors de la generation : {e}")

            generated_df = st.session_state.get("wizard_generated_df")
            unscheduled_df = st.session_state.get("wizard_unscheduled_df")
            manual_generated_df = st.session_state.get("wizard_manual_generated_df")

            if generated_df is not None and not generated_df.empty:
                st.subheader("Planning genere")
                st.dataframe(
                    generated_df[
                        [
                            "ot_id",
                            "description",
                            "atelier",
                            "duration_hours",
                            "planned_start_at",
                            "planned_end_at",
                            "planned_team_name",
                            "commentaire",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                )

            if manual_generated_df is not None and not manual_generated_df.empty:
                st.subheader("Actions manuelles planifiees")
                show_cols = [c for c in ["ot_id", "description", "atelier", "duration_h", "planned_start_at", "planned_end_at", "planned_team_name"] if c in manual_generated_df.columns]
                st.dataframe(manual_generated_df[show_cols], use_container_width=True, hide_index=True)

            if unscheduled_df is not None and not unscheduled_df.empty:
                st.subheader("OT non planifies")
                st.dataframe(unscheduled_df, use_container_width=True, hide_index=True)
