from datetime import datetime, time, timezone
import csv
import io
import math
import re
import random

import pandas as pd
import streamlit as st
from sqlalchemy import select

try:
    import plotly.express as px
    import plotly.graph_objects as go
    _PLOTLY_AVAILABLE = True
except ImportError:
    _PLOTLY_AVAILABLE = False

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
        # BUG FIX: tz_localize(None) raises TypeError in pandas 2.x on tz-aware timestamps.
        # Use tz_convert(None) to strip timezone without crashing.
        return ts.tz_convert(None)
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
        "wizard_csv_bytes": None,       # CSV bytes before planning exists (step 1 first)
        "wizard_csv_filename": None,    # CSV filename before planning exists
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
    st.session_state["wizard_csv_bytes"] = None
    st.session_state["wizard_csv_filename"] = None
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


def _load_planning_into_wizard(session, user, planning):
    """Restore wizard state from a saved planning for editing.
    BUG FIX: This function was missing, causing NameError on the edit button.
    """
    _reset_wizard()
    st.session_state["wizard_planning_id"] = planning.id

    mapping = _load_saved_mapping(session, user.organization_id)
    st.session_state["wizard_mapping"] = mapping

    # Restore CSV + tasks if available
    if planning.csv_bytes and mapping:
        try:
            df = _read_csv_safely(planning.csv_bytes)
            tasks_df = _build_tasks_df_from_mapping(df, mapping)

            # Restore per-atelier selections from DB
            db_selected = {t.external_ot_id for t in planning.tasks if t.selected}
            if db_selected:
                tasks_df["selected"] = tasks_df["ot_id"].astype(str).isin(db_selected)
                for t in planning.tasks:
                    if t.selected:
                        mask = tasks_df["ot_id"].astype(str) == str(t.external_ot_id)
                        if t.planned_start_at:
                            tasks_df.loc[mask, "planned_start_at"] = str(t.planned_start_at)
                        if t.planned_end_at:
                            tasks_df.loc[mask, "planned_end_at"] = str(t.planned_end_at)
                        if t.planned_team_name:
                            tasks_df.loc[mask, "planned_team_name"] = t.planned_team_name

            st.session_state["wizard_tasks_df"] = tasks_df

            # Restore ateliers / secteurs from DB tasks
            ateliers = sorted({t.atelier for t in planning.tasks if t.atelier})
            secteurs = sorted({t.secteur for t in planning.tasks if t.secteur and t.secteur != "None"})
            st.session_state["wizard_selected_ateliers"] = ateliers
            st.session_state["wizard_selected_secteurs"] = secteurs
            st.session_state["wizard_filtered_tasks_df"] = _build_filtered_tasks_df(tasks_df, ateliers, secteurs)

            # Restore teams from DB
            if planning.teams:
                team_rows = []
                for team in planning.teams:
                    team_rows.append({
                        "atelier": team.atelier,
                        "code": team.code,
                        "name": team.name,
                        "available_from": _normalize_ts(team.available_from).strftime("%Y-%m-%d %H:%M") if team.available_from else "",
                        "available_to": _normalize_ts(team.available_to).strftime("%Y-%m-%d %H:%M") if team.available_to else "",
                    })
                st.session_state["wizard_teams_df"] = pd.DataFrame(team_rows)

            # Restore selected df
            selected_tasks = [t for t in planning.tasks if t.selected]
            if selected_tasks and not tasks_df.empty:
                selected_ids = {t.external_ot_id for t in selected_tasks}
                sel_df = tasks_df[tasks_df["ot_id"].astype(str).isin(selected_ids)].copy()
                st.session_state["wizard_selected_df"] = sel_df

            st.session_state["wizard_active_section"] = 6 if planning.status == "generated" else 2
        except Exception:
            st.session_state["wizard_active_section"] = 1
    else:
        st.session_state["wizard_active_section"] = 1


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

    return df


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
    """Prepare manual actions for scheduling.

    FIX #2: Actions manuelles no longer require atelier/equipe.
    Required fields: description + slot_type (+ optionally start_time / duration_hours).
    Rows with no description are dropped; atelier stays empty if not provided.
    """
    _EMPTY_COLS = ["task_id", "ot_id", "description", "equipment_desc", "equipment",
                   "atelier", "duration_h", "predecessor_ot", "forced_start_dt",
                   "forced_teams_list", "priority_score", "status", "slot_type"]

    if actions_df is None or actions_df.empty:
        return pd.DataFrame(columns=_EMPTY_COLS)

    df = actions_df.copy()

    # Only require a non-empty description — atelier is now optional
    if "description" in df.columns:
        df = df[df["description"].astype(str).str.strip() != ""].copy()

    if df.empty:
        return pd.DataFrame(columns=_EMPTY_COLS)

    # Auto-assign IDs
    df["action_id"] = df.get("action_id", pd.Series([""] * len(df))).replace("", pd.NA)
    fallback_ids = [f"ACT_{i+1}" for i in range(len(df))]
    df["action_id"] = df["action_id"].fillna(pd.Series(fallback_ids, index=df.index))

    df["task_id"] = df["action_id"].astype(str)
    df["ot_id"] = df["action_id"].astype(str)
    df["equipment_desc"] = ""
    df["equipment"] = ""
    df["predecessor_ot"] = ""

    # atelier is optional — default to empty string
    if "atelier" not in df.columns:
        df["atelier"] = ""
    else:
        df["atelier"] = df["atelier"].fillna("").astype(str)

    # forced_team is optional
    if "forced_team" not in df.columns:
        df["forced_team"] = ""
    df["forced_teams_list"] = df["forced_team"].apply(
        lambda x: [t.strip() for t in re.split(r"[;,]", str(x)) if t.strip()]
    )

    # start_time → forced_start_dt (new simplified field)
    # Support both old "forced_start" and new "start_time" columns
    forced_src = df.get("start_time", df.get("forced_start", pd.Series([""] * len(df), index=df.index)))
    df["forced_start_dt"] = forced_src.apply(lambda x: _normalize_ts(_parse_dt_any(x, None)))

    df["duration_h"] = pd.to_numeric(df.get("duration_hours", 1.0), errors="coerce").fillna(1.0)
    df["priority_score"] = 999
    df["status"] = "manual"

    # Slot type default = DURING
    if "slot_type" not in df.columns:
        df["slot_type"] = "DURING"
    else:
        df["slot_type"] = df["slot_type"].fillna("DURING").replace("", "DURING")

    print(f"[DEBUG manual actions] {len(df)} action(s) à planifier:")
    for _, r in df.iterrows():
        print(f"  → {r['task_id']} | atelier='{r['atelier']}' | durée={r['duration_h']}h | slot={r['slot_type']} | start={r['forced_start_dt']}")

    return df[_EMPTY_COLS].copy()


def _next_valid_slot(current_ts, duration_h: float, site_open_hhmm: str, site_close_hhmm: str):
    """Find the next start/end pair that fits within site daily hours.

    FIX #8: The previous scheduler ignored daily site hours — tasks could overflow midnight.
    Now tasks are bounded by site_open→site_close each day, and pushed to the next day
    if they don't fit in the remaining day hours.

    Returns (start, end) as pd.Timestamps, or (None, None) if not schedulable.
    """
    from datetime import timedelta as _td

    def _hm(hhmm):
        h, m = map(int, hhmm.split(":"))
        return h, m

    open_h, open_m = _hm(site_open_hhmm)
    close_h, close_m = _hm(site_close_hhmm)
    day_cap_h = (close_h * 60 + close_m - open_h * 60 - open_m) / 60.0

    if duration_h <= 0:
        duration_h = 0.5
    if duration_h > day_cap_h:
        # Clamp to day capacity — task is too long for a single shift
        duration_h = day_cap_h

    ts = pd.Timestamp(current_ts)
    for _ in range(730):  # safety: max 2 years of days
        day_open = ts.replace(hour=open_h, minute=open_m, second=0, microsecond=0)
        day_close = ts.replace(hour=close_h, minute=close_m, second=0, microsecond=0)

        # Before site opens today → jump to opening
        if ts < day_open:
            ts = day_open

        # Past close today → jump to opening of next day
        if ts >= day_close:
            ts = (ts + _td(days=1)).replace(hour=open_h, minute=open_m, second=0, microsecond=0)
            continue

        end_candidate = ts + pd.Timedelta(hours=duration_h)
        if end_candidate <= day_close:
            return ts, end_candidate

        # Doesn't fit in remaining time today → push to next day
        ts = (ts + _td(days=1)).replace(hour=open_h, minute=open_m, second=0, microsecond=0)

    return None, None


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


def _schedule_standard_tasks(tasks_df, resources_by_atelier, window_start, window_end,
                              allow_overrun=False,
                              site_open_hhmm="00:00", site_close_hhmm="23:59"):
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
                raw_start = max(
                    _normalize_ts(c["current"]),
                    pred_end,
                    _normalize_ts(c["available_from"]),
                    window_start,
                )

                if forced_start is not None:
                    raw_start = max(raw_start, forced_start)

                if raw_start >= _normalize_ts(c["available_to"]):
                    continue

                # FIX #8: respect daily site hours — find the next valid slot
                start_candidate, end_candidate = _next_valid_slot(
                    raw_start, duration, site_open_hhmm, site_close_hhmm
                )
                if start_candidate is None:
                    continue

                # Resource availability bounds
                if start_candidate >= _normalize_ts(c["available_to"]):
                    continue

                hard_limit = _normalize_ts(c["available_to"])
                if not allow_overrun:
                    hard_limit = min(hard_limit, window_end)

                if end_candidate > hard_limit:
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


def _place_global_manual_actions(manual_df, window_start, window_end, planning_rows_so_far,
                                   site_open_hhmm, site_close_hhmm):
    """Place manual actions that have no atelier (global / non-routed actions).

    FIX #2: START → before window_start (at site_open - duration or forced_start)
             END   → after last planned OT or window_end (at forced_start or window_end)
             DURING → at forced_start or window_start

    Returns a list of planning_rows dicts (already with planned_start_at / planned_end_at).
    """
    from datetime import timedelta as _td

    if manual_df is None or manual_df.empty:
        return []

    # Only process rows without atelier
    no_atelier = manual_df[manual_df["atelier"].astype(str).str.strip() == ""].copy()
    if no_atelier.empty:
        return []

    # Latest planned end across all already-scheduled tasks
    latest_planned_end = window_end
    for row in planning_rows_so_far:
        pe = _normalize_ts(row.get("planned_end_at"))
        if pe and pe > latest_planned_end:
            latest_planned_end = pe

    placed = []
    for _, r in no_atelier.iterrows():
        duration = max(float(r.get("duration_h", 1.0) or 1.0), 0.25)
        forced = _normalize_ts(r.get("forced_start_dt"))
        slot = str(r.get("slot_type", "DURING")).upper()

        if forced is not None:
            start = forced
        elif slot == "START":
            # Place just before window_start
            start = window_start - pd.Timedelta(hours=duration)
            # Ensure within site hours
            s2, _ = _next_valid_slot(start, duration, site_open_hhmm, site_close_hhmm)
            if s2 is not None:
                start = s2
        elif slot == "END":
            start = latest_planned_end
            s2, _ = _next_valid_slot(start, duration, site_open_hhmm, site_close_hhmm)
            if s2 is not None:
                start = s2
        else:  # DURING
            start = window_start
            s2, _ = _next_valid_slot(start, duration, site_open_hhmm, site_close_hhmm)
            if s2 is not None:
                start = s2

        end = start + pd.Timedelta(hours=duration)

        rr = dict(r)
        rr["planned_start_at"] = start
        rr["planned_end_at"] = end
        rr["planned_team_name"] = "Manuel"
        rr["commentaire"] = f"Action manuelle ({slot})"
        placed.append(rr)

        # Update latest_planned_end so subsequent END actions chain
        if end > latest_planned_end:
            latest_planned_end = end

    return placed


def _generate_schedule(selected_df, teams_df, planning, manual_actions_df=None):
    selected_ateliers = sorted([x for x in selected_df["atelier"].dropna().astype(str).unique().tolist() if x])

    # FIX #8: extract site hours from planning
    site_open = getattr(planning, "site_open", None) or "00:00"
    site_close = getattr(planning, "site_close", None) or "23:59"

    resources_by_atelier = _build_resources_from_team_rosters(
        selected_ateliers=selected_ateliers,
        teams_df=teams_df,
        default_start=planning.start_at,
        default_end=planning.end_at,
    )

    all_tasks = _prepare_tasks_for_planning(selected_df)

    manual_df = _prepare_manual_actions_df(manual_actions_df)

    # Split manual into routed (have atelier) and global (no atelier)
    manual_routed = manual_df[manual_df["atelier"].astype(str).str.strip() != ""].copy() if not manual_df.empty else pd.DataFrame()
    manual_global = manual_df[manual_df["atelier"].astype(str).str.strip() == ""].copy() if not manual_df.empty else pd.DataFrame()

    if not manual_routed.empty:
        all_tasks = pd.concat([manual_routed, all_tasks], ignore_index=True)

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
        pr, ur, _ = _schedule_standard_tasks(
            start_df, resources_by_atelier, window_start, window_end,
            allow_overrun=False, site_open_hhmm=site_open, site_close_hhmm=site_close,
        )
        planning_rows += pr
        unscheduled_rows += ur

    if not during_df.empty:
        pr, ur, _ = _schedule_standard_tasks(
            during_df, resources_by_atelier, window_start, latest_end,
            allow_overrun=True, site_open_hhmm=site_open, site_close_hhmm=site_close,
        )
        planning_rows += pr
        unscheduled_rows += ur

    if not end_df.empty:
        pr, ur, _ = _schedule_standard_tasks(
            end_df, resources_by_atelier, window_end, latest_end,
            allow_overrun=True, site_open_hhmm=site_open, site_close_hhmm=site_close,
        )
        planning_rows += pr
        unscheduled_rows += ur

    # FIX #2: place global manual actions (no atelier) directly
    global_placed = _place_global_manual_actions(
        manual_global, window_start, window_end, planning_rows,
        site_open, site_close,
    )
    planning_rows += global_placed

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

    manual_from_df = planning_df[planning_df["task_id"].astype(str).str.startswith("ACT_")].copy() if not planning_df.empty else pd.DataFrame()

    # Add globally-placed manual actions (no atelier) to manual_result
    global_df = pd.DataFrame(global_placed) if global_placed else pd.DataFrame()
    if not global_df.empty and not manual_from_df.empty:
        manual_result = pd.concat([manual_from_df, global_df], ignore_index=True)
    elif not global_df.empty:
        manual_result = global_df
    else:
        manual_result = manual_from_df

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


# =========================================================
# DASHBOARD DEMO
# =========================================================

@st.cache_data(show_spinner=False)
def _build_demo_dashboard_data():
    random.seed(42)
    weeks = [f"2025-W{str(i).zfill(2)}" for i in range(48, 53)] + [f"2026-W{str(i).zfill(2)}" for i in range(1, 9)]

    rows = []
    for i, w in enumerate(weeks):
        plan_respect = max(68, min(98, 82 + random.randint(-9, 10) + (i // 4)))
        stop_window = max(70, min(99, 88 + random.randint(-11, 7)))
        load_usage = max(52, min(97, 73 + random.randint(-14, 12)))
        rows.append(
            {
                "Semaine": w,
                "Taux respect planning (%)": plan_respect,
                "Taux respect fenetre (%)": stop_window,
                "Taux charge utilisee ATP (%)": load_usage,
            }
        )

    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False)
def _build_demo_causes_df(weeks):
    random.seed(7)
    rows = []
    for w in weeks:
        rows.append(
            {
                "Semaine": w,
                "Manque de temps": random.randint(1, 5),
                "Ressource indisponible": random.randint(0, 4),
                "Attente production": random.randint(0, 4),
                "Pièce indisponible": random.randint(0, 3),
            }
        )
    return pd.DataFrame(rows)


def _build_demo_ai_comment():
    non_realized = [
        "1674377 | Controle interne niveau conche filtre LB",
        "1624551 | PLIC verification signal capteur",
        "1547333 | Graissage 8 MC pompe",
    ]
    causes = [
        "Manque de temps",
        "Ressource indisponible",
        "Attente production",
    ]

    comment = (
        "Analyse AI – dernière semaine : le respect du planning reste sous pression malgré une bonne utilisation de la capacité. "
        "Les OT non réalisés sont principalement liés à des arbitrages de fenêtre et à des indisponibilités de ressources. "
        f"Les OT les plus pénalisants observés sont : {non_realized[0]}, {non_realized[1]} et {non_realized[2]}. "
        f"Les causes dominantes sont : {causes[0]}, {causes[1]} et {causes[2]}. "
        "Recommandation : sécuriser les ressources critiques en amont, verrouiller les prérequis de production et transformer les OT récurrents non réalisés en actions de fond."
    )
    return comment


# =========================================================
# REX + ACTION PLAN
# =========================================================

def _render_rex_panel(session, user, planning_id):
    planning = session.get(Planning, planning_id)
    if not planning:
        st.warning("Planning introuvable.")
        return

    st.divider()
    st.subheader(f"Retour d'experience — {planning.name}")

    selected_tasks = [t for t in planning.tasks if t.selected]
    if not selected_tasks:
        st.info("Aucun OT selectionne dans ce planning.")
        return

    causes = session.scalars(
        select(RexCause)
        .where(RexCause.organization_id == user.organization_id)
        .where(RexCause.active == True)
        .order_by(RexCause.label_fr)
    ).all()

    cause_options = [""] + [c.label_fr for c in causes]
    cause_map = {c.label_fr: c.id for c in causes}

    rex_rows = []
    for t in selected_tasks:
        current_label = ""
        if getattr(t, "rex_cause_id", None):
            for c in causes:
                if c.id == t.rex_cause_id:
                    current_label = c.label_fr
                    break

        rex_rows.append(
            {
                "ot_id": t.external_ot_id,
                "description": t.description,
                "atelier": t.atelier,
                "planned_start_at": str(t.planned_start_at) if t.planned_start_at else "",
                "planned_team_name": t.planned_team_name or "",
                "realise": bool(t.rex_done) if t.rex_done is not None else False,
                "heure_debut": getattr(t, "rex_actual_start", "") or "",
                "heure_fin": getattr(t, "rex_actual_end", "") or "",
                "cause": current_label,
                "commentaire": t.rex_comment or "",
                "action": "",
                "responsable": "",
                "delai": "",
            }
        )

    rex_df = pd.DataFrame(rex_rows)

    edited = st.data_editor(
        rex_df,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        key=f"rex_editor_{planning_id}",
        column_config={
            "ot_id": st.column_config.TextColumn("OT", disabled=True),
            "description": st.column_config.TextColumn("Description", disabled=True),
            "atelier": st.column_config.TextColumn("Atelier", disabled=True),
            "planned_start_at": st.column_config.TextColumn("Début prévu", disabled=True),
            "planned_team_name": st.column_config.TextColumn("Equipe", disabled=True),
            "realise": st.column_config.CheckboxColumn("Réalisé"),
            "heure_debut": st.column_config.TextColumn("Heure début réelle (HH:MM)"),
            "heure_fin": st.column_config.TextColumn("Heure fin réelle (HH:MM)"),
            "cause": st.column_config.SelectboxColumn("Cause", options=cause_options),
            "commentaire": st.column_config.TextColumn("Commentaire"),
            "action": st.column_config.TextColumn("Action corrective"),
            "responsable": st.column_config.TextColumn("Responsable"),
            "delai": st.column_config.TextColumn("Délai (YYYY-MM-DD)"),
        },
    )

    st.markdown("#### Horaires réels de l'arrêt")
    c1, c2 = st.columns(2)
    with c1:
        effective_start = st.text_input(
            "Heure de démarrage effective (YYYY-MM-DD HH:MM)",
            key=f"rex_effective_start_{planning_id}",
        )
    with c2:
        effective_end = st.text_input(
            "Heure d'arrêt effective / fin effective (YYYY-MM-DD HH:MM)",
            key=f"rex_effective_end_{planning_id}",
        )

    if st.button("Enregistrer le REX", key=f"save_rex_{planning_id}",
                 use_container_width=True, type="primary"):
        try:
            from maintenance_toolbox.db import Action
            from datetime import datetime, timezone

            # Get current meeting session id if called from within a meeting
            hub_session_id = st.session_state.get("hub_session_id")
            hub_mt_id = None
            if hub_session_id:
                from maintenance_toolbox.db import MeetingSession, MeetingInstance
                ms_rec = session.get(MeetingSession, hub_session_id)
                if ms_rec:
                    inst = session.get(MeetingInstance, ms_rec.instance_id)
                    if inst:
                        hub_mt_id = inst.meeting_type_id

            for _, row in edited.iterrows():
                ot_id = _safe_text(row["ot_id"])
                task = next((x for x in selected_tasks if x.external_ot_id == ot_id), None)
                if not task:
                    continue

                task.rex_done = bool(row["realise"])
                label = _safe_text(row["cause"])
                task.rex_cause_id = cause_map.get(label) if label else None
                task.rex_comment = _safe_text(row["commentaire"])
                task.rex_actual_start = _safe_text(row.get("heure_debut", "")) or None
                task.rex_actual_end = _safe_text(row.get("heure_fin", "")) or None

                # Save action to global Action table if filled
                action_desc = _safe_text(row.get("action", ""))
                resp = _safe_text(row.get("responsable", ""))
                delai_str = _safe_text(row.get("delai", ""))
                if action_desc and resp and hub_session_id:
                    due_dt = None
                    if delai_str:
                        try:
                            due_dt = datetime.strptime(delai_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        except ValueError:
                            pass
                    existing_action = session.scalar(
                        select(Action).where(
                            Action.meeting_session_id == hub_session_id,
                            Action.description == action_desc,
                        )
                    )
                    if not existing_action:
                        session.add(Action(
                            description=action_desc,
                            owner=resp,
                            due_date=due_dt,
                            status="Open",
                            meeting_session_id=hub_session_id,
                            meeting_type_id=hub_mt_id,
                            organization_id=planning.organization_id,
                        ))

            session.commit()
            st.success("REX enregistré.")

            if effective_start or effective_end:
                st.info(
                    f"Fenêtre réelle : démarrage `{effective_start or '-'}` | fin `{effective_end or '-'}`"
                )

            action_plan = edited[["ot_id", "action", "responsable", "delai"]].copy()
            action_plan = action_plan[
                (action_plan["action"].astype(str).str.strip() != "") |
                (action_plan["responsable"].astype(str).str.strip() != "") |
                (action_plan["delai"].astype(str).str.strip() != "")
            ].copy()

            if not action_plan.empty:
                st.markdown("#### Actions correctives enregistrées dans le plan d'action global")
                st.dataframe(action_plan, use_container_width=True, hide_index=True)

        except Exception as e:
            session.rollback()
            st.error(f"Erreur lors de l'enregistrement du REX : {e}")


# =========================================================
# GANTT + PDF HELPERS
# =========================================================

# FIX #7: Mainnovation Gantt colour palette — greys per team, orange for manual
_MN_ORANGE = "#F5A623"
_MN_GREY_PALETTE = ["#6B6B6B", "#8A8A8A", "#ABABAB", "#C0C0C0", "#505050", "#3A3A3A", "#9E9E9E", "#757575"]


def _render_gantt(generated_df: "pd.DataFrame", planning, manual_df=None) -> None:
    """Render interactive Gantt by TEAM (not atelier) using Plotly.

    FIX #3: y-axis is now planned_team_name (one row per team, not per atelier).
    FIX #4: Day separator vertical lines at each midnight.
    FIX #7: Mainnovation grey palette for teams, orange for manual actions.
    """
    try:
        df = generated_df.copy()
        df["_start"] = pd.to_datetime(df["planned_start_at"], errors="coerce")
        df["_end"]   = pd.to_datetime(df["planned_end_at"],   errors="coerce")
        df = df.dropna(subset=["_start", "_end"])
        df["is_manual"] = False
        # FIX #3: use planned_team_name as y-axis row
        df["_row"]   = df.get("planned_team_name", df.get("atelier", "")).astype(str)
        df["_row"]   = df["_row"].where(df["_row"].str.strip() != "", df.get("atelier", "").astype(str))
        df["_color"] = df["_row"]   # colour by team
        df["_label"] = df["ot_id"].astype(str)
        df["_desc"]  = df.get("description", "").astype(str).str[:60]
        df["_team"]  = df["_row"]
        df["_dur"]   = df.get("duration_hours", 0).apply(lambda x: f"{round(float(x or 0), 1)} h")

        # Merge manual actions
        if manual_df is not None and not manual_df.empty:
            mdf = manual_df.copy()
            mdf["_start"] = pd.to_datetime(mdf["planned_start_at"], errors="coerce")
            mdf["_end"]   = pd.to_datetime(mdf["planned_end_at"],   errors="coerce")
            mdf = mdf.dropna(subset=["_start", "_end"])
            if not mdf.empty:
                mdf["is_manual"] = True
                team_col = mdf.get("planned_team_name", mdf.get("atelier", pd.Series(["Manuel"] * len(mdf), index=mdf.index)))
                mdf["_row"]   = team_col.fillna("Manuel").astype(str)
                mdf["_color"] = "Action manuelle"   # distinct orange
                mdf["_label"] = mdf.get("ot_id", mdf.get("task_id", "")).astype(str)
                mdf["_desc"]  = mdf.get("description", "").astype(str).str[:60]
                mdf["_team"]  = mdf["_row"]
                dur_src = mdf.get("duration_h", mdf.get("duration_hours", pd.Series([0] * len(mdf), index=mdf.index)))
                mdf["_dur"] = dur_src.apply(lambda x: f"{round(float(x or 0), 1)} h")
                common_cols = ["_row", "_start", "_end", "_color", "_label", "_desc", "_team", "_dur", "is_manual"]
                df = pd.concat([df[common_cols], mdf[common_cols]], ignore_index=True)

        if df.empty:
            st.info("Données insuffisantes pour le Gantt (pas de dates planifiées).")
            return

        if not _PLOTLY_AVAILABLE:
            _render_gantt_matplotlib(df, planning)
            return

        # Build colour map: teams → grey shades, manual → orange
        teams = [t for t in df["_color"].unique().tolist() if t != "Action manuelle"]
        color_map = {"Action manuelle": _MN_ORANGE}
        for i, t in enumerate(sorted(teams)):
            color_map[t] = _MN_GREY_PALETTE[i % len(_MN_GREY_PALETTE)]

        fig = px.timeline(
            df,
            x_start="_start",
            x_end="_end",
            y="_row",
            color="_color",
            text="_label",
            hover_data={
                "_label": True,
                "_desc": True,
                "_start": True,
                "_end": True,
                "_dur": True,
                "_team": True,
                "_color": False,
                "_row": False,
            },
            labels={
                "_label": "N° OT / Action",
                "_desc": "Description",
                "_start": "Début",
                "_end": "Fin",
                "_dur": "Durée",
                "_team": "Équipe",
                "_row": "Équipe",
            },
            color_discrete_map=color_map,
        )

        # FIX #4: add vertical day-separator lines at each midnight within the planning range
        if not df["_start"].isna().all():
            range_start = df["_start"].min().normalize()  # midnight of first day
            range_end   = df["_end"].max()
            day = range_start + pd.Timedelta(days=1)
            while day <= range_end + pd.Timedelta(days=1):
                fig.add_vline(
                    x=day.timestamp() * 1000,  # Plotly uses ms since epoch
                    line_width=2,
                    line_dash="dot",
                    line_color=_MN_ORANGE,
                    opacity=0.7,
                )
                # Date annotation at top of separator
                fig.add_annotation(
                    x=day.timestamp() * 1000,
                    y=1.02,
                    xref="x",
                    yref="paper",
                    text=day.strftime("%d/%m"),
                    showarrow=False,
                    font=dict(size=10, color=_MN_ORANGE, family="Arial"),
                    bgcolor="#FFF3E0",
                    bordercolor=_MN_ORANGE,
                    borderwidth=1,
                )
                day += pd.Timedelta(days=1)

        fig.update_yaxes(autorange="reversed", title="Équipe", tickfont=dict(size=11))
        fig.update_xaxes(
            title="Horaire",
            showgrid=True,
            gridwidth=1,
            gridcolor="#E8E8E8",
            tickformat="%d/%m %H:%M",
            tickfont=dict(size=10),
        )
        fig.update_traces(
            textposition="inside",
            insidetextanchor="middle",
            textfont=dict(size=10, color="white", family="Arial"),
        )
        fig.update_layout(
            title=dict(
                text=f"<b>Gantt — {planning.name}</b>",
                font=dict(size=15, color="#6B6B6B"),
            ),
            plot_bgcolor="#FFFFFF",
            paper_bgcolor="#F2F2F2",
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.12,
                xanchor="left",
                x=0,
                title="Équipes",
                font=dict(size=10),
                bgcolor="#F2F2F2",
                bordercolor="#6B6B6B",
                borderwidth=1,
            ),
            hovermode="closest",
            hoverlabel=dict(bgcolor="#FFF3E0", bordercolor=_MN_ORANGE, font_size=12),
            margin=dict(l=30, r=30, t=60, b=100),
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.warning(f"Gantt non affiché : {e}")
        import traceback
        st.caption(traceback.format_exc())


def _render_gantt_matplotlib(df, planning):
    """Fallback Gantt renderer using matplotlib when Plotly is unavailable."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        rows = df["_row"].unique().tolist()
        colors = {r: (_MN_ORANGE if df[df["_row"] == r]["is_manual"].any()
                      else _MN_GREY_PALETTE[i % len(_MN_GREY_PALETTE)])
                  for i, r in enumerate(rows)}

        fig_h = max(3.5, len(rows) * 0.9 + 2)
        fig, ax = plt.subplots(figsize=(14, fig_h))

        for i, row_key in enumerate(rows):
            sub = df[df["_row"] == row_key]
            for _, row in sub.iterrows():
                t0 = mdates.date2num(row["_start"].to_pydatetime())
                t1 = mdates.date2num(row["_end"].to_pydatetime())
                dur = max(t1 - t0, 1e-4)
                fc = _MN_ORANGE if row.get("is_manual") else colors[row_key]
                ax.broken_barh([(t0, dur)], (i - 0.38, 0.76), facecolors=fc, edgecolors="#fff", linewidth=0.8)
                ax.text(t0 + dur / 2, i, str(row.get("_label", ""))[:10],
                        ha="center", va="center", fontsize=6.5, color="#fff", fontweight="bold")

        ax.set_yticks(range(len(rows)))
        ax.set_yticklabels(rows, fontsize=9)
        ax.xaxis_date()
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m\n%H:%M"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.grid(True, axis="x", alpha=0.25, linestyle="--")
        ax.set_title(f"Gantt — {planning.name}", fontsize=11, fontweight="bold")
        plt.tight_layout()
        st.pyplot(fig, use_container_width=True)
        plt.close(fig)
    except Exception as e:
        st.warning(f"Gantt matplotlib non affiché : {e}")


def _safe_pdf_str(val, max_len=50):
    """Encode a value for PDF — strips non-latin1 chars to avoid fpdf2 encoding errors."""
    s = "" if val is None or (isinstance(val, float) and pd.isna(val)) else str(val)
    s = s[:max_len]
    return s.encode("latin-1", errors="replace").decode("latin-1")


def _generate_planning_pdf(generated_df: "pd.DataFrame", planning) -> bytes:
    """Generate a PDF summary of the planning using fpdf2.

    BUG FIX #7: The previous version could silently fail when fpdf2 received
    non-latin1 characters (accented text from descriptions). Fixed via _safe_pdf_str().
    Also ensured pdf.output() always returns bytes (not bytearray).
    """
    try:
        from fpdf import FPDF
    except ImportError:
        raise RuntimeError("fpdf2 n'est pas installé. Exécutez : pip install fpdf2")

    pdf = FPDF(orientation="L", format="A4")
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()

    plan_name = _safe_pdf_str(planning.name, 80)
    start_str = _safe_pdf_str(str(planning.start_at)[:19])
    end_str = _safe_pdf_str(str(planning.end_at)[:19])
    sectors = _safe_pdf_str(getattr(planning, "sectors_csv", "") or "")

    # ── Header ──────────────────────────────────────────────────────────────
    pdf.set_fill_color(245, 166, 35)  # Mainnovation orange
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 12, f"Planning : {plan_name}", new_x="LMARGIN", new_y="NEXT", fill=True, align="L")

    pdf.set_fill_color(242, 242, 242)
    pdf.set_text_color(107, 107, 107)
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 6, f"Periode : {start_str}  ->  {end_str}", new_x="LMARGIN", new_y="NEXT", fill=True)
    if sectors:
        pdf.cell(0, 6, f"Secteurs : {sectors}", new_x="LMARGIN", new_y="NEXT", fill=True)
    pdf.cell(0, 6, f"Statut : {_safe_pdf_str(planning.status)}", new_x="LMARGIN", new_y="NEXT", fill=True)
    pdf.ln(4)

    # ── Summary metrics ─────────────────────────────────────────────────────
    df = generated_df.copy()
    n_ot = len(df)
    total_h = round(float(df["duration_hours"].sum()) if "duration_hours" in df.columns else 0, 1)
    ateliers = df["atelier"].dropna().astype(str).unique().tolist() if "atelier" in df.columns else []
    ateliers_str = _safe_pdf_str(", ".join(ateliers[:8]), 100)

    pdf.set_fill_color(245, 166, 35)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(60, 8, f"OT planifies : {n_ot}", border=0, align="C", fill=True)
    pdf.cell(60, 8, f"Charge totale : {total_h} h", border=0, align="C", fill=True)
    pdf.cell(0, 8, f"Ateliers : {ateliers_str}", border=0, align="L", fill=True)
    pdf.ln(12)

    # ── Table header ─────────────────────────────────────────────────────────
    col_w = [22, 72, 28, 16, 38, 38, 34]
    headers = ["N OT", "Description", "Atelier", "Duree (h)", "Debut", "Fin", "Equipe"]

    pdf.set_fill_color(107, 107, 107)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 8)
    for h, w in zip(headers, col_w):
        pdf.cell(w, 8, h, border=1, align="C", fill=True)
    pdf.ln()

    # ── Table rows ────────────────────────────────────────────────────────────
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "", 7)
    fill = False
    for _, row in df.iterrows():
        if fill:
            pdf.set_fill_color(253, 232, 192)  # soft orange
        else:
            pdf.set_fill_color(255, 255, 255)
        dur_val = row.get("duration_hours", 0) or 0
        try:
            dur_str = str(round(float(dur_val), 1))
        except Exception:
            dur_str = "0"
        cells = [
            _safe_pdf_str(row.get("ot_id", ""), 14),
            _safe_pdf_str(row.get("description", ""), 50),
            _safe_pdf_str(row.get("atelier", ""), 18),
            dur_str,
            _safe_pdf_str(str(row.get("planned_start_at", ""))[:16]),
            _safe_pdf_str(str(row.get("planned_end_at", ""))[:16]),
            _safe_pdf_str(row.get("planned_team_name", ""), 20),
        ]
        for val, w in zip(cells, col_w):
            pdf.cell(w, 6, val, border=1, fill=True)
        pdf.ln()
        fill = not fill

    # ── Footer ────────────────────────────────────────────────────────────────
    pdf.ln(4)
    pdf.set_font("Helvetica", "I", 7)
    pdf.set_text_color(150, 150, 150)
    pdf.cell(0, 5, "Genere par MaintenOps — Mainnovation", align="C")

    raw = pdf.output()
    return bytes(raw) if not isinstance(raw, bytes) else raw


def _generate_planning_html_LEGACY(generated_df, planning, manual_df=None) -> bytes:
    """Legacy simple HTML export — kept for reference."""
    return _generate_planning_html(generated_df, planning, manual_df)


def _generate_planning_html(generated_df: "pd.DataFrame", planning, manual_df=None) -> bytes:
    """Generate a Microsoft-Project-style HTML Gantt planning export.

    FIX #6: Replaces the simple HTML table with a proper Gantt table:
    - Header: planning name, sectors, dates, ateliers
    - Column hierarchy: N°, Nom tâche, Durée, Début, Fin + hourly time columns per day
    - Gantt bars rendered as colored CSS cells inside the table
    - Hierarchy: Fenêtre d'arrêt > Atelier > Équipe > OT
    - Manual actions in orange (#F5A623)
    - A3 landscape print-optimized
    """
    from datetime import timedelta as _td, date as _date

    df = generated_df.copy()
    plan_name   = str(planning.name or "")
    start_str   = str(planning.start_at)[:19] if planning.start_at else ""
    end_str     = str(planning.end_at)[:19]   if planning.end_at   else ""
    sectors     = str(getattr(planning, "sectors_csv", "") or "")
    site_open   = getattr(planning, "site_open",  None) or "07:00"
    site_close  = getattr(planning, "site_close", None) or "22:00"

    # Parse site hours
    so_h, so_m = map(int, site_open.split(":"))
    sc_h, sc_m = map(int, site_close.split(":"))
    hours_per_day = list(range(so_h, sc_h + 1))  # e.g. [7,8,...,22]

    # Date range
    df["_start"] = pd.to_datetime(df["planned_start_at"], errors="coerce")
    df["_end"]   = pd.to_datetime(df["planned_end_at"],   errors="coerce")
    df_valid = df.dropna(subset=["_start", "_end"])

    if manual_df is not None and not manual_df.empty:
        mdf_v = manual_df.copy()
        mdf_v["_start"] = pd.to_datetime(mdf_v["planned_start_at"], errors="coerce")
        mdf_v["_end"]   = pd.to_datetime(mdf_v["planned_end_at"],   errors="coerce")
        mdf_v = mdf_v.dropna(subset=["_start", "_end"])
    else:
        mdf_v = pd.DataFrame()

    all_starts = list(df_valid["_start"]) + (list(mdf_v["_start"]) if not mdf_v.empty else [])
    all_ends   = list(df_valid["_end"])   + (list(mdf_v["_end"])   if not mdf_v.empty else [])

    if not all_starts:
        # Fallback if no dates
        return ("<html><body><p>Aucune donnée planifiée.</p></body></html>").encode("utf-8")

    first_day = min(all_starts).normalize()
    last_day  = max(all_ends).normalize()
    days: list = []
    d = first_day
    while d <= last_day:
        days.append(d)
        d += pd.Timedelta(days=1)

    n_days  = len(days)
    n_hours = len(hours_per_day)
    n_fixed = 5  # N°, Nom tâche, Durée, Début, Fin

    # ── Build Gantt bar for a single cell (day d, hour h) ──────────────────
    def _bar_pct(task_start, task_end, day_ts, hour_int, is_manual=False):
        """Return (left_pct, width_pct, color) or None if no overlap."""
        cell_s = day_ts.replace(hour=hour_int, minute=0, second=0, microsecond=0)
        cell_e = cell_s + pd.Timedelta(hours=1)
        overlap_s = max(task_start, cell_s)
        overlap_e = min(task_end,   cell_e)
        if overlap_e <= overlap_s:
            return None
        left_pct  = (overlap_s - cell_s).total_seconds() / 3600 * 100
        width_pct = (overlap_e - overlap_s).total_seconds() / 3600 * 100
        color = "#F5A623" if is_manual else "#6B6B6B"
        return left_pct, width_pct, color

    def _gantt_cell(task_start, task_end, day_ts, hour_int, is_manual=False):
        bar = _bar_pct(task_start, task_end, day_ts, hour_int, is_manual)
        if bar is None:
            return "<td class='gc'></td>"
        l, w, c = bar
        return (f"<td class='gc'>"
                f"<div class='bar' style='left:{l:.0f}%;width:{w:.0f}%;background:{c}'></div>"
                f"</td>")

    # ── Header row 1: day labels (spanning n_hours columns each) ───────────
    day_headers = ""
    for d_ts in days:
        label = d_ts.strftime("%A %d/%m/%Y")
        day_headers += f"<th colspan='{n_hours}' class='day-header'>{label}</th>"

    # ── Header row 2: hour labels ───────────────────────────────────────────
    hour_headers = ""
    for _ in days:
        for h in hours_per_day:
            hour_headers += f"<th class='hour-header'>{h:02d}</th>"

    # ── Build hierarchy: sort by atelier > team > start ────────────────────
    df_valid2 = df_valid.copy()
    df_valid2["atelier"]          = df_valid2.get("atelier", "").astype(str)
    df_valid2["planned_team_name"]= df_valid2.get("planned_team_name", "").astype(str)

    ateliers_list = sorted(df_valid2["atelier"].unique().tolist())
    n_ot     = len(df_valid2) + (len(mdf_v) if not mdf_v.empty else 0)
    total_h  = round(float(df_valid2.get("duration_hours", pd.Series([0])).sum()), 1)

    row_num = [0]
    rows_html = []

    def _info_cells(n, name, dur, debut, fin, indent=0):
        pad = indent * 16
        return (f"<td class='num'>{n}</td>"
                f"<td class='name' style='padding-left:{pad+8}px'>{name}</td>"
                f"<td class='dur'>{dur}</td>"
                f"<td class='ts'>{debut}</td>"
                f"<td class='ts'>{fin}</td>")

    def _gantt_cells(task_start, task_end, is_manual=False):
        cells = ""
        for d_ts in days:
            for h in hours_per_day:
                cells += _gantt_cell(task_start, task_end, d_ts, h, is_manual)
        return cells

    # Fenêtre d'arrêt row (top-level)
    ws = pd.to_datetime(planning.start_at)
    we = pd.to_datetime(planning.end_at)
    rows_html.append(
        f"<tr class='row-window'>"
        f"{_info_cells('', 'Fenêtre d\'arrêt', '', str(ws)[:16], str(we)[:16], indent=0)}"
        f"{_gantt_cells(ws, we, is_manual=False)}"
        f"</tr>"
    )

    for atelier in ateliers_list:
        at_df = df_valid2[df_valid2["atelier"] == atelier].copy()
        at_start = at_df["_start"].min()
        at_end   = at_df["_end"].max()
        at_dur   = round(float(at_df.get("duration_hours", pd.Series([0])).sum()), 1)

        rows_html.append(
            f"<tr class='row-atelier'>"
            f"{_info_cells('', atelier, f'{at_dur} h', str(at_start)[:16], str(at_end)[:16], indent=1)}"
            f"{_gantt_cells(at_start, at_end, is_manual=False)}"
            f"</tr>"
        )

        teams_list = sorted(at_df["planned_team_name"].unique().tolist())
        for team in teams_list:
            team_df = at_df[at_df["planned_team_name"] == team].copy().sort_values("_start")
            t_start = team_df["_start"].min()
            t_end   = team_df["_end"].max()
            t_dur   = round(float(team_df.get("duration_hours", pd.Series([0])).sum()), 1)

            rows_html.append(
                f"<tr class='row-team'>"
                f"{_info_cells('', team, f'{t_dur} h', str(t_start)[:16], str(t_end)[:16], indent=2)}"
                f"{_gantt_cells(t_start, t_end, is_manual=False)}"
                f"</tr>"
            )

            for _, ot_row in team_df.iterrows():
                row_num[0] += 1
                ot_id  = str(ot_row.get("ot_id", ""))
                desc   = str(ot_row.get("description", ""))[:60]
                dur_h  = round(float(ot_row.get("duration_hours", 0) or 0), 1)
                debut  = str(ot_row["_start"])[:16]
                fin    = str(ot_row["_end"])[:16]

                rows_html.append(
                    f"<tr class='row-ot'>"
                    f"{_info_cells(row_num[0], f'{ot_id} — {desc}', f'{dur_h} h', debut, fin, indent=3)}"
                    f"{_gantt_cells(ot_row['_start'], ot_row['_end'], is_manual=False)}"
                    f"</tr>"
                )

    # Manual actions
    if not mdf_v.empty:
        rows_html.append(
            f"<tr class='row-atelier' style='background:#FFF3E0'>"
            f"{_info_cells('', 'Actions manuelles', '', '', '', indent=1)}"
            f"{''.join('<td class=\"gc\"></td>' for _ in days for _ in hours_per_day)}"
            f"</tr>"
        )
        for _, mr in mdf_v.iterrows():
            row_num[0] += 1
            m_id   = str(mr.get("ot_id", mr.get("task_id", "")))
            m_desc = str(mr.get("description", ""))[:60]
            m_dur  = round(float(mr.get("duration_h", mr.get("duration_hours", 0)) or 0), 1)
            m_deb  = str(mr["_start"])[:16]
            m_fin  = str(mr["_end"])[:16]
            rows_html.append(
                f"<tr class='row-ot' style='background:#FFF8F0'>"
                f"{_info_cells(row_num[0], f'{m_id} — {m_desc}', f'{m_dur} h', m_deb, m_fin, indent=2)}"
                f"{_gantt_cells(mr['_start'], mr['_end'], is_manual=True)}"
                f"</tr>"
            )

    all_rows = "\n".join(rows_html)
    total_cols = n_fixed + n_days * n_hours
    header_fixed = "<th>N°</th><th>Nom de la tâche</th><th>Durée</th><th>Début</th><th>Fin</th>"

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="utf-8">
  <title>Planning — {plan_name}</title>
  <style>
    @page {{ size: A3 landscape; margin: 10mm; }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: Arial, Helvetica, sans-serif; font-size: 11px; color: #333; background: #F2F2F2; padding: 16px; }}
    .hdr {{ background: #6B6B6B; color: white; padding: 12px 18px; border-radius: 6px; margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; }}
    .hdr h1 {{ font-size: 14px; font-weight: 700; }}
    .hdr .meta {{ font-size: 10px; opacity: 0.85; }}
    .kpis {{ display: flex; gap: 10px; margin-bottom: 12px; }}
    .kpi {{ background: white; border-left: 3px solid #F5A623; padding: 8px 14px; border-radius: 4px; }}
    .kpi .v {{ font-size: 16px; font-weight: 700; color: #F5A623; }}
    .kpi .l {{ font-size: 9px; color: #6B6B6B; }}
    .wrap {{ overflow-x: auto; }}
    table {{ border-collapse: collapse; white-space: nowrap; background: white; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 3px 5px; }}
    th {{ background: #6B6B6B; color: white; font-size: 10px; text-align: center; position: sticky; top: 0; }}
    .day-header {{ background: #4A4A4A; font-size: 10px; font-weight: 700; text-align: center; }}
    .hour-header {{ background: #6B6B6B; font-size: 9px; min-width: 24px; max-width: 24px; width: 24px; }}
    .row-window {{ background: #E8E8E8; font-weight: 700; }}
    .row-atelier {{ background: #F0F0F0; font-weight: 600; }}
    .row-team {{ background: #F8F8F8; font-style: italic; }}
    .row-ot {{ background: #FFFFFF; }}
    .row-ot:hover {{ background: #FFF3E0; }}
    .num {{ text-align: center; width: 30px; font-size: 9px; color: #888; }}
    .name {{ max-width: 220px; overflow: hidden; text-overflow: ellipsis; }}
    .dur {{ text-align: center; width: 50px; }}
    .ts {{ width: 110px; font-size: 9px; }}
    .gc {{ position: relative; min-width: 24px; max-width: 24px; width: 24px; height: 18px; padding: 0; }}
    .bar {{ position: absolute; top: 2px; height: 14px; border-radius: 2px; opacity: 0.88; }}
    .footer {{ text-align: center; margin-top: 12px; font-size: 9px; color: #aaa; }}
    @media print {{
      body {{ padding: 0; background: white; }}
      .wrap {{ overflow: visible; }}
      th {{ background: #6B6B6B !important; -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
      .bar {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
    }}
  </style>
</head>
<body>
  <div class="hdr">
    <div>
      <h1>Planning : {plan_name}</h1>
      <div class="meta">
        Période : {start_str} → {end_str}
        {(' &nbsp;|&nbsp; Secteurs : ' + sectors) if sectors else ''}
        &nbsp;|&nbsp; Plage site : {site_open} – {site_close}
      </div>
    </div>
    <div class="meta">Généré le {datetime.now().strftime("%d/%m/%Y %H:%M")}</div>
  </div>

  <div class="kpis">
    <div class="kpi"><div class="v">{n_ot}</div><div class="l">OT planifiés</div></div>
    <div class="kpi"><div class="v">{total_h} h</div><div class="l">Charge totale</div></div>
    <div class="kpi"><div class="v">{n_days} jour(s)</div><div class="l">Durée planning</div></div>
    <div class="kpi"><div class="v">{len(ateliers_list)}</div><div class="l">Ateliers</div></div>
  </div>

  <div class="wrap">
  <table>
    <thead>
      <tr>
        {header_fixed}
        {day_headers}
      </tr>
      <tr>
        <th colspan="{n_fixed}"></th>
        {hour_headers}
      </tr>
    </thead>
    <tbody>
      {all_rows}
    </tbody>
  </table>
  </div>

  <div class="footer">
    MaintenOps — Mainnovation &nbsp;|&nbsp; Légende :
    <span style="display:inline-block;width:14px;height:10px;background:#6B6B6B;vertical-align:middle;border-radius:2px"></span> OT planifiés &nbsp;
    <span style="display:inline-block;width:14px;height:10px;background:#F5A623;vertical-align:middle;border-radius:2px"></span> Actions manuelles
  </div>
</body>
</html>"""

    return html.encode("utf-8")


# =========================================================
# MAIN UI
# =========================================================

def render_scheduling_module(session, user):
    st.title("Scheduling")

    if not user.organization_id:
        st.warning("Aucune organisation n'est associée à cet utilisateur.")
        return

    _init_wizard_state()

    # FIX: resolve pending navigation BEFORE the widget renders.
    # Directly setting st.session_state["scheduling_view"] after the radio widget
    # has been instantiated raises StreamlitAPIException. Instead we set a shadow
    # key "_sched_view_pending" and transfer it here, before the widget exists.
    if "_sched_view_pending" in st.session_state:
        st.session_state["scheduling_view"] = st.session_state.pop("_sched_view_pending")

    st.radio(
        "Navigation Scheduling",
        options=["Mes plannings", "Créer un planning", "Tableau de bord"],
        horizontal=True,
        label_visibility="collapsed",
        key="scheduling_view",
    )

    # =====================================================
    # DASHBOARD
    # =====================================================
    if st.session_state["scheduling_view"] == "Tableau de bord":
        st.subheader("Tableau de bord")

        dash_df = _build_demo_dashboard_data()
        causes_df = _build_demo_causes_df(dash_df["Semaine"].tolist())

        c1, c2, c3 = st.columns(3)
        c1.metric("Dernier respect planning", f"{int(dash_df.iloc[-1]['Taux respect planning (%)'])}%")
        c2.metric("Dernier respect fenêtre", f"{int(dash_df.iloc[-1]['Taux respect fenetre (%)'])}%")
        c3.metric("Dernière charge utilisée ATP", f"{int(dash_df.iloc[-1]['Taux charge utilisee ATP (%)'])}%")

        r1c1, r1c2 = st.columns(2)
        with r1c1:
            st.markdown("#### Respect du planning")
            st.line_chart(
                dash_df.set_index("Semaine")[["Taux respect planning (%)"]],
                height=220,
                use_container_width=True,
            )

        with r1c2:
            st.markdown("#### Respect de la fenêtre d'arrêt")
            st.line_chart(
                dash_df.set_index("Semaine")[["Taux respect fenetre (%)"]],
                height=220,
                use_container_width=True,
            )

        r2c1, r2c2 = st.columns(2)
        with r2c1:
            st.markdown("#### Charge utilisée ATP")
            st.line_chart(
                dash_df.set_index("Semaine")[["Taux charge utilisee ATP (%)"]],
                height=220,
                use_container_width=True,
            )

        with r2c2:
            st.markdown("#### Causes de non-respect du planning")
            st.bar_chart(
                causes_df.set_index("Semaine"),
                height=220,
                use_container_width=True,
            )

        st.markdown("#### Commentaire AI – dernière semaine")
        st.info(_build_demo_ai_comment())
        return

    # =====================================================
    # MES PLANNINGS
    # =====================================================
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
                            f"Période : {p.start_at} → {p.end_at}  \n"
                            f"CSV : {'Oui' if p.csv_filename else 'Non'}"
                        )

                    with c2:
                        if st.button("✏️", key=f"edit_{p.id}", use_container_width=True):
                            _load_planning_into_wizard(session, user, p)
                            # Use shadow key to avoid modifying widget-bound key after render
                            st.session_state["_sched_view_pending"] = "Créer un planning"
                            st.rerun()

                    with c3:
                        if st.button("🗑️", key=f"delete_{p.id}", use_container_width=True):
                            _delete_planning(session, p.id)
                            st.success("Planning supprimé.")
                            st.rerun()

                    with c4:
                        # BUG FIX: REX button now toggles instead of always rerunning,
                        # so the panel stays visible without requiring scroll.
                        rex_active = st.session_state.get("rex_planning_id") == p.id
                        label = "✅ REX actif" if rex_active else "📝 REX"
                        if st.button(label, key=f"rex_{p.id}", use_container_width=True):
                            if rex_active:
                                st.session_state["rex_planning_id"] = None
                            else:
                                st.session_state["rex_planning_id"] = p.id
                            st.rerun()

                    st.divider()

            if st.session_state.get("rex_planning_id"):
                st.markdown('<div id="rex-anchor"></div>', unsafe_allow_html=True)
                _render_rex_panel(session, user, st.session_state["rex_planning_id"])

        except Exception as e:
            st.error(f"Erreur lors du chargement des plannings : {e}")

        return

    # =====================================================
    # CREATE / EDIT PLANNING
    # =====================================================
    planning = _get_current_planning(session, st.session_state["wizard_planning_id"])

    top_col1, top_col2 = st.columns([3, 1])
    with top_col1:
        st.subheader("Wizard de création / modification de planning")
    with top_col2:
        if st.button("Nouveau planning", key="new_planning_btn", use_container_width=True):
            _reset_wizard()
            st.rerun()

    if planning:
        st.info(
            f"Planning en cours : **{planning.name}** | "
            f"Statut : **{planning.status}** | "
            f"Période : **{planning.start_at} → {planning.end_at}**"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # FIX #1 — ÉTAPE 1 : Import CSV + Mapping (AVANT les paramètres)
    # Cause : l'upload CSV nécessitait un planning.id (créé à l'étape paramètres).
    # Solution : step 1 stocke csv_bytes en session_state; le planning est créé
    # à l'étape 2, puis les csv_bytes y sont rattachés.
    # ─────────────────────────────────────────────────────────────────────────
    with st.expander("1. Import CSV + Mapping", expanded=st.session_state["wizard_active_section"] == 1):
        # Support both: uploading a new file OR loading from an existing planning
        existing_csv_bytes = st.session_state.get("wizard_csv_bytes") or (planning.csv_bytes if planning else None)
        existing_csv_name  = st.session_state.get("wizard_csv_filename") or (planning.csv_filename if planning else None)

        if existing_csv_name:
            st.caption(f"Fichier actuel : **{existing_csv_name}**")

        uploaded_file = st.file_uploader(
            "Charger un fichier CSV",
            type=["csv"],
            key="csv_upload_wizard",
        )

        local_df = None

        if uploaded_file is not None:
            try:
                csv_bytes = uploaded_file.getvalue()
                st.session_state["wizard_csv_bytes"]    = csv_bytes
                st.session_state["wizard_csv_filename"] = uploaded_file.name
                local_df = _read_csv_safely(csv_bytes)
                # Also persist to planning if it already exists
                if planning:
                    planning.csv_filename = uploaded_file.name
                    planning.csv_bytes    = csv_bytes
                    planning.updated_at   = datetime.now(timezone.utc)
                    session.commit()
            except Exception as e:
                if planning:
                    session.rollback()
                st.error(f"Erreur lors du chargement du CSV : {e}")

        elif existing_csv_bytes:
            try:
                local_df = _read_csv_safely(existing_csv_bytes)
            except Exception as e:
                st.error(f"Erreur de lecture du CSV enregistré : {e}")

        if local_df is not None:
            columns = list(local_df.columns)
            current_mapping = st.session_state.get("wizard_mapping") or {}
            if not current_mapping:
                current_mapping = _load_saved_mapping(session, user.organization_id)

            targets = [
                ("ot_id", "OT"),
                ("description", "Description"),
                ("status", "Statut"),
                ("atelier", "Atelier"),
                ("secteur", "Secteur"),
                ("equipment", "Equipement"),
                ("equipment_desc", "Description equipement"),
                ("created_at", "Créé le"),
                ("created_by", "Créé par"),
                ("requested_week", "Sem. souhaitée"),
                ("condition", "Condition réalisation"),
                ("estimated_hours", "Durée estimée"),
            ]

            with st.form("mapping_form"):
                mapping = {}
                options = [""] + columns
                col_a, col_b = st.columns(2)
                for idx, (key, label) in enumerate(targets):
                    col = col_a if idx % 2 == 0 else col_b
                    with col:
                        default_val = current_mapping.get(key, "")
                        default_index = options.index(default_val) if default_val in options else 0
                        mapping[key] = st.selectbox(label, options, index=default_index, key=f"map_{key}")
                submitted_mapping = st.form_submit_button("Valider le mapping", use_container_width=True, type="primary")

            if submitted_mapping:
                if not mapping["ot_id"] or not mapping["description"] or not mapping["atelier"]:
                    st.error("Au minimum, mappe OT, Description et Atelier.")
                else:
                    st.session_state["wizard_mapping"] = mapping
                    _save_mapping(session, user.organization_id, mapping)
                    st.session_state["wizard_tasks_df"] = _build_tasks_df_from_mapping(local_df, mapping)
                    st.session_state["wizard_active_section"] = 2

    # ─────────────────────────────────────────────────────────────────────────
    # FIX #1 — ÉTAPE 2 : Paramètres de l'arrêt (crée le planning en DB)
    # ─────────────────────────────────────────────────────────────────────────
    default_name        = planning.name if planning else ""
    default_sectors     = planning.sectors_csv if planning else ""
    default_start_date  = planning.start_at.date() if planning else datetime.now().date()
    default_end_date    = planning.end_at.date() if planning else datetime.now().date()
    default_daily_open  = planning.daily_open if planning else "07:00"
    default_daily_close = planning.daily_close if planning else "15:00"
    default_site_open   = getattr(planning, "site_open",  None) or "06:00" if planning else "06:00"
    default_site_close  = getattr(planning, "site_close", None) or "22:00" if planning else "22:00"

    with st.expander("2. Paramètres de l'arrêt", expanded=st.session_state["wizard_active_section"] == 2):
        with st.form("create_planning_form"):
            name = st.text_input("Nom du planning", value=default_name)
            sectors_txt = st.text_input("Secteurs (séparés par des virgules)", value=default_sectors)

            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Date de début de l'arrêt", value=default_start_date)
                daily_open = st.text_input(
                    "🔧 Heure de début fenêtre d'arrêt (HH:MM)",
                    value=default_daily_open,
                    help="Heure de début des travaux (ex : 07:00)",
                )
            with col2:
                end_date = st.date_input("Date de fin de l'arrêt", value=default_end_date)
                daily_close = st.text_input(
                    "🔧 Heure de fin fenêtre d'arrêt (HH:MM)",
                    value=default_daily_close,
                    help="Heure de fin des travaux (ex : 15:00)",
                )

            st.markdown("**Plage horaire du site (multi-jours)**")
            st.caption("Bornes autorisées pour planifier chaque jour. La fenêtre d'arrêt doit être à l'intérieur de ces bornes.")
            scol1, scol2 = st.columns(2)
            with scol1:
                site_open = st.text_input(
                    "🏭 Heure d'ouverture du site (HH:MM)",
                    value=default_site_open,
                    help="Ex : 06:00",
                )
            with scol2:
                site_close = st.text_input(
                    "🏭 Heure de fermeture du site (HH:MM)",
                    value=default_site_close,
                    help="Ex : 22:00",
                )

            submitted = st.form_submit_button("Valider les paramètres", use_container_width=True, type="primary")

        if submitted:
            try:
                start_at = _combine_date_time(start_date, daily_open)
                end_at   = _combine_date_time(end_date, daily_close)
                csv_bytes_to_save    = st.session_state.get("wizard_csv_bytes")
                csv_filename_to_save = st.session_state.get("wizard_csv_filename")

                if planning:
                    planning.name        = name.strip()
                    planning.sectors_csv = sectors_txt.strip()
                    planning.start_at    = start_at
                    planning.end_at      = end_at
                    planning.daily_open  = daily_open
                    planning.daily_close = daily_close
                    planning.site_open   = site_open
                    planning.site_close  = site_close
                    planning.updated_at  = datetime.now(timezone.utc)
                    if csv_bytes_to_save and not planning.csv_bytes:
                        planning.csv_bytes    = csv_bytes_to_save
                        planning.csv_filename = csv_filename_to_save
                    session.commit()
                else:
                    planning = Planning(
                        organization_id   = user.organization_id,
                        created_by_user_id= user.id,
                        name              = name.strip(),
                        sectors_csv       = sectors_txt.strip(),
                        start_at          = start_at,
                        end_at            = end_at,
                        daily_open        = daily_open,
                        daily_close       = daily_close,
                        site_open         = site_open,
                        site_close        = site_close,
                        status            = "draft",
                        csv_bytes         = csv_bytes_to_save,
                        csv_filename      = csv_filename_to_save,
                    )
                    session.add(planning)
                    session.commit()
                    session.refresh(planning)
                    st.session_state["wizard_planning_id"] = planning.id

                st.session_state["wizard_active_section"] = 3

            except Exception as e:
                session.rollback()
                st.error(f"Erreur lors de l'enregistrement des paramètres : {e}")

    planning = _get_current_planning(session, st.session_state["wizard_planning_id"])
    if not planning:
        return

    with st.expander("3. Sélection ateliers / secteurs", expanded=st.session_state["wizard_active_section"] == 3):
        tasks_df = st.session_state.get("wizard_tasks_df")

        if tasks_df is None:
            st.info("Valide d'abord le mapping.")
        else:
            ateliers = sorted([x for x in tasks_df["atelier"].dropna().astype(str).unique().tolist() if x])
            secteurs = sorted([x for x in tasks_df["secteur"].dropna().astype(str).unique().tolist() if x])

            with st.form("scope_form"):
                selected_ateliers = st.multiselect(
                    "Ateliers sélectionnés",
                    options=ateliers,
                    default=st.session_state["wizard_selected_ateliers"] or ateliers,
                )
                selected_secteurs = st.multiselect(
                    "Secteurs sélectionnés",
                    options=secteurs,
                    default=st.session_state["wizard_selected_secteurs"] or secteurs,
                )

                submit_scope = st.form_submit_button("Valider le scope", use_container_width=True)

            if submit_scope:
                st.session_state["wizard_selected_ateliers"] = selected_ateliers
                st.session_state["wizard_selected_secteurs"] = selected_secteurs
                st.session_state["wizard_filtered_tasks_df"] = _build_filtered_tasks_df(tasks_df, selected_ateliers, selected_secteurs)
                st.session_state["wizard_active_section"] = 4

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

                st.markdown(f"**{atelier}** — charge calculée : {round(total_hours,1)} h")

                nb_equipes = st.number_input(
                    f"{atelier} - nombre d'équipes disponibles",
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
                                "available_from": st.column_config.TextColumn("Début disponibilité"),
                                "available_to": st.column_config.TextColumn("Fin disponibilité"),
                            },
                        )
                        all_rosters.append(edited_roster.copy())
                    else:
                        st.info("Aucune équipe pour cet atelier.")

            if st.button("Valider les équipes", key="validate_teams_btn", use_container_width=True):
                st.session_state["wizard_teams_df"] = (
                    pd.concat(all_rosters, ignore_index=True)
                    if all_rosters
                    else pd.DataFrame(columns=["atelier", "code", "name", "available_from", "available_to"])
                )
                st.session_state["wizard_current_atelier_idx"] = 0
                st.session_state["wizard_active_section"] = 5

    with st.expander("5. Sélection des OT par atelier", expanded=st.session_state["wizard_active_section"] == 5):
        filtered_df = st.session_state.get("wizard_filtered_tasks_df")
        teams_df = st.session_state.get("wizard_teams_df")
        selected_ateliers = st.session_state.get("wizard_selected_ateliers", [])

        if filtered_df is None or teams_df is None or not selected_ateliers:
            st.info("Valide d'abord les équipes.")
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
                with st.expander(f"{atelier} — charge sélectionnée : {round(selected_hours,1)} h", expanded=expanded):
                    with st.form(f"atelier_main_form_{_safe_key(atelier)}"):
                        # #3 AMÉLIORATION: ajout colonne "Code équipement"
                        available_cols = ["selected", "ot_id", "description", "equipment", "equipment_desc", "status", "duration_hours"]
                        display_cols_ot = [c for c in available_cols if c in work_df.columns]
                        display_df = work_df[display_cols_ot].copy()
                        display_df["description"] = display_df["description"].apply(lambda x: _shorten(x, 40))
                        if "equipment_desc" in display_df.columns:
                            display_df["equipment_desc"] = display_df["equipment_desc"].apply(lambda x: _shorten(x, 28))
                        if "equipment" in display_df.columns:
                            display_df["equipment"] = display_df["equipment"].apply(lambda x: _shorten(x, 20))

                        col_cfg_ot = {
                            "selected": st.column_config.CheckboxColumn("✓"),
                            "ot_id": st.column_config.TextColumn("N° OT", disabled=True),
                            "description": st.column_config.TextColumn("Description", disabled=True),
                            "equipment": st.column_config.TextColumn("Code équipement", disabled=True),
                            "equipment_desc": st.column_config.TextColumn("Desc. équipement", disabled=True),
                            "status": st.column_config.TextColumn("Statut", disabled=True),
                            "duration_hours": st.column_config.NumberColumn("Durée retenue (h)", min_value=0.0, step=0.5),
                        }

                        edited_main = st.data_editor(
                            display_df,
                            use_container_width=True,
                            hide_index=True,
                            num_rows="fixed",
                            key=f"main_editor_{_safe_key(atelier)}",
                            column_config=col_cfg_ot,
                        )

                        validate_selection = st.form_submit_button("Valider la sélection", use_container_width=True)

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
                        st.markdown("#### Contraintes des OT sélectionnés")

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
                                # #2 AMÉLIORATION: affichage N°OT | durée | description AVANT dépliage
                                h_sel = round(float(r.get("duration_hours", 0) or 0), 1)
                                expander_label = f"**{ot_id}** | {h_sel} h | {_shorten(r['description'], 60)}"

                                with st.expander(expander_label, expanded=False):
                                    pred_options = [""] + [x for x in pred_labels.keys() if x != ot_id]
                                    current_pred = _safe_text(r.get("predecessor_ot", ""))

                                    pred_val = st.selectbox(
                                        f"Prédécesseur pour OT {ot_id}",
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
                                            f"Date forcée pour OT {ot_id}",
                                            value=default_date,
                                            key=f"forced_date_{_safe_key(atelier)}_{ot_id}"
                                        )
                                    with c2:
                                        forced_time = st.time_input(
                                            f"Heure forcée pour OT {ot_id}",
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
                                        f"Equipes forcées pour OT {ot_id}",
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

            if st.button("Finaliser la sélection des OT", key="finalize_ot_btn", use_container_width=True):
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

    with st.expander("6. Génération du planning", expanded=st.session_state["wizard_active_section"] == 6):
        selected_df = st.session_state.get("wizard_selected_df")
        teams_df = st.session_state.get("wizard_teams_df")

        if selected_df is None or teams_df is None:
            st.info("Valide d'abord la sélection OT et les équipes.")
        else:
            coactivity_df = _build_coactivity_df(selected_df)
            st.markdown("#### Coactivités détectées")
            if coactivity_df.empty:
                st.info("Aucune coactivité détectée.")
            else:
                st.dataframe(coactivity_df, use_container_width=True, hide_index=True)

            st.markdown("#### Actions manuelles à insérer")
            st.caption("Ajoutez des actions libres (consignation, réunion, pause...) qui seront planifiées avec les OT. Cliquez sur **Sauvegarder** après édition.")

            actions_default = pd.DataFrame(
                columns=["action_id", "slot_type", "description", "atelier", "duration_hours", "forced_start", "forced_team"]
            )
            if st.session_state["wizard_slot_actions_df"] is None:
                st.session_state["wizard_slot_actions_df"] = actions_default

            # BUG FIX #5: Show current saved state (not the live editor result) as the
            # source of truth. A separate "Sauvegarder" button commits the edits.
            edited_actions = st.data_editor(
                st.session_state["wizard_slot_actions_df"],
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                key="manual_actions_editor",
                column_config={
                    "action_id": st.column_config.TextColumn("ID action"),
                    "slot_type": st.column_config.SelectboxColumn("Type planif.", options=["DURING", "START", "END"]),
                    "description": st.column_config.TextColumn("Description"),
                    "atelier": st.column_config.SelectboxColumn("Atelier", options=st.session_state.get("wizard_selected_ateliers", [])),
                    "duration_hours": st.column_config.NumberColumn("Durée (h)", min_value=0.0, step=0.5),
                    "forced_start": st.column_config.TextColumn("Début forcé (YYYY-MM-DD HH:MM)"),
                    "forced_team": st.column_config.TextColumn("Equipe forcée (code)"),
                },
            )

            # BUG FIX #5: Explicit save button — ensures state is persisted before generation
            if st.button("💾 Sauvegarder les actions manuelles", key="save_manual_actions_btn"):
                saved = edited_actions.copy()
                # Strip fully empty rows
                filled = saved[
                    (saved.get("description", pd.Series([""] * len(saved))).astype(str).str.strip() != "") |
                    (saved.get("atelier", pd.Series([""] * len(saved))).astype(str).str.strip() != "")
                ].copy() if not saved.empty else saved
                st.session_state["wizard_slot_actions_df"] = filled
                n = len(filled)
                st.markdown(
                    f'<span class="mn-badge-success">✓ {n} action(s) sauvegardée(s)</span>',
                    unsafe_allow_html=True,
                )
                print(f"[DEBUG] {n} action(s) manuelle(s) sauvegardée(s) en session_state.")
            else:
                # Always keep state in sync even without explicit save click
                st.session_state["wizard_slot_actions_df"] = edited_actions.copy()

            # Show count of currently saved actions
            n_saved = len(st.session_state["wizard_slot_actions_df"])
            if n_saved > 0:
                st.markdown(
                    f'<span class="mn-badge-orange">{n_saved} action(s) en attente de planification</span>',
                    unsafe_allow_html=True,
                )

            selected_ateliers = st.session_state.get("wizard_selected_ateliers", [])
            stop_hours = max(
                1.0,
                (_normalize_ts(planning.end_at) - _normalize_ts(planning.start_at)).total_seconds() / 3600
            )

            total_selected_hours = float(selected_df["duration_hours"].sum()) if not selected_df.empty else 0.0
            total_capacity_hours = float(sum(len(teams_df[teams_df["atelier"] == a]) * stop_hours for a in selected_ateliers))

            c1, c2, c3 = st.columns(3)
            c1.metric("OT sélectionnés", len(selected_df))
            c2.metric("Charge sélectionnée (h)", round(total_selected_hours, 1))
            c3.metric("Capacité théorique totale (h)", round(total_capacity_hours, 1))

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

                    st.success("Planning généré et enregistré.")

                except Exception as e:
                    session.rollback()
                    st.error(f"Erreur lors de la génération : {e}")

            generated_df = st.session_state.get("wizard_generated_df")
            unscheduled_df = st.session_state.get("wizard_unscheduled_df")
            manual_generated_df = st.session_state.get("wizard_manual_generated_df")

            if generated_df is not None and not generated_df.empty:
                st.subheader("Planning généré")

                # FIX #5: Add explicit date column extracted from planned_start_at
                display_df = generated_df.copy()
                if "planned_start_at" in display_df.columns:
                    display_df["date"] = pd.to_datetime(
                        display_df["planned_start_at"], errors="coerce"
                    ).dt.strftime("%d/%m/%Y").fillna("")

                display_cols = [c for c in [
                    "date", "ot_id", "description", "atelier",
                    "duration_hours", "planned_start_at", "planned_end_at",
                    "planned_team_name", "commentaire",
                ] if c in display_df.columns]

                st.dataframe(
                    display_df[display_cols],
                    use_container_width=True,
                    hide_index=True,
                )

                # ── Gantt chart (Plotly interactif) ──────────────────────────
                st.subheader("Diagramme de Gantt interactif")
                _render_gantt(generated_df, planning, manual_df=manual_generated_df)

                # ── Exports ───────────────────────────────────────────────────
                st.subheader("Téléchargement")
                dl_col1, dl_col2 = st.columns(2)

                with dl_col1:
                    try:
                        pdf_bytes = _generate_planning_pdf(generated_df, planning)
                        st.download_button(
                            label="📄 Télécharger le planning PDF",
                            data=pdf_bytes,
                            file_name=f"planning_{_safe_key(planning.name)}.pdf",
                            mime="application/pdf",
                            use_container_width=True,
                        )
                    except Exception as pdf_err:
                        st.error(f"Erreur PDF : {pdf_err}")
                        import traceback
                        st.caption(traceback.format_exc())

                with dl_col2:
                    try:
                        html_bytes = _generate_planning_html(generated_df, planning, manual_df=manual_generated_df)
                        st.download_button(
                            label="🌐 Télécharger le planning HTML",
                            data=html_bytes,
                            file_name=f"planning_{_safe_key(planning.name)}.html",
                            mime="text/html",
                            use_container_width=True,
                        )
                    except Exception as html_err:
                        st.error(f"Erreur HTML : {html_err}")

            if manual_generated_df is not None and not manual_generated_df.empty:
                st.subheader("Actions manuelles planifiées")
                show_cols = [c for c in ["ot_id", "description", "atelier", "duration_h", "planned_start_at", "planned_end_at", "planned_team_name"] if c in manual_generated_df.columns]
                st.dataframe(manual_generated_df[show_cols], use_container_width=True, hide_index=True)

            if unscheduled_df is not None and not unscheduled_df.empty:
                st.subheader("OT non planifiés")
                st.dataframe(unscheduled_df, use_container_width=True, hide_index=True)
