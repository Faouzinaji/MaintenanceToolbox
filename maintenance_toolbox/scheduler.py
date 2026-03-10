from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd


@dataclass
class TeamSlot:
    code: str
    name: str
    atelier: str
    available_from: pd.Timestamp
    available_to: pd.Timestamp
    current: pd.Timestamp


def build_schedule(tasks: list[dict[str, Any]], teams: list[dict[str, Any]], start_at: datetime, end_at: datetime) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool]:
    """Simple greedy scheduler.
    Returns planning_rows, unscheduled_rows, fits_window.
    """
    planning_rows: list[dict[str, Any]] = []
    unscheduled_rows: list[dict[str, Any]] = []

    team_slots: list[TeamSlot] = []
    for t in teams:
        team_slots.append(
            TeamSlot(
                code=str(t["code"]),
                name=str(t["name"]),
                atelier=str(t["atelier"]),
                available_from=pd.Timestamp(t["available_from"]),
                available_to=pd.Timestamp(t["available_to"]),
                current=max(pd.Timestamp(t["available_from"]), pd.Timestamp(start_at)),
            )
        )

    tasks_df = pd.DataFrame(tasks)
    if tasks_df.empty:
        return planning_rows, unscheduled_rows, True

    if "selected_warning" not in tasks_df.columns:
        tasks_df["selected_warning"] = ""
    if "priority_score" not in tasks_df.columns:
        tasks_df["priority_score"] = 0

    # unfinished old OT become top priority
    tasks_df["priority_boost"] = tasks_df["selected_warning"].fillna("").apply(lambda x: 1000 if str(x).strip() else 0)
    tasks_df = tasks_df.sort_values(
        by=["priority_boost", "priority_score", "estimated_hours"],
        ascending=[False, False, True]
    ).copy()

    task_map = {str(r["external_ot_id"]): r for _, r in tasks_df.iterrows()}
    scheduled_map: dict[str, dict[str, Any]] = {}
    unscheduled = set(task_map.keys())

    progress = True
    while unscheduled and progress:
        progress = False
        ready = []
        for ot_id in unscheduled:
            pred = str(task_map[ot_id].get("predecessor_ot_id") or "").strip()
            if not pred or pred in scheduled_map:
                ready.append(ot_id)

        for ot_id in ready:
            task = task_map[ot_id]
            atelier = str(task.get("atelier") or "")
            dur = float(task.get("estimated_hours") or 0.0)
            forced_codes = [x.strip() for x in str(task.get("forced_team_codes") or "").split(";") if x.strip()]
            forced_start = task.get("forced_start_at")
            pred = str(task.get("predecessor_ot_id") or "").strip()
            earliest = pd.Timestamp(start_at)
            if pred and pred in scheduled_map:
                earliest = scheduled_map[pred]["planned_end_at"]
            if forced_start:
                earliest = max(earliest, pd.Timestamp(forced_start))

            candidates = [x for x in team_slots if x.atelier == atelier]
            if forced_codes:
                forced_norm = {c.lower() for c in forced_codes}
                candidates = [x for x in candidates if x.code.lower() in forced_norm or x.name.lower() in forced_norm]

            best = None
            best_end = None
            for c in candidates:
                start_candidate = max(c.current, c.available_from, earliest)
                end_candidate = start_candidate + pd.Timedelta(hours=dur)
                if end_candidate > c.available_to:
                    continue
                if best_end is None or end_candidate < best_end:
                    best = c
                    best_end = end_candidate

            if best is None:
                rr = dict(task)
                rr["reason"] = "No compatible team capacity"
                unscheduled_rows.append(rr)
                unscheduled.remove(ot_id)
                progress = True
                continue

            best.current = best_end
            row = dict(task)
            row["planned_start_at"] = max(best.current - pd.Timedelta(hours=dur), pd.Timestamp(start_at))
            row["planned_end_at"] = best_end
            row["planned_team_name"] = best.name
            row["planned_team_code"] = best.code
            planning_rows.append(row)
            scheduled_map[ot_id] = row
            unscheduled.remove(ot_id)
            progress = True

    for ot_id in unscheduled:
        rr = dict(task_map[ot_id])
        rr["reason"] = "Dependency cycle or blocked predecessor"
        unscheduled_rows.append(rr)

    fits_window = True
    if planning_rows:
        max_end = max(pd.Timestamp(r["planned_end_at"]) for r in planning_rows)
        fits_window = max_end <= pd.Timestamp(end_at)

    return planning_rows, unscheduled_rows, fits_window
