# =======================================================================
# webapp/airflow_client.py — Thin wrapper over Airflow's stable REST API
# (/api/v1) used to trigger and monitor edf_pipeline_complet.
# =======================================================================

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import requests

from config import AIRFLOW_BASE_URL, AIRFLOW_PASSWORD, AIRFLOW_USERNAME, EDF_DAG_ID

# States Airflow considers "done" for a task instance (used for progress %).
DONE_STATES = {"success", "failed", "skipped", "upstream_failed", "removed"}
# States Airflow considers "still active" for a DAG run.
ACTIVE_RUN_STATES = {"queued", "running", None}

_AUTH = (AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
_TIMEOUT = 30


class AirflowApiError(RuntimeError):
    """Raised when the Airflow REST API returns an error response."""


def _request(method: str, path: str, **kwargs: Any) -> requests.Response:
    url = f"{AIRFLOW_BASE_URL}{path}"
    try:
        resp = requests.request(method, url, auth=_AUTH, timeout=_TIMEOUT, **kwargs)
    except requests.RequestException as exc:
        raise AirflowApiError(f"Could not reach Airflow at {url}: {exc}") from exc
    if resp.status_code >= 400:
        raise AirflowApiError(
            f"Airflow API {method} {path} failed ({resp.status_code}): {resp.text[:500]}"
        )
    return resp


def trigger_dag_run(years: list[int], dag_id: str = EDF_DAG_ID) -> str:
    """Trigger a run of ``dag_id`` with ``years`` in the conf. Returns the dag_run_id."""
    dag_run_id = f"web__{datetime.now(timezone.utc):%Y%m%dT%H%M%S}__{uuid.uuid4().hex[:8]}"
    payload = {
        "dag_run_id": dag_run_id,
        "conf": {"years": sorted(years)},
    }
    resp = _request("POST", f"/api/v1/dags/{dag_id}/dagRuns", json=payload)
    return resp.json()["dag_run_id"]


def get_dag_run(dag_run_id: str, dag_id: str = EDF_DAG_ID) -> dict[str, Any]:
    """Return the DAG run's state and timing."""
    resp = _request("GET", f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}")
    return resp.json()


def get_latest_dag_run(dag_id: str = EDF_DAG_ID) -> dict[str, Any] | None:
    """Return the currently active run if there is one, else the most recently
    started run, else None if the DAG never ran.

    A queued run has no start_date yet, so ordering by "-start_date" alone
    would rank it above an actually-running run (NULLs sort first in
    descending order) — check for an active run explicitly first.
    """
    for state in ("running", "queued"):
        resp = _request(
            "GET",
            f"/api/v1/dags/{dag_id}/dagRuns",
            params={"state": state, "limit": 1},
        )
        runs = resp.json().get("dag_runs", [])
        if runs:
            return runs[0]

    resp = _request(
        "GET",
        f"/api/v1/dags/{dag_id}/dagRuns",
        params={"order_by": "-start_date", "limit": 1},
    )
    runs = resp.json().get("dag_runs", [])
    return runs[0] if runs else None


def list_task_instances(dag_run_id: str, dag_id: str = EDF_DAG_ID) -> list[dict[str, Any]]:
    """Return every task instance (task_id, state, try_number, timing) for this run."""
    resp = _request(
        "GET", f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    )
    return resp.json().get("task_instances", [])


def get_run_progress(dag_run_id: str, dag_id: str = EDF_DAG_ID) -> dict[str, Any]:
    """Aggregate DAG run state + task instances into a single progress payload."""
    dag_run = get_dag_run(dag_run_id, dag_id)
    tasks = list_task_instances(dag_run_id, dag_id)
    total = len(tasks)
    done = sum(1 for t in tasks if t.get("state") in DONE_STATES)
    return {
        "dag_run_id": dag_run_id,
        "state": dag_run.get("state"),
        "start_date": dag_run.get("start_date"),
        "end_date": dag_run.get("end_date"),
        "progress": {
            "done": done,
            "total": total,
            "percent": round(100 * done / total, 1) if total else 0,
        },
        "tasks": [
            {
                "task_id": t.get("task_id"),
                "state": t.get("state"),
                "try_number": t.get("try_number") or 1,
                "start_date": t.get("start_date"),
                "end_date": t.get("end_date"),
            }
            for t in tasks
        ],
    }


def stop_dag_run(dag_run_id: str, dag_id: str = EDF_DAG_ID) -> dict[str, Any]:
    """Mark a DAG run as failed — same effect as "Mark Failed" in the Airflow UI.

    Airflow cascades this to the run's incomplete task instances; the next
    heartbeat of any actively running task detects the state mismatch and
    terminates it, invoking the operator's on_kill (e.g. SparkRestSubmitOperator
    kills the underlying Spark submission). Not instantaneous — typically
    within one heartbeat cycle (a few seconds up to ~30s).
    """
    resp = _request(
        "PATCH",
        f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}",
        json={"state": "failed"},
    )
    return resp.json()


def stop_all_dag_runs(dag_id: str = EDF_DAG_ID) -> dict[str, Any]:
    """Stop every active (running or queued) DAG run — used by the dashboard's
    Stop button so one click clears the whole queue, not just the run the
    browser happens to be tracking.
    """
    stopped: list[str] = []
    for run in list_all_dag_runs(dag_id):
        if run.get("state") in ("running", "queued"):
            stop_dag_run(run["dag_run_id"], dag_id)
            stopped.append(run["dag_run_id"])
    return {"stopped": stopped, "count": len(stopped)}


def list_all_dag_runs(dag_id: str = EDF_DAG_ID, limit: int = 500) -> list[dict[str, Any]]:
    """Return up to `limit` DAG runs, most recent first."""
    resp = _request(
        "GET",
        f"/api/v1/dags/{dag_id}/dagRuns",
        params={"order_by": "-start_date", "limit": limit},
    )
    return resp.json().get("dag_runs", [])


def delete_dag_run(dag_run_id: str, dag_id: str = EDF_DAG_ID) -> None:
    """Delete a DAG run's history entry (and its task instance records)."""
    _request("DELETE", f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}")


def clear_dag_run_history(dag_id: str = EDF_DAG_ID) -> dict[str, int]:
    """Delete every finished DAG run's history. Active runs are left untouched."""
    deleted = 0
    skipped_active = 0
    for run in list_all_dag_runs(dag_id):
        if run.get("state") in ACTIVE_RUN_STATES:
            skipped_active += 1
            continue
        delete_dag_run(run["dag_run_id"], dag_id)
        deleted += 1
    return {"deleted": deleted, "skipped_active": skipped_active}


def get_task_log(
    dag_run_id: str,
    task_id: str,
    try_number: int,
    dag_id: str = EDF_DAG_ID,
) -> str:
    """Return the plain-text log for one task instance attempt."""
    resp = _request(
        "GET",
        f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}",
        headers={"Accept": "text/plain"},
        params={"full_content": "true"},
    )
    return resp.text
