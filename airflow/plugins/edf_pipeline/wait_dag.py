# =======================================================================
# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# airflow/plugins/edf_pipeline/wait_dag.py — Trigger and wait for an Airflow DAG (Makefile = prod).
# Parsing JSON (sans Airflow) testable via ``make test`` hors conteneur.
# =======================================================================

from __future__ import annotations

import json
import logging
import subprocess
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SEC = 8 * 3600
POLL_INTERVAL_SEC = 15
QUEUED_STUCK_SEC = 180


def _extract_json_text(stdout: str) -> str:
    """Ignore the lines of Airflow log preceding the JSON (--output json)."""
    text = stdout.strip()
    if not text:
        raise RuntimeError("Empty response from `airflow dags trigger`")

    for line in text.splitlines():
        candidate = line.strip()
        if not candidate or candidate[0] not in "[{":
            continue
        try:
            json.loads(candidate)
        except json.JSONDecodeError:
            continue
        return candidate

    raise RuntimeError(
        "No JSON in the output of `airflow dags trigger`: "
        f"{text[:500]!r}"
    )


def _parse_trigger_payload(stdout: str) -> dict[str, Any]:
    """Parse the trigger response."""
    payload = json.loads(_extract_json_text(stdout))
    if isinstance(payload, list):
        if not payload:
            raise RuntimeError("Empty response from `airflow dags trigger`")
        item = payload[0]
    elif isinstance(payload, dict):
        item = payload
    else:
        raise RuntimeError(f"Unexpected JSON format: {type(payload)!r}")

    if not isinstance(item, dict):
        raise RuntimeError(f"Unexpected JSON input: {type(item)!r}")
    return item


def _run_id_from_trigger_payload(payload: dict[str, Any]) -> str:
    """Extract the run_id from the trigger response."""
    run_id = payload.get("run_id") or payload.get("dag_run_id")
    if not run_id:
        raise RuntimeError(
            "Field run_id/dag_run_id absent in the trigger response: "
            f"{payload!r}"
        )
    return str(run_id)


def ensure_dag_unpaused(dag_id: str) -> bool:
    """Unpause the DAG if necessary (Airflow creates DAGs in pause by default)."""
    from airflow.models import DagModel
    from airflow.utils.session import create_session

    with create_session() as session:
        dag_model = (
            session.query(DagModel).filter(DagModel.dag_id == dag_id).one_or_none()
        )
        if dag_model is None:
            raise RuntimeError(
                f"DAG {dag_id} not found. Check the DAG loading "
                "(airflow dags list)."
            )
        if not dag_model.is_paused:
            return False
        dag_model.is_paused = False
        session.commit()
        logger.info("DAG %s unpaused for execution", dag_id)
        return True


def is_dag_paused(dag_id: str) -> bool:
    """Check if the DAG is paused."""
    from airflow.models import DagModel
    from airflow.utils.session import create_session

    with create_session() as session:
        dag_model = (
            session.query(DagModel).filter(DagModel.dag_id == dag_id).one_or_none()
        )
        return bool(dag_model is None or dag_model.is_paused)


def fail_blocking_runs(dag_id: str) -> int:
    """Mark the runs ``queued``/``running`` as failed before a new trigger."""
    from airflow.models import DagRun
    from airflow.utils.session import create_session
    from airflow.utils.state import State

    with create_session() as session:
        runs = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == dag_id,
                DagRun.state.in_([State.QUEUED, State.RUNNING]),
            )
            .all()
        )
        for dr in runs:
            dr.state = State.FAILED
        session.commit()
        count = len(runs)
        if count:
            logger.warning(
                "DAG %s : %d run(s) active(s) interrupted (queued/running)",
                dag_id,
                count,
            )
        return count


def _blocking_run_id(dag_id: str, run_id: str) -> str | None:
    """Return another run ``running`` that blocks ``max_active_runs=1``."""
    from airflow.models import DagRun
    from airflow.utils.session import create_session
    from airflow.utils.state import State

    with create_session() as session:
        blocker = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == dag_id,
                DagRun.run_id != run_id,
                DagRun.state == State.RUNNING,
            )
            .order_by(DagRun.start_date.desc())
            .first()
        )
        return str(blocker.run_id) if blocker is not None else None


def trigger_dag(dag_id: str) -> str:
    """Trigger a DAG and return the ``run_id``."""
    proc = subprocess.run(
        ["airflow", "dags", "trigger", dag_id, "--output", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Failed to trigger DAG {dag_id}: {proc.stderr or proc.stdout}"
        )
    run_id = _run_id_from_trigger_payload(_parse_trigger_payload(proc.stdout))
    logger.info("DAG %s triggered — run_id=%s", dag_id, run_id)
    return run_id


def _task_log_paths(ti: Any) -> list[Path]:
    """Log file paths (last attempt first)."""
    from airflow.configuration import conf

    base = conf.get("logging", "base_log_folder")
    if not base:
        return []
    last_attempt = ti.try_number or 1
    root = (
        Path(base)
        / f"dag_id={ti.dag_id}"
        / f"run_id={ti.run_id}"
        / f"task_id={ti.task_id}"
    )
    return [root / f"attempt={attempt}.log" for attempt in range(last_attempt, 0, -1)]


def _error_line_from_log(path: Path) -> str | None:
    """Extract the error line from the log."""
    if not path.exists():
        return None
    for line in reversed(path.read_text(errors="replace").splitlines()):
        stripped = line.strip()
        if not stripped:
            continue
        for prefix in ("ValueError:", "RuntimeError:", "FileNotFoundError:"):
            if prefix in stripped:
                return stripped.split(prefix, 1)[-1].strip()
        if "ClassNotFoundException:" in stripped:
            return stripped.split("ClassNotFoundException:", 1)[-1].strip()
        if "ERROR -" in stripped and (
            "Sources XLS" in stripped
            or "Quality checks" in stripped
            or "Class org.apache.hadoop" in stripped
        ):
            return stripped.split("ERROR -", 1)[-1].strip()
    return None


def _failed_task_summary(dag_id: str, run_id: str) -> str:
    """Extract the failed task and an excerpt from the Airflow log."""
    from airflow.models import TaskInstance
    from airflow.utils.session import create_session
    from airflow.utils.state import State

    with create_session() as session:
        ti = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == run_id,
                TaskInstance.state == State.FAILED,
            )
            .order_by(TaskInstance.end_date.desc())
            .first()
        )
        if ti is None:
            return ""

        summary = f"Failed task : {ti.task_id}"
        try:
            for path in _task_log_paths(ti):
                detail = _error_line_from_log(path)
                if detail:
                    return f"{summary} — {detail}"
        except OSError:
            return summary
        return summary


def wait_for_dag_run(
    dag_id: str,
    run_id: str,
    *,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
) -> dict[str, str]:
    """Wait for the end of a ``DagRun`` (success or failed)."""
    from airflow.models import DagRun
    from airflow.utils.session import create_session

    deadline = time.time() + timeout_sec
    last_state = "unknown"
    queued_since: float | None = None

    while time.time() < deadline:
        with create_session() as session:
            dr = (
                session.query(DagRun)
                .filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id)
                .one_or_none()
            )
            if dr is not None:
                last_state = str(dr.state)
                if last_state == "success":
                    logger.info("DAG %s completed with success (%s)", dag_id, run_id)
                    return {"dag_id": dag_id, "run_id": run_id, "state": last_state}
                if last_state == "failed":
                    detail = _failed_task_summary(dag_id, run_id)
                    message = f"DAG {dag_id} failed (run_id={run_id})."
                    if detail:
                        message = f"{message} {detail}"
                    else:
                        message = (
                            f"{message} Check the Airflow UI for task logs."
                        )
                    raise RuntimeError(message)
                if last_state == "queued":
                    if queued_since is None:
                        queued_since = time.time()
                    elif time.time() - queued_since >= QUEUED_STUCK_SEC:
                        if is_dag_paused(dag_id):
                            raise RuntimeError(
                                f"DAG {dag_id} is paused — the run {run_id} remains "
                                "in queued. Unpause the DAG in the Airflow UI or "
                                "restart the pipeline."
                            )
                        blocker = _blocking_run_id(dag_id, run_id)
                        if blocker:
                            logger.info(
                                "Run %s in queued (max_active_runs=1, "
                                "active run: %s)",
                                run_id,
                                blocker,
                            )
                            queued_since = time.time()
                        else:
                            raise RuntimeError(
                                f"DAG {dag_id} blocked in queued since "
                                f"{int(time.time() - queued_since)}s "
                                f"(run_id={run_id}). Check the Airflow scheduler "
                                "(docker logs edf-airflow-scheduler)."
                            )
                else:
                    queued_since = None
        time.sleep(POLL_INTERVAL_SEC)

    raise TimeoutError(
        f"Timeout ({timeout_sec}s) waiting for DAG {dag_id} "
        f"(run_id={run_id}, last state={last_state})"
    )


def trigger_and_wait(
    dag_id: str,
    *,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
) -> dict[str, str]:
    """Makefile entry point — same orchestration as in production."""
    ensure_dag_unpaused(dag_id)
    fail_blocking_runs(dag_id)
    run_id = trigger_dag(dag_id)
    return wait_for_dag_run(dag_id, run_id, timeout_sec=timeout_sec)
