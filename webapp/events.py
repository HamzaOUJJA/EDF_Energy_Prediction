# =======================================================================
# webapp/events.py — Merges real audit data (etl.pipeline_runs +
# etl.data_quality_checks) into one time-sorted system event log.
# =======================================================================

from __future__ import annotations

from typing import Any

import db

_LEVELS = {"info", "warn", "error"}


def _pipeline_run_events(limit: int) -> list[dict[str, Any]]:
    rows = db.query_all(
        """
        SELECT run_id, dag_id, run_type, status, started_at, finished_at,
               rows_written, error_message
        FROM etl.pipeline_runs
        ORDER BY started_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    events = []
    for row in rows:
        status = (row.get("status") or "").lower()
        if status == "failed":
            level = "error"
        elif status in ("success_with_warnings", "running"):
            level = "warn"
        else:
            level = "info"
        message = f"{row['dag_id']} ({row.get('run_type') or 'run'}) -> {row['status']}"
        if row.get("rows_written") is not None:
            message += f" — {row['rows_written']} rows"
        if row.get("error_message"):
            message += f" — {row['error_message']}"
        events.append(
            {
                "ts": (row.get("finished_at") or row.get("started_at")),
                "level": level,
                "source": "pipeline_runs",
                "message": message,
            }
        )
    return events


def _quality_check_events(limit: int) -> list[dict[str, Any]]:
    rows = db.query_all(
        """
        SELECT check_name, table_name, expected_value, actual_value, passed,
               checked_at
        FROM etl.data_quality_checks
        ORDER BY checked_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    events = []
    for row in rows:
        passed = row.get("passed")
        level = "info" if passed else "warn"
        message = f"quality check '{row['check_name']}' on {row.get('table_name') or '?'}"
        message += " passed" if passed else " FAILED"
        if row.get("expected_value") is not None and row.get("actual_value") is not None:
            message += f" (expected {row['expected_value']}, actual {row['actual_value']})"
        events.append(
            {
                "ts": row.get("checked_at"),
                "level": level,
                "source": "data_quality_checks",
                "message": message,
            }
        )
    return events


def get_events(limit: int = 100, levels: set[str] | None = None) -> list[dict[str, Any]]:
    """Merged, time-sorted event feed. `levels` filters to a subset of {info,warn,error}."""
    events = _pipeline_run_events(limit) + _quality_check_events(limit)
    events = [e for e in events if e["ts"] is not None]
    events.sort(key=lambda e: e["ts"], reverse=True)

    if levels:
        events = [e for e in events if e["level"] in levels]

    events = events[:limit]
    for e in events:
        e["ts"] = e["ts"].isoformat()
    return events
