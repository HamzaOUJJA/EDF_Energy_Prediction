# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# test_wait_dag.py — Tests parsing trigger Airflow (logs + dag_run_id)
# =======================================================================

from __future__ import annotations

import pytest

from edf_pipeline.wait_dag import (
    DEFAULT_TIMEOUT_SEC,
    QUEUED_STUCK_SEC,
    _parse_trigger_payload,
    _run_id_from_trigger_payload,
)


SAMPLE_STDOUT = """\
[2026-05-23T20:52:18.513+0000] {__init__.py:43} INFO - Loaded API auth backend: airflow.api.auth.backend.session
[{"conf": {}, "dag_id": "edf_pipeline_complet", "dag_run_id": "manual__2026-05-23T20:52:19+00:00", "state": "queued"}]
"""

def test_parse_trigger_payload_skips_log_prefix():
    payload = _parse_trigger_payload(SAMPLE_STDOUT)
    assert payload["dag_id"] == "edf_pipeline_complet"
    assert payload["dag_run_id"] == "manual__2026-05-23T20:52:19+00:00"

# 
def test_run_id_from_dag_run_id():
    payload = _parse_trigger_payload(SAMPLE_STDOUT)
    assert _run_id_from_trigger_payload(payload) == "manual__2026-05-23T20:52:19+00:00"


def test_run_id_prefers_explicit_run_id():
    assert _run_id_from_trigger_payload({"run_id": "custom_run"}) == "custom_run"


def test_parse_trigger_payload_object():
    payload = _parse_trigger_payload('{"dag_run_id": "manual__1"}')
    assert _run_id_from_trigger_payload(payload) == "manual__1"


def test_parse_trigger_payload_empty_raises():
    with pytest.raises(RuntimeError, match="Empty response"):
        _parse_trigger_payload("   ")


def test_queued_stuck_timeout_is_reasonable():
    assert QUEUED_STUCK_SEC >= 60
    assert QUEUED_STUCK_SEC < DEFAULT_TIMEOUT_SEC
