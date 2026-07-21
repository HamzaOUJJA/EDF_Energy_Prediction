# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# test_config.py — Tests planification DAGs (dev vs prod)
# =======================================================================

from __future__ import annotations

import importlib
import pytest
import edf_pipeline.config as cfg

@pytest.mark.parametrize(
    ("getter_name", "cron_attr"),
    [
        ("get_pipeline_schedule", "PROD_PIPELINE_SCHEDULE"),
        ("get_etl_schedule", "PROD_ETL_SCHEDULE"),
        ("get_ml_schedule", "PROD_ML_SCHEDULE"),
        ("get_quality_schedule", "PROD_QUALITY_SCHEDULE"),
    ],
)
# Test schedules none in dev
def test_schedules_none_in_dev(monkeypatch, getter_name, cron_attr):
    monkeypatch.setenv("EDF_ENVIRONMENT", "dev")
    importlib.reload(cfg)
    prod_cron = getattr(cfg, cron_attr)
    assert cfg.get_schedule(prod_cron) is None
    assert getattr(cfg, getter_name)() is None

# Test get schedule returns cron in prod
def test_get_schedule_returns_cron_in_prod(monkeypatch):
    monkeypatch.setenv("EDF_ENVIRONMENT", "prod")
    importlib.reload(cfg)
    assert cfg.get_schedule("0 2 * * *") == "0 2 * * *"
    assert cfg.get_pipeline_schedule() == cfg.PROD_PIPELINE_SCHEDULE
