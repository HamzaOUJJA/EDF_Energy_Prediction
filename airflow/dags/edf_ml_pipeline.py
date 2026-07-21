# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# airflow/dags/edf_ml_pipeline.py — ML pipeline DAG (Makefile = prod).
# =======================================================================

from __future__ import annotations

from datetime import datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from edf_pipeline.config import DEFAULT_ARGS, EDF_ENVIRONMENT, PROD_ML_SCHEDULE, get_ml_schedule
from edf_pipeline.eda_report import (
    generate_eda_ml_report,
    mark_eda_ml_pending,
)
from edf_pipeline.metadata import record_ml_run
from edf_pipeline.schema import ensure_model_metrics_schema
from edf_pipeline.spark_operators import spark_job
from edf_pipeline.tasks import check_ml_readiness, skip_ml_training

ML_DEFAULT_ARGS = {
    **DEFAULT_ARGS,
    "owner": "edf-ml",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="edf_ml_pipeline",
    description="ML weekly — training alone (without reloading the ETL)",
    default_args=ML_DEFAULT_ARGS,
    schedule_interval=get_ml_schedule(),
    catchup=False,
    max_active_runs=1,
    tags=["edf", "ml", "weekly"],
    doc_md=f"""
## ML alone

- **Environment** : `{EDF_ENVIRONMENT}`
- **prod** : cron ``{PROD_ML_SCHEDULE}`` — training alone (without reloading the ETL)
- **dev** : manual trigger (`airflow dags trigger edf_ml_pipeline`)
- Complete pipeline (batch + ML) : ``edf_pipeline_complet``
""",
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    t_schema = PythonOperator(
        task_id="ensure_metrics_schema",
        python_callable=ensure_model_metrics_schema,
    )
    t_check = BranchPythonOperator(
        task_id="check_data_availability",
        python_callable=partial(
            check_ml_readiness,
            task_train="run_gold_to_model",
            task_skip="skip_training",
        ),
    )
    t_skip = PythonOperator(
        task_id="skip_training",
        python_callable=skip_ml_training,
    )
    t_eda_pending = PythonOperator(
        task_id="mark_eda_ml_pending",
        python_callable=mark_eda_ml_pending,
    )
    t_train = spark_job(
        "run_gold_to_model",
        "gold_to_model.py",
        ml=True,
        execution_timeout=timedelta(hours=4),
    )
    t_eda_ml = PythonOperator(
        task_id="generate_eda_ml_report",
        python_callable=generate_eda_ml_report,
        execution_timeout=timedelta(minutes=10),
        retries=1,
    )
    t_meta = PythonOperator(
        task_id="update_pipeline_metadata",
        python_callable=record_ml_run,
        trigger_rule="all_done",
    )

    start >> t_schema >> t_check >> [t_train, t_skip]
    t_train >> t_eda_ml >> t_meta >> end
    t_skip >> t_eda_pending >> end
