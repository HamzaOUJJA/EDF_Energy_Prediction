# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# airflow/dags/edf_pipeline_complet.py — complete pipeline DAG (Makefile = prod).
# =======================================================================

from __future__ import annotations

from datetime import datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup

from edf_pipeline.config import (
    DAG_ID_PIPELINE_COMPLET,
    DEFAULT_ARGS,
    EDF_ENVIRONMENT,
    PROD_ETL_SCHEDULE,
    PROD_ML_SCHEDULE,
    PROD_PIPELINE_SCHEDULE,
    PROD_QUALITY_SCHEDULE,
    get_pipeline_schedule,
)
from edf_pipeline.eda_report import (
    generate_eda_data_report,
    generate_eda_ml_report,
    mark_eda_ml_pending,
)
from edf_pipeline.metadata import finalize_pipeline_run
from edf_pipeline.schema import init_postgres_schema
from edf_pipeline.spark_operators import spark_job
from edf_pipeline.tasks import (
    check_ml_readiness,
    run_post_etl_quality,
    skip_ml_training,
    validate_xls_sources,
)

DOC_MD = f"""
## Complete EDF Pipeline (batch + ML)

Orchestrate all steps achievable via ``make pipeline`` (Airflow) or ``make pipeline-spark`` (debug) :

| Step | Makefile (prod-like) | Makefile (debug) | Technology |
|-------|------------------------|------------------|-------------|
| Validation XLS | ``make pipeline`` | ``validate-xls-sources`` | Python |
| DW Schema | (DAG) | ``init-postgres-schema`` | PostgreSQL |
| Bronze | (DAG) | ``run-xls-to-bronze`` | Spark |
| Silver | (DAG) | ``run-bronze-to-silver`` | Spark |
| Gold | (DAG) | ``run-silver-to-gold`` | Spark |
| Quality | (DAG) | ``run-quality-checks`` | PostgreSQL |
| ML | (DAG) | ``run-ml`` | Spark |
| Rapport EDA | (DAG) | ``report-eda`` | Python + MinIO |
| Audit | (DAG) | ``record-pipeline-run`` | ``etl.pipeline_runs`` |

### Current environment : **{EDF_ENVIRONMENT}**

- **dev** : all DAGs in manual trigger (`airflow dags trigger <dag_id>`)
- **prod** : crons — batch ``{PROD_PIPELINE_SCHEDULE}`` | streaming ``{PROD_ETL_SCHEDULE}`` | ML ``{PROD_ML_SCHEDULE}`` | quality ``{PROD_QUALITY_SCHEDULE}``

### Additional DAGs (same logic dev/prod)

- ``edf_etl_pipeline`` : real-time Kafka stream (daily)
- ``edf_ml_pipeline`` : ML alone (weekly, without reloading the ETL)
"""

with DAG(
    dag_id=DAG_ID_PIPELINE_COMPLET,
    default_args=DEFAULT_ARGS,
    description="Pipeline batch complet : XLS -> Bronze -> Silver -> Gold -> ML",
    schedule_interval=get_pipeline_schedule(),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["edf", "pipeline", "batch", "ml", "production"],
    doc_md=DOC_MD,
    params={
        "years": Param(
            default=[],
            type="array",
            title="Years to process",
            description=(
                "RTE XLS years to (re)ingest into Bronze. "
                "Empty = all years found in data_dir."
            ),
        ),
    },
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    with TaskGroup(group_id="prerequisites", tooltip="Validation and DW schema") as prereq:
        validate_xls = PythonOperator(
            task_id="validate_xls_sources",
            python_callable=validate_xls_sources,
        )
        init_schema = PythonOperator(
            task_id="init_postgres_schema",
            python_callable=init_postgres_schema,
            execution_timeout=timedelta(minutes=15),
        )
        validate_xls >> init_schema

    with TaskGroup(group_id="bronze", tooltip="XLS -> Parquet MinIO") as bronze:
        bronze_task = spark_job(
            "run_xls_to_bronze",
            "xls_to_bronze.py",
            [
                "/opt/spark-data/raw",
                "",
                "{{ (dag_run.conf.get('years') or []) | map('string') | join(',') }}",
            ],
        )

    with TaskGroup(group_id="silver", tooltip="Bronze -> PostgreSQL Silver") as silver:
        silver_task = spark_job(
            "run_bronze_to_silver",
            "bronze_to_silver.py",
        )

    with TaskGroup(group_id="gold", tooltip="Daily / monthly aggregations") as gold:
        gold_task = spark_job(
            "run_silver_to_gold",
            "silver_to_gold.py",
        )

    with TaskGroup(group_id="quality", tooltip="Post-ETL quality checks") as quality:
        quality_task = PythonOperator(
            task_id="run_post_etl_quality",
            python_callable=run_post_etl_quality,
            execution_timeout=timedelta(minutes=30),
        )

    with TaskGroup(group_id="ml", tooltip="Model training (if enough data)") as ml:
        branch_ml = BranchPythonOperator(
            task_id="check_ml_readiness",
            python_callable=partial(
                check_ml_readiness,
                task_train="ml.run_gold_to_model",
                task_skip="ml.skip_ml_training",
            ),
        )
        train_model = spark_job(
            "run_gold_to_model",
            "gold_to_model.py",
            ml=True,
            execution_timeout=timedelta(hours=4),
        )
        skip_ml = PythonOperator(
            task_id="skip_ml_training",
            python_callable=skip_ml_training,
        )
        branch_ml >> [train_model, skip_ml]

    with TaskGroup(
        group_id="reporting",
        tooltip="Professional EDA charts (local + MinIO rapport-eda)",
    ) as reporting:
        eda_data = PythonOperator(
            task_id="generate_eda_data_report",
            python_callable=generate_eda_data_report,
            execution_timeout=timedelta(minutes=20),
            retries=1,
        )
        eda_ml = PythonOperator(
            task_id="generate_eda_ml_report",
            python_callable=generate_eda_ml_report,
            execution_timeout=timedelta(minutes=10),
            retries=1,
        )
        eda_ml_pending = PythonOperator(
            task_id="mark_eda_ml_pending",
            python_callable=mark_eda_ml_pending,
        )

    finalize = PythonOperator(
        task_id="finalize_pipeline_run",
        python_callable=finalize_pipeline_run,
        trigger_rule="none_failed_min_one_success",
    )

    start >> prereq >> bronze >> silver >> gold >> quality
    quality >> eda_data >> branch_ml
    train_model >> eda_ml >> finalize >> end
    skip_ml >> eda_ml_pending >> finalize
