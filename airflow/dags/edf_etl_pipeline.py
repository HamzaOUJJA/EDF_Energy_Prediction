# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# airflow/dags/edf_etl_pipeline.py — ETL pipeline DAG (Makefile = prod).
# =======================================================================

from __future__ import annotations

from datetime import datetime
from functools import partial
"""
Pipeline ETL daily RTE — Kafka -> Bronze -> Silver -> Gold (streaming).

Complement of the batch pipeline ``edf_pipeline_complet`` (massive XLS load).
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from edf_pipeline.config import DEFAULT_ARGS, EDF_ENVIRONMENT, PROD_ETL_SCHEDULE, get_etl_schedule
from edf_pipeline.metadata import record_streaming_run
from edf_pipeline.spark_operators import spark_job
from edf_pipeline.tasks import (
    check_kafka_available,
    consume_kafka_to_bronze,
    publish_to_kafka,
    run_daily_quality_checks,
)

STREAMING_DEFAULT_ARGS = {
    **DEFAULT_ARGS,
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="edf_etl_pipeline",
    description="ETL daily RTE — streaming Kafka -> Bronze -> Silver -> Gold",
    default_args=STREAMING_DEFAULT_ARGS,
    schedule_interval=get_etl_schedule(),
    catchup=False,
    max_active_runs=1,
    tags=["edf", "streaming", "production", "daily"],
    doc_md=f"""
## Streaming pipeline

- **Environment** : `{EDF_ENVIRONMENT}`
- **prod** : cron ``{PROD_ETL_SCHEDULE}`` — Kafka delta J/J-1 ingestion
- **dev** : manual trigger (`airflow dags trigger edf_etl_pipeline`)
- If Kafka is unavailable : graceful finish (the batch ``edf_pipeline_complet`` remains the safety net)
- For a complete historical load : ``make trigger-pipeline`` (``edf_pipeline_complet``)
""",
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    t_check_kafka = BranchPythonOperator(
        task_id="check_kafka_available",
        python_callable=check_kafka_available,
    )
    t_kafka_unavailable = EmptyOperator(task_id="kafka_unavailable")

    t_publish = PythonOperator(
        task_id="publish_to_kafka",
        python_callable=publish_to_kafka,
    )
    t_consume = PythonOperator(
        task_id="consume_kafka_to_bronze",
        python_callable=consume_kafka_to_bronze,
    )
    t_silver = spark_job("transform_bronze_to_silver", "bronze_to_silver.py")
    t_gold = spark_job("compute_gold_aggregates", "silver_to_gold.py")
    t_quality = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_daily_quality_checks,
    )
    t_meta = PythonOperator(
        task_id="update_etl_metadata",
        python_callable=record_streaming_run,
        trigger_rule="all_done",
    )

    start >> t_check_kafka >> [t_publish, t_kafka_unavailable]
    t_publish >> t_consume >> t_silver >> t_gold >> t_quality >> t_meta >> end
    t_kafka_unavailable >> end
