# =======================================================================
# webapp/app.py — Flask web app: trigger edf_pipeline_complet, pick years,
# watch progress and per-task logs.
# =======================================================================

from __future__ import annotations

from flask import Flask, jsonify, render_template, request

import airflow_client
import config
import events
import health
import storage_stats
import topology
from data_years import list_available_years

app = Flask(__name__)


@app.get("/")
def index():
    years = list_available_years()
    return render_template("index.html", years=years, dag_id=config.EDF_DAG_ID)


@app.post("/api/trigger")
def trigger():
    payload = request.get_json(silent=True) or {}
    years = payload.get("years") or []
    try:
        years = sorted({int(y) for y in years})
    except (TypeError, ValueError):
        return jsonify({"error": "years must be a list of integers"}), 400

    try:
        dag_run_id = airflow_client.trigger_dag_run(years)
    except airflow_client.AirflowApiError as exc:
        return jsonify({"error": str(exc)}), 502

    return jsonify({"dag_run_id": dag_run_id})


@app.get("/api/runs/<path:dag_run_id>")
def run_progress(dag_run_id: str):
    try:
        return jsonify(airflow_client.get_run_progress(dag_run_id))
    except airflow_client.AirflowApiError as exc:
        return jsonify({"error": str(exc)}), 502


@app.get("/api/runs/<path:dag_run_id>/tasks/<path:task_id>/logs")
def task_logs(dag_run_id: str, task_id: str):
    try_number = request.args.get("try_number", default=1, type=int)
    try:
        log_text = airflow_client.get_task_log(dag_run_id, task_id, try_number)
    except airflow_client.AirflowApiError as exc:
        return jsonify({"error": str(exc)}), 502
    return jsonify({"task_id": task_id, "try_number": try_number, "log": log_text})


@app.post("/api/runs/<path:dag_run_id>/stop")
def stop_run(dag_run_id: str):
    try:
        return jsonify(airflow_client.stop_dag_run(dag_run_id))
    except airflow_client.AirflowApiError as exc:
        return jsonify({"error": str(exc)}), 502


@app.post("/api/stop-all")
def stop_all():
    try:
        return jsonify(airflow_client.stop_all_dag_runs())
    except airflow_client.AirflowApiError as exc:
        return jsonify({"error": str(exc)}), 502


@app.post("/api/history/clear")
def clear_history():
    try:
        return jsonify(airflow_client.clear_dag_run_history())
    except airflow_client.AirflowApiError as exc:
        return jsonify({"error": str(exc)}), 502


@app.get("/api/topology")
def topology_view():
    dag_run_id = request.args.get("run_id")
    try:
        if not dag_run_id:
            latest = airflow_client.get_latest_dag_run()
            dag_run_id = latest["dag_run_id"] if latest else None
        task_instances = (
            airflow_client.list_task_instances(dag_run_id) if dag_run_id else []
        )
    except airflow_client.AirflowApiError as exc:
        return jsonify({"error": str(exc)}), 502

    return jsonify({"dag_run_id": dag_run_id, "stages": topology.build_topology(task_instances)})


@app.get("/api/infra")
def infra_view():
    return jsonify(health.check_all())


@app.get("/api/storage")
def storage_view():
    return jsonify(storage_stats.get_storage_summary())


@app.get("/api/events")
def events_view():
    severity = request.args.get("severity")
    levels = {s.strip() for s in severity.split(",") if s.strip()} if severity else None
    limit = request.args.get("limit", default=100, type=int)
    return jsonify({"events": events.get_events(limit=limit, levels=levels)})


@app.get("/api/config")
def config_view():
    return jsonify(
        {
            "dag_id": config.EDF_DAG_ID,
            "links": {
                "airflow": config.AIRFLOW_UI_URL,
                "spark_master": config.SPARK_MASTER_UI_URL,
                "minio_console": config.MINIO_CONSOLE_URL,
                "grafana": config.GRAFANA_BASE_URL,
            },
            "airflow_base_url": config.AIRFLOW_UI_URL,
            "postgres": {
                "host": config.PG_HOST,
                "port": config.PG_PORT,
                "database": config.PG_DATABASE,
            },
            "minio_endpoint": config.MINIO_ENDPOINT,
            "data_dir": str(config.DATA_DIR),
        }
    )


if __name__ == "__main__":
    app.run(
        host=config.FLASK_HOST,
        port=config.FLASK_PORT,
        debug=config.FLASK_DEBUG,
        threaded=True,
    )
