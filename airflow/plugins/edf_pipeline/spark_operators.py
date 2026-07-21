# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# airflow/plugins/edf_pipeline/spark_operators.py — Spark operators (Makefile = prod).
# =======================================================================

from __future__ import annotations

import json
import os
import socket
import time
import urllib.error
import urllib.request
from datetime import timedelta
from typing import Any

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

from edf_pipeline.config import AIRFLOW_SPARK_POOL

SPARK_CONN_ID = "spark_rest"
SPARK_MAIN_CLASS = "org.apache.spark.deploy.SparkSubmit"
SPARK_VERSION = os.getenv("SPARK_VERSION", "3.5.1")
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_PYTHONPATH = os.getenv("SPARK_PYTHONPATH", "/opt/spark-project")
JOBS_DIR = os.getenv("SPARK_JOBS_DIR", "/opt/spark-jobs")
POLL_INTERVAL = int(os.getenv("SPARK_REST_POLL_SECONDS", "10"))
# ETL batch (bronze/silver/gold) — 4 h par défaut (bronze→silver peut dépasser 2 h).
POLL_TIMEOUT = int(os.getenv("SPARK_REST_TIMEOUT_SECONDS", "14400"))
POLL_TIMEOUT_ML = int(os.getenv("SPARK_REST_ML_TIMEOUT_SECONDS", "14400"))
NETWORK_RETRIES = int(os.getenv("SPARK_REST_NETWORK_RETRIES", "8"))
REQUEST_TIMEOUT = int(os.getenv("SPARK_REST_REQUEST_TIMEOUT_SECONDS", "60"))
# Marge Airflow > poll Spark (soumission REST, retries DNS, dernier sleep).
EXECUTION_TIMEOUT_BUFFER = int(os.getenv("SPARK_REST_EXECUTION_BUFFER_SECONDS", "900"))
# ML en mode cluster : driver 1,5G + executor 2G sur workers 3G → blocage SUBMITTED.
ML_DEPLOY_MODE = os.getenv("SPARK_ML_DEPLOY_MODE", "client")
# ETL : client = driver sur master (10G, aligné Makefile) ; cluster = driver sur worker 3G.
ETL_DEPLOY_MODE = os.getenv("SPARK_ETL_DEPLOY_MODE", "client")
SUBMITTED_TIMEOUT = int(os.getenv("SPARK_REST_SUBMITTED_TIMEOUT_SECONDS", "1800"))
SUBMITTED_WARN_INTERVAL = int(os.getenv("SPARK_REST_SUBMITTED_WARN_SECONDS", "300"))

SPARK_EXTRA_JARS = os.getenv(
    "SPARK_EXTRA_JARS",
    "/opt/spark/extra-jars/hadoop-aws-3.3.4.jar,"
    "/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar,"
    "/opt/spark/extra-jars/postgresql-42.7.4.jar",
)

# Fallback Ivy (Makefile pipeline-spark) — avoided in prod REST if SPARK_EXTRA_JARS is present.
SPARK_PACKAGES = os.getenv(
    "SPARK_PACKAGES",
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.postgresql:postgresql:42.7.4",
)


def _is_transient_network_error(exc: BaseException) -> bool:
    """Network/DNS Docker Desktop errors — retryable during polling."""
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, urllib.error.URLError):
        reason = exc.reason
        if isinstance(reason, (TimeoutError, socket.timeout)):
            return True
        if isinstance(reason, OSError):
            # EAI_AGAIN (-3), ECONNREFUSED, ETIMEDOUT, etc.
            return True
    if isinstance(exc, OSError):
        return True
    return False


def _spark_rest_base(conn_id: str) -> str:
    """URL REST of the master — env ``SPARK_REST_URL`` or Airflow ``spark_rest`` connection."""
    env_url = os.getenv("SPARK_REST_URL")
    if env_url:
        return env_url.rstrip("/")
    conn = BaseHook.get_connection(conn_id)
    host = conn.host or "spark-master"
    port = conn.port or 6066
    scheme = conn.schema if conn.schema in ("http", "https") else "http"
    return f"{scheme}://{host}:{port}"


def _file_uri(path: str) -> str:
    """Convert a path to a file URI."""
    return path if path.startswith("file://") else f"file://{path}"


def _jars_spark_property() -> str:
    """List ``file://`` for ``spark.jars`` (identical paths on all workers)."""
    jars = [j.strip() for j in SPARK_EXTRA_JARS.split(",") if j.strip()]
    return ",".join(_file_uri(j) for j in jars)


def _minio_conf() -> dict[str, str]:
    """MinIO configuration for Spark."""
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "edfadmin"))
    secret = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "edfpassword123"))
    return {
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.access.key": access,
        "spark.hadoop.fs.s3a.secret.key": secret,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    }


def _memory_conf(*, cluster: bool = False) -> dict[str, str]:
    conf: dict[str, str] = {}
    mapping = {
        "SPARK_EXECUTOR_MEMORY": "spark.executor.memory",
        "SPARK_EXECUTOR_MEMORY_OVERHEAD": "spark.executor.memoryOverhead",
        "SPARK_SQL_SHUFFLE_PARTITIONS": "spark.sql.shuffle.partitions",
        "SPARK_CORES_MAX": "spark.cores.max",
    }
    if cluster:
        mapping["SPARK_CLUSTER_DRIVER_MEMORY"] = "spark.driver.memory"
    else:
        mapping["SPARK_DRIVER_MEMORY"] = "spark.driver.memory"
    for env_key, spark_key in mapping.items():
        value = os.getenv(env_key)
        if value:
            conf[spark_key] = value
    if cluster:
        conf.setdefault("spark.driver.memory", "1536m")
        conf.setdefault("spark.driver.cores", "1")
    else:
        conf.setdefault("spark.driver.memory", "2g")
        conf.setdefault("spark.driver.cores", "2")
    conf.setdefault("spark.executor.memory", "2g")
    conf.setdefault("spark.executor.memoryOverhead", "768m")
    conf.setdefault("spark.cores.max", "4")
    conf.setdefault("spark.sql.shuffle.partitions", "4")
    return conf


def _ml_memory_conf(*, cluster: bool = True) -> dict[str, str]:
    """ML memory configuration for Spark."""
    conf = _memory_conf(cluster=cluster)
    if cluster:
        ml_driver = os.getenv("SPARK_CLUSTER_ML_DRIVER_MEMORY", "2g")
        conf["spark.driver.memory"] = ml_driver
    else:
        ml_driver = os.getenv("SPARK_ML_DRIVER_MEMORY", "3g")
        conf["spark.driver.memory"] = ml_driver
    ml_executor = os.getenv("SPARK_ML_EXECUTOR_MEMORY")
    if ml_executor:
        conf["spark.executor.memory"] = ml_executor
    return conf


def _base_spark_conf(*, ml: bool = False, deploy_mode: str = "cluster") -> dict[str, str]:
    """Spark REST configuration — cluster (driver worker) or client (driver master, recommended ML)."""
    driver_on_worker = deploy_mode == "cluster"
    memory = (
        _ml_memory_conf(cluster=driver_on_worker)
        if ml
        else _memory_conf(cluster=driver_on_worker)
    )
    conf = {
        "spark.driverEnv.PYTHONPATH": SPARK_PYTHONPATH,
        "spark.executorEnv.PYTHONPATH": SPARK_PYTHONPATH,
        **_minio_conf(),
        **memory,
    }
    if SPARK_EXTRA_JARS.strip():
        conf["spark.jars"] = _jars_spark_property()
    if ml:
        for key in ("POSTGRES_CONN",):
            value = os.getenv(key)
            if value:
                conf[f"spark.driverEnv.{key}"] = value
    return conf


def _driver_env_from_conf(conf: dict[str, str]) -> dict[str, str]:
    """Extract the driver environment from the configuration."""
    env: dict[str, str] = {"PYTHONPATH": SPARK_PYTHONPATH}
    for key, value in conf.items():
        if key.startswith("spark.driverEnv."):
            env[key.removeprefix("spark.driverEnv.")] = value
    for key in (
        "MINIO_ENDPOINT",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "POSTGRES_CONN",
        "BRONZE_PATH",
        "SILVER_PATH",
        "GOLD_PATH",
    ):
        value = os.getenv(key)
        if value:
            env[key] = value
    return env


class SparkRestSubmitOperator(BaseOperator):
    """Submit a PySpark job via ``POST /v1/submissions/create`` and wait for the end."""

    template_fields = ("application_args",)

    def __init__(
        self,
        application: str,
        application_args: list[str] | None = None,
        conf: dict[str, str] | None = None,
        packages: str | None = None,
        jars: str | None = None,
        conn_id: str = SPARK_CONN_ID,
        name: str | None = None,
        deploy_mode: str = "cluster",
        poll_timeout: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.application = application
        self.application_args = application_args or []
        self.conf = conf or {}
        self.packages = packages
        self.jars = jars
        self.conn_id = conn_id
        self.name = name or self.task_id
        self.deploy_mode = deploy_mode
        self.poll_timeout = poll_timeout if poll_timeout is not None else POLL_TIMEOUT
        self._submission_id: str | None = None
        self._rest_base: str | None = None

    def on_kill(self) -> None:
        """Kill the Spark driver when Airflow cancels or times out the task."""
        if not self._submission_id or not self._rest_base:
            return
        try:
            self._request(
                self._rest_base,
                "POST",
                f"/v1/submissions/kill/{self._submission_id}",
                max_retries=2,
            )
            self.log.info("Spark submission %s killed", self._submission_id)
        except Exception as exc:
            self.log.warning("Could not kill Spark submission %s: %s", self._submission_id, exc)

    def _kill_submission(self, submission_id: str, rest_base: str) -> None:
        try:
            self._request(
                rest_base,
                "POST",
                f"/v1/submissions/kill/{submission_id}",
                max_retries=2,
            )
            self.log.info("Spark submission %s killed", submission_id)
        except Exception as exc:
            self.log.warning("Could not kill Spark submission %s: %s", submission_id, exc)

    def _request(
        self,
        base_url: str,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        max_retries: int | None = None,
    ) -> dict[str, Any]:
        """Make a request to the Spark REST API."""
        url = f"{base_url.rstrip('/')}{path}"
        data = json.dumps(payload).encode() if payload is not None else None
        attempts = max_retries if max_retries is not None else NETWORK_RETRIES
        last_exc: BaseException | None = None

        for attempt in range(1, attempts + 1):
            req = urllib.request.Request(
                url,
                data=data,
                headers={"Content-Type": "application/json"},
                method=method,
            )
            try:
                with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                    body = resp.read().decode()
                    return json.loads(body) if body else {}
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode()
                raise RuntimeError(
                    f"Spark REST {method} {path} failed ({exc.code}): {detail}"
                ) from exc
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                last_exc = exc
                if attempt >= attempts or not _is_transient_network_error(exc):
                    raise
                wait_s = min(30, attempt * 3)
                self.log.warning(
                    "Spark REST %s %s — erreur réseau transitoire (%s), "
                    "tentative %d/%d, nouvel essai dans %ds",
                    method,
                    path,
                    exc,
                    attempt,
                    attempts,
                    wait_s,
                )
                time.sleep(wait_s)

        assert last_exc is not None
        raise last_exc

    def execute(self, context: dict[str, Any]) -> str:
        """Execute the Spark job."""
        rest_base = _spark_rest_base(self.conn_id)
        app_path = self.application.removeprefix("file://")
        app_resource = _file_uri(app_path)
        # REST cluster + PySpark : SparkSubmit + script en tête de appArgs (doc Spark 3.5+).
        app_args = [app_path, *self.application_args]

        spark_props = {
            "spark.master": SPARK_MASTER,
            "spark.submit.deployMode": self.deploy_mode,
            "spark.app.name": self.name,
            **self.conf,
        }
        if self.jars:
            spark_props["spark.jars"] = self.jars
        elif self.packages and not spark_props.get("spark.jars"):
            spark_props["spark.jars.packages"] = self.packages
            spark_props.setdefault("spark.jars.ivy", "/home/spark/.ivy2")

        payload = {
            "action": "CreateSubmissionRequest",
            "appResource": app_resource,
            "clientSparkVersion": SPARK_VERSION,
            "mainClass": SPARK_MAIN_CLASS,
            "appArgs": app_args,
            "sparkProperties": spark_props,
            "environmentVariables": _driver_env_from_conf(self.conf),
        }

        self.log.info(
            "Spark REST submit (conn_id=%s, deployMode=%s) → %s | app=%s",
            self.conn_id,
            self.deploy_mode,
            rest_base,
            self.application,
        )
        created = self._request(rest_base, "POST", "/v1/submissions/create", payload)
        submission_id = created["submissionId"]
        self._submission_id = submission_id
        self._rest_base = rest_base
        self.log.info("Submission ID: %s", submission_id)

        submitted_since: float | None = None
        submitted_warn_at: float | None = None
        deadline = time.monotonic() + self.poll_timeout
        while time.monotonic() < deadline:
            try:
                status = self._request(
                    rest_base,
                    "GET",
                    f"/v1/submissions/status/{submission_id}",
                )
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                if not _is_transient_network_error(exc):
                    raise
                self.log.warning(
                    "Impossible de lire le statut Spark (%s) — le job peut "
                    "encore tourner ; nouvel essai dans %ss",
                    exc,
                    POLL_INTERVAL,
                )
                time.sleep(POLL_INTERVAL)
                continue

            state = status.get("driverState", "UNKNOWN")
            self.log.info("Driver state: %s", state)
            if state == "SUBMITTED":
                now = time.monotonic()
                if submitted_since is None:
                    submitted_since = now
                stuck_s = now - submitted_since
                if stuck_s > 1800 and (
                    submitted_warn_at is None or now - submitted_warn_at >= SUBMITTED_WARN_INTERVAL
                ):
                    submitted_warn_at = now
                    self.log.warning(
                        "Driver %s bloqué en SUBMITTED depuis %d min — "
                        "workers saturés ou drivers zombies (UI Spark :8082). "
                        "Essayer : make restart-spark",
                        submission_id,
                        int(stuck_s // 60),
                    )
                if stuck_s > SUBMITTED_TIMEOUT:
                    self._kill_submission(submission_id, rest_base)
                    raise RuntimeError(
                        f"Spark driver {submission_id} stuck in SUBMITTED for "
                        f"{int(stuck_s // 60)} min (no worker resources). "
                        "Run `make restart-spark` to clear zombie drivers, then retry."
                    )
            else:
                submitted_since = None
                submitted_warn_at = None
            if state == "FINISHED":
                return submission_id
            if state in ("FAILED", "ERROR"):
                raise RuntimeError(f"Spark job failed: {json.dumps(status)}")
            if state in ("KILLED",):
                raise RuntimeError(f"Spark job killed: {json.dumps(status)}")
            time.sleep(POLL_INTERVAL)

        raise TimeoutError(
            f"Spark job {submission_id} did not finish within {self.poll_timeout}s"
        )


def _minimum_execution_timeout(poll_timeout: int) -> timedelta:
    """Airflow must outlive the Spark poll loop (submit + retries + final sleep)."""
    return timedelta(seconds=poll_timeout + EXECUTION_TIMEOUT_BUFFER)


def _resolve_execution_timeout(
    poll_timeout: int,
    execution_timeout: timedelta | None,
    *,
    ml: bool,
) -> timedelta | None:
    """Align Airflow ``execution_timeout`` with ``poll_timeout`` to avoid race kills."""
    minimum = _minimum_execution_timeout(poll_timeout)
    if execution_timeout is None:
        return minimum if ml else None
    return max(execution_timeout, minimum)


def spark_job(
    task_id: str,
    script: str,
    application_args: list[str] | None = None,
    *,
    execution_timeout: timedelta | None = None,
    poll_timeout: int | None = None,
    ml: bool = False,
    conn_id: str = SPARK_CONN_ID,
    pool: str | None = AIRFLOW_SPARK_POOL,
) -> SparkRestSubmitOperator:
    """Factory — job PySpark on the cluster (Airflow ``spark_rest`` connection)."""
    deploy_mode = ML_DEPLOY_MODE if ml else ETL_DEPLOY_MODE
    conf = _base_spark_conf(ml=ml, deploy_mode=deploy_mode)
    jars = conf.pop("spark.jars", None)
    effective_poll = (
        poll_timeout if poll_timeout is not None else (POLL_TIMEOUT_ML if ml else POLL_TIMEOUT)
    )
    resolved_timeout = _resolve_execution_timeout(
        effective_poll, execution_timeout, ml=ml
    )
    operator_kwargs: dict[str, Any] = {
        "task_id": task_id,
        "application": f"{JOBS_DIR}/{script}",
        "application_args": application_args or [],
        "jars": jars,
        "packages": SPARK_PACKAGES if not jars else None,
        "conf": conf,
        "conn_id": conn_id,
        "deploy_mode": deploy_mode,
        "poll_timeout": effective_poll,
    }
    if resolved_timeout is not None:
        operator_kwargs["execution_timeout"] = resolved_timeout
    if pool:
        operator_kwargs["pool"] = pool
    return SparkRestSubmitOperator(**operator_kwargs)
