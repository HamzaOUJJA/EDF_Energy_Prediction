# =======================================================================
# webapp/health.py — Live health pings for each infra service. No Docker
# dependency: every check talks to the service's own exposed port, the same
# way a real client would.
# =======================================================================

from __future__ import annotations

import socket
import time
from typing import Any

import requests

import config

_TIMEOUT = 2.0


def _timed(fn) -> dict[str, Any]:
    start = time.monotonic()
    try:
        fn()
        return {"status": "up", "latency_ms": round((time.monotonic() - start) * 1000, 1)}
    except Exception as exc:
        return {
            "status": "down",
            "latency_ms": round((time.monotonic() - start) * 1000, 1),
            "error": str(exc),
        }


def _tcp_check(host: str, port: int) -> None:
    with socket.create_connection((host, port), timeout=_TIMEOUT):
        pass


def _http_check(url: str) -> None:
    resp = requests.get(url, timeout=_TIMEOUT)
    resp.raise_for_status()


def check_kafka() -> dict[str, Any]:
    return _timed(lambda: _tcp_check(config.KAFKA_BOOTSTRAP_HOST, config.KAFKA_BOOTSTRAP_PORT))


def check_redis() -> dict[str, Any]:
    return _timed(lambda: _tcp_check(config.REDIS_HOST, config.REDIS_PORT))


def check_minio() -> dict[str, Any]:
    return _timed(lambda: _http_check(f"{config.MINIO_ENDPOINT}/minio/health/live"))


def check_postgres() -> dict[str, Any]:
    import db

    def _ping() -> None:
        if db.query_one("SELECT 1 AS ok") is None:
            raise RuntimeError("no response")

    return _timed(_ping)


def check_airflow() -> dict[str, Any]:
    return _timed(lambda: _http_check(f"{config.AIRFLOW_BASE_URL}/health"))


def check_spark() -> dict[str, Any]:
    result = _timed(lambda: _http_check(f"{config.SPARK_MASTER_INTERNAL_URL}/json/"))
    if result["status"] == "up":
        try:
            resp = requests.get(f"{config.SPARK_MASTER_INTERNAL_URL}/json/", timeout=_TIMEOUT)
            data = resp.json()
            result["workers_alive"] = data.get("aliveworkers")
            result["workers_total"] = len(data.get("workers", []))
            result["status_text"] = data.get("status")
        except Exception:
            pass
    return result


def check_all() -> dict[str, dict[str, Any]]:
    return {
        "airflow": check_airflow(),
        "postgres": check_postgres(),
        "minio": check_minio(),
        "spark": check_spark(),
        "kafka": check_kafka(),
        "redis": check_redis(),
    }
