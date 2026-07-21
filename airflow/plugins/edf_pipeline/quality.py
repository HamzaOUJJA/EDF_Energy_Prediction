# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# airflow/plugins/edf_pipeline/quality.py — shared quality checks (Makefile = prod).
# =======================================================================

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import psycopg2

from edf_pipeline.db import get_pg_conn

logger = logging.getLogger(__name__)

DATA_DIR: str = os.getenv("DATA_DIR", "/opt/airflow/data/raw")


def _env_bool(key: str, default: bool = False) -> bool:
    """Convert an environment variable to a boolean."""
    return os.getenv(key, str(default)).lower() in ("1", "true", "yes")


def allow_year_gaps() -> bool:
    """Allow year gaps (dev local without complete RTE export)."""
    explicit = os.getenv("QUALITY_ALLOW_YEAR_GAPS")
    if explicit is not None:
        return _env_bool("QUALITY_ALLOW_YEAR_GAPS", False)
    return os.getenv("EDF_ENVIRONMENT", "dev").lower() != "prod"


def find_year_gaps(years: set[int]) -> list[int]:
    """Return the missing years between min and max (e.g. {2024, 2026} -> [2025])."""
    if not years:
        return []
    min_y, max_y = min(years), max(years)
    return [y for y in range(min_y, max_y + 1) if y not in years]


POST_ETL_CHECKS: list[dict[str, Any]] = [
    {
        "name": "silver_row_count",
        "severity": "critical",
        "table_name": "dw.fact_consumption_silver",
        "sql": "SELECT COUNT(*) FROM dw.fact_consumption_silver",
        "operator": ">=",
        "threshold": 1000,
    },
    {
        "name": "null_consumption_rate",
        "severity": "critical",
        "table_name": "dw.fact_consumption_silver",
        "sql": """
            SELECT COUNT(*) FILTER (WHERE consumption_mw IS NULL)::float
                   / NULLIF(COUNT(*), 0)
            FROM dw.fact_consumption_silver
        """,
        "operator": "<=",
        "threshold": 0.05,
    },
    {
        "name": "avg_quality_score",
        "severity": "warning",
        "table_name": "dw.fact_consumption_silver",
        "sql": "SELECT COALESCE(AVG(quality_score), 0) FROM dw.fact_consumption_silver",
        "operator": ">=",
        "threshold": 0.75,
    },
    {
        "name": "gold_daily_count",
        "severity": "critical",
        "table_name": "dw.agg_daily",
        "sql": "SELECT COUNT(*) FROM dw.agg_daily",
        "operator": ">=",
        "threshold": 30,
    },
    {
        "name": "year_coverage_gaps",
        "severity": "critical",
        "table_name": "dw.fact_consumption_silver",
        "description": "No year gaps between MIN(year) and MAX(year) in Silver",
        "sql": """
            SELECT COALESCE(
                (MAX(year) - MIN(year) + 1) - COUNT(DISTINCT year),
                0
            )::float
            FROM dw.fact_consumption_silver
            WHERE year IS NOT NULL
        """,
        "operator": "<=",
        "threshold": 0,
    },
]


MONITORING_CHECKS: list[dict[str, Any]] = [
    {
        "name": "silver_volume_total",
        "description": "At least 10 000 Silver lines in the database",
        "severity": "critical",
        "table_name": "dw.fact_consumption_silver",
        "sql": "SELECT COUNT(*) FROM dw.fact_consumption_silver",
        "operator": ">=",
        "threshold": 10_000,
    },
    {
        "name": "agg_daily_calcules",
        "description": "At least 100 daily aggregates calculated",
        "severity": "critical",
        "table_name": "dw.agg_daily",
        "sql": "SELECT COUNT(*) FROM dw.agg_daily",
        "operator": ">=",
        "threshold": 100,
    },
    {
        "name": "agg_monthly_calcules",
        "description": "At least 1 monthly aggregate calculated",
        "severity": "warning",
        "table_name": "dw.agg_monthly",
        "sql": "SELECT COUNT(*) FROM dw.agg_monthly",
        "operator": ">=",
        "threshold": 1,
    },
    {
        "name": "score_qualite_global",
        "description": "Global average quality score >= 0.6",
        "severity": "warning",
        "table_name": "dw.fact_consumption_silver",
        "sql": "SELECT COALESCE(AVG(quality_score), 0) FROM dw.fact_consumption_silver",
        "operator": ">=",
        "threshold": 0.6,
    },
    {
        "name": "taux_null_consommation",
        "description": "Null rate on consumption_mw < 30%",
        "severity": "warning",
        "table_name": "dw.fact_consumption_silver",
        "sql": """
            SELECT COUNT(*) FILTER (WHERE consumption_mw IS NULL)::float
                   / NULLIF(COUNT(*), 0)
            FROM dw.fact_consumption_silver
        """,
        "operator": "<=",
        "threshold": 0.30,
    },
    {
        "name": "plage_consommation_global",
        "description": "Consumption outside range 10 000–120 000 MW < 1%",
        "severity": "warning",
        "table_name": "dw.fact_consumption_silver",
        "sql": """
            SELECT COALESCE(
                COUNT(*) FILTER (
                    WHERE consumption_mw IS NOT NULL
                      AND consumption_mw NOT BETWEEN 10000 AND 120000
                )::float / NULLIF(COUNT(*) FILTER (WHERE consumption_mw IS NOT NULL), 0),
                0
            ) FROM dw.fact_consumption_silver
        """,
        "operator": "<=",
        "threshold": 0.01,
    },
    {
        "name": "annees_distinctes",
        "description": "At least 1 distinct data year",
        "severity": "critical",
        "table_name": "dw.fact_consumption_silver",
        "sql": """
            SELECT COUNT(DISTINCT year)
            FROM dw.fact_consumption_silver
            WHERE year IS NOT NULL
        """,
        "operator": ">=",
        "threshold": 1,
    },
    {
        "name": "coherence_nucleaire",
        "description": "Average nuclear > 10 000 MW (France consistency)",
        "severity": "warning",
        "table_name": "dw.fact_consumption_silver",
        "sql": """
            SELECT COALESCE(AVG(nuclear_mw), 0)
            FROM dw.fact_consumption_silver
            WHERE nuclear_mw IS NOT NULL
        """,
        "operator": ">=",
        "threshold": 10_000,
    },
    {
        "name": "doublons_recents",
        "description": "No duplicates datetime on recently loaded data",
        "severity": "warning",
        "table_name": "dw.fact_consumption_silver",
        "sql": """
            SELECT COUNT(*) FROM (
                SELECT datetime
                FROM dw.fact_consumption_silver
                WHERE processed_at >= NOW() - INTERVAL '12 hours'
                GROUP BY datetime
                HAVING COUNT(*) > 1
            ) dups
        """,
        "operator": "<=",
        "threshold": 0,
    },
    {
        "name": "pipeline_runs_enregistres",
        "description": "At least 1 ETL run recorded",
        "severity": "warning",
        "table_name": "etl.pipeline_runs",
        "sql": "SELECT COUNT(*) FROM etl.pipeline_runs",
        "operator": ">=",
        "threshold": 1,
    },
]


def get_post_etl_checks() -> list[dict[str, Any]]:
    """Return the post-ETL checks (severity adjusted according to the environment)."""
    checks = [dict(check) for check in POST_ETL_CHECKS]
    if allow_year_gaps():
        for check in checks:
            if check["name"] == "year_coverage_gaps":
                check["severity"] = "warning"
    return checks


def get_monitoring_checks() -> list[dict[str, Any]]:
    """Global checks of the ``edf_quality_monitoring`` DAG."""
    return [dict(check) for check in MONITORING_CHECKS]


def get_streaming_daily_checks(run_date: str) -> list[dict[str, Any]]:
    """Quality checks on the day window (streaming DAG)."""
    return [
        {
            "name": "completude_24h",
            "severity": "warning",
            "table_name": "dw.fact_consumption_silver",
            "sql": (
                "SELECT COUNT(*) FROM dw.fact_consumption_silver "
                f"WHERE DATE(datetime)='{run_date}'"
            ),
            "operator": ">=",
            "threshold": 40,
        },
        {
            "name": "null_consumption",
            "severity": "warning",
            "table_name": "dw.fact_consumption_silver",
            "sql": (
                "SELECT COUNT(*) FILTER (WHERE consumption_mw IS NULL)::float "
                "/ NULLIF(COUNT(*),0) FROM dw.fact_consumption_silver "
                f"WHERE DATE(datetime)='{run_date}'"
            ),
            "operator": "<=",
            "threshold": 0.05,
        },
        {
            "name": "avg_quality_score",
            "severity": "warning",
            "table_name": "dw.fact_consumption_silver",
            "sql": (
                "SELECT AVG(quality_score) FROM dw.fact_consumption_silver "
                f"WHERE DATE(datetime)='{run_date}'"
            ),
            "operator": ">=",
            "threshold": 0.75,
        },
    ]


def _check_passed(value: float, operator: str, threshold: float) -> bool:
    if operator == "<=":
        return value <= threshold
    if operator == ">=":
        return value >= threshold
    raise ValueError(f"Unsupported operator: {operator}")


def _silver_year_coverage(cur: Any) -> dict[str, Any]:
    """Années présentes en Silver et trous entre min/max."""
    cur.execute(
        """
        SELECT ARRAY_AGG(DISTINCT year ORDER BY year)
        FROM dw.fact_consumption_silver
        WHERE year IS NOT NULL
        """
    )
    row_years = cur.fetchone()[0] or []
    year_set = {int(y) for y in row_years}
    gaps = find_year_gaps(year_set)
    return {
        "years_present": sorted(year_set),
        "missing_years": gaps,
        "human_detail": (
            f"Année(s) manquante(s) : {gaps} "
            f"(présentes : {sorted(year_set)} — ajoutez un export RTE couvrant la période)"
            if gaps
            else "Couverture annuelle continue"
        ),
    }


def run_checks(
    checks: list[dict[str, Any]],
    *,
    source: str,
    conn: Any | None = None,
    persist: bool = True,
    fail_on_critical: bool = True,
    fail_on_warning: bool | None = None,
    skip_errors: bool = False,
) -> dict[str, Any]:
    """Execute a list of SQL checks and persist in ``etl.data_quality_checks``."""
    if fail_on_warning is None:
        fail_on_warning = _env_bool("QUALITY_FAIL_ON_WARNING", False)

    own_conn = conn is None
    if own_conn:
        conn = psycopg2.connect(get_pg_conn())
    cur = conn.cursor()

    results: list[dict[str, Any]] = []
    failed_critical: list[str] = []
    failed_warning: list[str] = []

    for check in checks:
        name = check["name"]
        op = check["operator"]
        threshold = float(check["threshold"])
        severity = check.get("severity", "warning")

        try:
            cur.execute(check["sql"])
            row = cur.fetchone()
            value = float(row[0]) if row and row[0] is not None else 0.0
            passed = _check_passed(value, op, threshold)

            result = {
                "check_name": name,
                "check": name,
                "description": check.get("description", ""),
                "severity": severity,
                "actual_value": round(value, 4),
                "value": value,
                "threshold": threshold,
                "operator": op,
                "passed": passed,
            }
            if name == "year_coverage_gaps":
                result.update(_silver_year_coverage(cur))

            results.append(result)

            status = "PASS" if passed else ("CRIT" if severity == "critical" else "WARN")
            logger.info(
                "   [%s] %s: %.4f %s %s",
                status,
                name,
                value,
                op,
                threshold,
            )

            if not passed:
                if severity == "critical":
                    failed_critical.append(name)
                else:
                    failed_warning.append(name)

            if persist:
                detail = json.dumps({**result, "source": source})
                cur.execute(
                    """
                    INSERT INTO etl.data_quality_checks
                        (check_name, table_name, expected_value, actual_value, passed, detail)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        name,
                        check.get("table_name", "dw"),
                        threshold,
                        value,
                        passed,
                        detail,
                    ),
                )
        except Exception as exc:
            if own_conn:
                conn.rollback()
            logger.error("Check error %s: %s", name, exc)
            result = {
                "check_name": name,
                "check": name,
                "description": check.get("description", ""),
                "severity": severity,
                "passed": False,
                "error": str(exc),
            }
            results.append(result)
            if severity == "critical":
                failed_critical.append(name)
            else:
                failed_warning.append(name)
            if skip_errors:
                continue
            raise

    if own_conn:
        conn.commit()
        cur.close()
        conn.close()

    summary = {
        "checks": results,
        "failed_critical": failed_critical,
        "failed_warning": failed_warning,
        "failed": failed_critical + failed_warning,
        "all_passed": not failed_critical and not failed_warning,
    }

    should_fail = (fail_on_critical and failed_critical) or (
        fail_on_warning and failed_warning
    )
    if should_fail:
        failed = failed_critical + (
            failed_warning if fail_on_warning else []
        )
        raise ValueError(
            "Quality checks failed "
            f"({source}) : {', '.join(failed)}"
        )

    if failed_critical:
        logger.warning(
            "Critical checks failed (%s) : %s",
            source,
            failed_critical,
        )
    if failed_warning:
        logger.warning(
            "Warning checks failed (%s) : %s",
            source,
            failed_warning,
        )

    return summary


def run_post_etl_quality_checks(
    *,
    source: str = "post_etl",
    fail_on_critical: bool = False,
    fail_on_warning: bool | None = None,
) -> dict[str, Any]:
    """Post-ETL checks (Silver/Gold)."""
    return run_checks(
        get_post_etl_checks(),
        source=source,
        fail_on_critical=fail_on_critical,
        fail_on_warning=fail_on_warning,
    )


def run_post_etl_quality_or_raise(source: str = "make pipeline") -> dict[str, Any]:
    """Makefile entry point — block the pipeline dev if critical failure."""
    return run_post_etl_quality_checks(
        source=source,
        fail_on_critical=True,
        fail_on_warning=_env_bool("QUALITY_FAIL_ON_WARNING", False),
    )


def run_monitoring_quality_checks(**context) -> dict[str, Any]:
    """DAG ``edf_quality_monitoring`` entry point — global checks."""
    from edf_pipeline.config import DAG_ID_QUALITY

    summary = run_checks(
        get_monitoring_checks(),
        source=f"DAG {DAG_ID_QUALITY}",
        fail_on_critical=True,
        fail_on_warning=False,
        skip_errors=True,
    )
    results = summary["checks"]
    total = len(results)
    passed_cnt = sum(1 for r in results if r.get("passed", False))
    score_pct = passed_cnt / total * 100 if total else 0

    logger.info("=" * 55)
    logger.info("GLOBAL QUALITY: %d/%d (%.0f%%)", passed_cnt, total, score_pct)
    if summary["failed_critical"]:
        logger.error("CRITICAL (%d): %s", len(summary["failed_critical"]), summary["failed_critical"])
    if summary["failed_warning"]:
        logger.warning("WARNING (%d): %s", len(summary["failed_warning"]), summary["failed_warning"])
    logger.info("=" * 55)

    ti = context.get("ti")
    if ti is not None:
        ti.xcom_push(key="quality_results", value=results)
        ti.xcom_push(key="failed_critical", value=summary["failed_critical"])
        ti.xcom_push(key="score_pct", value=score_pct)

    return {
        "total": total,
        "passed": passed_cnt,
        "score_pct": score_pct,
        "failed_critical": summary["failed_critical"],
        "failed_warning": summary["failed_warning"],
    }


def generate_quality_report(**context) -> str:
    """Aggregated report + trace in ``etl.pipeline_runs``."""
    ti = context["ti"]
    results = ti.xcom_pull(key="quality_results") or []
    critical = ti.xcom_pull(key="failed_critical") or []
    score_pct = ti.xcom_pull(key="score_pct") or 0

    passed = sum(1 for r in results if r.get("passed", False))
    total = len(results)

    lines = [
        f"=== EDF RTE Quality Report — {context['ds']} ===",
        f"Score: {score_pct:.0f}% ({passed}/{total} checks OK)",
        "",
        f"{'Check':<35} {'Valeur':>12}  {'Seuil':>10}  Statut",
        "-" * 70,
    ]
    for result in results:
        if result.get("passed"):
            status = "PASS"
        elif result.get("severity") == "critical":
            status = "CRITICAL"
        else:
            status = "WARNING"
        lines.append(
            f"{result.get('check_name', '?'):<35} "
            f"{str(result.get('actual_value', '?')):>12}  "
            f"{result.get('operator', '?')} {str(result.get('threshold', '?')):<8}  {status}"
        )

    report = "\n".join(lines)
    logger.info("\n%s", report)

    conn = psycopg2.connect(get_pg_conn())
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO etl.pipeline_runs (dag_id, run_type, status, metadata)
        VALUES (%s, %s, %s, %s::jsonb)
        """,
        (
            context["dag"].dag_id,
            "quality_check",
            "success" if not critical else "warning",
            json.dumps(
                {
                    "score_pct": score_pct,
                    "checks": total,
                    "passed": passed,
                    "failed_critical": critical,
                }
            ),
        ),
    )
    conn.commit()
    cur.close()
    conn.close()
    return report


def validate_xls_sources(data_dir: str | None = None) -> dict[str, Any]:
    """Inspect the consumption XLS (outside Tempo) : presence, ranges, year gaps."""
    from rte_pipeline.parsing import parse_xls_file

    data_path = Path(data_dir or DATA_DIR)
    if not data_path.is_dir():
        raise FileNotFoundError(
            f"DATA_DIR not found: {data_path}. "
            "Place the RTE exports in data/raw/."
        )

    all_xls = sorted(
        list(data_path.glob("*.xls")) + list(data_path.glob("*.xlsx"))
    )
    if not all_xls:
        raise FileNotFoundError(
            f"No .xls/.xlsx file in {data_path}. "
            "Place the RTE exports in data/raw/."
        )

    consumption_files = sorted(
        f for f in all_xls if "tempo" not in f.name.lower()
    )
    tempo_files = sorted(f for f in all_xls if "tempo" in f.name.lower())

    file_ranges: list[dict[str, Any]] = []
    years: set[int] = set()

    for path in consumption_files:
        records = list(parse_xls_file(str(path)))
        dts = [r["datetime"] for r in records if r.get("datetime")]
        if not dts:
            file_ranges.append(
                {
                    "file": path.name,
                    "rows": 0,
                    "min_datetime": None,
                    "max_datetime": None,
                    "years": [],
                }
            )
            continue

        file_years = {int(str(dt)[:4]) for dt in dts}
        years |= file_years
        file_ranges.append(
            {
                "file": path.name,
                "rows": len(records),
                "min_datetime": min(dts),
                "max_datetime": max(dts),
                "years": sorted(file_years),
            }
        )

    consumption_year_gaps = find_year_gaps(years)
    tempo_years: set[int] = set()
    for path in tempo_files:
        from rte_pipeline.parsing import parse_tempo_file

        for row in parse_tempo_file(str(path)):
            tempo_years.add(int(row["date"][:4]))

    tempo_only_years = sorted(tempo_years - years)

    summary = {
        "data_dir": str(data_path),
        "file_count": len(all_xls),
        "consumption_files": [f.name for f in consumption_files],
        "tempo_files": [f.name for f in tempo_files],
        "file_ranges": file_ranges,
        "years_covered": sorted(years),
        "consumption_year_gaps": consumption_year_gaps,
        "tempo_only_years": tempo_only_years,
        "passed": not consumption_year_gaps and bool(years),
    }

    for item in file_ranges:
        logger.info(
            "   %s : %s lignes, %s → %s",
            item["file"],
            item["rows"],
            item["min_datetime"],
            item["max_datetime"],
        )

    if consumption_year_gaps:
        log_fn = logger.warning if allow_year_gaps() else logger.error
        log_fn(
            "Year(s) gap in consumption sources : %s "
            "(present years : %s)",
            consumption_year_gaps,
            sorted(years),
        )
    if tempo_only_years:
        logger.warning(
            "Tempo years without consumption XLS : %s "
            "(Tempo calendar alone does not fill the gap)",
            tempo_only_years,
        )

    return summary


def validate_xls_sources_or_raise(data_dir: str | None = None) -> dict[str, Any]:
    """Makefile / pre-Bronze — block if consumption sources incomplete."""
    summary = validate_xls_sources(data_dir)
    gaps = summary.get("consumption_year_gaps") or []
    if gaps:
        message = (
            "Incomplete XLS sources : missing year(s) "
            f"{gaps} between {summary['years_covered']}. "
            "Add e.g. eCO2mix_RTE_Annuel-Definitif_<year>.xls "
            "or an ongoing export covering the period."
        )
        if allow_year_gaps():
            logger.warning(
                "%s (allowed in dev — QUALITY_ALLOW_YEAR_GAPS / EDF_ENVIRONMENT=dev)",
                message,
            )
        else:
            raise ValueError(message)
    if not summary.get("years_covered"):
        raise ValueError(
            "No consumption data parsed in the XLS (outside Tempo)."
        )
    return summary
