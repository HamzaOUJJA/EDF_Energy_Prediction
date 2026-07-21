# =======================================================================
# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# rte_pipeline/parsing/__init__.py — Parsing RTE éco2mix (XLS/TSV).
# =======================================================================

"""
Parsing RTE éco2mix — shared module (batch Spark, Kafka, Airflow).

Single entry point for column mapping and XLS/TSV file reading.
"""

from rte_pipeline.parsing.xls import (
    COLUMN_MAP,
    NULL_SENTINELS,
    NUMERIC_COLUMNS,
    RTE_COLUMNS,
    build_datetime,
    iter_tempo_file,
    iter_xls_file,
    parse_tempo_file,
    parse_xls_file,
    parse_xls_row,
)

__all__ = [
    "COLUMN_MAP",
    "NULL_SENTINELS",
    "NUMERIC_COLUMNS",
    "RTE_COLUMNS",
    "build_datetime",
    "iter_tempo_file",
    "iter_xls_file",
    "parse_tempo_file",
    "parse_xls_file",
    "parse_xls_row",
]
