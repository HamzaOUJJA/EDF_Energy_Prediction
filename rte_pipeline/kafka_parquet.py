# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# rte_pipeline/kafka_parquet.py — Parquet utilities for Kafka → Bronze (tests and S3 key conventions).
# =======================================================================

from __future__ import annotations
import io
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

# RTE schema.
RTE_SCHEMA: pa.Schema = pa.schema([
    pa.field("perimeter", pa.string()),
    pa.field("nature", pa.string()),
    pa.field("date", pa.string()),
    pa.field("time", pa.string()),
    pa.field("datetime", pa.string()),
    pa.field("consumption_mw", pa.float64()),
    pa.field("forecast_j1_mw", pa.float64()),
    pa.field("forecast_j_mw", pa.float64()),
    pa.field("fuel_mw", pa.float64()),
    pa.field("coal_mw", pa.float64()),
    pa.field("gas_mw", pa.float64()),
    pa.field("nuclear_mw", pa.float64()),
    pa.field("wind_mw", pa.float64()),
    pa.field("solar_mw", pa.float64()),
    pa.field("hydro_mw", pa.float64()),
    pa.field("pumping_mw", pa.float64()),
    pa.field("bioenergy_mw", pa.float64()),
    pa.field("physical_exchanges_mw", pa.float64()),
    pa.field("co2_rate", pa.float64()),
    pa.field("exchange_uk_mw", pa.float64()),
    pa.field("exchange_spain_mw", pa.float64()),
    pa.field("exchange_italy_mw", pa.float64()),
    pa.field("exchange_switzerland_mw", pa.float64()),
    pa.field("exchange_germany_belgium_mw", pa.float64()),
    pa.field("wind_onshore_mw", pa.float64()),
    pa.field("wind_offshore_mw", pa.float64()),
    pa.field("hydro_river_mw", pa.float64()),
    pa.field("hydro_lake_mw", pa.float64()),
    pa.field("hydro_step_mw", pa.float64()),
    pa.field("bioenergy_waste_mw", pa.float64()),
    pa.field("bioenergy_biomass_mw", pa.float64()),
    pa.field("bioenergy_biogas_mw", pa.float64()),
    pa.field("battery_storage_mw", pa.float64()),
    pa.field("battery_release_mw", pa.float64()),
    pa.field("source_file", pa.string()),
    pa.field("ingested_at", pa.string()),
    pa.field("kafka_topic", pa.string()),
    pa.field("kafka_partition", pa.int32()),
    pa.field("kafka_offset", pa.int64()),
])

# Tempo schema.
TEMPO_SCHEMA: pa.Schema = pa.schema([
    pa.field("date", pa.string()),
    pa.field("tempo_color", pa.string()),
    pa.field("source_file", pa.string()),
    pa.field("ingested_at", pa.string()),
    pa.field("kafka_topic", pa.string()),
])


def records_to_parquet(records: list[dict[str, Any]], schema: pa.Schema) -> bytes:
    """Serializes JSON records to Parquet Snappy."""
    if not records:
        return b""

    cols: dict[str, list[Any]] = {field.name: [] for field in schema}
    for rec in records:
        for field in schema:
            val = rec.get(field.name)
            if val is not None:
                if pa.types.is_floating(field.type):
                    try:
                        val = float(val)
                    except (TypeError, ValueError):
                        val = None
                elif pa.types.is_integer(field.type):
                    try:
                        val = int(val)
                    except (TypeError, ValueError):
                        val = None
                elif pa.types.is_string(field.type):
                    val = str(val)
            cols[field.name].append(val)

    table = pa.table(cols, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def build_streaming_s3_key(topic: str, run_date: str) -> str:
    """Bronze streaming key (daily DAG) : ``rte/streaming/…/date/data.parquet``."""
    topic_path = topic.replace(".", "/")
    return f"rte/streaming/{topic_path}/{run_date}/data.parquet"
