# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# rte_pipeline/parsing/xls.py — Parsing RTE éco2mix (XLS in TSV latin-1).
# =======================================================================

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

logger = logging.getLogger(__name__)

# Functional schema for RTE columns.
RTE_COLUMNS: list[str] = [
    "perimeter", "nature", "date", "time",
    "consumption_mw", "forecast_j1_mw", "forecast_j_mw",
    "fuel_mw", "coal_mw", "gas_mw", "nuclear_mw",
    "wind_mw", "solar_mw", "hydro_mw", "pumping_mw",
    "bioenergy_mw", "physical_exchanges_mw", "co2_rate",
    "exchange_uk_mw", "exchange_spain_mw", "exchange_italy_mw",
    "exchange_switzerland_mw", "exchange_germany_belgium_mw",
    "fuel_tac_mw", "fuel_cogen_mw", "fuel_other_mw",
    "gas_tac_mw", "gas_cogen_mw", "gas_ccg_mw", "gas_other_mw",
    "hydro_river_mw", "hydro_lake_mw", "hydro_step_mw",
    "bioenergy_waste_mw", "bioenergy_biomass_mw", "bioenergy_biogas_mw",
    "battery_storage_mw", "battery_release_mw",
    "wind_onshore_mw", "wind_offshore_mw",
]

# Columns to cast as float (all except perimeter, nature, date, time).
NUMERIC_COLUMNS: frozenset[str] = frozenset(RTE_COLUMNS[4:])

# RTE labels → canonical identifiers.
COLUMN_MAP: dict[str, str] = {
    "Périmètre": "perimeter",
    "Nature": "nature",
    "Date": "date",
    "Heures": "time",
    "Consommation": "consumption_mw",
    "Prévision J-1": "forecast_j1_mw",
    "Prévision J": "forecast_j_mw",
    "Fioul": "fuel_mw",
    "Charbon": "coal_mw",
    "Gaz": "gas_mw",
    "Nucléaire": "nuclear_mw",
    "Eolien": "wind_mw",
    "Solaire": "solar_mw",
    "Hydraulique": "hydro_mw",
    "Pompage": "pumping_mw",
    "Bioénergies": "bioenergy_mw",
    "Ech. physiques": "physical_exchanges_mw",
    "Taux de Co2": "co2_rate",
    "Ech. comm. Angleterre": "exchange_uk_mw",
    "Ech. comm. Espagne": "exchange_spain_mw",
    "Ech. comm. Italie": "exchange_italy_mw",
    "Ech. comm. Suisse": "exchange_switzerland_mw",
    "Ech. comm. Allemagne-Belgique": "exchange_germany_belgium_mw",
    "Fioul - TAC": "fuel_tac_mw",
    "Fioul - Cogén.": "fuel_cogen_mw",
    "Fioul - Autres": "fuel_other_mw",
    "Gaz - TAC": "gas_tac_mw",
    "Gaz - Cogén.": "gas_cogen_mw",
    "Gaz - CCG": "gas_ccg_mw",
    "Gaz - Autres": "gas_other_mw",
    "Hydraulique - Fil de l?eau + éclusée": "hydro_river_mw",
    "Hydraulique - Lacs": "hydro_lake_mw",
    "Hydraulique - STEP turbinage": "hydro_step_mw",
    "Bioénergies - Déchets": "bioenergy_waste_mw",
    "Bioénergies - Biomasse": "bioenergy_biomass_mw",
    "Bioénergies - Biogaz": "bioenergy_biogas_mw",
    " Stockage batterie": "battery_storage_mw",
    "Déstockage batterie": "battery_release_mw",
    "Eolien terrestre": "wind_onshore_mw",
    "Eolien offshore": "wind_offshore_mw",
}

NULL_SENTINELS: frozenset[str] = frozenset({"", "ND", "n/a", "-"})
RTE_COPYRIGHT_MARKER = "RTE ne pourra"
MIN_VALID_FIELDS = 5


def build_datetime(date_str: str | None, time_str: str | None) -> str | None:
    """Builds an ISO 8601 UTC timestamp from RTE date and time."""
    if not date_str or len(str(date_str)) != 10:
        return None
    hour = str(time_str or "00:00").strip() or "00:00"
    return f"{date_str}T{hour}:00+00:00"


def _parse_cell(raw: str, column: str) -> Any:
    """Converts a TSV cell to a typed value."""
    raw = raw.strip()
    if raw in NULL_SENTINELS:
        return None
    if column in NUMERIC_COLUMNS:
        try:
            return float(raw.replace(",", "."))
        except ValueError:
            return None
    return raw


def _read_latin1_lines(filepath: str) -> list[str]:
    """Reads an RTE file and returns the decoded lines (latin-1)."""
    with open(filepath, "rb") as handle:
        return handle.read().decode("latin-1").split("\n")


def _header_index_map(headers_raw: list[str]) -> dict[str, int]:
    """Associates each canonical column name with its index in the TSV line."""
    return {COLUMN_MAP.get(header.strip(), header.strip()): idx
            for idx, header in enumerate(headers_raw)}

def parse_xls_row(
    headers: list[str],
    values: list[str],
    source_file: str,
    *,
    add_ingestion_meta: bool = False,
) -> dict[str, Any] | None:
    """Converts an RTE TSV line to a structured dictionary.

    Parameters
    ----------
    headers
        RTE headers (French).
    values
        Cells of the line.
    source_file
        Source file name (traceability).
    add_ingestion_meta
        If True, adds ``ingested_at`` (Kafka / streaming).
    """
    if len(values) < MIN_VALID_FIELDS:
        return None

    record: dict[str, Any] = {"source_file": source_file}
    for index, header in enumerate(headers):
        if index >= len(values):
            continue
        column = COLUMN_MAP.get(header.strip(), header.strip().lower().replace(" ", "_"))
        record[column] = _parse_cell(values[index], column)

    date_val = record.get("date")
    if not date_val or date_val == "Date":
        return None

    record["datetime"] = build_datetime(str(date_val), str(record.get("time", "00:00")))
    if not record["datetime"]:
        return None

    if add_ingestion_meta:
        record["ingested_at"] = datetime.now(timezone.utc).isoformat()

    return record


def iter_xls_file(
    filepath: str,
    *,
    add_ingestion_meta: bool = False,
) -> Iterator[dict[str, Any]]:
    """Iterates lazily over the records of an RTE XLS file."""
    path = Path(filepath)
    lines = _read_latin1_lines(filepath)
    if not lines:
        return

    headers = [h.strip() for h in lines[0].split("\t")]
    logger.info("%s — %d colonnes, %d lignes brutes", path.name, len(headers), len(lines) - 1)

    for line in lines[1:]:
        line = line.strip()
        if not line or RTE_COPYRIGHT_MARKER in line:
            if RTE_COPYRIGHT_MARKER in line:
                break
            continue
        record = parse_xls_row(
            headers,
            line.split("\t"),
            path.name,
            add_ingestion_meta=add_ingestion_meta,
        )
        if record:
            yield record


def parse_xls_file(filepath: str) -> list[dict[str, Any]]:
    """Parses an RTE XLS file and returns all records (batch)."""
    records = list(iter_xls_file(filepath, add_ingestion_meta=False))
    logger.info("  %s: %d lignes parsées", Path(filepath).name, len(records))
    return records


def iter_tempo_file(filepath: str) -> Iterator[dict[str, Any]]:
    """Iterates over the Tempo EDF days (BLUE / WHITE / RED)."""
    path = Path(filepath)
    lines = _read_latin1_lines(filepath)
    for line in lines[1:]:
        line = line.strip()
        if not line or "\t" not in line:
            continue
        parts = line.split("\t")
        if len(parts) >= 2:
            yield {
                "date": parts[0].strip(),
                "tempo_color": parts[1].strip().upper(),
                "source_file": path.name,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }


def parse_tempo_file(filepath: str) -> list[dict[str, Any]]:
    """Parses a Tempo file and returns the list of days."""
    return list(iter_tempo_file(filepath))
