# =======================================================================
# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# rte_pipeline/producers/rte_producer.py — Kafka producer for RTE éco2mix (XLS to Kafka).
# =======================================================================

"""
Role
----
Kafka producer that ingests RTE éco2mix (XLS in TSV format) and publishes each record to the appropriate Kafka topic:

    ┌──────────────────────────────┬───────────────────────────────────-───┐
    │ Source file                  │ Kafka topic                           │
    ├──────────────────────────────┼─────────────────────────────────────-─┤
    │ eCO2mix_RTE_Annuel-*.xls     │ rte.raw      (annual history)         │
    │ eCO2mix_RTE_En-cours-*.xls   │ rte.realtime (real-time J/J-1)        │
    │ eCO2mix_RTE_tempo*.xls       │ rte.tempo    (blue/white/red calendar)│
    └──────────────────────────────┴───────────────────────────────────────┘

The XLS parsing is centralized in ``rte_pipeline.parsing`` (single source).
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Iterator

from kafka import KafkaProducer
from kafka.errors import KafkaError

from rte_pipeline.parsing import (
    COLUMN_MAP,
    iter_tempo_file,
    iter_xls_file,
    parse_xls_row,
)

# Re-export for compatibility tests / existing imports
__all__ = [
    "COLUMN_MAP",
    "create_producer",
    "ingest_all",
    "iter_tempo_file",
    "iter_xls_file",
    "parse_xls_row",
    "publish_file",
]


# Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("rte_producer")

KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

TOPIC_RAW: str = "rte.raw"
TOPIC_TEMPO: str = "rte.tempo"
TOPIC_REALTIME: str = "rte.realtime"

KAFKA_BATCH_SIZE: int = 16_384
KAFKA_LINGER_MS: int = 10
KAFKA_MAX_REQUEST_SIZE: int = 10 * 1024 * 1024
KAFKA_RETRIES: int = 3
KAFKA_BATCH_RECORDS: int = 500


def create_producer() -> KafkaProducer:
    """Creates a Kafka producer with JSON serialization and production tuning."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=KAFKA_RETRIES,
        batch_size=KAFKA_BATCH_SIZE,
        linger_ms=KAFKA_LINGER_MS,
        compression_type="gzip",
        max_request_size=KAFKA_MAX_REQUEST_SIZE,
    )

def _flush_batch(
    producer: KafkaProducer,
    topic: str,
    batch: list[dict[str, Any]],
    stats: dict[str, int],
) -> None:
    """Sends a batch of records to Kafka (keys = datetime or date)."""
    for record in batch:
        key = record.get("datetime") or record.get("date", "")
        try:
            producer.send(topic, key=key, value=record)
            stats["sent"] += 1
        except KafkaError as exc:
            logger.error("Erreur Kafka : %s", exc)
            stats["errors"] += 1


def publish_file(
    producer: KafkaProducer,
    filepath: str,
    topic: str,
    *,
    is_tempo: bool = False,
) -> dict[str, int]:
    """Publishes an RTE XLS file to a Kafka topic."""
    stats = {"sent": 0, "errors": 0}
    iterator: Iterator[dict[str, Any]] = (
        iter_tempo_file(filepath)
        if is_tempo
        else iter_xls_file(filepath, add_ingestion_meta=True)
    )
    batch: list[dict[str, Any]] = []

    for record in iterator:
        batch.append(record)
        if len(batch) >= KAFKA_BATCH_RECORDS:
            _flush_batch(producer, topic, batch, stats)
            batch.clear()

    if batch:
        _flush_batch(producer, topic, batch, stats)

    producer.flush()
    logger.info(
        "  %s → %s : %d envoyés, %d erreurs",
        Path(filepath).name,
        topic,
        stats["sent"],
        stats["errors"],
    )
    return stats


def ingest_all(data_dir: str) -> dict[str, dict[str, int]]:
    """Ingests all XLS files in a directory into Kafka."""
    data_path = Path(data_dir)
    producer = create_producer()
    summary: dict[str, dict[str, int]] = {}

    for pattern, topic in [
        ("eCO2mix_RTE_Annuel-*.xls", TOPIC_RAW),
        ("eCO2mix_RTE_En-cours-*.xls", TOPIC_REALTIME),
    ]:
        for filepath in sorted(data_path.glob(pattern)):
            summary[filepath.name] = publish_file(producer, str(filepath), topic)

    for filepath in sorted(data_path.glob("eCO2mix_RTE_tempo*.xls")):
        summary[filepath.name] = publish_file(
            producer, str(filepath), TOPIC_TEMPO, is_tempo=True
        )

    producer.close()
    return summary


if __name__ == "__main__":
    directory = sys.argv[1] if len(sys.argv) > 1 else "/opt/airflow/data/raw"
    logger.info("Ingestion Kafka from %s", directory)
    start = time.time()
    results = ingest_all(directory)
    elapsed = time.time() - start
    total = sum(s.get("sent", 0) for s in results.values())
    logger.info("Completed — %d messages in %.1fs", total, elapsed)
