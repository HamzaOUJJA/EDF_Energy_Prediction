# =======================================================================
# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# airflow/plugins/edf_pipeline/pools.py — Airflow pools (Spark cluster slot).
# =======================================================================

from __future__ import annotations

import logging
import os

from airflow.models.pool import Pool
from airflow.utils.db import create_session

logger = logging.getLogger(__name__)

DEFAULT_SPARK_POOL = "spark_cluster"
DEFAULT_SPARK_POOL_SLOTS = 1
DEFAULT_SPARK_POOL_DESC = (
    "Un seul job Spark REST actif sur le cluster (mode professionnel)."
)


def ensure_spark_pool(
    pool_name: str | None = None,
    slots: int | None = None,
    *,
    description: str = DEFAULT_SPARK_POOL_DESC,
) -> dict:
    """Create or update the Spark cluster pool (idempotent)."""
    name = pool_name or os.getenv("AIRFLOW_SPARK_POOL", DEFAULT_SPARK_POOL)
    slot_count = slots if slots is not None else int(
        os.getenv("AIRFLOW_SPARK_POOL_SLOTS", str(DEFAULT_SPARK_POOL_SLOTS))
    )

    with create_session() as session:
        pool = session.query(Pool).filter(Pool.pool == name).first()
        if pool is None:
            pool = Pool(
                pool=name,
                slots=slot_count,
                description=description,
                include_deferred=False,
            )
            session.add(pool)
            logger.info("Airflow pool created: %s (%d slot(s))", name, slot_count)
        else:
            pool.slots = slot_count
            pool.description = description
            logger.info("Airflow pool updated: %s (%d slot(s))", name, slot_count)
        session.commit()

    return {"status": "success", "pool": name, "slots": slot_count}
