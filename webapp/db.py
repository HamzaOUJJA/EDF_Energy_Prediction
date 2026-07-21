# =======================================================================
# webapp/db.py — Small Postgres helper for the dashboard. Queries degrade
# gracefully (return None/[]) when a table doesn't exist yet, since the DW
# schema can lag behind the code until a full pipeline run recreates it.
# =======================================================================

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import psycopg2
import psycopg2.errors
import psycopg2.extras

import config


@contextmanager
def _connection() -> Iterator[psycopg2.extensions.connection]:
    conn = psycopg2.connect(
        host=config.PG_HOST,
        port=config.PG_PORT,
        user=config.PG_USER,
        password=config.PG_PASSWORD,
        dbname=config.PG_DATABASE,
        connect_timeout=3,
    )
    try:
        yield conn
    finally:
        conn.close()


def query_one(sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    """Run a query expecting at most one row. Returns None on missing table/no rows."""
    try:
        with _connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None
    except psycopg2.errors.UndefinedTable:
        return None
    except psycopg2.OperationalError:
        return None


def query_all(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    """Run a query expecting multiple rows. Returns [] on missing table/connection error."""
    try:
        with _connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
    except psycopg2.errors.UndefinedTable:
        return []
    except psycopg2.OperationalError:
        return []
