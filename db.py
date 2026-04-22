"""
db.py
===============================================================================
Postgres connection layer for FII Guia FastAPI backend.

Uses a ThreadedConnectionPool so we don't open/close a connection per request
(slow) or exhaust Railway's connection limit. Cursors return RealDictCursor
rows (dict-like), preserving the row-as-dict access pattern from the old
sqlite3.Row code.

Typical usage in main.py
------------------------

    from fastapi import Depends
    from db import get_conn, query_all, query_one

    # Option A: raw connection via dependency (for multi-statement work)
    @app.get("/fundo/preco")
    def fundo_preco(ticker: str, conn = Depends(get_conn)):
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM cotahist WHERE cod_neg = %s AND tp_merc = 10 "
                "ORDER BY dt_pregao DESC LIMIT 500",
                (ticker,)
            )
            rows = cur.fetchall()
        return {"history": rows}

    # Option B: helper functions for simple reads
    @app.get("/fundo/dividendos")
    def fundo_dividendos(ticker: str):
        rows = query_all(
            "SELECT * FROM dividendos WHERE cod_negociacao = %s "
            "ORDER BY data_base DESC LIMIT 13",
            (ticker,)
        )
        return {"dividendos": rows}

Lifecycle
---------
    from contextlib import asynccontextmanager
    from fastapi import FastAPI
    from db import init_pool, close_pool

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_pool()
        yield
        close_pool()

    app = FastAPI(lifespan=lifespan)
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Pool configuration
# -----------------------------------------------------------------------------

# Railway's Postgres plans have connection limits; keep the pool modest.
# Hobby plan allows ~20 connections; leave headroom for scrapers and Airflow.
MIN_POOL_SIZE = int(os.environ.get("DB_POOL_MIN", "1"))
MAX_POOL_SIZE = int(os.environ.get("DB_POOL_MAX", "10"))

_pool: ThreadedConnectionPool | None = None


def init_pool() -> None:
    """Initialize the global connection pool. Call once at app startup."""
    global _pool
    if _pool is not None:
        return

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL environment variable is not set")

    _pool = ThreadedConnectionPool(
        minconn=MIN_POOL_SIZE,
        maxconn=MAX_POOL_SIZE,
        dsn=dsn,
        cursor_factory=RealDictCursor,
    )
    logger.info(
        "DB pool initialized (min=%d, max=%d)", MIN_POOL_SIZE, MAX_POOL_SIZE
    )


def close_pool() -> None:
    """Close all pool connections. Call at app shutdown."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None
        logger.info("DB pool closed")


# -----------------------------------------------------------------------------
# Connection helpers
# -----------------------------------------------------------------------------

@contextmanager
def connection() -> Iterator[psycopg2.extensions.connection]:
    """
    Acquire a connection from the pool, yield it, and return it cleanly.

    Usage:
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT ...")
                rows = cur.fetchall()
    """
    if _pool is None:
        init_pool()
    assert _pool is not None

    conn = _pool.getconn()
    try:
        yield conn
        # Autocommit reads; explicit commits not needed for SELECT-only work.
        # If the caller ran writes, they should commit before exiting the block.
        if not conn.autocommit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


def get_conn() -> Iterator[psycopg2.extensions.connection]:
    """
    FastAPI dependency wrapper. Use with `Depends(get_conn)` in endpoint sigs.

        @app.get("/foo")
        def foo(conn = Depends(get_conn)):
            with conn.cursor() as cur:
                cur.execute(...)
    """
    with connection() as conn:
        yield conn


# -----------------------------------------------------------------------------
# Query helpers (for simple one-shot reads)
# -----------------------------------------------------------------------------

def query_all(sql: str, params: tuple | dict | None = None) -> list[dict[str, Any]]:
    """Execute a SELECT and return all rows as a list of dicts."""
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def query_one(sql: str, params: tuple | dict | None = None) -> dict[str, Any] | None:
    """Execute a SELECT and return the first row (or None)."""
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()


def query_scalar(sql: str, params: tuple | dict | None = None) -> Any:
    """Execute a SELECT and return the first column of the first row."""
    row = query_one(sql, params)
    if row is None:
        return None
    # RealDictRow preserves insertion order; first value is the scalar we want
    return next(iter(row.values()))


def execute(sql: str, params: tuple | dict | None = None) -> int:
    """Execute an INSERT/UPDATE/DELETE. Returns affected row count."""
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount


# -----------------------------------------------------------------------------
# Domain helper (replaces the old get_fund_type() file loader)
# -----------------------------------------------------------------------------

def get_fund_type(ticker: str) -> str | None:
    """
    Return the classification ('Papel', 'Tijolo', 'FOF', 'Hibrido', 'Fiagro',
    'Outros') for a given ticker, or None if unknown.
    """
    row = query_one(
        "SELECT classificacao FROM fund_types WHERE ticker = %s",
        (ticker,),
    )
    return row["classificacao"] if row else None


def get_all_fund_types() -> dict[str, str]:
    """Return {ticker: classificacao} for all classified funds."""
    rows = query_all("SELECT ticker, classificacao FROM fund_types")
    return {r["ticker"]: r["classificacao"] for r in rows}
