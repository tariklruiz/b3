"""
COTAHIST Parser - B3 Fixed-Width File to Postgres
Uses Polars for fast parsing, writes to Railway Postgres via db.py.

Usage:
    python parser.py --file COTAHIST_D260421.TXT
    python parser.py --file COTAHIST_A2026.TXT     # annual file backfill
    python parser.py --file COTAHIST_A2025.TXT --verbose

The script is idempotent: rows already in the database are skipped via
ON CONFLICT DO NOTHING on the PK (cod_neg, dt_pregao). So re-running a file
is safe.

Env:
    DATABASE_URL   Required. Railway Postgres public URL (from .env file or shell).
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import polars as pl

# Load .env if present (so you don't have to set DATABASE_URL every shell session)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv is optional; env var can be set in the shell instead

from db import connection, init_pool, close_pool, query_one

# ---------------------------------------------------------------------------
# Column layout — B3 COTAHIST positional spec
# (start, end, name, type)  — end is exclusive (Python slice convention)
# ---------------------------------------------------------------------------
COLUMNS = [
    (0,   2,   "TpReg",               "str"),
    (2,   10,  "DtPregao",            "date"),
    (10,  12,  "CodBIDI",             "int"),
    (12,  24,  "CodNeg",              "str"),
    (24,  27,  "TpMerc",              "int"),
    (27,  39,  "NomResumido",         "str"),
    (39,  49,  "Especi",              "str"),
    (49,  52,  "PrazOT",              "str"),
    (52,  56,  "ModRef",              "str"),
    (56,  69,  "PrecoAbertura",       "price"),
    (69,  82,  "PrecoMaximo",         "price"),
    (82,  95,  "PrecoMinimo",         "price"),
    (95,  108, "PrecoMedio",          "price"),
    (108, 121, "PrecoUltimo",         "price"),
    (121, 134, "PrecoMOC",            "price"),
    (134, 147, "PrecoMOV",            "price"),
    (147, 152, "NumNegocios",         "int"),
    (152, 170, "QtdNegocios",         "int"),
    (170, 188, "VolNegocios",         "price"),
    (188, 201, "PrecoExercicio",      "price"),
    (201, 202, "IndCorrecao",         "int"),
    (202, 210, "DataVencimento",      "date"),
    (210, 217, "FatorCorrecao",       "int"),
    (217, 230, "PrecoExercicioPonto", "price"),
    (230, 242, "CodISI",              "str"),
    (242, 245, "NumDistriPapel",      "int"),
]

# Subset that matches our Postgres cotahist schema (10 columns).
# Dropped vs SQLite: Especi, PrecoMedio, QtdNegocios (not in Postgres schema).
KEEP_COLUMNS = [
    "DtPregao",
    "CodNeg",
    "TpMerc",
    "NomResumido",
    "PrecoAbertura",
    "PrecoMaximo",
    "PrecoMinimo",
    "PrecoUltimo",
    "NumNegocios",
    "VolNegocios",
]

# Map polars column names -> Postgres snake_case
PG_COLUMN_MAP = {
    "DtPregao":       "dt_pregao",
    "CodNeg":         "cod_neg",
    "TpMerc":         "tp_merc",
    "NomResumido":    "nom_resumido",
    "PrecoAbertura":  "preco_abertura",
    "PrecoMaximo":    "preco_maximo",
    "PrecoMinimo":    "preco_minimo",
    "PrecoUltimo":    "preco_ultimo",
    "NumNegocios":    "num_negocios",
    "VolNegocios":    "vol_negocios",
}


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------
def parse_cotahist(filepath: str) -> pl.DataFrame:
    path = Path(filepath)
    if not path.exists():
        print(f"[ERROR] File not found: {filepath}", file=sys.stderr)
        sys.exit(1)

    log(f"Reading {path.name} ({path.stat().st_size / 1024 / 1024:.1f} MB)...")

    raw = pl.read_csv(
        filepath,
        has_header=False,
        new_columns=["Col"],
        infer_schema_length=0,
        truncate_ragged_lines=True,
    )

    # Keep only type "01" (daily quote records); drop header/trailer
    raw = raw.filter(pl.col("Col").str.slice(0, 2) == "01")
    log(f"Parsing {len(raw):,} records...")

    # Build column-extraction expressions
    exprs = []
    for start, end, name, col_type in COLUMNS:
        length = end - start
        expr = pl.col("Col").str.slice(start, length)

        if col_type == "str":
            expr = expr.str.strip_chars().alias(name)
        elif col_type == "int":
            expr = expr.str.strip_chars().cast(pl.Int64, strict=False).alias(name)
        elif col_type == "price":
            expr = (
                expr.str.strip_chars()
                .cast(pl.Float64, strict=False)
                .truediv(100)
                .alias(name)
            )
        elif col_type == "date":
            expr = (
                expr.str.strip_chars()
                .str.replace("99991231", "20991231")
                .str.to_date(format="%Y%m%d", strict=False)
                .alias(name)
            )
        exprs.append(expr)

    df = raw.select(exprs).select(KEEP_COLUMNS)

    # Strip whitespace from remaining string columns
    str_cols = [c for c, t in df.schema.items() if t == pl.Utf8]
    df = df.with_columns([pl.col(c).str.strip_chars() for c in str_cols])

    # Rename to Postgres snake_case
    df = df.rename(PG_COLUMN_MAP)

    log(f"Parsed — {len(df):,} rows ready for Postgres")
    return df


# ---------------------------------------------------------------------------
# Load into Postgres
# ---------------------------------------------------------------------------
def load_to_postgres(df: pl.DataFrame, verbose: bool = False) -> tuple[int, int]:
    """
    Bulk insert via psycopg2 execute_values with ON CONFLICT DO NOTHING.
    Returns (total_attempted, newly_inserted).
    """
    from psycopg2.extras import execute_values

    if len(df) == 0:
        log("No rows to load.")
        return (0, 0)

    # Convert polars -> list of tuples
    rows = df.rows()
    total = len(rows)

    log(f"Inserting {total:,} rows into Postgres cotahist...")

    # Pre-count rows in Postgres to measure how many are genuinely new
    before_row = query_one("SELECT COUNT(*) AS n FROM cotahist")
    before = before_row["n"] if before_row else 0

    sql = """
        INSERT INTO cotahist (
            dt_pregao, cod_neg, tp_merc, nom_resumido,
            preco_abertura, preco_maximo, preco_minimo, preco_ultimo,
            num_negocios, vol_negocios
        ) VALUES %s
        ON CONFLICT (cod_neg, dt_pregao) DO NOTHING
    """

    with connection() as conn:
        with conn.cursor() as cur:
            # Batch in chunks of 10k — fast for daily files, handles annual too
            batch_size = 10_000
            for i in range(0, total, batch_size):
                batch = rows[i:i + batch_size]
                execute_values(cur, sql, batch, page_size=batch_size)
                if verbose:
                    log(f"  ...committed {min(i + batch_size, total):,}/{total:,}")
            conn.commit()

    after_row = query_one("SELECT COUNT(*) AS n FROM cotahist")
    after = after_row["n"] if after_row else 0
    inserted = after - before

    log(f"Loaded — {inserted:,} new rows ({total - inserted:,} already existed)")
    log(f"Total cotahist rows now: {after:,}")
    return (total, inserted)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Parse B3 COTAHIST file -> Postgres")
    parser.add_argument("--file",    required=True, help="Path to COTAHIST file (daily or annual)")
    parser.add_argument("--verbose", action="store_true", help="Log each batch commit")
    args = parser.parse_args()

    if not os.environ.get("DATABASE_URL"):
        print("ERROR: DATABASE_URL not set.", file=sys.stderr)
        print("Create a .env file in this folder with:", file=sys.stderr)
        print("  DATABASE_URL=postgresql://USER:PASS@HOST:PORT/railway", file=sys.stderr)
        print("(Get the public URL from Railway: Postgres service -> Variables -> DATABASE_PUBLIC_URL)", file=sys.stderr)
        return 1

    start = datetime.now()
    init_pool()
    try:
        df = parse_cotahist(args.file)
        load_to_postgres(df, verbose=args.verbose)
    finally:
        close_pool()

    elapsed = (datetime.now() - start).total_seconds()
    log(f"Completed in {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
