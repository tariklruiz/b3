"""
COTAHIST Parser - B3 Fixed-Width File to SQLite
Uses Polars for fast parsing.

Usage:
    python parser.py --file COTAHIST_A2026.TXT --db b3.db
    python parser.py --file COTAHIST_A2026.TXT --db b3.db --append
"""

import polars as pl
import sqlite3
import argparse
import sys
from pathlib import Path
from datetime import datetime

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

# Only these columns will be stored in SQLite (keep it lean)
KEEP_COLUMNS = [
    "DtPregao",
    "CodNeg",
    "TpMerc",
    "NomResumido",
    "Especi",
    "PrecoAbertura",
    "PrecoMaximo",
    "PrecoMinimo",
    "PrecoMedio",
    "PrecoUltimo",
    "NumNegocios",
    "QtdNegocios",
    "VolNegocios",
]


def parse_cotahist(filepath: str) -> pl.DataFrame:
    path = Path(filepath)
    if not path.exists():
        print(f"[ERROR] File not found: {filepath}")
        sys.exit(1)

    print(f"[{datetime.now():%H:%M:%S}] Reading {path.name} ...")

    # Read all lines as plain text — one column
    raw = pl.read_csv(
        filepath,
        has_header=False,
        new_columns=["Col"],
        infer_schema_length=0,      # everything as string
        truncate_ragged_lines=True,
    )

    # Keep only type "01" — daily quote records (drop header type 00, trailer type 99)
    raw = raw.filter(pl.col("Col").str.slice(0, 2) == "01")

    print(f"[{datetime.now():%H:%M:%S}] Parsing {len(raw):,} records ...")

    # Build each column via str.slice, then cast
    exprs = []
    for start, end, name, col_type in COLUMNS:
        length = end - start
        expr = pl.col("Col").str.slice(start, length)

        if col_type == "str":
            expr = expr.str.strip_chars().alias(name)

        elif col_type == "int":
            expr = expr.str.strip_chars().cast(pl.Int64, strict=False).alias(name)

        elif col_type == "price":
            # B3 stores prices as integers with 2 implied decimals
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

    df = raw.select(exprs)

    # Keep only the columns we care about
    df = df.select(KEEP_COLUMNS)

    # Strip whitespace from string columns
    str_cols = [c for c, t in df.schema.items() if t == pl.Utf8]
    df = df.with_columns([pl.col(c).str.strip_chars() for c in str_cols])

    print(f"[{datetime.now():%H:%M:%S}] Parsed — {len(df):,} rows, {len(df.columns)} columns")
    return df


def load_to_sqlite(df: pl.DataFrame, db_path: str, append: bool = False):
    print(f"[{datetime.now():%H:%M:%S}] Loading into SQLite: {db_path} ...")

    conn = sqlite3.connect(db_path)

    # Convert date columns to string for SQLite compatibility
    df_sqlite = df.with_columns(pl.col("DtPregao").cast(pl.Utf8))

    mode = "append" if append else "replace"
    df_sqlite.to_pandas().to_sql(
        name="cotahist",
        con=conn,
        if_exists=mode,
        index=False,
        chunksize=50_000,
    )

    # Indexes for fast querying
    print(f"[{datetime.now():%H:%M:%S}] Creating indexes ...")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_codneg    ON cotahist (CodNeg)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dtpregao  ON cotahist (DtPregao)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_codneg_dt ON cotahist (CodNeg, DtPregao)")
    conn.commit()

    row_count = conn.execute("SELECT COUNT(*) FROM cotahist").fetchone()[0]
    conn.close()

    print(f"[{datetime.now():%H:%M:%S}] Done — {row_count:,} total rows in database")


def main():
    parser = argparse.ArgumentParser(description="Parse B3 COTAHIST file into SQLite")
    parser.add_argument("--file",   required=True, help="Path to COTAHIST_AYYYY.TXT")
    parser.add_argument("--db",     required=True, help="Path to output SQLite database")
    parser.add_argument("--append", action="store_true",
                        help="Append to existing DB (use when loading multiple years)")
    args = parser.parse_args()

    start = datetime.now()
    df = parse_cotahist(args.file)
    load_to_sqlite(df, args.db, append=args.append)
    elapsed = (datetime.now() - start).total_seconds()
    print(f"[{datetime.now():%H:%M:%S}] Completed in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
