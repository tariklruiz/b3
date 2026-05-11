"""
load_fund_listing.py — One-off loader for B3 fund CSVs into fund_listing.

Reads the two CSVs B3 publishes on its "Fundos Listados" pages:
    - fundosListados_fii.csv     (513+ FIIs)
    - fundosListados_fiagro.csv  (48+ FIAGROs)

Both files are Latin-1 (ISO-8859-1), semicolon-separated, with a trailing
empty column. Schema:
    "Razao Social";"Fundo";"Codigo";

Inserts rows into fund_listing with ON CONFLICT DO UPDATE so re-running is
idempotent — useful when B3 adds new funds.

Usage:
    python load_fund_listing.py path/to/fundosListados_fii.csv path/to/fundosListados_fiagro.csv

With no args, looks for both CSVs in the current directory.
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from db import close_pool, execute, init_pool, query_one


def load_csv(path: Path, tipo_fundo: str) -> tuple[int, int]:
    """
    Load one CSV into fund_listing. Returns (written, skipped).
    Skipped rows are those missing codigo or razao_social.
    """
    written = 0
    skipped = 0
    with open(path, encoding="latin-1") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            # Note: column name has accented characters in the CSV header
            codigo = ""
            razao = ""
            fundo = ""
            for k, v in row.items():
                if k is None:
                    continue
                kl = k.strip().lower()
                vs = (v or "").strip()
                if "digo" in kl:  # Codigo
                    codigo = vs
                elif "social" in kl:  # Razao Social
                    razao = vs
                elif kl == "fundo":
                    fundo = vs

            if not codigo or not razao:
                skipped += 1
                continue

            execute(
                """
                INSERT INTO fund_listing (codigo, razao_social, fundo, tipo_fundo, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (codigo) DO UPDATE SET
                    razao_social = EXCLUDED.razao_social,
                    fundo        = EXCLUDED.fundo,
                    tipo_fundo   = EXCLUDED.tipo_fundo,
                    updated_at   = NOW()
                """,
                (codigo, razao, fundo or None, tipo_fundo),
            )
            written += 1
    return written, skipped


def main() -> int:
    args = sys.argv[1:]
    if args:
        paths = [Path(a) for a in args]
    else:
        paths = [
            Path("fundosListados_fii.csv"),
            Path("fundosListados_fiagro.csv"),
        ]

    for p in paths:
        if not p.exists():
            print(f"Not found: {p}", file=sys.stderr)
            return 2

    init_pool()
    try:
        for p in paths:
            name = p.name.lower()
            if "fiagro" in name:
                tipo = "FIAGRO"
            elif "fii" in name:
                tipo = "FII"
            else:
                print(f"Cannot determine tipo_fundo from filename {p.name}", file=sys.stderr)
                return 2

            written, skipped = load_csv(p, tipo)
            print(f"  {p.name} ({tipo}): {written} loaded, {skipped} skipped")

        # Sanity check: confirm all universe tickers have a matching listing row
        row = query_one(
            """
            SELECT
                COUNT(*) AS universe_count,
                COUNT(fl.codigo) AS matched_count
            FROM relatorio_universe ru
            LEFT JOIN fund_listing fl ON fl.ticker = ru.ticker
            WHERE ru.active = TRUE
            """
        )
        if row:
            uc = row["universe_count"]
            mc = row["matched_count"]
            print(f"\nUniverse coverage: {mc}/{uc} active funds matched")
            if mc < uc:
                missing = query_one(
                    """
                    SELECT string_agg(ru.ticker, ', ') AS tickers
                    FROM relatorio_universe ru
                    LEFT JOIN fund_listing fl ON fl.ticker = ru.ticker
                    WHERE ru.active = TRUE AND fl.codigo IS NULL
                    """
                )
                if missing and missing.get("tickers"):
                    print(f"Unmatched: {missing['tickers']}")
    finally:
        close_pool()

    return 0


if __name__ == "__main__":
    sys.exit(main())
