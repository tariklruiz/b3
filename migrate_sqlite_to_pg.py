"""
migrate_sqlite_to_pg.py
===============================================================================
One-shot SQLite -> Railway Postgres migration for FII Guia.

Reads from local SQLite files (b3.db, dividendos.db, informe_mensal.db,
gestores.db) and fund_types.json, writes to Postgres via psycopg2 with batched
execute_values. Idempotent: uses ON CONFLICT DO NOTHING so re-running is safe.

Usage
-----
    # Load everything
    python migrate_sqlite_to_pg.py --all

    # Load specific tables only
    python migrate_sqlite_to_pg.py --tables cotahist,dividendos

    # Verify row counts match without writing
    python migrate_sqlite_to_pg.py --verify

    # Custom paths (defaults read from ./backend/data/)
    python migrate_sqlite_to_pg.py --all --sqlite-dir /path/to/data

Env
---
    DATABASE_URL   Required. Railway Postgres connection string.

Notes
-----
- Column renaming: SQLite CamelCase/PascalCase -> Postgres snake_case.
- Date conversion: TEXT 'YYYY-MM-DD' -> native DATE.
- Batch size 10_000 rows by default (override with --batch-size).
- For cotahist (largest table), consider dropping indexes before load and
  recreating after for a ~3x speedup. Commented recipe at bottom of file.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

import psycopg2
from psycopg2.extras import Json, execute_values


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

DEFAULT_SQLITE_DIR = Path("backend/data")
DEFAULT_FUND_TYPES_PATH = Path("fund_types.json")
DEFAULT_BATCH_SIZE = 10_000

# Tables to migrate, in dependency order (none have FKs, but keep stable order)
TABLES = ["cotahist", "dividendos", "erros", "informe_mensal", "gestores", "fund_types"]


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def parse_date(val: Any) -> date | None:
    """Convert SQLite TEXT 'YYYY-MM-DD' (or None) to a Python date."""
    if val is None or val == "":
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    try:
        return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def parse_jsonb(val: Any) -> Any:
    """Convert SQLite TEXT JSON to a Python object for JSONB insert."""
    if val is None or val == "":
        return None
    if isinstance(val, (dict, list)):
        return Json(val)
    try:
        return Json(json.loads(val))
    except (json.JSONDecodeError, TypeError):
        return None


def parse_bool(val: Any) -> bool | None:
    """SQLite stores booleans as 0/1/None; convert to Python bool."""
    if val is None:
        return None
    return bool(val)


def batched(it: Iterable, size: int) -> Iterator[list]:
    """Yield successive lists of `size` items from `it`."""
    batch: list = []
    for item in it:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


@contextmanager
def pg_conn(dsn: str):
    conn = psycopg2.connect(dsn)
    try:
        yield conn
    finally:
        conn.close()


def sqlite_open(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(f"SQLite DB not found: {path}")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def bulk_insert(
    pg_conn,
    table: str,
    columns: list[str],
    rows: Iterable[tuple],
    conflict_target: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    """
    Batched insert with ON CONFLICT DO NOTHING. Returns total rows inserted
    (as reported by cursor.rowcount, summed across batches).
    """
    cols_sql = ", ".join(columns)
    sql = (
        f"INSERT INTO {table} ({cols_sql}) VALUES %s "
        f"ON CONFLICT {conflict_target} DO NOTHING"
    )

    total_inserted = 0
    total_seen = 0
    t0 = time.time()

    with pg_conn.cursor() as cur:
        for batch in batched(rows, batch_size):
            execute_values(cur, sql, batch, page_size=batch_size)
            pg_conn.commit()
            inserted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            total_inserted += inserted
            total_seen += len(batch)
            elapsed = time.time() - t0
            rate = total_seen / elapsed if elapsed > 0 else 0
            log(f"  {table}: {total_seen:,} seen, {total_inserted:,} inserted ({rate:,.0f} rows/s)")

    return total_inserted


# -----------------------------------------------------------------------------
# Table migrators
# -----------------------------------------------------------------------------

def migrate_cotahist(sqlite_dir: Path, pg, batch_size: int) -> int:
    """b3.db / cotahist -> cotahist"""
    src = sqlite_open(sqlite_dir / "b3.db")
    cur = src.execute(
        "SELECT CodNeg, DtPregao, PrecoUltimo, PrecoAbertura, PrecoMaximo, "
        "PrecoMinimo, VolNegocios, NumNegocios, NomResumido, TpMerc "
        "FROM cotahist"
    )

    def rows():
        for r in cur:
            dt = parse_date(r["DtPregao"])
            if dt is None or r["CodNeg"] is None or r["TpMerc"] is None:
                continue  # skip rows that would violate NOT NULL / PK
            yield (
                r["CodNeg"],
                dt,
                r["PrecoUltimo"],
                r["PrecoAbertura"],
                r["PrecoMaximo"],
                r["PrecoMinimo"],
                r["VolNegocios"],
                r["NumNegocios"],
                r["NomResumido"],
                r["TpMerc"],
            )

    columns = [
        "cod_neg", "dt_pregao", "preco_ultimo", "preco_abertura", "preco_maximo",
        "preco_minimo", "vol_negocios", "num_negocios", "nom_resumido", "tp_merc",
    ]
    inserted = bulk_insert(pg, "cotahist", columns, rows(), "(cod_neg, dt_pregao)", batch_size)
    src.close()
    return inserted


def migrate_dividendos(sqlite_dir: Path, pg, batch_size: int) -> int:
    """dividendos.db / dividendos -> dividendos"""
    src = sqlite_open(sqlite_dir / "dividendos.db")
    cur = src.execute(
        "SELECT id_documento, cod_negociacao, cnpj_fundo, data_base, valor_provento, "
        "data_pagamento, data_informacao, inserido_em, isento_ir FROM dividendos"
    )

    def rows():
        for r in cur:
            if r["id_documento"] is None:
                continue
            yield (
                r["id_documento"],
                r["cod_negociacao"],
                r["cnpj_fundo"],
                parse_date(r["data_base"]),
                r["valor_provento"],
                parse_date(r["data_pagamento"]),
                parse_date(r["data_informacao"]),
                parse_date(r["inserido_em"]) or datetime.now(),
                parse_bool(r["isento_ir"]),
            )

    columns = [
        "id_documento", "cod_negociacao", "cnpj_fundo", "data_base",
        "valor_provento", "data_pagamento", "data_informacao", "inserido_em", "isento_ir",
    ]
    inserted = bulk_insert(pg, "dividendos", columns, rows(), "(id_documento)", batch_size)
    src.close()
    return inserted


def migrate_erros(sqlite_dir: Path, pg, batch_size: int) -> int:
    """dividendos.db / erros -> erros"""
    src = sqlite_open(sqlite_dir / "dividendos.db")
    # erros schema in SQLite is loose; read whatever columns exist
    src_cur = src.execute("PRAGMA table_info(erros)")
    existing_cols = {row[1] for row in src_cur.fetchall()}
    if not existing_cols:
        log("  erros: table not found in SQLite, skipping")
        src.close()
        return 0

    # Build select list from whatever's available
    select_map = {
        "id_documento": "id_documento",
        "cod_negociacao": "cod_negociacao",
        "motivo": "motivo",
        "registrado_em": "registrado_em",
    }
    available = [(pg_col, sqlite_col) for pg_col, sqlite_col in select_map.items()
                 if sqlite_col in existing_cols]
    if not available:
        log("  erros: no compatible columns, skipping")
        src.close()
        return 0

    sqlite_cols = ", ".join(c[1] for c in available)
    cur = src.execute(f"SELECT {sqlite_cols} FROM erros")

    def rows():
        for r in cur:
            row = []
            for pg_col, _ in available:
                val = r[pg_col]
                if pg_col == "registrado_em":
                    val = parse_date(val) or datetime.now()
                row.append(val)
            yield tuple(row)

    pg_cols = [c[0] for c in available]
    # erros has no unique constraint -> conflict target is effectively no-op
    # Use ON CONFLICT DO NOTHING on PK (id); since id is serial, no conflicts expected
    inserted = bulk_insert(pg, "erros", pg_cols, rows(), "(id)", batch_size)
    src.close()
    return inserted


def migrate_informe_mensal(sqlite_dir: Path, pg, batch_size: int) -> int:
    """informe_mensal.db / informe_mensal -> informe_mensal"""
    src = sqlite_open(sqlite_dir / "informe_mensal.db")
    cur = src.execute(
        "SELECT id_documento, cnpj_fundo, competencia, nome_fundo, classificacao, "
        "subclassificacao, tipo_gestao, nome_administrador, total_cotistas, "
        "patrimonio_liquido, num_cotas_emitidas, valor_patr_cotas, despesas_tx_adm, "
        "dividend_yield_mes, rent_patr_mensal, rendimentos_distribuir "
        "FROM informe_mensal"
    )

    def rows():
        for r in cur:
            if r["id_documento"] is None:
                continue
            yield (
                r["id_documento"],
                r["cnpj_fundo"],
                parse_date(r["competencia"]),
                r["nome_fundo"], r["classificacao"], r["subclassificacao"],
                r["tipo_gestao"], r["nome_administrador"],
                r["total_cotistas"], r["patrimonio_liquido"], r["num_cotas_emitidas"],
                r["valor_patr_cotas"], r["despesas_tx_adm"], r["dividend_yield_mes"],
                r["rent_patr_mensal"], r["rendimentos_distribuir"],
            )

    columns = [
        "id_documento", "cnpj_fundo", "competencia", "nome_fundo", "classificacao",
        "subclassificacao", "tipo_gestao", "nome_administrador", "total_cotistas",
        "patrimonio_liquido", "num_cotas_emitidas", "valor_patr_cotas",
        "despesas_tx_adm", "dividend_yield_mes", "rent_patr_mensal",
        "rendimentos_distribuir",
    ]
    inserted = bulk_insert(pg, "informe_mensal", columns, rows(),
                           "(id_documento)", batch_size)
    src.close()
    return inserted


def migrate_gestores(sqlite_dir: Path, pg, batch_size: int) -> int:
    """gestores.db / gestores -> gestores (with JSONB conversion)"""
    src = sqlite_open(sqlite_dir / "gestores.db")

    # Debug: report source row count so we can tell if 0 rows loaded means
    # 'source is empty' vs 'all rows were filtered out'
    src_count = src.execute("SELECT COUNT(*) FROM gestores").fetchone()[0]
    log(f"  gestores source has {src_count} rows in SQLite")
    if src_count == 0:
        log("  gestores: source is empty, nothing to migrate")
        src.close()
        return 0

    cur = src.execute(
        "SELECT ticker, competencia, classificacao, tom_gestor, pl_total_brl, "
        "cota_mercado, cota_patrimonial, spread_credito_bps, ltv_medio, "
        "resultado_por_cota, distribuicao_por_cota, reserva_monetaria_brl, "
        "vacancia_pct, contratos_vencer_12m_pct, cap_rate, "
        "contexto_meses, cris_em_observacao, alocacao_fundos, "
        "mudancas_portfolio, resumo, alertas_dados, processado_em "
        "FROM gestores"
    )

    skipped = 0

    def rows():
        nonlocal skipped
        for r in cur:
            comp = parse_date(r["competencia"])
            if comp is None or r["ticker"] is None:
                skipped += 1
                continue
            yield (
                r["ticker"], comp, r["classificacao"], r["tom_gestor"],
                r["pl_total_brl"], r["cota_mercado"], r["cota_patrimonial"],
                r["spread_credito_bps"], r["ltv_medio"],
                r["resultado_por_cota"], r["distribuicao_por_cota"],
                r["reserva_monetaria_brl"], r["vacancia_pct"],
                r["contratos_vencer_12m_pct"], r["cap_rate"],
                parse_jsonb(r["contexto_meses"]),
                parse_jsonb(r["cris_em_observacao"]),
                parse_jsonb(r["alocacao_fundos"]),
                r["mudancas_portfolio"], r["resumo"], r["alertas_dados"],
                parse_date(r["processado_em"]) or datetime.now(),
            )

    columns = [
        "ticker", "competencia", "classificacao", "tom_gestor", "pl_total_brl",
        "cota_mercado", "cota_patrimonial", "spread_credito_bps", "ltv_medio",
        "resultado_por_cota", "distribuicao_por_cota", "reserva_monetaria_brl",
        "vacancia_pct", "contratos_vencer_12m_pct", "cap_rate",
        "contexto_meses", "cris_em_observacao", "alocacao_fundos",
        "mudancas_portfolio", "resumo", "alertas_dados", "processado_em",
    ]
    inserted = bulk_insert(pg, "gestores", columns, rows(),
                           "(ticker, competencia)", batch_size)
    if skipped > 0:
        log(f"  gestores: skipped {skipped} rows with NULL ticker or competencia")
    src.close()
    return inserted


def migrate_fund_types(json_path: Path, pg, batch_size: int) -> int:
    """fund_types.json -> fund_types table"""
    if not json_path.exists():
        raise FileNotFoundError(f"fund_types JSON not found: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    fonte = data.get("fonte")
    atualizado = data.get("atualizado") or datetime.now().isoformat()
    fundos = data.get("fundos", {})

    def rows():
        for ticker, classificacao in fundos.items():
            if not ticker or not classificacao:
                continue
            yield (ticker, classificacao, fonte, atualizado)

    columns = ["ticker", "classificacao", "fonte", "atualizado"]
    return bulk_insert(pg, "fund_types", columns, rows(), "(ticker)", batch_size)


MIGRATORS: dict[str, Callable] = {
    "cotahist": migrate_cotahist,
    "dividendos": migrate_dividendos,
    "erros": migrate_erros,
    "informe_mensal": migrate_informe_mensal,
    "gestores": migrate_gestores,
    "fund_types": migrate_fund_types,  # special-cased in main()
}


# -----------------------------------------------------------------------------
# Verification
# -----------------------------------------------------------------------------

def verify_counts(sqlite_dir: Path, fund_types_path: Path, pg) -> None:
    """Compare row counts between SQLite sources and Postgres tables."""
    log("Verifying row counts...")

    checks = [
        ("cotahist",       sqlite_dir / "b3.db",            "SELECT COUNT(*) FROM cotahist"),
        ("dividendos",     sqlite_dir / "dividendos.db",    "SELECT COUNT(*) FROM dividendos"),
        ("erros",          sqlite_dir / "dividendos.db",    "SELECT COUNT(*) FROM erros"),
        ("informe_mensal", sqlite_dir / "informe_mensal.db","SELECT COUNT(*) FROM informe_mensal"),
        ("gestores",       sqlite_dir / "gestores.db",      "SELECT COUNT(*) FROM gestores"),
    ]

    all_match = True
    for table, sqlite_path, sqlite_sql in checks:
        # SQLite count
        sqlite_count = 0
        if sqlite_path.exists():
            sconn = sqlite3.connect(sqlite_path)
            try:
                sqlite_count = sconn.execute(sqlite_sql).fetchone()[0]
            except sqlite3.OperationalError:
                sqlite_count = 0  # table doesn't exist
            sconn.close()

        # Postgres count — use a fresh cursor per table to avoid transaction
        # state bleeding between queries
        try:
            pg.rollback()  # clear any aborted transaction
            with pg.cursor() as pg_cur:
                pg_cur.execute(f"SELECT COUNT(*) FROM {table}")
                pg_count = pg_cur.fetchone()[0]
                if isinstance(pg_count, dict):
                    pg_count = next(iter(pg_count.values()))
        except Exception as e:
            log(f"  {table:20s} ERROR querying Postgres: {e}")
            all_match = False
            continue

        match = "OK" if sqlite_count == pg_count else "MISMATCH"
        if sqlite_count != pg_count:
            all_match = False
        log(f"  {table:20s} sqlite={sqlite_count:>12,}  pg={pg_count:>12,}  [{match}]")

    # fund_types: compare JSON dict size to table count
    if fund_types_path.exists():
        with open(fund_types_path, "r", encoding="utf-8") as f:
            json_count = len(json.load(f).get("fundos", {}))
        try:
            pg.rollback()
            with pg.cursor() as pg_cur:
                pg_cur.execute("SELECT COUNT(*) FROM fund_types")
                pg_count = pg_cur.fetchone()[0]
                if isinstance(pg_count, dict):
                    pg_count = next(iter(pg_count.values()))
            match = "OK" if json_count == pg_count else "MISMATCH"
            if json_count != pg_count:
                all_match = False
            log(f"  {'fund_types':20s} json={json_count:>12,}  pg={pg_count:>12,}  [{match}]")
        except Exception as e:
            log(f"  fund_types           ERROR querying Postgres: {e}")
            all_match = False

    log("Verification " + ("passed" if all_match else "FAILED"))


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="SQLite -> Postgres migration for FII Guia")
    parser.add_argument("--all", action="store_true", help="Migrate all tables")
    parser.add_argument("--tables", type=str, default="",
                        help=f"Comma-separated subset of: {','.join(TABLES)}")
    parser.add_argument("--verify", action="store_true",
                        help="Only verify row counts, do not write")
    parser.add_argument("--sqlite-dir", type=Path, default=DEFAULT_SQLITE_DIR,
                        help=f"Directory containing SQLite DBs (default: {DEFAULT_SQLITE_DIR})")
    parser.add_argument("--fund-types-path", type=Path, default=DEFAULT_FUND_TYPES_PATH,
                        help=f"Path to fund_types.json (default: {DEFAULT_FUND_TYPES_PATH})")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args()

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("ERROR: DATABASE_URL environment variable not set", file=sys.stderr)
        return 1

    with pg_conn(dsn) as pg:
        if args.verify:
            verify_counts(args.sqlite_dir, args.fund_types_path, pg)
            return 0

        if args.all:
            tables = TABLES
        elif args.tables:
            tables = [t.strip() for t in args.tables.split(",") if t.strip()]
            invalid = set(tables) - set(TABLES)
            if invalid:
                print(f"ERROR: unknown tables: {invalid}", file=sys.stderr)
                return 1
        else:
            parser.print_help()
            return 1

        log(f"Migrating tables: {', '.join(tables)}")
        log(f"SQLite dir:      {args.sqlite_dir}")
        log(f"Batch size:      {args.batch_size:,}")

        t_start = time.time()
        for table in tables:
            log(f"--- {table} ---")
            t0 = time.time()
            if table == "fund_types":
                n = migrate_fund_types(args.fund_types_path, pg, args.batch_size)
            else:
                n = MIGRATORS[table](args.sqlite_dir, pg, args.batch_size)
            log(f"  {table}: {n:,} new rows in {time.time() - t0:.1f}s")

        log(f"Total elapsed: {time.time() - t_start:.1f}s")

        # Refresh planner statistics after bulk load
        log("Running ANALYZE on loaded tables...")
        with pg.cursor() as cur:
            for table in tables:
                cur.execute(f"ANALYZE {table}")
            pg.commit()

        log("Done. Run with --verify to compare row counts.")

    return 0


if __name__ == "__main__":
    sys.exit(main())


# =============================================================================
# OPTIONAL: faster cotahist load by dropping indexes before and recreating after
# -----------------------------------------------------------------------------
# For the ~1GB cotahist load, dropping indexes before bulk insert and recreating
# them after can cut load time from ~15min to ~5min. Run manually if needed:
#
#   psql "$DATABASE_URL" <<EOF
#   BEGIN;
#   DROP INDEX IF EXISTS idx_cotahist_fii_spot;
#   DROP INDEX IF EXISTS idx_cotahist_dt_pregao;
#   COMMIT;
#   EOF
#
#   python migrate_sqlite_to_pg.py --tables cotahist
#
#   psql "$DATABASE_URL" -f migrations/001_initial.sql  # recreates indexes (IF NOT EXISTS)
#   psql "$DATABASE_URL" -c "ANALYZE cotahist;"
# =============================================================================
