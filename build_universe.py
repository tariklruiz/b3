"""
build_universe.py — Refresh relatorio_universe table.

Picks top-50 FII + top-10 FIAGRO by 30-day trade count (sum of num_negocios in
cotahist over the last 30 calendar days), maps tickers to CNPJs via dividendos,
and upserts into relatorio_universe.

Tickers without a CNPJ in dividendos are skipped with a warning — they would
otherwise pass the scraper a NULL CNPJ and break the grid filter.

Run monthly (or whenever the universe should be refreshed). Idempotent.

Usage:
    python build_universe.py                # refresh both FII and FIAGRO
    python build_universe.py --fii-only
    python build_universe.py --fiagro-only
    python build_universe.py --dry-run      # show what would change, write nothing

Env:
    DATABASE_URL  required — Postgres connection string
"""
from __future__ import annotations

import argparse
import logging
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from db import init_pool, close_pool, query_all, execute, connection

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
TOP_N_FII    = 50
TOP_N_FIAGRO = 10

# fund_types.classificacao values that count as FII (vs FIAGRO vs Outros)
FII_CLASSIFICATIONS    = ("Tijolo", "Papel", "Híbrido", "FOF")
FIAGRO_CLASSIFICATIONS = ("Fiagro",)

# Window for liquidity ranking
LIQUIDITY_WINDOW_DAYS = 30


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build_universe")


# ---------------------------------------------------------------------------
# Ticker → CNPJ resolution via dividendos
# ---------------------------------------------------------------------------
def get_ticker_cnpj_map() -> dict[str, str]:
    """
    Build {ticker: cnpj} from the dividendos table. Uses the most recent
    dividendo row per ticker as authoritative (in case a ticker appears with
    different CNPJs over time — shouldn't happen, but safer than aggregating).
    """
    rows = query_all(
        """
        SELECT DISTINCT ON (cod_negociacao)
               cod_negociacao AS ticker,
               cnpj_fundo
        FROM dividendos
        WHERE cod_negociacao IS NOT NULL
          AND cnpj_fundo IS NOT NULL
        ORDER BY cod_negociacao, data_base DESC
        """
    )
    return {r["ticker"]: r["cnpj_fundo"] for r in rows}


# ---------------------------------------------------------------------------
# Liquidity ranking
# ---------------------------------------------------------------------------
def rank_by_trade_count(classifications: tuple[str, ...], top_n: int) -> list[dict]:
    """
    Returns top-N tickers by sum of num_negocios over the last
    LIQUIDITY_WINDOW_DAYS days, restricted to fund_types matching the given
    classifications. Standard market lots only (tp_merc=10).
    """
    placeholders = ",".join(["%s"] * len(classifications))
    rows = query_all(
        f"""
        SELECT c.cod_neg              AS ticker,
               ft.classificacao       AS classificacao,
               SUM(c.num_negocios)    AS trade_count_30d
        FROM cotahist c
        JOIN fund_types ft ON ft.ticker = c.cod_neg
        WHERE c.dt_pregao >= CURRENT_DATE - INTERVAL '{LIQUIDITY_WINDOW_DAYS} days'
          AND c.tp_merc   = 10
          AND ft.classificacao IN ({placeholders})
        GROUP BY c.cod_neg, ft.classificacao
        HAVING SUM(c.num_negocios) > 0
        ORDER BY trade_count_30d DESC
        LIMIT %s
        """,
        (*classifications, top_n),
    )
    return rows


# ---------------------------------------------------------------------------
# Universe rebuild
# ---------------------------------------------------------------------------
def rebuild_universe(scope: str, dry_run: bool = False) -> dict:
    """
    scope ∈ {'fii', 'fiagro', 'both'}.
    Returns a summary dict with counts per fund type.
    """
    ticker_to_cnpj = get_ticker_cnpj_map()
    log.info(f"Loaded {len(ticker_to_cnpj):,} ticker->CNPJ mappings from dividendos")

    summary = {"fii": 0, "fiagro": 0, "skipped_no_cnpj": []}

    targets = []
    if scope in ("fii", "both"):
        targets.append(("FII", FII_CLASSIFICATIONS, TOP_N_FII))
    if scope in ("fiagro", "both"):
        targets.append(("FIAGRO", FIAGRO_CLASSIFICATIONS, TOP_N_FIAGRO))

    universe_rows: list[tuple] = []  # (cnpj, ticker, tipo, classificacao, ranking, trade_count)

    for tipo_fundo, classifications, top_n in targets:
        log.info(f"[{tipo_fundo}] Ranking by 30-day num_negocios (top {top_n})...")
        ranked = rank_by_trade_count(classifications, top_n)

        if not ranked:
            log.warning(f"[{tipo_fundo}] No tickers found — empty cotahist window?")
            continue

        for rank_idx, row in enumerate(ranked, start=1):
            ticker = row["ticker"]
            cnpj = ticker_to_cnpj.get(ticker)
            if not cnpj:
                log.warning(f"[{tipo_fundo}] Skipping {ticker} (rank {rank_idx}) — no CNPJ in dividendos")
                summary["skipped_no_cnpj"].append(ticker)
                continue

            universe_rows.append((
                cnpj,
                ticker,
                tipo_fundo,
                row["classificacao"],
                rank_idx,
                int(row["trade_count_30d"]),
            ))

        kept = sum(1 for r in universe_rows if r[2] == tipo_fundo)
        summary[tipo_fundo.lower()] = kept
        log.info(f"[{tipo_fundo}] {kept} tickers ready for universe (after CNPJ resolution)")

    if dry_run:
        log.info("DRY RUN — would upsert the following rows:")
        for cnpj, ticker, tipo, cls, rk, tc in universe_rows:
            log.info(f"  [{tipo} #{rk:>2}] {ticker:8s}  cnpj={cnpj}  trades_30d={tc:,}  ({cls})")
        return summary

    # Truncate-then-insert pattern: full rebuild keeps semantics simple.
    # Wrap in a transaction so a failure mid-rebuild leaves the prior universe intact.
    with connection() as conn:
        with conn.cursor() as cur:
            if scope == "both":
                cur.execute("TRUNCATE TABLE relatorio_universe")
            else:
                cur.execute(
                    "DELETE FROM relatorio_universe WHERE tipo_fundo = %s",
                    (scope.upper(),),
                )

            cur.executemany(
                """
                INSERT INTO relatorio_universe
                    (cnpj_fundo, ticker, tipo_fundo, classificacao, ranking, trade_count_30d, active, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW())
                """,
                universe_rows,
            )
        conn.commit()

    log.info(f"Universe rebuilt: {summary['fii']} FII + {summary['fiagro']} FIAGRO inserted")
    if summary["skipped_no_cnpj"]:
        log.warning(f"Skipped tickers (no CNPJ in dividendos): {', '.join(summary['skipped_no_cnpj'])}")
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild relatorio_universe table.")
    parser.add_argument("--fii-only", action="store_true", help="Refresh only FII rows")
    parser.add_argument("--fiagro-only", action="store_true", help="Refresh only FIAGRO rows")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without writing")
    args = parser.parse_args()

    if args.fii_only and args.fiagro_only:
        log.error("--fii-only and --fiagro-only are mutually exclusive")
        return 2

    scope = "fii" if args.fii_only else "fiagro" if args.fiagro_only else "both"

    init_pool()
    try:
        rebuild_universe(scope=scope, dry_run=args.dry_run)
    finally:
        close_pool()

    return 0


if __name__ == "__main__":
    sys.exit(main())
