"""
add_corporate_action.py — add a single corporate action (split / grupamento /
bonificacao) and immediately recompute adjusted prices for the affected ticker.

Usage:
    python add_corporate_action.py \
        --ticker CARE11 \
        --event-date 2026-02-12 \
        --event-type desdobramento \
        --ratio 1:5

    python add_corporate_action.py \
        --ticker XYZF11 \
        --event-date 2025-08-15 \
        --event-type grupamento \
        --ratio 10:1

    python add_corporate_action.py \
        --ticker ABCD11 \
        --event-date 2025-06-01 \
        --event-type bonificacao \
        --factor 0.95238       # if you want to specify factor directly

The factor is computed automatically from the ratio for splits/grupamentos:
    desdobramento N:M  → price multiplied by N/M for prior dates
    grupamento N:M     → price multiplied by N/M for prior dates

For bonificacao, you generally pass --factor directly (1 / (1 + bonus_pct)).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import psycopg2

# Reuse the recompute logic
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from recompute_adjusted_prices import recompute_ticker


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def parse_ratio(ratio: str) -> float:
    """
    Parse '1:5' or '10:1' format. Returns the factor that should multiply
    prices BEFORE the event_date.

    Examples:
        '1:5'  → 1/5 = 0.2  (split: 1 cota becomes 5, price divides 5x)
        '10:1' → 10/1 = 10  (reverse split: 10 cotas become 1, price multiplies 10x)
        '1:10' → 1/10 = 0.1
    """
    parts = ratio.replace(" ", "").split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid ratio format: {ratio!r}. Expected 'N:M'.")
    try:
        a, b = float(parts[0]), float(parts[1])
    except ValueError:
        raise ValueError(f"Invalid numeric ratio: {ratio!r}")
    if b == 0:
        raise ValueError(f"Ratio {ratio!r} has zero second part")
    return a / b


def clean_cnpj(val: str | None) -> str | None:
    """
    Normalize a CNPJ to digits-only. Accepts formatted ('00.000.000/0000-00')
    or raw ('00000000000000'). Returns None if input doesn't yield 14 digits.
    """
    if not val:
        return None
    digits = "".join(c for c in val if c.isdigit())
    return digits if len(digits) == 14 else None


def get_cnpj_for_ticker(conn, ticker: str) -> str | None:
    """Look up CNPJ from informe_mensal via codigo_isin pattern."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cnpj_fundo
            FROM informe_mensal
            WHERE codigo_isin LIKE %s
            LIMIT 1
        """, (f"%{ticker[:4]}%",))
        row = cur.fetchone()
        return row[0] if row else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", required=True, help="e.g. MXRF11")
    parser.add_argument("--event-date", required=True,
                        help="ISO date YYYY-MM-DD (the 'ex' date)")
    parser.add_argument("--event-type", required=True,
                        choices=["desdobramento", "grupamento", "bonificacao"])
    parser.add_argument("--ratio", help="e.g. '1:5' for desdobramento, '10:1' for grupamento")
    parser.add_argument("--factor", type=float,
                        help="Direct factor (use for bonificacao or to override)")
    parser.add_argument("--cnpj", help="Override CNPJ lookup")
    parser.add_argument("--source", default="manual",
                        help="Source tag (default: manual)")
    parser.add_argument("--source-doc-id", type=int, default=None,
                        help="Optional CVM doc ID for traceability")
    parser.add_argument("--notes", default=None)
    parser.add_argument("--no-recompute", action="store_true",
                        help="Skip recompute step (just insert the event)")
    args = parser.parse_args()

    if not args.ratio and args.factor is None:
        log.error("Must provide either --ratio or --factor")
        return 1

    ticker = args.ticker.upper()
    try:
        event_date = datetime.strptime(args.event_date, "%Y-%m-%d").date()
    except ValueError:
        log.error(f"Invalid date format: {args.event_date}")
        return 1

    if args.factor is not None:
        factor = args.factor
        ratio_text = args.ratio
    else:
        factor = parse_ratio(args.ratio)
        ratio_text = args.ratio

    if args.event_type == "desdobramento" and factor > 1:
        log.warning(f"Desdobramento with factor > 1 is unusual (got {factor}). "
                    f"Splits typically reduce per-cota price, so factor should be < 1.")
    if args.event_type == "grupamento" and factor < 1:
        log.warning(f"Grupamento with factor < 1 is unusual (got {factor}). "
                    f"Reverse splits typically increase per-cota price, so factor should be > 1.")

    url = os.environ.get("DATABASE_URL")
    if not url:
        log.error("DATABASE_URL not set")
        return 1
    conn = psycopg2.connect(url)

    cnpj = args.cnpj or get_cnpj_for_ticker(conn, ticker)
    cnpj = clean_cnpj(cnpj)
    if not cnpj:
        log.error(f"Could not find a valid 14-digit CNPJ for ticker {ticker}. "
                  f"Pass it explicitly with --cnpj.")
        return 1

    log.info(f"Adding event: {ticker} ({cnpj}) {args.event_type} on {event_date} "
             f"factor={factor:.6f} ratio={ratio_text}")

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO corporate_actions
                (cnpj_fundo, ticker, event_date, factor, event_type,
                 ratio_text, source, source_doc_id, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cnpj_fundo, event_date, event_type) DO UPDATE SET
                factor = EXCLUDED.factor,
                ratio_text = EXCLUDED.ratio_text,
                source = EXCLUDED.source,
                source_doc_id = EXCLUDED.source_doc_id,
                notes = EXCLUDED.notes
            RETURNING id, (xmax = 0) AS inserted
        """, (cnpj, ticker, event_date, factor, args.event_type,
              ratio_text, args.source, args.source_doc_id, args.notes))
        row = cur.fetchone()
        action_id = row[0]
        was_inserted = row[1]
    conn.commit()

    log.info(f"Event {'inserted' if was_inserted else 'updated'} (id={action_id})")

    if args.no_recompute:
        log.info("Skipping recompute (--no-recompute).")
        conn.close()
        return 0

    log.info(f"Recomputing adjusted prices for {ticker}...")
    stats = recompute_ticker(conn, ticker, dry_run=False)
    log.info(f"  {stats['updates']:,} cotahist rows updated "
             f"(of {stats['rows']:,} total, {stats['events']} events)")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
