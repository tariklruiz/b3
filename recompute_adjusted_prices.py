"""
recompute_adjusted_prices.py — recalculates cotahist.preco_ultimo_adj based on
the events in split_grouping.

Run when:
  - You add a new corporate action (split, grupamento, bonificacao)
  - You correct an existing event
  - You backfill historical events for the first time

Usage:
    python recompute_adjusted_prices.py                    # all funds
    python recompute_adjusted_prices.py --ticker MXRF11    # one ticker
    python recompute_adjusted_prices.py --cnpj 12345678901234   # one CNPJ
    python recompute_adjusted_prices.py --dry-run          # show what would change

The script is idempotent and safe to re-run.

Math:
    For each ticker, get all corporate actions sorted by event_date asc.
    For each price row at date `t`:
        adjusted(t) = raw(t) × ∏ factor for all events where event_date > t

    A price from before any split gets multiplied by ALL factors.
    A price from yesterday (after all events) gets multiplier 1.0.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import psycopg2
import psycopg2.extras


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def get_connection():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(url)


# ---------------------------------------------------------------------------
def list_tickers_with_data(conn) -> list[str]:
    """Tickers that have cotahist data."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT cod_neg
            FROM cotahist
            WHERE cod_neg IS NOT NULL
            ORDER BY cod_neg
        """)
        return [r["cod_neg"] for r in cur.fetchall()]


def list_actions_for_ticker(conn, ticker: str) -> list[dict]:
    """All corporate actions for a ticker, sorted by event_date asc."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT event_date, factor, event_type, ratio_text
            FROM split_grouping
            WHERE ticker = %s
            ORDER BY event_date ASC
        """, (ticker,))
        return list(cur.fetchall())


# ---------------------------------------------------------------------------
def cumulative_factor(events: list[dict], price_date: date) -> float:
    """
    Product of factors for all events that occur AFTER price_date.
    A price from yesterday has multiplier 1.0 (no future events).
    A price from before all events gets multiplied by the product of all factors.
    """
    factor = 1.0
    for ev in events:
        if ev["event_date"] > price_date:
            factor *= float(ev["factor"])
    return factor


def recompute_ticker(conn, ticker: str, dry_run: bool = False) -> dict:
    """
    Recompute preco_ultimo_adj for one ticker. Returns stats dict.
    """
    events = list_actions_for_ticker(conn, ticker)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT dt_pregao, preco_ultimo, preco_ultimo_adj
            FROM cotahist
            WHERE cod_neg = %s
            ORDER BY dt_pregao ASC
        """, (ticker,))
        rows = cur.fetchall()

    if not rows:
        return {"ticker": ticker, "rows": 0, "events": len(events), "updates": 0}

    # If no events, adjusted = raw. Update any rows out of sync.
    if not events:
        if dry_run:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) AS n FROM cotahist
                    WHERE cod_neg = %s
                      AND (preco_ultimo_adj IS DISTINCT FROM preco_ultimo)
                """, (ticker,))
                n_updates = cur.fetchone()["n"]
        else:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE cotahist
                    SET preco_ultimo_adj = preco_ultimo
                    WHERE cod_neg = %s
                      AND (preco_ultimo_adj IS DISTINCT FROM preco_ultimo)
                """, (ticker,))
                n_updates = cur.rowcount
            conn.commit()
        return {"ticker": ticker, "rows": len(rows), "events": 0, "updates": n_updates}

    # Compute adjusted price for each row
    updates = []
    for row in rows:
        dt_pregao    = row["dt_pregao"]
        preco_ultimo = row["preco_ultimo"]
        current_adj  = row["preco_ultimo_adj"]
        if preco_ultimo is None:
            continue
        factor = cumulative_factor(events, dt_pregao)
        new_adj = float(preco_ultimo) * factor
        # Only update if it actually changed
        if current_adj is None or abs(float(current_adj) - new_adj) > 1e-6:
            updates.append((new_adj, ticker, dt_pregao))

    if dry_run:
        return {"ticker": ticker, "rows": len(rows), "events": len(events),
                "updates": len(updates)}

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, """
            UPDATE cotahist
            SET preco_ultimo_adj = %s
            WHERE cod_neg = %s AND dt_pregao = %s
        """, updates, page_size=500)

    conn.commit()
    return {"ticker": ticker, "rows": len(rows), "events": len(events),
            "updates": len(updates)}


# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", help="Single ticker (e.g. MXRF11)")
    parser.add_argument("--cnpj", help="Single fund by CNPJ")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would change without writing")
    args = parser.parse_args()

    conn = get_connection()

    if args.ticker:
        tickers = [args.ticker.upper()]
    elif args.cnpj:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ticker FROM split_grouping
                WHERE cnpj_fundo = %s AND ticker IS NOT NULL
            """, (args.cnpj,))
            tickers = [r["ticker"] for r in cur.fetchall()]
        if not tickers:
            log.error(f"No tickers found in split_grouping for CNPJ {args.cnpj}")
            return 1
    else:
        log.info("Recomputing for all tickers with cotahist data...")
        tickers = list_tickers_with_data(conn)

    log.info(f"Processing {len(tickers):,} tickers (dry_run={args.dry_run})")

    total_rows = 0
    total_events = 0
    total_updates = 0
    affected_tickers = 0

    for i, ticker in enumerate(tickers, 1):
        try:
            stats = recompute_ticker(conn, ticker, dry_run=args.dry_run)
        except Exception as e:
            log.error(f"  {ticker}: failed — {e}")
            continue

        total_rows += stats["rows"]
        total_events += stats["events"]
        total_updates += stats["updates"]
        if stats["updates"] > 0:
            affected_tickers += 1
            if stats["events"] > 0:
                log.info(f"  {ticker}: {stats['updates']:,} rows updated "
                         f"({stats['events']} events)")

        if i % 100 == 0:
            log.info(f"  Progress: {i:,}/{len(tickers):,}")

    log.info(f"Done — {affected_tickers:,} tickers updated, "
             f"{total_updates:,} rows changed across "
             f"{total_rows:,} total rows ({total_events} corporate actions).")

    if args.dry_run:
        log.info("(dry run — no changes written)")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
