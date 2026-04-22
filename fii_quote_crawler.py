"""
fii_quote_crawler.py — FII Guia
Crawls current prices for a fixed list of FIIs from Yahoo Finance and writes
them to the `fii_quotes_latest` Postgres table. One row per ticker, overwritten
on each run — no history accumulation.

Usage:
    python fii_quote_crawler.py                # all default tickers
    python fii_quote_crawler.py --ticker MXRF11,HGLG11   # just these

Env:
    DATABASE_URL   Required — Postgres connection string (Railway provides)

Notes:
- Uses yfinance's batch-download API: one HTTP request for all tickers.
- `.history()` API is used (more stable than `.info` since Yahoo's 2024 auth change).
- Suffixes `.SA` for B3 tickers. Works for standard FIIs (ending in 11).
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime

# Load .env so DATABASE_URL is available when running locally
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv is optional; Railway sets env vars directly

import yfinance as yf

from db import execute, init_pool, close_pool, query_all

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
DEFAULT_TICKERS = [
    "BTHF11", "DEVA11", "GGRC11", "HABT11", "HCTR11", "HGLG11", "IRDM11",
    "IRIM11", "MXRF11", "RBFM11", "RECT11", "RZAK11", "RZTR11", "RURA11",
    "SNAG11", "SNCI11", "SNID11", "TGAR11", "TRXF11", "URPR11", "VGIA11",
    "VRTM11",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------
def fetch_prices(tickers: list[str]) -> dict[str, float | None]:
    """
    Batch-download latest close prices for `tickers` via yfinance.
    Returns {ticker_without_suffix: price or None}.
    """
    # Build the .SA-suffixed symbols Yahoo expects for B3 tickers
    yahoo_symbols = [f"{t}.SA" for t in tickers]
    symbol_str = " ".join(yahoo_symbols)

    log.info(f"Fetching {len(tickers)} tickers from Yahoo Finance...")
    data = yf.download(
        symbol_str,
        period="1d",
        progress=False,
        auto_adjust=True,   # price adjusted for splits/dividends (FIIs pay monthly)
        threads=True,        # parallel downloads inside yfinance
    )

    prices: dict[str, float | None] = {}

    # yf.download returns a MultiIndex DataFrame when multiple tickers are
    # passed. Shape is (dates x (metric, ticker)). We want the most recent
    # Close per ticker.
    if "Close" not in data.columns.get_level_values(0):
        log.error("Unexpected yfinance response — no 'Close' column found")
        return {t: None for t in tickers}

    close_df = data["Close"]

    for ticker in tickers:
        yahoo_sym = f"{ticker}.SA"
        try:
            series = close_df[yahoo_sym]
            last_price = series.dropna().iloc[-1]
            prices[ticker] = float(last_price)
        except (KeyError, IndexError):
            log.warning(f"  [MISS] {ticker} — no price data returned")
            prices[ticker] = None
        except Exception as e:
            log.warning(f"  [ERR]  {ticker} — {type(e).__name__}: {e}")
            prices[ticker] = None

    return prices


# ---------------------------------------------------------------------------
# Postgres writes
# ---------------------------------------------------------------------------
UPSERT_SQL = """
    INSERT INTO fii_quotes_latest (cod_neg, preco, atualizado)
    VALUES (%s, %s, now())
    ON CONFLICT (cod_neg) DO UPDATE SET
        preco      = EXCLUDED.preco,
        atualizado = EXCLUDED.atualizado
"""


def write_prices(prices: dict[str, float | None]) -> tuple[int, int]:
    """Upsert each (ticker, price) into fii_quotes_latest.
    Returns (written, skipped_null)."""
    written = 0
    skipped = 0
    for ticker, price in prices.items():
        if price is None:
            skipped += 1
            continue
        execute(UPSERT_SQL, (ticker, price))
        written += 1
    return written, skipped


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Crawl FII prices -> Postgres")
    parser.add_argument(
        "--ticker",
        help="Comma-separated tickers (overrides default list)",
    )
    args = parser.parse_args()

    if args.ticker:
        tickers = [t.strip().upper() for t in args.ticker.split(",") if t.strip()]
    else:
        tickers = DEFAULT_TICKERS

    if not tickers:
        log.error("No tickers to crawl.")
        return 1

    init_pool()
    start = datetime.now()
    try:
        prices = fetch_prices(tickers)

        log.info("Results:")
        for ticker in tickers:
            price = prices.get(ticker)
            if price is not None:
                log.info(f"  [OK]   {ticker:8s} R$ {price:>8.2f}")
            else:
                log.info(f"  [MISS] {ticker:8s} (no price)")

        written, skipped = write_prices(prices)
        log.info(f"Wrote {written} prices to Postgres ({skipped} skipped/null)")

    finally:
        close_pool()

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"Completed in {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
