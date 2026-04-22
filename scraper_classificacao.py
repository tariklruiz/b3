"""
scraper_classificacao.py — FII Guia (Postgres edition)
Scrapes fund classification from investidor10.com.br and writes to the
`fund_types` Postgres table.

Usage:
    python scraper_classificacao.py                   # scrape all tickers from dividendos
    python scraper_classificacao.py --ticker MXRF11   # single ticker
    python scraper_classificacao.py --force           # re-scrape already-mapped tickers

Env:
    DATABASE_URL  required — Postgres connection string (Railway provides this)
"""

from __future__ import annotations

import argparse
import re
import time
from collections import Counter
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from db import execute, query_all, init_pool, close_pool

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BASE_URL = "https://investidor10.com.br/fiis/{ticker}/"
DELAY = 1.5   # seconds between requests
SOURCE = "investidor10.com.br"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# investidor10 "Tipo" values -> our categories
TYPE_MAP = {
    "papel":           "Papel",
    "recebíveis":      "Papel",
    "tijolo":          "Tijolo",
    "logística":       "Tijolo",
    "lajes":           "Tijolo",
    "shopping":        "Tijolo",
    "shoppings":       "Tijolo",
    "hospital":        "Tijolo",
    "hotel":           "Tijolo",
    "residencial":     "Tijolo",
    "rural":           "Tijolo",
    "agências":        "Tijolo",
    "fundo de fundos": "FOF",
    "fof":             "FOF",
    "híbrido":         "Híbrido",
    "multiestratégia": "Híbrido",
    "misto":           "Híbrido",
    "desenvolvimento": "Tijolo",
    "fiagro":          "Fiagro",
    "agro":            "Fiagro",
}


def normalise(raw: str) -> str:
    if not raw:
        return "Outros"
    r = raw.strip().lower()
    if r in TYPE_MAP:
        return TYPE_MAP[r]
    for key, val in TYPE_MAP.items():
        if key in r:
            return val
    return "Outros"


# ---------------------------------------------------------------------------
# HTTP session (unchanged)
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    retry = Retry(
        total=3, backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


# ---------------------------------------------------------------------------
# Scraping (unchanged from SQLite version)
# ---------------------------------------------------------------------------

def scrape_ticker(ticker: str, session: requests.Session) -> str | None:
    url = BASE_URL.format(ticker=ticker.lower())
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [ERR] {ticker}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Strategy 1: #table-indicators — find row where name span = "TIPO DE FUNDO"
    table = soup.find(id="table-indicators")
    if table:
        for cell in table.find_all(class_=re.compile(r"(cell|item|row)", re.I)):
            name_el = cell.find(class_=re.compile(r"name", re.I))
            if name_el and "TIPO DE FUNDO" in name_el.get_text(strip=True).upper():
                val_el = cell.find(class_=re.compile(r"value", re.I))
                if val_el:
                    return normalise(val_el.get_text(strip=True))

    # Strategy 2: label + sibling value fallback
    for el in soup.find_all(string=re.compile(r"TIPO\s+DE\s+FUNDO", re.I)):
        parent = el.find_parent()
        if parent:
            nxt = parent.find_next_sibling()
            if nxt:
                return normalise(nxt.get_text(strip=True))
            nxt2 = parent.find_parent()
            if nxt2:
                sib = nxt2.find_next_sibling()
                if sib:
                    val = sib.find(class_=re.compile(r"value", re.I))
                    if val:
                        return normalise(val.get_text(strip=True))

    # Strategy 3: meta description fallback
    meta = soup.find("meta", {"name": "description"})
    if meta:
        content = (meta.get("content") or "").lower()
        for key, val in TYPE_MAP.items():
            if f"fii de {key}" in content or f"fundo de {key}" in content:
                return val

    return None


# ---------------------------------------------------------------------------
# Data access (Postgres)
# ---------------------------------------------------------------------------

def load_tickers() -> list[str]:
    """
    Return all FII tickers known to the dividendos table. Filters to strings
    <= 8 chars ending in '11' (the standard FII ticker pattern).
    """
    rows = query_all("""
        SELECT DISTINCT cod_negociacao
        FROM dividendos
        WHERE cod_negociacao IS NOT NULL
          AND cod_negociacao <> ''
          AND length(cod_negociacao) <= 8
          AND cod_negociacao ~ '^[A-Z][A-Z0-9]*11.*$'
        ORDER BY cod_negociacao
    """)
    return [r["cod_negociacao"].strip().upper() for r in rows if r["cod_negociacao"]]


def load_existing() -> dict[str, str]:
    """Current mapping from the fund_types table."""
    rows = query_all("SELECT ticker, classificacao FROM fund_types")
    return {r["ticker"]: r["classificacao"] for r in rows}


def upsert_classification(ticker: str, classificacao: str) -> None:
    """Insert or update a single ticker's classification."""
    execute(
        """
        INSERT INTO fund_types (ticker, classificacao, fonte, atualizado)
        VALUES (%s, %s, %s, now())
        ON CONFLICT (ticker) DO UPDATE SET
            classificacao = EXCLUDED.classificacao,
            fonte         = EXCLUDED.fonte,
            atualizado    = EXCLUDED.atualizado
        """,
        (ticker, classificacao, SOURCE),
    )


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", help="Scrape a single ticker")
    parser.add_argument("--force", action="store_true",
                        help="Re-scrape already-mapped tickers")
    args = parser.parse_args()

    init_pool()
    try:
        existing = load_existing()

        if args.ticker:
            tickers = [args.ticker.upper()]
        else:
            tickers = load_tickers()

        print("FII Guia — Classification Scraper (investidor10 -> Postgres)")
        print(f"Tickers to process: {len(tickers)}")
        print(f"Already classified: {len(existing)}")
        print()

        session = make_session()
        ok = skip = fail = 0

        for ticker in tickers:
            # Skip already classified (unless --force or "Outros")
            if not args.force and ticker in existing and existing[ticker] != "Outros":
                skip += 1
                continue

            cls = scrape_ticker(ticker, session)

            if cls and cls != "Outros":
                upsert_classification(ticker, cls)
                existing[ticker] = cls
                ok += 1
                print(f"  [OK]   {ticker:8s} -> {cls}")
            elif cls == "Outros":
                upsert_classification(ticker, "Outros")
                existing[ticker] = "Outros"
                fail += 1
                print(f"  [?]    {ticker:8s} -> Outros (page found, type unclear)")
            else:
                # Only mark Outros if we've never classified this ticker before
                if ticker not in existing:
                    upsert_classification(ticker, "Outros")
                    existing[ticker] = "Outros"
                fail += 1
                print(f"  [MISS] {ticker:8s} -> not found")

            time.sleep(DELAY)

        cats = Counter(existing.values())
        print(f"\nConcluído: {ok} classificados · {skip} ignorados · {fail} sem resultado")
        print(f"Total no fund_types: {len(existing)} fundos")
        print("Breakdown:", dict(sorted(cats.items(), key=lambda x: -x[1])))

        # Spot check
        print("\nSpot check:")
        for t in ["MXRF11", "HGLG11", "KNCR11", "HFOF11", "XPLG11", "KNRI11", "BTLG11", "RZAK11"]:
            print(f"  {t}: {existing.get(t, 'NOT IN TABLE')}")

        print(f"\nCompleted at {datetime.now().isoformat(timespec='seconds')}")
        return 0

    finally:
        close_pool()


if __name__ == "__main__":
    raise SystemExit(main())
