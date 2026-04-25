"""
dividend_scraper.py — FII Guia (Postgres edition)
Scrapes B3/CVM "Aviso aos Cotistas - Estruturado" (dividend announcements)
and writes to the `dividendos` Postgres table.

Usage:
    python dividend_scraper.py                  # defaults to --incremental
    python dividend_scraper.py --full           # full scrape (rare)
    python dividend_scraper.py --resume         # skip already-loaded docs
    python dividend_scraper.py --retry-errors   # retry docs in erros table
    python dividend_scraper.py --incremental    # cutoff = MAX(inserido_em) - 7 days
    python dividend_scraper.py --dedup          # dedup + exit
    python dividend_scraper.py --dedup-dry-run  # show dupes without deleting

Env:
    DATABASE_URL  required — Postgres connection string (Railway provides)

Schema notes:
- Our Postgres dividendos has 9 columns: id_documento (PK), cod_negociacao,
  cnpj_fundo, data_base, valor_provento, data_pagamento, data_informacao,
  inserido_em, isento_ir. The XML has more fields that we parse but discard.
- isento_ir: SQLite stored 'Sim'/'Não' strings; Postgres stores BOOLEAN.
  Converted at insert time.
- Dedup handles B3 resubmissions: same fund + data_base filed under a new
  id_documento. Keep MAX(id_documento) per (cnpj_fundo, cod_negociacao,
  data_base) group.
"""

from __future__ import annotations

import argparse
import base64
import json as _json
import logging
import random
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, date

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from db import (
    connection, execute, query_all, query_one, init_pool, close_pool
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
API_BASE        = "https://fnet.bmfbovespa.com.br/fnet/publico"
GRID_ENDPOINT   = f"{API_BASE}/pesquisarGerenciadorDocumentosDados"
DL_ENDPOINT     = f"{API_BASE}/downloadDocumento"
PAGE_SIZE       = 100
MAX_RETRIES     = 5
RETRY_DELAY     = 30          # increased from 15 to give CVM longer to cool down
RETRY_BACKOFF   = 2.0
REQUEST_DELAY   = 2.5         # increased from 1.0 — slower per-page rate
REQUEST_JITTER  = 1.5         # ± random seconds added to each delay
MAX_GRID_PAGES  = 10          # cap pagination depth in incremental mode
COMMIT_BATCH    = 50
CONNECT_TIMEOUT = 15
READ_TIMEOUT    = 90

GRID_PARAMS = {
    "idCategoriaDocumento":  14,
    "idTipoDocumento":       41,   # "Rendimentos e Amortizações"
    "idEspecieDocumento":    0,
    "situacao":              "A",
    "isSession":             "false",
    "o[0][dataEntrega]":     "desc",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

# ---------------------------------------------------------------------------
# Logging (Railway captures stdout)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------
def jittered_sleep(base: float, jitter: float = REQUEST_JITTER) -> None:
    """Sleep `base` seconds plus a random ± jitter, never less than 0.5s.
    Reduces 'mechanical timing' fingerprint that WAFs use to flag bots."""
    delay = base + random.uniform(-jitter, jitter)
    time.sleep(max(0.5, delay))


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    retry = Retry(
        total=3, backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=8)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


def robust_get(session: requests.Session, url: str,
               params: dict | None = None,
               max_attempts: int = MAX_RETRIES) -> requests.Response:
    """GET with exponential backoff on Timeout / ConnectionError."""
    delay = RETRY_DELAY
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(
                url, params=params,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", delay * 2))
                log.warning(
                    f"Rate limited (429) — waiting {wait}s "
                    f"(attempt {attempt}/{max_attempts})"
                )
                time.sleep(wait)
                continue
            return resp
        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            if attempt < max_attempts:
                log.warning(
                    f"  Attempt {attempt}/{max_attempts} — {type(e).__name__} "
                    f"— retrying in {delay:.0f}s"
                )
                time.sleep(delay)
                delay = min(delay * RETRY_BACKOFF, 120)
            else:
                log.error(f"  All {max_attempts} attempts exhausted for {url}")
    raise last_exc  # type: ignore[misc]


def fetch_grid_json(session: requests.Session, params: dict) -> dict:
    """GET the CVM grid endpoint, return parsed JSON with informative errors."""
    resp = robust_get(session, GRID_ENDPOINT, params=params)
    try:
        return resp.json()
    except ValueError as e:
        body = (resp.text or "").strip()
        preview = body[:300].replace("\n", " ")
        raise RuntimeError(
            f"CVM returned non-JSON: status={resp.status_code} "
            f"content-type={resp.headers.get('Content-Type', 'unknown')} "
            f"body_len={len(body)} preview={preview!r}"
        ) from e


# ---------------------------------------------------------------------------
# Helpers — value parsing
# ---------------------------------------------------------------------------
def parse_date(val) -> date | None:
    """Convert 'YYYY-MM-DD' string (or None) to a Python date."""
    if not val:
        return None
    try:
        return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def parse_bool_isento(val) -> bool | None:
    """CVM's RendimentoIsentoIR is 'Sim' or 'Não' (or None)."""
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in ("sim", "s", "true", "1", "yes"):
        return True
    if s in ("não", "nao", "n", "false", "0", "no"):
        return False
    return None


# ---------------------------------------------------------------------------
# Grid fetch — full mode
# ---------------------------------------------------------------------------
def fetch_all_document_ids(resume_ids: set, session: requests.Session) -> list:
    log.info("Fetching full document list from B3 API...")
    all_docs: list = []
    offset = 0

    data = fetch_grid_json(session, {**GRID_PARAMS, "s": 0, "l": PAGE_SIZE, "d": 1})
    total = data["recordsFiltered"]
    all_docs.extend(data["data"])
    log.info(f"Total documents on B3: {total:,}")
    offset += PAGE_SIZE

    while offset < total:
        try:
            data = fetch_grid_json(session, {
                **GRID_PARAMS, "s": offset, "l": PAGE_SIZE,
                "d": offset // PAGE_SIZE + 1,
            })
            all_docs.extend(data["data"])
        except Exception as e:
            log.error(f"Failed page at offset {offset} — skipping: {e}")
        offset += PAGE_SIZE
        jittered_sleep(REQUEST_DELAY)

    if resume_ids:
        before = len(all_docs)
        all_docs = [d for d in all_docs if d["id"] not in resume_ids]
        log.info(f"Skipping {before - len(all_docs):,} already processed docs")

    log.info(f"Documents to process: {len(all_docs):,}")
    return all_docs


# ---------------------------------------------------------------------------
# Grid fetch — incremental mode
# ---------------------------------------------------------------------------
# B3 IDs are NOT monotonic — a doc filed today can have a lower ID than one
# filed yesterday. Stopping at MAX(id_documento) would miss new docs with
# lower IDs. Instead we use a date-based cutoff: all docs with dataEntrega
# >= cutoff. We also dedup against known_ids to skip docs we already have.
# ---------------------------------------------------------------------------
def fetch_incremental_document_ids(
    cutoff_date: str, known_ids: set, session: requests.Session
) -> list:
    """
    cutoff_date: 'YYYY-MM-DD' — fetch all docs with dataEntrega >= this date
    known_ids:   id_documentos already in DB — used to skip dupes
    """
    log.info(f"Incremental mode — fetching docs with dataEntrega >= {cutoff_date}")
    new_docs: list = []
    offset = 0
    done = False

    # dataEntrega format: "17/04/2026 17:11" (DD/MM/YYYY HH:MM)
    y, m, d = cutoff_date.split("-")
    cutoff_entrega = f"{d}/{m}/{y}"  # "DD/MM/YYYY"

    data = fetch_grid_json(session, {**GRID_PARAMS, "s": 0, "l": PAGE_SIZE, "d": 1})
    total = data["recordsFiltered"]
    log.info(f"Total documents on B3: {total:,}")

    for doc in data["data"]:
        doc_date = (doc.get("dataEntrega") or "")[:10]  # "DD/MM/YYYY"
        if doc_date < cutoff_entrega:
            done = True
            break
        if doc["id"] not in known_ids:
            new_docs.append(doc)
    offset += PAGE_SIZE

    while not done and offset < total:
        # Hard cap on pagination depth — incremental runs typically need 1-3 pages.
        # If we go beyond this, something's wrong (cutoff date too old, or CVM
        # not returning 'desc' order). Better to error out than hammer the API.
        if offset >= MAX_GRID_PAGES * PAGE_SIZE:
            log.warning(
                f"Hit MAX_GRID_PAGES cap ({MAX_GRID_PAGES}) — stopping. "
                f"If new docs are missing, run --full once to backfill."
            )
            break

        try:
            data = fetch_grid_json(session, {
                **GRID_PARAMS, "s": offset, "l": PAGE_SIZE,
                "d": offset // PAGE_SIZE + 1,
            })
            for doc in data["data"]:
                doc_date = (doc.get("dataEntrega") or "")[:10]
                if doc_date < cutoff_entrega:
                    done = True
                    break
                if doc["id"] not in known_ids:
                    new_docs.append(doc)
        except Exception as e:
            log.error(f"Failed page at offset {offset} — skipping: {e}")

        if done:
            log.info(f"Reached cutoff date {cutoff_date} — stopping grid scan")
            break

        offset += PAGE_SIZE
        jittered_sleep(REQUEST_DELAY)

    log.info(f"New documents to download: {len(new_docs):,}")
    return new_docs


# ---------------------------------------------------------------------------
# Download and parse one document
# ---------------------------------------------------------------------------
def download_and_parse(doc_id: int, session: requests.Session) -> dict:
    resp = robust_get(session, DL_ENDPOINT, params={"id": doc_id})
    resp.raise_for_status()

    raw = resp.text.strip()
    content_type = resp.headers.get("Content-Type", "")

    if not raw:
        raise ValueError(f"Empty response body (HTTP {resp.status_code})")

    # Response is either raw XML or a JSON-wrapped base64 XML
    if raw.startswith('"'):
        try:
            b64 = _json.loads(raw)
            b64 += "=" * (-len(b64) % 4)
            xml_text = base64.b64decode(b64).decode("utf-8")
        except Exception as e:
            raise ValueError(
                f"Cannot decode base64 response ({type(e).__name__}: {e}). "
                f"HTTP {resp.status_code}, Content-Type={content_type!r}, "
                f"body[:300]={raw[:300]!r}"
            )
    elif raw.startswith("<?xml") or raw.startswith("<"):
        xml_text = raw
    else:
        raise ValueError(
            f"Unknown response format. HTTP {resp.status_code}, "
            f"Content-Type={content_type!r}, body[:300]={raw[:300]!r}"
        )

    xml_text = xml_text.lstrip("\ufeff\r\n \t")

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        preview = xml_text[:200]
        raise ValueError(f"XML parse failed ({e}). xml[:200]={preview!r}")

    def get(path):
        el = root.find(path)
        return el.text.strip() if el is not None and el.text else None

    valor_raw = get(".//ValorProvento")
    try:
        valor = float(valor_raw) if valor_raw else None
    except ValueError:
        valor = None

    # Return only the fields we store in the 9-column Postgres schema.
    # XML also contains nome_fundo, nome_administrador, cnpj_administrador,
    # ano, cod_isin, periodo_referencia — those are read but discarded.
    return {
        "cnpj_fundo":      get(".//CNPJFundo"),
        "cod_negociacao":  get(".//CodNegociacao"),
        "data_informacao": parse_date(get(".//DataInformacao")),
        "data_base":       parse_date(get(".//DataBase")),
        "valor_provento":  valor,
        "data_pagamento":  parse_date(get(".//DataPagamento")),
        "isento_ir":       parse_bool_isento(get(".//RendimentoIsentoIR")),
    }


# ---------------------------------------------------------------------------
# Postgres writes
# ---------------------------------------------------------------------------
INSERT_SQL = """
    INSERT INTO dividendos (
        id_documento, cod_negociacao, cnpj_fundo,
        data_base, valor_provento, data_pagamento,
        data_informacao, inserido_em, isento_ir
    ) VALUES (
        %(id_documento)s, %(cod_negociacao)s, %(cnpj_fundo)s,
        %(data_base)s, %(valor_provento)s, %(data_pagamento)s,
        %(data_informacao)s, now(), %(isento_ir)s
    )
    ON CONFLICT (id_documento) DO UPDATE SET
        cod_negociacao  = EXCLUDED.cod_negociacao,
        cnpj_fundo      = EXCLUDED.cnpj_fundo,
        data_base       = EXCLUDED.data_base,
        valor_provento  = EXCLUDED.valor_provento,
        data_pagamento  = EXCLUDED.data_pagamento,
        data_informacao = EXCLUDED.data_informacao,
        isento_ir       = EXCLUDED.isento_ir
"""


def insert_parsed(doc_id: int, parsed: dict) -> None:
    execute(INSERT_SQL, {"id_documento": doc_id, **parsed})


def insert_batch(rows: list[dict]) -> None:
    """Batch insert via a single psycopg2 connection/transaction."""
    from psycopg2.extras import execute_batch
    with connection() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, INSERT_SQL, rows, page_size=COMMIT_BATCH)


def record_error(doc_id: int, motivo: str) -> None:
    """Append error row (append-only audit log)."""
    execute(
        "INSERT INTO erros (id_documento, motivo, registrado_em) "
        "VALUES (%s, %s, now())",
        (doc_id, motivo[:1000]),
    )


def clear_error(doc_id: int) -> None:
    execute("DELETE FROM erros WHERE id_documento = %s", (doc_id,))


def loaded_doc_ids() -> set[int]:
    rows = query_all("SELECT id_documento FROM dividendos")
    return {r["id_documento"] for r in rows}


def errored_doc_ids() -> list[int]:
    rows = query_all(
        "SELECT DISTINCT id_documento FROM erros WHERE id_documento IS NOT NULL"
    )
    return [r["id_documento"] for r in rows]


def max_inserido_em() -> datetime | None:
    row = query_one("SELECT MAX(inserido_em) AS m FROM dividendos")
    return row["m"] if row and row["m"] else None


# ---------------------------------------------------------------------------
# Dedup — remove B3 resubmissions
# ---------------------------------------------------------------------------
# Same fund + same data_base filed under multiple id_documento values = B3
# resubmitted the announcement. Keep MAX(id_documento) per group, delete others.
# Postgres can do this in a single CTE — much faster than SQLite's Python loop.
# ---------------------------------------------------------------------------
DEDUP_PREVIEW_SQL = """
    SELECT cnpj_fundo, cod_negociacao, data_base, COUNT(*) AS cnt
    FROM dividendos
    WHERE cnpj_fundo IS NOT NULL AND data_base IS NOT NULL
    GROUP BY cnpj_fundo, cod_negociacao, data_base
    HAVING COUNT(*) > 1
"""

DEDUP_DELETE_SQL = """
    WITH ranked AS (
        SELECT id_documento,
               ROW_NUMBER() OVER (
                   PARTITION BY cnpj_fundo, cod_negociacao, data_base
                   ORDER BY id_documento DESC
               ) AS rn
        FROM dividendos
        WHERE cnpj_fundo IS NOT NULL AND data_base IS NOT NULL
    )
    DELETE FROM dividendos
    WHERE id_documento IN (SELECT id_documento FROM ranked WHERE rn > 1)
"""


def dedup_db(dry_run: bool = False) -> int:
    """Remove duplicate rows, keeping MAX(id_documento) per fund+date group.
    Returns number of rows deleted (or would-be-deleted in dry_run mode)."""
    dupes = query_all(DEDUP_PREVIEW_SQL)
    if not dupes:
        log.info("Dedup: no duplicates found.")
        return 0

    would_delete = sum(d["cnt"] - 1 for d in dupes)
    log.info(
        f"Dedup: found {len(dupes):,} fund+date groups with duplicates "
        f"({would_delete:,} rows would be removed)"
    )

    if dry_run:
        log.info("Dedup: dry-run mode — no changes made")
        return would_delete

    deleted = execute(DEDUP_DELETE_SQL)
    log.info(f"Dedup: deleted {deleted:,} duplicate rows")
    return deleted


# ---------------------------------------------------------------------------
# Progress bar helper
# ---------------------------------------------------------------------------
try:
    from tqdm import tqdm
    def wrap(it, **kwargs):
        return tqdm(it, **kwargs)
except ImportError:
    def wrap(it, **kwargs):
        return it


# ---------------------------------------------------------------------------
# Main scraping loop
# ---------------------------------------------------------------------------
def scrape(resume: bool = False, retry_errors: bool = False,
           incremental: bool = False):
    session = make_session()

    if retry_errors:
        doc_ids = errored_doc_ids()
        log.info(f"Retry-errors mode — {len(doc_ids):,} failed docs to retry")
        docs = [{"id": doc_id} for doc_id in doc_ids]

    elif incremental:
        latest = max_inserido_em()
        if latest is None:
            log.warning("DB is empty — falling back to full scrape")
            docs = fetch_all_document_ids(set(), session)
        else:
            # Cutoff = latest insert date - 7 days (buffer for late-filed docs)
            cutoff_dt = (latest.date() if isinstance(latest, datetime)
                         else latest) - timedelta(days=7)
            cutoff = cutoff_dt.strftime("%Y-%m-%d")
            log.info(f"Latest insert in DB: {latest} — cutoff set to {cutoff}")
            known = loaded_doc_ids()
            log.info(f"Known IDs in DB: {len(known):,}")
            docs = fetch_incremental_document_ids(cutoff, known, session)

    else:
        resume_ids: set[int] = set()
        if resume:
            resume_ids |= loaded_doc_ids()
            resume_ids |= set(errored_doc_ids())
            log.info(f"Resume mode — {len(resume_ids):,} docs already processed")
        docs = fetch_all_document_ids(resume_ids, session)

    if not docs:
        log.info("Nothing to do — DB is already up to date.")
        return

    success = 0
    failed = 0
    batch_buf: list[dict] = []

    bar_format = "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}"
    pbar = wrap(docs, desc="Downloading dividends", unit="doc", bar_format=bar_format)

    for doc in pbar:
        doc_id = doc["id"]
        last_error = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                parsed = download_and_parse(doc_id, session)
                batch_buf.append({"id_documento": doc_id, **parsed})
                success += 1
                last_error = None
                break
            except ValueError as e:
                # Bad/undecodable response — don't retry
                last_error = str(e)
                log.warning(f"  Doc {doc_id} bad response (no retry): {last_error[:300]}")
                break
            except ET.ParseError as e:
                last_error = str(e)
                log.warning(f"  Doc {doc_id} XML parse error (no retry): {last_error}")
                break
            except Exception as e:
                last_error = str(e)
                if attempt < MAX_RETRIES:
                    delay = RETRY_DELAY * (RETRY_BACKOFF ** (attempt - 1))
                    log.warning(
                        f"  Doc {doc_id} attempt {attempt}/{MAX_RETRIES} "
                        f"[{type(e).__name__}] — retrying in {delay:.0f}s"
                    )
                    time.sleep(delay)

        # Flush batch periodically
        if len(batch_buf) >= COMMIT_BATCH:
            insert_batch(batch_buf)
            batch_buf.clear()

        if last_error:
            record_error(doc_id, last_error)
            failed += 1
            log.error(f"Doc {doc_id} failed after {MAX_RETRIES} attempts — {last_error}")
        else:
            # Clear any stale errors for docs that succeeded this run
            clear_error(doc_id)

        if hasattr(pbar, "set_postfix_str"):
            pbar.set_postfix_str(f"OK:{success:,} ERR:{failed:,}")
        jittered_sleep(REQUEST_DELAY)

    # Flush remaining buffer
    if batch_buf:
        insert_batch(batch_buf)

    log.info(f"Done — {success:,} inserted/updated, {failed:,} failed")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape B3 dividends -> Postgres")
    parser.add_argument("--resume",        action="store_true")
    parser.add_argument("--retry-errors",  action="store_true")
    parser.add_argument("--incremental",   action="store_true")
    parser.add_argument("--full",          action="store_true",
                        help="Force full scrape (no incremental cutoff)")
    parser.add_argument("--dedup",         action="store_true",
                        help="Remove duplicate rows and exit")
    parser.add_argument("--dedup-dry-run", action="store_true",
                        help="Show duplicates without deleting")
    args = parser.parse_args()

    # Mutually exclusive mode flags
    mode_flags = sum([
        args.resume, args.retry_errors, args.incremental,
        args.full, args.dedup, args.dedup_dry_run,
    ])
    if mode_flags > 1:
        parser.error(
            "Choose at most one of --resume, --retry-errors, "
            "--incremental, --full, --dedup, --dedup-dry-run"
        )
    # Default mode = incremental (what cron runs)
    if mode_flags == 0:
        args.incremental = True
        log.info("No mode specified — defaulting to --incremental")

    init_pool()
    start = datetime.now()
    try:
        if args.dedup or args.dedup_dry_run:
            dedup_db(dry_run=args.dedup_dry_run)
        else:
            scrape(
                resume=args.resume,
                retry_errors=args.retry_errors,
                incremental=args.incremental,
            )
            # Auto-dedup after incremental (catches B3 resubmissions)
            if args.incremental:
                dedup_db()
    finally:
        close_pool()

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"Total time: {elapsed/60:.1f} minutes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
