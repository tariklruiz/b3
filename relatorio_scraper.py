"""
relatorio_scraper.py — Download Relatório Gerencial PDFs from CVM fnet.

Pipeline:
    1. Load universe from relatorio_universe (top-50 FII + top-10 FIAGRO).
    2. For each tipo_fundo (FII=1, FIAGRO=11), scan the CVM grid for new
       relatório gerencial documents (idCategoriaDocumento=7, idTipoDocumento=9).
    3. Filter grid results to CNPJs in the universe.
    4. For each new doc (not already in relatorios_gerenciais), download the PDF
       to /mnt/volumes/relatorios/{ticker}/{doc_id}.pdf and insert metadata.

Usage:
    python relatorio_scraper.py                  # full scrape (FII + FIAGRO)
    python relatorio_scraper.py --fii-only
    python relatorio_scraper.py --fiagro-only
    python relatorio_scraper.py --since 2026-01-01  # override retention cutoff
    python relatorio_scraper.py --retry-errors      # retry docs that previously failed

Env:
    DATABASE_URL       required — Postgres connection string
    RELATORIOS_PATH    optional — base directory for PDFs (default: /mnt/volumes/relatorios)
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import requests

from db import connection, execute, init_pool, close_pool, query_all, query_one

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_BASE      = "https://fnet.bmfbovespa.com.br/fnet/publico"
GRID_ENDPOINT = f"{API_BASE}/pesquisarGerenciadorDocumentosDados"
DL_ENDPOINT   = f"{API_BASE}/downloadDocumento"

PAGE_SIZE     = 100
MAX_RETRIES   = 3
RETRY_DELAY   = 30
REQUEST_DELAY = 1.5
RETENTION_DAYS = 60

RELATORIOS_PATH = Path(os.environ.get("RELATORIOS_PATH", "/mnt/volumes/relatorios"))

# Browser-like headers to avoid Cloudflare/WAF 403s on CVM fnet.
# Same pattern as informe_mensal_scraper.py — UA alone is not enough.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM",
    "X-Requested-With": "XMLHttpRequest",
}

# Grid filters for "Relatório Gerencial":
#   idCategoriaDocumento=7 (Relatórios)
#   idTipoDocumento=9     (Relatório Gerencial)
# Same filters work for both FII (tipoFundo=1) and FIAGRO (tipoFundo=11).
BASE_GRID_PARAMS = {
    "idCategoriaDocumento": 7,
    "idTipoDocumento":      9,
    "idEspecieDocumento":   0,
    "situacao":             "A",
    "isSession":            "false",
}

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s -- %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("relatorio_scraper")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_DIGITS_RE = re.compile(r"\D+")

def clean_cnpj(raw: str | None) -> str | None:
    """Normalize CNPJ to digits-only. Same helper as informe_parsers / dividend_scraper."""
    if not raw:
        return None
    digits = _DIGITS_RE.sub("", raw)
    return digits or None


def retention_cutoff_str() -> str:
    """CVM grid expects dates as DD/MM/YYYY in the dataReferencia filter."""
    cutoff = datetime.now() - timedelta(days=RETENTION_DAYS)
    return cutoff.strftime("%d/%m/%Y")


def parse_grid_date(s: str | None):
    """Grid dates come as 'DD/MM/YYYY HH:MM:SS' or 'DD/MM/YYYY'. Return datetime or None."""
    if not s:
        return None
    s = s.strip()
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def parse_referencia(s: str | None):
    """
    dataReferencia in the grid for relatórios is typically 'MM/YYYY' or 'YYYY'.
    We normalize to a DATE: first day of the period.
    """
    if not s:
        return None
    s = s.strip()
    if re.match(r"^\d{2}/\d{4}$", s):
        return datetime.strptime(s, "%m/%Y").date()
    if re.match(r"^\d{4}$", s):
        return datetime.strptime(s, "%Y").date().replace(month=12, day=31)
    if re.match(r"^\d{2}/\d{2}/\d{4}$", s):
        return datetime.strptime(s, "%d/%m/%Y").date()
    return None


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_ticker(ticker: str | None) -> str:
    """Sanitize a ticker for use as a folder name. Defensive — tickers should already be clean."""
    if not ticker:
        return "_unknown"
    return re.sub(r"[^A-Za-z0-9_-]", "_", ticker)


# ---------------------------------------------------------------------------
# Universe loading
# ---------------------------------------------------------------------------
def load_universe(tipo_fundo: str | None = None) -> dict[str, dict]:
    """
    Returns {cnpj: {ticker, tipo_fundo, ranking}} for active universe rows.
    If tipo_fundo is given ('FII' or 'FIAGRO'), restrict to that type.
    """
    if tipo_fundo:
        rows = query_all(
            """
            SELECT cnpj_fundo, ticker, tipo_fundo, ranking
            FROM relatorio_universe
            WHERE active = TRUE AND tipo_fundo = %s
            """,
            (tipo_fundo,),
        )
    else:
        rows = query_all(
            """
            SELECT cnpj_fundo, ticker, tipo_fundo, ranking
            FROM relatorio_universe
            WHERE active = TRUE
            """
        )
    return {
        r["cnpj_fundo"]: {
            "ticker": r["ticker"],
            "tipo_fundo": r["tipo_fundo"],
            "ranking": r["ranking"],
        }
        for r in rows
    }


# ---------------------------------------------------------------------------
# CVM grid fetch
# ---------------------------------------------------------------------------
def _grid_params(tipo_fundo_id: int, cutoff_str: str) -> dict:
    return {
        **BASE_GRID_PARAMS,
        "tipoFundo": tipo_fundo_id,
        "o[0][dataReferencia]": "desc",
        "dataInicial": cutoff_str,
    }


def fetch_grid_json(session: requests.Session, params: dict) -> dict:
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(GRID_ENDPOINT, params=params, timeout=60)
            if resp.status_code != 200:
                raise RuntimeError(f"grid HTTP {resp.status_code}")
            return resp.json()
        except Exception as e:
            last_exc = e
            if attempt < MAX_RETRIES:
                log.warning(f"  grid retry {attempt}/{MAX_RETRIES} -- {e}")
                time.sleep(RETRY_DELAY)
    raise RuntimeError(f"grid fetch exhausted retries: {last_exc}")


def scan_grid(tipo_fundo_id: int, label: str, since_str: str) -> list[dict]:
    """
    Page through the CVM grid for the given tipoFundo, collecting all
    relatório gerencial entries since the cutoff. Returns raw grid rows.
    """
    log.info(f"[{label}] Scanning CVM grid since {since_str}...")
    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)

    docs: list[dict] = []
    offset = 0
    total = None

    while True:
        params = {
            **_grid_params(tipo_fundo_id, since_str),
            "s": offset,
            "l": PAGE_SIZE,
            "d": 1 if offset == 0 else 2,
        }
        try:
            data = fetch_grid_json(session, params)
        except Exception as e:
            log.error(f"[{label}] Grid fetch failed at offset {offset}: {e}")
            break

        if total is None:
            total = data.get("recordsTotal", 0)
            log.info(f"[{label}] {total:,} docs in grid")

        page = data.get("data", [])
        if not page:
            break
        docs.extend(page)
        offset += PAGE_SIZE
        if offset >= total:
            break
        time.sleep(REQUEST_DELAY)

    log.info(f"[{label}] {len(docs):,} grid rows fetched")
    return docs


# ---------------------------------------------------------------------------
# Filtering & deduplication
# ---------------------------------------------------------------------------
def existing_doc_ids() -> set[int]:
    """Return all doc_ids already in relatorios_gerenciais."""
    rows = query_all("SELECT doc_id FROM relatorios_gerenciais")
    return {int(r["doc_id"]) for r in rows}


def filter_universe(rows: list[dict], universe: dict[str, dict],
                    seen_doc_ids: set[int]) -> list[dict]:
    """
    Keep grid rows whose CNPJ is in the universe and whose doc_id is new.
    Annotates each kept row with ticker and tipo_fundo from the universe.
    """
    kept = []
    for r in rows:
        cnpj = clean_cnpj(r.get("cnpjFundo") or r.get("cnpj"))
        if not cnpj or cnpj not in universe:
            continue
        try:
            doc_id = int(r.get("id"))
        except (TypeError, ValueError):
            continue
        if doc_id in seen_doc_ids:
            continue
        info = universe[cnpj]
        kept.append({
            "doc_id": doc_id,
            "cnpj_fundo": cnpj,
            "ticker": info["ticker"],
            "tipo_fundo": info["tipo_fundo"],
            "data_referencia_raw": r.get("dataReferencia"),
            "data_entrega_raw": r.get("dataEntrega"),
            "versao": r.get("versao") or r.get("versaoDocumento"),
            "nome_arquivo": r.get("nomeArquivo") or r.get("descricaoTipoDocumento"),
        })
    return kept


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------
def download_pdf(doc_id: int, dest_path: Path, session: requests.Session) -> bool:
    """Stream the PDF to dest_path. Returns True on success."""
    url = f"{DL_ENDPOINT}?id={doc_id}"
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=120, stream=True)
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}")

            ct = resp.headers.get("Content-Type", "").lower()
            if "pdf" not in ct and "octet-stream" not in ct:
                # CVM occasionally serves HTML error pages with 200; sniff first bytes
                first = next(resp.iter_content(chunk_size=8), b"")
                if not first.startswith(b"%PDF"):
                    raise RuntimeError(f"not a PDF (Content-Type={ct!r}, head={first!r})")
                # Stream rest after the sniff
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                with dest_path.open("wb") as f:
                    f.write(first)
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)
                return True

            dest_path.parent.mkdir(parents=True, exist_ok=True)
            with dest_path.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
            return True
        except Exception as e:
            last_exc = e
            if attempt < MAX_RETRIES:
                log.warning(f"  download {doc_id} retry {attempt}/{MAX_RETRIES} -- {e}")
                time.sleep(RETRY_DELAY)
    log.error(f"download {doc_id} exhausted retries: {last_exc}")
    if dest_path.exists():
        try:
            dest_path.unlink()
        except OSError:
            pass
    return False


def record_error(doc_id: int, msg: str) -> None:
    try:
        execute(
            """
            INSERT INTO erros (origem, doc_id, mensagem, ocorrido_em)
            VALUES (%s, %s, %s, NOW())
            """,
            ("relatorio_scraper", doc_id, msg[:1000]),
        )
    except Exception as e:
        log.warning(f"could not record error for {doc_id}: {e}")


# ---------------------------------------------------------------------------
# Main pass
# ---------------------------------------------------------------------------
def process_pass(tipo_fundo: str, tipo_fundo_id: int, since_str: str) -> dict:
    """Run a single FII or FIAGRO pass: scan grid, filter, download new PDFs."""
    universe = load_universe(tipo_fundo=tipo_fundo)
    if not universe:
        log.warning(f"[{tipo_fundo}] No universe rows -- skipping. Did you run build_universe.py?")
        return {"label": tipo_fundo, "fetched_grid": 0, "new_docs": 0, "downloaded": 0, "errors": 0}

    log.info(f"[{tipo_fundo}] Universe: {len(universe)} CNPJs")
    grid_rows = scan_grid(tipo_fundo_id, tipo_fundo, since_str)

    seen = existing_doc_ids()
    candidates = filter_universe(grid_rows, universe, seen)
    log.info(f"[{tipo_fundo}] {len(candidates)} new docs to fetch")

    if not candidates:
        return {"label": tipo_fundo, "fetched_grid": len(grid_rows), "new_docs": 0,
                "downloaded": 0, "errors": 0}

    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)

    downloaded = 0
    errors = 0

    for doc in candidates:
        doc_id = doc["doc_id"]
        ticker_safe = safe_ticker(doc["ticker"])
        dest = RELATORIOS_PATH / ticker_safe / f"{doc_id}.pdf"

        if dest.exists():
            log.info(f"  {doc_id} already on disk at {dest}, indexing only")
            ok = True
        else:
            ok = download_pdf(doc_id, dest, session)
            time.sleep(REQUEST_DELAY)

        if not ok:
            record_error(doc_id, "download failed")
            errors += 1
            continue

        try:
            file_size = dest.stat().st_size
            file_hash = sha256_of(dest)
            execute(
                """
                INSERT INTO relatorios_gerenciais
                    (doc_id, cnpj_fundo, ticker, tipo_fundo, data_referencia,
                     data_entrega, versao, nome_arquivo, pdf_path, file_size_bytes, sha256)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (doc_id) DO NOTHING
                """,
                (
                    doc_id,
                    doc["cnpj_fundo"],
                    doc["ticker"],
                    doc["tipo_fundo"],
                    parse_referencia(doc["data_referencia_raw"]),
                    parse_grid_date(doc["data_entrega_raw"]),
                    int(doc["versao"]) if doc["versao"] not in (None, "") else None,
                    doc["nome_arquivo"],
                    str(dest),
                    file_size,
                    file_hash,
                ),
            )
            downloaded += 1
        except Exception as e:
            log.error(f"  {doc_id} downloaded but DB insert failed: {e}")
            record_error(doc_id, f"db insert failed: {e}")
            errors += 1

    log.info(f"[{tipo_fundo}] downloaded={downloaded} errors={errors}")
    return {"label": tipo_fundo, "fetched_grid": len(grid_rows),
            "new_docs": len(candidates), "downloaded": downloaded, "errors": errors}


# ---------------------------------------------------------------------------
# Retry-errors mode
# ---------------------------------------------------------------------------
def retry_errors() -> dict:
    """Re-attempt downloads for doc_ids logged in erros where origem='relatorio_scraper'."""
    rows = query_all(
        """
        SELECT DISTINCT e.doc_id
        FROM erros e
        WHERE e.origem = 'relatorio_scraper'
          AND e.doc_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM relatorios_gerenciais r WHERE r.doc_id = e.doc_id
          )
        """
    )
    if not rows:
        log.info("No outstanding errors to retry")
        return {"retried": 0, "downloaded": 0}

    log.info(f"Retrying {len(rows)} previously-failed doc_ids")
    # We don't have grid metadata for these, so we can only re-download — but we
    # need ticker info for the path. Look it up via universe + dividendos lookup.
    # Simplest approach: skip retries that we can't resolve a ticker for.
    log.warning("Retry mode: skipping — implement when needed (need ticker resolution)")
    return {"retried": len(rows), "downloaded": 0}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape relatórios gerenciais from CVM.")
    parser.add_argument("--fii-only", action="store_true")
    parser.add_argument("--fiagro-only", action="store_true")
    parser.add_argument("--since", help="Override retention cutoff (DD/MM/YYYY or YYYY-MM-DD)")
    parser.add_argument("--retry-errors", action="store_true",
                        help="Re-attempt previously-failed doc_ids")
    args = parser.parse_args()

    if args.fii_only and args.fiagro_only:
        log.error("--fii-only and --fiagro-only are mutually exclusive")
        return 2

    if args.since:
        s = args.since.strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            since_str = datetime.strptime(s, "%Y-%m-%d").strftime("%d/%m/%Y")
        elif re.match(r"^\d{2}/\d{2}/\d{4}$", s):
            since_str = s
        else:
            log.error(f"--since must be DD/MM/YYYY or YYYY-MM-DD, got {s!r}")
            return 2
    else:
        since_str = retention_cutoff_str()

    init_pool()
    try:
        if args.retry_errors:
            retry_errors()
            return 0

        if not args.fiagro_only:
            process_pass("FII", tipo_fundo_id=1, since_str=since_str)
            time.sleep(120)  # cooldown between passes (matches informe scraper pattern)
        if not args.fii_only:
            process_pass("FIAGRO", tipo_fundo_id=11, since_str=since_str)
    finally:
        close_pool()

    return 0


if __name__ == "__main__":
    sys.exit(main())
