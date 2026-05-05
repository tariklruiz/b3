"""
relatorio_scraper.py — Download Relatório Gerencial PDFs from CVM fnet.

Pipeline:
    1. Load universe from relatorio_universe (top-50 FII + top-10 FIAGRO).
    2. For each tipo_fundo (FII=1, FIAGRO=11), scan the CVM grid for new
       relatório gerencial documents (idCategoriaDocumento=7, idTipoDocumento=9).
    3. Filter grid results to CNPJs in the universe.
    4. For each new doc (not already in relatorios_gerenciais), download the PDF
       to /mnt/volumes/relatorios/{ticker}/{doc_id}.pdf and insert metadata
       with processed_at=NULL.
    5. End-of-run cleanup: remove PDFs whose DB row has processed_at IS NOT NULL
       but the file still exists (catches orphans from failed agent deletions).

Lifecycle (option C — process-then-delete):
    - Scraper writes PDFs to disk; agent reads, extracts, writes JSON to gestores,
      marks processed_at=NOW(), then deletes the PDF.
    - On any agent failure, processed_at stays NULL and the PDF remains for retry.
    - This scraper's cleanup pass is belt-and-suspenders: handles the case where
      the agent succeeded at marking processed_at but the file delete itself failed.

Usage:
    python relatorio_scraper.py                  # full scrape (FII + FIAGRO)
    python relatorio_scraper.py --fii-only
    python relatorio_scraper.py --fiagro-only
    python relatorio_scraper.py --since 2026-01-01  # override retention cutoff
    python relatorio_scraper.py --cleanup-only      # skip scrape, only prune orphans

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
MAX_RETRIES   = 1                  # reduced from 3 — repeated retries on a blocked IP
                                   # only reinforce the bot signature in Cloudflare's profile
RETRY_DELAY   = 30
REQUEST_DELAY = 8                  # base delay between calls; jitter is added on top
JITTER_RANGE  = (4, 8)             # random extra seconds, sampled uniformly
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


def jittered_sleep(base: float = REQUEST_DELAY) -> None:
    """Sleep base + uniform random jitter. Used between every CVM hit."""
    import random
    extra = random.uniform(*JITTER_RANGE)
    time.sleep(base + extra)


def warm_up_session(session: requests.Session) -> bool:
    """
    Hit the public search-tool landing page first. This populates the session
    with whatever cookies CVM/Cloudflare set on first visit (cf_clearance,
    JSESSIONID, etc.) before we start querying the API. Mimics what a real
    browser does when a user opens the search tool and then clicks Search.

    Returns True if the warm-up completed without obvious blocks.
    """
    landing_url = "https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM"
    try:
        resp = session.get(landing_url, timeout=30)
        log.info(f"Warm-up: GET {landing_url} -> HTTP {resp.status_code}, "
                 f"cookies set: {len(session.cookies)}")
        # Real browser would now load assets, idle a bit, then hit the API
        jittered_sleep(base=3)
        return resp.status_code == 200
    except Exception as e:
        log.warning(f"Warm-up failed: {e}")
        return False


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
def _grid_params_cnpj(cnpj: str, cutoff_str: str) -> dict:
    """
    Build query params for a per-CNPJ grid query. CVM accepts both `cnpj` and
    `cnpjFundo` (sending both, mirroring CVM's own UI). When CNPJ is provided,
    tipoFundo is not required — the CNPJ uniquely identifies the fund.
    """
    return {
        **BASE_GRID_PARAMS,
        "cnpj":         cnpj,
        "cnpjFundo":    cnpj,
        "o[0][dataReferencia]": "desc",
        "dataInicial":  cutoff_str,
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


def scan_grid_for_cnpj(session: requests.Session, cnpj: str, ticker: str,
                       since_str: str, label: str) -> list[dict]:
    """
    Fetch all relatório gerencial entries for a single CNPJ since the cutoff.
    Returns the raw grid rows (typically 1-15 per fund for a 60-day window).
    Per-CNPJ queries are small enough that we don't need to paginate in
    practice, but we handle it defensively in case a fund publishes weekly.
    """
    docs: list[dict] = []
    offset = 0
    total = None

    while True:
        params = {
            **_grid_params_cnpj(cnpj, since_str),
            "s": offset,
            "l": PAGE_SIZE,
            "d": 1 if offset == 0 else 2,
        }
        try:
            data = fetch_grid_json(session, params)
        except Exception as e:
            log.error(f"  [{label}] {ticker} ({cnpj}): grid fetch failed: {e}")
            return docs

        if total is None:
            total = data.get("recordsTotal", 0)

        page = data.get("data", [])
        if not page:
            break
        docs.extend(page)
        offset += PAGE_SIZE
        if offset >= total:
            break
        jittered_sleep()

    return docs


def annotate_grid_rows(rows: list[dict], cnpj: str, ticker: str,
                       tipo_fundo: str, seen_doc_ids: set[int]) -> list[dict]:
    """Convert raw grid rows into the dict shape process_pass expects."""
    out = []
    for r in rows:
        try:
            doc_id = int(r.get("id"))
        except (TypeError, ValueError):
            continue
        if doc_id in seen_doc_ids:
            continue
        out.append({
            "doc_id": doc_id,
            "cnpj_fundo": cnpj,
            "ticker": ticker,
            "tipo_fundo": tipo_fundo,
            "data_referencia_raw": r.get("dataReferencia"),
            "data_entrega_raw": r.get("dataEntrega"),
            "versao": r.get("versao"),
            "nome_arquivo": r.get("nomePregao") or r.get("descricaoFundo"),
        })
    return out


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------
def existing_doc_ids() -> set[int]:
    """Return all doc_ids already in relatorios_gerenciais."""
    rows = query_all("SELECT doc_id FROM relatorios_gerenciais")
    return {int(r["doc_id"]) for r in rows}


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
    """
    Run a single FII or FIAGRO pass: for each CNPJ in the universe, query the
    CVM grid filtered to that CNPJ, then download any new docs. tipo_fundo_id
    is unused (kept for backward CLI compatibility); CVM identifies the fund
    uniquely from the CNPJ.
    """
    universe = load_universe(tipo_fundo=tipo_fundo)
    if not universe:
        log.warning(f"[{tipo_fundo}] No universe rows -- skipping. Did you run build_universe.py?")
        return {"label": tipo_fundo, "scanned": 0, "new_docs": 0, "downloaded": 0, "errors": 0}

    log.info(f"[{tipo_fundo}] Universe: {len(universe)} CNPJs -- scanning per fund since {since_str}")

    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)

    # Warm up the session against the public landing page before hitting the
    # API. Cloudflare often issues a session cookie on first page load that
    # subsequent API calls expect. Skipping this makes us look like a bot
    # that jumped straight to the JSON endpoint.
    warm_up_session(session)

    seen = existing_doc_ids()
    candidates: list[dict] = []
    total_funds = len(universe)

    # Discovery phase: hit the grid once per CNPJ
    for idx, (cnpj, info) in enumerate(universe.items(), start=1):
        ticker = info["ticker"]
        rows = scan_grid_for_cnpj(session, cnpj, ticker, since_str, tipo_fundo)
        new_for_fund = annotate_grid_rows(rows, cnpj, ticker, tipo_fundo, seen)
        candidates.extend(new_for_fund)
        log.info(
            f"[{tipo_fundo}] ({idx}/{total_funds}) {ticker}: "
            f"{len(rows)} docs in window, {len(new_for_fund)} new"
        )
        jittered_sleep()

    log.info(f"[{tipo_fundo}] Discovery complete: {len(candidates)} new docs to download")

    if not candidates:
        return {"label": tipo_fundo, "scanned": total_funds, "new_docs": 0,
                "downloaded": 0, "errors": 0}

    # Download phase
    downloaded = 0
    errors = 0
    total_to_fetch = len(candidates)

    for idx, doc in enumerate(candidates, start=1):
        doc_id = doc["doc_id"]
        ticker_safe = safe_ticker(doc["ticker"])
        dest = RELATORIOS_PATH / ticker_safe / f"{doc_id}.pdf"

        log.info(
            f"[{tipo_fundo}] download ({idx}/{total_to_fetch}) {doc['ticker']} "
            f"doc {doc_id} ({doc.get('data_referencia_raw') or 'n/d'})"
        )

        if dest.exists():
            log.info(f"  already on disk at {dest}, indexing only")
            ok = True
        else:
            ok = download_pdf(doc_id, dest, session)
            jittered_sleep()

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
    return {"label": tipo_fundo, "scanned": total_funds,
            "new_docs": len(candidates), "downloaded": downloaded, "errors": errors}


# ---------------------------------------------------------------------------
# Cleanup: prune PDFs whose agent processing succeeded
# ---------------------------------------------------------------------------
def prune_processed_pdfs() -> dict:
    """
    Belt-and-suspenders cleanup. The agent is supposed to delete PDFs after
    successful extraction and mark pdf_deleted_at. This function catches the
    case where processed_at was set but the file delete failed (or was never
    attempted). Idempotent — safe to run on every scraper invocation.

    Only deletes when:
      - processed_at IS NOT NULL (agent finished successfully)
      - pdf_deleted_at IS NULL   (we haven't already marked it deleted)

    Files in the universe that haven't been processed yet are left alone, so
    this never destroys data the agent still needs.
    """
    rows = query_all(
        """
        SELECT doc_id, ticker, pdf_path
        FROM relatorios_gerenciais
        WHERE processed_at IS NOT NULL
          AND pdf_deleted_at IS NULL
        """
    )
    if not rows:
        log.info("Cleanup: no PDFs to prune")
        return {"checked": 0, "deleted": 0, "missing": 0, "errors": 0}

    log.info(f"Cleanup: {len(rows)} processed PDFs flagged for removal")
    deleted = 0
    missing = 0
    errors = 0

    for r in rows:
        path = Path(r["pdf_path"])
        try:
            if path.exists():
                path.unlink()
                deleted += 1
            else:
                # Already gone from disk (manual cleanup, volume reset, etc).
                # We still want to mark pdf_deleted_at so we stop re-checking it.
                missing += 1
            execute(
                """
                UPDATE relatorios_gerenciais
                SET pdf_deleted_at = NOW()
                WHERE doc_id = %s
                """,
                (r["doc_id"],),
            )
        except Exception as e:
            log.warning(f"Cleanup: failed to remove {path}: {e}")
            errors += 1

    # Try to remove now-empty ticker directories
    if RELATORIOS_PATH.exists():
        for ticker_dir in RELATORIOS_PATH.iterdir():
            if ticker_dir.is_dir():
                try:
                    next(ticker_dir.iterdir())
                except StopIteration:
                    # Empty directory — safe to remove
                    try:
                        ticker_dir.rmdir()
                    except OSError:
                        pass

    log.info(f"Cleanup: deleted={deleted} missing_on_disk={missing} errors={errors}")
    return {"checked": len(rows), "deleted": deleted, "missing": missing, "errors": errors}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape relatórios gerenciais from CVM.")
    parser.add_argument("--fii-only", action="store_true")
    parser.add_argument("--fiagro-only", action="store_true")
    parser.add_argument("--since", help="Override retention cutoff (DD/MM/YYYY or YYYY-MM-DD)")
    parser.add_argument("--cleanup-only", action="store_true",
                        help="Skip scrape; only prune PDFs the agent has already processed")
    parser.add_argument("--no-cleanup", action="store_true",
                        help="Skip the end-of-run cleanup pass (for debugging)")
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
        if args.cleanup_only:
            prune_processed_pdfs()
            return 0

        if not args.fiagro_only:
            process_pass("FII", tipo_fundo_id=1, since_str=since_str)
            time.sleep(120)  # cooldown between passes (matches informe scraper pattern)
        if not args.fii_only:
            process_pass("FIAGRO", tipo_fundo_id=11, since_str=since_str)

        if not args.no_cleanup:
            prune_processed_pdfs()
    finally:
        close_pool()

    return 0


if __name__ == "__main__":
    sys.exit(main())
