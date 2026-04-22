"""
informe_mensal_scraper.py — FII Guia (Postgres edition)
Scrapes B3/CVM's Informe Mensal Estruturado API and writes to the
`informe_mensal` Postgres table.

Usage:
    # First full run
    python informe_mensal_scraper.py

    # Resume after a crash (skip already-loaded docs, full grid scan)
    python informe_mensal_scraper.py --resume

    # Only retry docs in the erros table (no grid scan)
    python informe_mensal_scraper.py --retry-errors

    # Monthly update — only fetch docs newer than max(id_documento) in DB
    python informe_mensal_scraper.py --incremental

Env:
    DATABASE_URL  required — Postgres connection string (Railway provides)
"""

from __future__ import annotations

import argparse
import base64
import logging
import time
import xml.etree.ElementTree as ET
from datetime import datetime

import requests

from db import connection, execute, init_pool, close_pool, query_all, query_one

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
API_BASE      = "https://fnet.bmfbovespa.com.br/fnet/publico"
GRID_ENDPOINT = f"{API_BASE}/pesquisarGerenciadorDocumentosDados"
DL_ENDPOINT   = f"{API_BASE}/downloadDocumento"
PAGE_SIZE     = 100
MAX_RETRIES   = 3
RETRY_DELAY   = 5
REQUEST_DELAY = 0.5

GRID_PARAMS = {
    "idCategoriaDocumento": 6,    # Informes Periódicos
    "idTipoDocumento":      40,   # Informe Mensal Estruturado
    "idEspecieDocumento":   0,
    "situacao":             "A",
    "isSession":            "false",
    "o[0][dataReferencia]": "desc",   # newest first (critical for incremental)
}

# ---------------------------------------------------------------------------
# Logging — Railway captures stdout, so a file handler is optional but nice
# to keep when running locally
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers — XML parsing
# ---------------------------------------------------------------------------
def _get(root, path):
    el = root.find(path)
    return el.text.strip() if el is not None and el.text else None


def to_float(val):
    try:
        return float(val) if val else None
    except (ValueError, TypeError):
        return None


def to_int(val):
    try:
        return int(val) if val else None
    except (ValueError, TypeError):
        return None


def parse_competencia(val: str | None):
    """CVM sends competencia as 'YYYY-MM-DD' string; we want a date."""
    if not val:
        return None
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# HTTP helper with informative errors
# ---------------------------------------------------------------------------
def fetch_grid_json(session: requests.Session, params: dict) -> dict:
    """
    GET the CVM grid endpoint and return parsed JSON. Raises with a helpful
    error message if the response is HTML, empty, or otherwise non-JSON.
    CVM occasionally serves WAF challenges or maintenance pages; those are
    worth seeing in the logs.
    """
    resp = session.get(GRID_ENDPOINT, params=params, timeout=60)
    try:
        return resp.json()
    except ValueError as e:
        # Not JSON. Log what we actually got so we can tell if it's a WAF
        # page, a maintenance HTML, or empty body.
        body = (resp.text or "").strip()
        preview = body[:300].replace("\n", " ")
        raise RuntimeError(
            f"CVM returned non-JSON: status={resp.status_code} "
            f"content-type={resp.headers.get('Content-Type', 'unknown')} "
            f"body_len={len(body)} preview={preview!r}"
        ) from e


# ---------------------------------------------------------------------------
# Grid fetch — full mode
# ---------------------------------------------------------------------------
def fetch_all_document_ids(resume_ids: set) -> list:
    log.info("Fetching full document list from B3 API...")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36", "Accept": "application/json, text/plain, */*", "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"})
    all_docs: list = []
    offset = 0
    total = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            params = {**GRID_PARAMS, "s": 0, "l": PAGE_SIZE, "d": 1}
            data = fetch_grid_json(session, params)
            total = data["recordsFiltered"]
            all_docs.extend(data["data"])
            log.info(f"Total documents on B3: {total:,}")
            break
        except Exception as e:
            log.warning(f"First page attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                raise

    offset += PAGE_SIZE
    while offset < total:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                params = {**GRID_PARAMS, "s": offset, "l": PAGE_SIZE,
                          "d": offset // PAGE_SIZE + 1}
                data = fetch_grid_json(session, params)
                batch = data["data"]
                all_docs.extend(batch)
                break
            except Exception as e:
                log.warning(f"Page error at offset {offset} attempt {attempt}/{MAX_RETRIES}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)

    if resume_ids:
        before = len(all_docs)
        all_docs = [d for d in all_docs if d["id"] not in resume_ids]
        log.info(f"Skipping {before - len(all_docs):,} already processed docs")

    log.info(f"Documents to process: {len(all_docs):,}")
    return all_docs


# ---------------------------------------------------------------------------
# Grid fetch — incremental mode (stops at first known ID)
# ---------------------------------------------------------------------------
def fetch_incremental_document_ids(max_known_id: int) -> list:
    log.info(f"Incremental mode — fetching docs newer than ID {max_known_id:,}...")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36", "Accept": "application/json, text/plain, */*", "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"})
    new_docs: list = []
    offset = 0
    total = None
    done = False

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            params = {**GRID_PARAMS, "s": 0, "l": PAGE_SIZE, "d": 1}
            data = fetch_grid_json(session, params)
            total = data["recordsFiltered"]
            log.info(f"Total documents on B3: {total:,}")
            for doc in data["data"]:
                if doc["id"] <= max_known_id:
                    done = True
                    break
                new_docs.append(doc)
            break
        except Exception as e:
            log.warning(f"First page attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                raise

    offset += PAGE_SIZE
    while not done and offset < total:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                params = {**GRID_PARAMS, "s": offset, "l": PAGE_SIZE,
                          "d": offset // PAGE_SIZE + 1}
                data = fetch_grid_json(session, params)
                batch = data["data"]
                for doc in batch:
                    if doc["id"] <= max_known_id:
                        done = True
                        break
                    new_docs.append(doc)
                break
            except Exception as e:
                log.warning(f"Page error at offset {offset} attempt {attempt}/{MAX_RETRIES}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)

        if done:
            log.info(f"Reached known ID {max_known_id:,} — stopping grid scan")
            break

        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)

    log.info(f"New documents to download: {len(new_docs):,}")
    return new_docs


# ---------------------------------------------------------------------------
# Download and parse one document
# ---------------------------------------------------------------------------
def download_and_parse(doc_id: int, session: requests.Session) -> dict:
    url = f"{DL_ENDPOINT}?id={doc_id}"
    resp = session.get(url, timeout=60)
    resp.raise_for_status()

    raw = resp.text.strip().strip('"')
    if raw.startswith("<?xml") or raw.startswith("<"):
        xml_text = raw
    else:
        xml_text = base64.b64decode(raw).decode("utf-8")

    root = ET.fromstring(xml_text)

    cotistas_el = root.find(".//Cotistas")
    total_cotistas = to_int(
        cotistas_el.get("total") if cotistas_el is not None else None
    )

    investido_el = root.find(".//TotalInvestido")
    total_investido = to_float(
        investido_el.get("total") if investido_el is not None else None
    )

    return {
        "nome_fundo":             _get(root, ".//NomeFundo"),
        "cnpj_fundo":             _get(root, ".//CNPJFundo"),
        "data_funcionamento":     parse_competencia(_get(root, ".//DataFuncionamento")),
        "publico_alvo":           _get(root, ".//PublicoAlvo"),
        "classificacao":          _get(root, ".//Classificacao"),
        "subclassificacao":       _get(root, ".//Subclassificacao"),
        "tipo_gestao":            _get(root, ".//TipoGestao"),
        "nome_administrador":     _get(root, ".//NomeAdministrador"),
        "cnpj_administrador":     _get(root, ".//CNPJAdministrador"),
        "competencia":            parse_competencia(_get(root, ".//Competencia")),
        "total_cotistas":         total_cotistas,
        "pessoa_fisica":          to_int(_get(root, ".//PessoaFisica")),
        "ativo_total":            to_float(_get(root, ".//Ativo")),
        "patrimonio_liquido":     to_float(_get(root, ".//PatrimonioLiquido")),
        "num_cotas_emitidas":     to_float(_get(root, ".//NumCotasEmitidas")),
        "valor_patr_cotas":       to_float(_get(root, ".//ValorPatrCotas")),
        "despesas_tx_adm":        to_float(_get(root, ".//DespesasTxAdministracao")),
        "rent_patr_mensal":       to_float(_get(root, ".//RentPatrimonialMes")),
        "dividend_yield_mes":     to_float(_get(root, ".//DividendYieldMes")),
        "total_investido":        total_investido,
        "imoveis_renda":          to_float(_get(root, ".//ImoveisRendaAcabados")),
        "titulos_privados":       to_float(_get(root, ".//TitulosPrivados")),
        "fundos_renda_fixa":      to_float(_get(root, ".//FundosRendaFixa")),
        "cri_cra":                to_float(_get(root, ".//CriCra")),
        "total_passivo":          to_float(_get(root, ".//TotalPassivo")),
        "rendimentos_distribuir": to_float(_get(root, ".//RendimentosDistribuir")),
    }


# ---------------------------------------------------------------------------
# Postgres writes
# ---------------------------------------------------------------------------
INSERT_SQL = """
    INSERT INTO informe_mensal (
        id_documento, nome_fundo, cnpj_fundo, data_funcionamento, publico_alvo,
        classificacao, subclassificacao, tipo_gestao, nome_administrador,
        cnpj_administrador, competencia, total_cotistas, pessoa_fisica,
        ativo_total, patrimonio_liquido, num_cotas_emitidas, valor_patr_cotas,
        despesas_tx_adm, rent_patr_mensal, dividend_yield_mes, total_investido,
        imoveis_renda, titulos_privados, fundos_renda_fixa, cri_cra,
        total_passivo, rendimentos_distribuir
    ) VALUES (
        %(id_documento)s, %(nome_fundo)s, %(cnpj_fundo)s, %(data_funcionamento)s, %(publico_alvo)s,
        %(classificacao)s, %(subclassificacao)s, %(tipo_gestao)s, %(nome_administrador)s,
        %(cnpj_administrador)s, %(competencia)s, %(total_cotistas)s, %(pessoa_fisica)s,
        %(ativo_total)s, %(patrimonio_liquido)s, %(num_cotas_emitidas)s, %(valor_patr_cotas)s,
        %(despesas_tx_adm)s, %(rent_patr_mensal)s, %(dividend_yield_mes)s, %(total_investido)s,
        %(imoveis_renda)s, %(titulos_privados)s, %(fundos_renda_fixa)s, %(cri_cra)s,
        %(total_passivo)s, %(rendimentos_distribuir)s
    )
    ON CONFLICT (id_documento) DO UPDATE SET
        nome_fundo             = EXCLUDED.nome_fundo,
        cnpj_fundo             = EXCLUDED.cnpj_fundo,
        data_funcionamento     = EXCLUDED.data_funcionamento,
        publico_alvo           = EXCLUDED.publico_alvo,
        classificacao          = EXCLUDED.classificacao,
        subclassificacao       = EXCLUDED.subclassificacao,
        tipo_gestao            = EXCLUDED.tipo_gestao,
        nome_administrador     = EXCLUDED.nome_administrador,
        cnpj_administrador     = EXCLUDED.cnpj_administrador,
        competencia            = EXCLUDED.competencia,
        total_cotistas         = EXCLUDED.total_cotistas,
        pessoa_fisica          = EXCLUDED.pessoa_fisica,
        ativo_total            = EXCLUDED.ativo_total,
        patrimonio_liquido     = EXCLUDED.patrimonio_liquido,
        num_cotas_emitidas     = EXCLUDED.num_cotas_emitidas,
        valor_patr_cotas       = EXCLUDED.valor_patr_cotas,
        despesas_tx_adm        = EXCLUDED.despesas_tx_adm,
        rent_patr_mensal       = EXCLUDED.rent_patr_mensal,
        dividend_yield_mes     = EXCLUDED.dividend_yield_mes,
        total_investido        = EXCLUDED.total_investido,
        imoveis_renda          = EXCLUDED.imoveis_renda,
        titulos_privados       = EXCLUDED.titulos_privados,
        fundos_renda_fixa      = EXCLUDED.fundos_renda_fixa,
        cri_cra                = EXCLUDED.cri_cra,
        total_passivo          = EXCLUDED.total_passivo,
        rendimentos_distribuir = EXCLUDED.rendimentos_distribuir
"""


def insert_parsed(doc_id: int, parsed: dict) -> None:
    execute(INSERT_SQL, {"id_documento": doc_id, **parsed})


def record_error(doc_id: int, motivo: str) -> None:
    """Append an error row. The erros table is an append-only audit log
    (BIGSERIAL id), so multiple retries create multiple rows — that history
    is useful for forensics. On success, we clear prior errors for this doc."""
    execute(
        "INSERT INTO erros (id_documento, motivo, registrado_em) VALUES (%s, %s, now())",
        (doc_id, motivo[:1000]),  # truncate huge tracebacks
    )


def clear_error(doc_id: int) -> None:
    execute("DELETE FROM erros WHERE id_documento = %s", (doc_id,))


def loaded_doc_ids() -> set[int]:
    rows = query_all("SELECT id_documento FROM informe_mensal")
    return {r["id_documento"] for r in rows}


def errored_doc_ids() -> list[int]:
    rows = query_all(
        "SELECT DISTINCT id_documento FROM erros WHERE id_documento IS NOT NULL"
    )
    return [r["id_documento"] for r in rows]


def max_loaded_id() -> int:
    row = query_one("SELECT MAX(id_documento) AS m FROM informe_mensal")
    return (row["m"] if row and row["m"] else 0) or 0


# ---------------------------------------------------------------------------
# Progress bar helper (tqdm is optional; fallback is a no-op iterator)
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
def scrape(resume: bool = False, retry_errors: bool = False, incremental: bool = False):
    if retry_errors:
        doc_ids = errored_doc_ids()
        log.info(f"Retry-errors mode — {len(doc_ids):,} failed docs to retry")
        docs = [{"id": doc_id} for doc_id in doc_ids]

    elif incremental:
        max_id = max_loaded_id()
        if max_id == 0:
            log.warning("DB is empty — falling back to full scrape")
            docs = fetch_all_document_ids(set())
        else:
            log.info(f"Highest ID in DB: {max_id:,}")
            docs = fetch_incremental_document_ids(max_id)

    else:
        resume_ids: set[int] = set()
        if resume:
            resume_ids |= loaded_doc_ids()
            resume_ids |= set(errored_doc_ids())
            log.info(f"Resume mode — {len(resume_ids):,} docs already processed")
        docs = fetch_all_document_ids(resume_ids)

    if not docs:
        log.info("Nothing to do — DB is already up to date.")
        return

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36", "Accept": "application/json, text/plain, */*", "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"})
    success = 0
    failed = 0

    bar_format = "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}"
    pbar = wrap(docs, desc="Downloading informes", unit="doc", bar_format=bar_format)

    for doc in pbar:
        doc_id = doc["id"]
        last_error = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                parsed = download_and_parse(doc_id, session)
                insert_parsed(doc_id, parsed)
                clear_error(doc_id)   # successful -> remove any prior error rows
                success += 1
                last_error = None
                break
            except Exception as e:
                last_error = str(e)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)

        if last_error:
            record_error(doc_id, last_error)
            failed += 1
            log.error(f"Doc {doc_id} failed after {MAX_RETRIES} attempts — {last_error}")

        if hasattr(pbar, "set_postfix_str"):
            pbar.set_postfix_str(f"OK:{success:,} ERR:{failed:,}")
        time.sleep(REQUEST_DELAY)

    log.info(f"Done — {success:,} inserted/updated, {failed:,} failed")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape B3 Informe Mensal Estruturado -> Postgres")
    parser.add_argument("--resume",       action="store_true", help="Skip already processed docs (full grid scan)")
    parser.add_argument("--retry-errors", action="store_true", help="Only retry docs in the erros table")
    parser.add_argument("--incremental",  action="store_true", help="Only fetch docs newer than max ID in DB")
    parser.add_argument("--full",         action="store_true", help="Force full scrape (no incremental cutoff)")
    args = parser.parse_args()

    # Mutually exclusive check
    mode_flags = sum([args.resume, args.retry_errors, args.incremental, args.full])
    if mode_flags > 1:
        parser.error("Choose at most one of --resume, --retry-errors, --incremental, --full")
    # Default mode (no flags) = incremental — the right default for cron runs
    if mode_flags == 0:
        args.incremental = True
        log.info("No mode specified — defaulting to --incremental")

    init_pool()
    start = datetime.now()
    try:
        scrape(
            resume=args.resume,
            retry_errors=args.retry_errors,
            incremental=args.incremental,
        )
    finally:
        close_pool()

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"Total time: {elapsed/60:.1f} minutes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
