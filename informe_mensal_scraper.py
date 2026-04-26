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
from datetime import datetime, timedelta

# Load .env so DATABASE_URL is available when running locally.
# On Railway this is a no-op (env vars are injected by the platform).
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

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
RETRY_DELAY   = 30   # was 5; CVM needs longer to cool down
REQUEST_DELAY = 1.5  # was 0.5; slower per-page rate to look less bot-like

# Hard retention window. We only fetch docs whose dataReferencia falls within
# this many months from today. Older filings are ignored entirely — they
# wouldn't get inserted, and existing rows older than this stay frozen.
RETENTION_MONTHS = 24

GRID_PARAMS = {
    "idCategoriaDocumento": 6,    # Informes Periódicos
    "idTipoDocumento":      40,   # Informe Mensal Estruturado
    "idEspecieDocumento":   0,
    "situacao":             "A",
    "isSession":            "false",
    "o[0][dataReferencia]": "desc",   # newest first (critical for incremental)
    # CVM-side date filter — sends only docs with dataReferencia >= this.
    # Format: DD/MM/YYYY. URL-encoded by requests at send time.
    "dataInicial":          "01/01/2024",
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


def to_bool(val):
    """Parse XML boolean strings ('true'/'false', '1'/'0', case-insensitive)."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ("true", "1", "sim", "s", "yes"):
        return True
    if s in ("false", "0", "nao", "não", "n", "no"):
        return False
    return None


def retention_cutoff_str() -> str:
    """
    Returns the retention cutoff date in 'DD/MM/YYYY' format, matching what
    CVM uses in dataReferencia. Docs older than this are skipped entirely.
    """
    cutoff_dt = datetime.now() - timedelta(days=RETENTION_MONTHS * 30)
    return cutoff_dt.strftime("%d/%m/%Y")


def is_within_retention(doc: dict, cutoff_str: str) -> bool:
    """
    Whether a CVM grid result falls within the retention window. Compares
    dataReferencia (DD/MM/YYYY format) against the cutoff string lexically
    — works because DD/MM/YYYY isn't lex-sortable, so we parse explicitly.
    """
    ref = (doc.get("dataReferencia") or "")[:10]
    if not ref or len(ref) < 10:
        return True  # be conservative — keep ambiguous rows
    try:
        ref_dt = datetime.strptime(ref, "%d/%m/%Y")
        cutoff_dt = datetime.strptime(cutoff_str, "%d/%m/%Y")
        return ref_dt >= cutoff_dt
    except ValueError:
        return True  # malformed date — keep it, let downstream handle


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
    resp = session.get(GRID_ENDPOINT, params=params, timeout=120)
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
    cutoff_str = retention_cutoff_str()
    log.info(f"Fetching documents from CVM API (retention cutoff: {cutoff_str})...")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36", "Accept": "application/json, text/plain, */*", "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"})
    all_docs: list = []
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
                if not is_within_retention(doc, cutoff_str):
                    done = True
                    break
                all_docs.append(doc)
            break
        except Exception as e:
            log.warning(f"First page attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                raise

    offset += PAGE_SIZE
    n_pages_total = (total + PAGE_SIZE - 1) // PAGE_SIZE
    grid_pbar = wrap(
        range(1, n_pages_total),  # we already fetched page 1 above
        desc="Scanning CVM grid",
        unit="page",
        total=n_pages_total - 1,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}",
    )

    grid_iter = iter(grid_pbar)

    while not done and offset < total:
        try:
            next(grid_iter)
        except StopIteration:
            break

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                params = {**GRID_PARAMS, "s": offset, "l": PAGE_SIZE,
                          "d": offset // PAGE_SIZE + 1}
                data = fetch_grid_json(session, params)
                for doc in data["data"]:
                    if not is_within_retention(doc, cutoff_str):
                        done = True
                        break
                    all_docs.append(doc)
                break
            except Exception as e:
                log.warning(f"Page error at offset {offset} attempt {attempt}/{MAX_RETRIES}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)

        if hasattr(grid_pbar, "set_postfix_str"):
            grid_pbar.set_postfix_str(f"docs: {len(all_docs):,}")

        if done:
            log.info(f"Reached retention cutoff {cutoff_str} — stopping grid scan")
            break

        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)

    if hasattr(grid_pbar, "close"):
        grid_pbar.close()

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
    cutoff_str = retention_cutoff_str()
    log.info(f"Incremental mode — fetching docs newer than ID {max_known_id:,} "
             f"(retention cutoff: {cutoff_str})...")
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
                if not is_within_retention(doc, cutoff_str):
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
                    if not is_within_retention(doc, cutoff_str):
                        done = True
                        break
                    new_docs.append(doc)
                break
            except Exception as e:
                log.warning(f"Page error at offset {offset} attempt {attempt}/{MAX_RETRIES}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)

        if done:
            log.info(f"Stopped grid scan (hit known ID or retention cutoff)")
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
    resp = session.get(url, timeout=120)
    resp.raise_for_status()

    raw = resp.text.strip().strip('"')
    if raw.startswith("<?xml") or raw.startswith("<"):
        xml_text = raw
    else:
        xml_text = base64.b64decode(raw).decode("utf-8")

    root = ET.fromstring(xml_text)

    investido_el = root.find(".//TotalInvestido")
    total_investido = to_float(
        investido_el.get("total") if investido_el is not None else None
    )

    return {
        # ----- IDENTITY
        "nome_fundo":                       _get(root, ".//NomeFundo"),
        "cnpj_fundo":                       _get(root, ".//CNPJFundo"),
        "codigo_isin":                      _get(root, ".//CodigoISIN"),
        "data_funcionamento":               parse_competencia(_get(root, ".//DataFuncionamento")),
        "publico_alvo":                     _get(root, ".//PublicoAlvo"),
        "fundo_exclusivo":                  to_bool(_get(root, ".//FundoExclusivo")),
        "prazo_duracao":                    _get(root, ".//PrazoDuracao"),
        "encerramento_exercicio":           _get(root, ".//EncerramentoExercicio"),

        "classificacao":                    _get(root, ".//Classificacao"),
        "subclassificacao":                 _get(root, ".//Subclassificacao"),
        "tipo_gestao":                      _get(root, ".//TipoGestao"),
        "segmento_atuacao":                 _get(root, ".//SegmentoAtuacao"),

        # Mercado de negociação (booleans)
        "mercado_bolsa":                    to_bool(_get(root, ".//Bolsa")),
        "mercado_bvmf":                     to_bool(_get(root, ".//BVMF")),
        "mercado_cetip":                    to_bool(_get(root, ".//CETIP")),
        "mercado_mb":                       to_bool(_get(root, ".//MB")),
        "mercado_mbo":                      to_bool(_get(root, ".//MBO")),

        # ----- ADMINISTRADOR (identity + contact)
        "nome_administrador":               _get(root, ".//NomeAdministrador"),
        "cnpj_administrador":               _get(root, ".//CNPJAdministrador"),
        "administrador_logradouro":         _get(root, ".//Logradouro"),
        "administrador_numero":             _get(root, ".//Numero"),
        "administrador_complemento":        _get(root, ".//Complemento"),
        "administrador_bairro":             _get(root, ".//Bairro"),
        "administrador_cidade":             _get(root, ".//Cidade"),
        "administrador_estado":             _get(root, ".//Estado"),
        "administrador_cep":                _get(root, ".//CEP"),
        "administrador_telefone":           _get(root, ".//Telefone1"),
        "administrador_email":              _get(root, ".//Email"),
        "administrador_site":               _get(root, ".//Site"),

        # ----- COMPETÊNCIA
        "competencia":                      parse_competencia(_get(root, ".//Competencia")),

        # ----- COTISTAS (number of unitholders by type)
        "pessoa_fisica":                    to_int(_get(root, ".//PessoaFisica")),
        "pj_nao_financeira":                to_int(_get(root, ".//PJNaoFinanceira")),
        "banco_comercial":                  to_int(_get(root, ".//BancoComercial")),
        "corretora_distribuidora":          to_int(_get(root, ".//CorretoraDistribuidora")),
        "outras_pj_financeiras":            to_int(_get(root, ".//OutrasPJFinanceiras")),
        "investidores_nao_residentes":      to_int(_get(root, ".//InvestidoresNaoResidentes")),
        "entidade_aberta_prev_compl":       to_int(_get(root, ".//EntidadeAbertaPrevCompl")),
        "entidade_fechada_prev_compl":      to_int(_get(root, ".//EntidadeFechadaPrevCompl")),
        "regime_proprio_prev":              to_int(_get(root, ".//RegimeProprioPrev")),
        "sociedade_seguradora":             to_int(_get(root, ".//SociedadeSeguradora")),
        "sociedade_cap_arrend_mercantil":   to_int(_get(root, ".//SociedadeCapArrendMercantil")),
        "fundos_inv_imobiliario":           to_int(_get(root, ".//FundosInvImobiliario")),
        "outros_fundos_inv":                to_int(_get(root, ".//OutrosFundosInv")),
        "cotistas_dist_fundo":              to_int(_get(root, ".//CotistasDistFundo")),
        "outros_tipos_cotistas":            to_int(_get(root, ".//OutrosTiposCotistas")),

        # ----- ITEMS 1-8 (financial summary)
        "ativo_total":                      to_float(_get(root, ".//Ativo")),
        "patrimonio_liquido":               to_float(_get(root, ".//PatrimonioLiquido")),
        "qtd_cotas_emitidas":               to_float(_get(root, ".//QtdCotasEmitidas")),
        "num_cotas_emitidas":               to_float(_get(root, ".//NumCotasEmitidas")),
        "valor_patr_cotas":                 to_float(_get(root, ".//ValorPatrCotas")),
        "despesas_tx_administracao":        to_float(_get(root, ".//DespesasTxAdministracao")),
        "despesas_ag_custodiante":          to_float(_get(root, ".//DespesasAgCustodiante")),
        "rent_patrimonial_mes":             to_float(_get(root, ".//RentPatrimonialMes")),
        "dividend_yield_mes":               to_float(_get(root, ".//DividendYieldMes")),
        "amortiz_acoes_cotas":              to_float(_get(root, ".//AmortizAcoesCotas")),

        # ----- ITEM 9 (liquidez)
        "disponibilidades":                 to_float(_get(root, ".//Disponibilidades")),
        "titulos_publicos":                 to_float(_get(root, ".//TitulosPublicos")),
        "titulos_privados":                 to_float(_get(root, ".//TitulosPrivados")),
        "fundos_renda_fixa":                to_float(_get(root, ".//FundosRendaFixa")),

        # ----- ITEM 10 (investimentos)
        "terrenos":                         to_float(_get(root, ".//Terrenos")),
        "imoveis_renda_acabados":           to_float(_get(root, ".//ImoveisRendaAcabados")),
        "imoveis_renda_construcao":         to_float(_get(root, ".//ImoveisRendaConstrucao")),
        "imoveis_venda_acabados":           to_float(_get(root, ".//ImoveisVendaAcabados")),
        "imoveis_venda_construcao":         to_float(_get(root, ".//ImoveisVendaConstrucao")),
        "outros_direitos_reais":            to_float(_get(root, ".//OutrosDireitosReais")),

        "acoes":                            to_float(_get(root, ".//Acoes")),
        "debentures":                       to_float(_get(root, ".//Debentures")),
        "bonus_subscricao":                 to_float(_get(root, ".//BonusSubscricao")),
        "certificados_dep_val_mob":         to_float(_get(root, ".//CertificadosDepositoValMob")),
        "fia":                              to_float(_get(root, ".//FIA")),
        "fip":                              to_float(_get(root, ".//FIP")),
        "fii":                              to_float(_get(root, ".//FII")),
        "fdic":                             to_float(_get(root, ".//FDIC")),
        "outras_cotas_fi":                  to_float(_get(root, ".//OutrasCotasFI")),
        "notas_promissorias":               to_float(_get(root, ".//NotasPromissorias")),
        "notas_comerciais":                 to_float(_get(root, ".//NotasComerciais")),
        "acoes_sociedades_ativ_fii":        to_float(_get(root, ".//AcoesSociedadesAtivFII")),
        "cotas_sociedades_ativ_fii":        to_float(_get(root, ".//CotasSociedadesAtivFII")),
        "cepac":                            to_float(_get(root, ".//CEPAC")),
        "cri_cra":                          to_float(_get(root, ".//CriCra")),
        "letras_hipotecarias":              to_float(_get(root, ".//LetrasHipotecarias")),
        "lci_lca":                          to_float(_get(root, ".//LciLca")),
        "lig":                              to_float(_get(root, ".//LIG")),
        "outros_valores_mobiliarios":       to_float(_get(root, ".//OutrosValoresMobliarios")),

        # ----- ITEM 11 (valores a receber)
        "contas_receber_alugueis":          to_float(_get(root, ".//ContasReceberAlugueis")),
        "contas_receber_venda_imov":        to_float(_get(root, ".//ContasReceberVendaImov")),
        "outros_valores_receber":           to_float(_get(root, ".//OutrosValoresReceber")),

        # Cross-cutting aggregates
        "alugueis":                         to_float(_get(root, ".//Alugueis")),
        "outros_valores":                   to_float(_get(root, ".//OutrosValores")),
        "total_investido":                  total_investido,

        # ----- ITEMS 12-22 (passivo)
        "rendimentos_distribuir":           to_float(_get(root, ".//RendimentosDistribuir")),
        "tx_administracao_pagar":           to_float(_get(root, ".//TxAdministracaoPagar")),
        "tx_performance_pagar":             to_float(_get(root, ".//TxPerformancePagar")),
        "obrigacoes_aquisicao_imov":        to_float(_get(root, ".//ObrigacoesAquisicaoImov")),
        "adiantamento_venda_imov":          to_float(_get(root, ".//AdiantamentoVendaImov")),
        "adiantamento_alugueis":            to_float(_get(root, ".//AdiantamentoAlugueis")),
        "obrigacoes_sec_recebiveis":        to_float(_get(root, ".//ObrigacoesSecRecebiveis")),
        "instrumentos_financeiros_deriv":   to_float(_get(root, ".//InstrumentosFinanceirosDeriv")),
        "provisoes_contingencias":          to_float(_get(root, ".//ProvisoesContigencias")),
        "outros_valores_pagar":             to_float(_get(root, ".//OutrosValoresPagar")),
        "provisoes_garantias":              to_float(_get(root, ".//ProvisoesGarantias")),
        "total_passivo":                    to_float(_get(root, ".//TotalPassivo")),

        # ----- ITEMS 23-25 (informações adicionais)
        "total_imoveis_onus":               to_float(_get(root, ".//TotalImoveisOnus")),
        "total_garantias_classe":           to_float(_get(root, ".//TotalGarantiasClasse")),
        "total_garantias_cotistas":         to_float(_get(root, ".//TotalGarantiasCotistas")),
    }


# ---------------------------------------------------------------------------
# Postgres writes
# ---------------------------------------------------------------------------
# Generated to match the schema in migration 007. ON CONFLICT DO UPDATE so
# re-fetching a doc (e.g., during a backfill or error retry) overwrites the
# row in place rather than failing.
INSERT_SQL = """
    INSERT INTO informe_mensal (
        id_documento,
        nome_fundo, cnpj_fundo, codigo_isin, data_funcionamento,
        publico_alvo, fundo_exclusivo, prazo_duracao, encerramento_exercicio,
        classificacao, subclassificacao, tipo_gestao, segmento_atuacao,
        mercado_bolsa, mercado_bvmf, mercado_cetip, mercado_mb, mercado_mbo,
        nome_administrador, cnpj_administrador,
        administrador_logradouro, administrador_numero, administrador_complemento,
        administrador_bairro, administrador_cidade, administrador_estado,
        administrador_cep, administrador_telefone, administrador_email,
        administrador_site,
        competencia,
        pessoa_fisica, pj_nao_financeira, banco_comercial, corretora_distribuidora,
        outras_pj_financeiras, investidores_nao_residentes,
        entidade_aberta_prev_compl, entidade_fechada_prev_compl,
        regime_proprio_prev, sociedade_seguradora, sociedade_cap_arrend_mercantil,
        fundos_inv_imobiliario, outros_fundos_inv, cotistas_dist_fundo,
        outros_tipos_cotistas,
        ativo_total, patrimonio_liquido, qtd_cotas_emitidas, num_cotas_emitidas,
        valor_patr_cotas, despesas_tx_administracao, despesas_ag_custodiante,
        rent_patrimonial_mes, dividend_yield_mes, amortiz_acoes_cotas,
        disponibilidades, titulos_publicos, titulos_privados, fundos_renda_fixa,
        terrenos, imoveis_renda_acabados, imoveis_renda_construcao,
        imoveis_venda_acabados, imoveis_venda_construcao, outros_direitos_reais,
        acoes, debentures, bonus_subscricao, certificados_dep_val_mob,
        fia, fip, fii, fdic, outras_cotas_fi, notas_promissorias, notas_comerciais,
        acoes_sociedades_ativ_fii, cotas_sociedades_ativ_fii, cepac, cri_cra,
        letras_hipotecarias, lci_lca, lig, outros_valores_mobiliarios,
        contas_receber_alugueis, contas_receber_venda_imov, outros_valores_receber,
        alugueis, outros_valores, total_investido,
        rendimentos_distribuir, tx_administracao_pagar, tx_performance_pagar,
        obrigacoes_aquisicao_imov, adiantamento_venda_imov, adiantamento_alugueis,
        obrigacoes_sec_recebiveis, instrumentos_financeiros_deriv,
        provisoes_contingencias, outros_valores_pagar, provisoes_garantias,
        total_passivo,
        total_imoveis_onus, total_garantias_classe, total_garantias_cotistas
    ) VALUES (
        %(id_documento)s,
        %(nome_fundo)s, %(cnpj_fundo)s, %(codigo_isin)s, %(data_funcionamento)s,
        %(publico_alvo)s, %(fundo_exclusivo)s, %(prazo_duracao)s, %(encerramento_exercicio)s,
        %(classificacao)s, %(subclassificacao)s, %(tipo_gestao)s, %(segmento_atuacao)s,
        %(mercado_bolsa)s, %(mercado_bvmf)s, %(mercado_cetip)s, %(mercado_mb)s, %(mercado_mbo)s,
        %(nome_administrador)s, %(cnpj_administrador)s,
        %(administrador_logradouro)s, %(administrador_numero)s, %(administrador_complemento)s,
        %(administrador_bairro)s, %(administrador_cidade)s, %(administrador_estado)s,
        %(administrador_cep)s, %(administrador_telefone)s, %(administrador_email)s,
        %(administrador_site)s,
        %(competencia)s,
        %(pessoa_fisica)s, %(pj_nao_financeira)s, %(banco_comercial)s, %(corretora_distribuidora)s,
        %(outras_pj_financeiras)s, %(investidores_nao_residentes)s,
        %(entidade_aberta_prev_compl)s, %(entidade_fechada_prev_compl)s,
        %(regime_proprio_prev)s, %(sociedade_seguradora)s, %(sociedade_cap_arrend_mercantil)s,
        %(fundos_inv_imobiliario)s, %(outros_fundos_inv)s, %(cotistas_dist_fundo)s,
        %(outros_tipos_cotistas)s,
        %(ativo_total)s, %(patrimonio_liquido)s, %(qtd_cotas_emitidas)s, %(num_cotas_emitidas)s,
        %(valor_patr_cotas)s, %(despesas_tx_administracao)s, %(despesas_ag_custodiante)s,
        %(rent_patrimonial_mes)s, %(dividend_yield_mes)s, %(amortiz_acoes_cotas)s,
        %(disponibilidades)s, %(titulos_publicos)s, %(titulos_privados)s, %(fundos_renda_fixa)s,
        %(terrenos)s, %(imoveis_renda_acabados)s, %(imoveis_renda_construcao)s,
        %(imoveis_venda_acabados)s, %(imoveis_venda_construcao)s, %(outros_direitos_reais)s,
        %(acoes)s, %(debentures)s, %(bonus_subscricao)s, %(certificados_dep_val_mob)s,
        %(fia)s, %(fip)s, %(fii)s, %(fdic)s, %(outras_cotas_fi)s,
        %(notas_promissorias)s, %(notas_comerciais)s,
        %(acoes_sociedades_ativ_fii)s, %(cotas_sociedades_ativ_fii)s, %(cepac)s, %(cri_cra)s,
        %(letras_hipotecarias)s, %(lci_lca)s, %(lig)s, %(outros_valores_mobiliarios)s,
        %(contas_receber_alugueis)s, %(contas_receber_venda_imov)s, %(outros_valores_receber)s,
        %(alugueis)s, %(outros_valores)s, %(total_investido)s,
        %(rendimentos_distribuir)s, %(tx_administracao_pagar)s, %(tx_performance_pagar)s,
        %(obrigacoes_aquisicao_imov)s, %(adiantamento_venda_imov)s, %(adiantamento_alugueis)s,
        %(obrigacoes_sec_recebiveis)s, %(instrumentos_financeiros_deriv)s,
        %(provisoes_contingencias)s, %(outros_valores_pagar)s, %(provisoes_garantias)s,
        %(total_passivo)s,
        %(total_imoveis_onus)s, %(total_garantias_classe)s, %(total_garantias_cotistas)s
    )
    ON CONFLICT (id_documento) DO UPDATE SET
        nome_fundo                       = EXCLUDED.nome_fundo,
        cnpj_fundo                       = EXCLUDED.cnpj_fundo,
        codigo_isin                      = EXCLUDED.codigo_isin,
        data_funcionamento               = EXCLUDED.data_funcionamento,
        publico_alvo                     = EXCLUDED.publico_alvo,
        fundo_exclusivo                  = EXCLUDED.fundo_exclusivo,
        prazo_duracao                    = EXCLUDED.prazo_duracao,
        encerramento_exercicio           = EXCLUDED.encerramento_exercicio,
        classificacao                    = EXCLUDED.classificacao,
        subclassificacao                 = EXCLUDED.subclassificacao,
        tipo_gestao                      = EXCLUDED.tipo_gestao,
        segmento_atuacao                 = EXCLUDED.segmento_atuacao,
        mercado_bolsa                    = EXCLUDED.mercado_bolsa,
        mercado_bvmf                     = EXCLUDED.mercado_bvmf,
        mercado_cetip                    = EXCLUDED.mercado_cetip,
        mercado_mb                       = EXCLUDED.mercado_mb,
        mercado_mbo                      = EXCLUDED.mercado_mbo,
        nome_administrador               = EXCLUDED.nome_administrador,
        cnpj_administrador               = EXCLUDED.cnpj_administrador,
        administrador_logradouro         = EXCLUDED.administrador_logradouro,
        administrador_numero             = EXCLUDED.administrador_numero,
        administrador_complemento        = EXCLUDED.administrador_complemento,
        administrador_bairro             = EXCLUDED.administrador_bairro,
        administrador_cidade             = EXCLUDED.administrador_cidade,
        administrador_estado             = EXCLUDED.administrador_estado,
        administrador_cep                = EXCLUDED.administrador_cep,
        administrador_telefone           = EXCLUDED.administrador_telefone,
        administrador_email              = EXCLUDED.administrador_email,
        administrador_site               = EXCLUDED.administrador_site,
        competencia                      = EXCLUDED.competencia,
        pessoa_fisica                    = EXCLUDED.pessoa_fisica,
        pj_nao_financeira                = EXCLUDED.pj_nao_financeira,
        banco_comercial                  = EXCLUDED.banco_comercial,
        corretora_distribuidora          = EXCLUDED.corretora_distribuidora,
        outras_pj_financeiras            = EXCLUDED.outras_pj_financeiras,
        investidores_nao_residentes      = EXCLUDED.investidores_nao_residentes,
        entidade_aberta_prev_compl       = EXCLUDED.entidade_aberta_prev_compl,
        entidade_fechada_prev_compl      = EXCLUDED.entidade_fechada_prev_compl,
        regime_proprio_prev              = EXCLUDED.regime_proprio_prev,
        sociedade_seguradora             = EXCLUDED.sociedade_seguradora,
        sociedade_cap_arrend_mercantil   = EXCLUDED.sociedade_cap_arrend_mercantil,
        fundos_inv_imobiliario           = EXCLUDED.fundos_inv_imobiliario,
        outros_fundos_inv                = EXCLUDED.outros_fundos_inv,
        cotistas_dist_fundo              = EXCLUDED.cotistas_dist_fundo,
        outros_tipos_cotistas            = EXCLUDED.outros_tipos_cotistas,
        ativo_total                      = EXCLUDED.ativo_total,
        patrimonio_liquido               = EXCLUDED.patrimonio_liquido,
        qtd_cotas_emitidas               = EXCLUDED.qtd_cotas_emitidas,
        num_cotas_emitidas               = EXCLUDED.num_cotas_emitidas,
        valor_patr_cotas                 = EXCLUDED.valor_patr_cotas,
        despesas_tx_administracao        = EXCLUDED.despesas_tx_administracao,
        despesas_ag_custodiante          = EXCLUDED.despesas_ag_custodiante,
        rent_patrimonial_mes             = EXCLUDED.rent_patrimonial_mes,
        dividend_yield_mes               = EXCLUDED.dividend_yield_mes,
        amortiz_acoes_cotas              = EXCLUDED.amortiz_acoes_cotas,
        disponibilidades                 = EXCLUDED.disponibilidades,
        titulos_publicos                 = EXCLUDED.titulos_publicos,
        titulos_privados                 = EXCLUDED.titulos_privados,
        fundos_renda_fixa                = EXCLUDED.fundos_renda_fixa,
        terrenos                         = EXCLUDED.terrenos,
        imoveis_renda_acabados           = EXCLUDED.imoveis_renda_acabados,
        imoveis_renda_construcao         = EXCLUDED.imoveis_renda_construcao,
        imoveis_venda_acabados           = EXCLUDED.imoveis_venda_acabados,
        imoveis_venda_construcao         = EXCLUDED.imoveis_venda_construcao,
        outros_direitos_reais            = EXCLUDED.outros_direitos_reais,
        acoes                            = EXCLUDED.acoes,
        debentures                       = EXCLUDED.debentures,
        bonus_subscricao                 = EXCLUDED.bonus_subscricao,
        certificados_dep_val_mob         = EXCLUDED.certificados_dep_val_mob,
        fia                              = EXCLUDED.fia,
        fip                              = EXCLUDED.fip,
        fii                              = EXCLUDED.fii,
        fdic                             = EXCLUDED.fdic,
        outras_cotas_fi                  = EXCLUDED.outras_cotas_fi,
        notas_promissorias               = EXCLUDED.notas_promissorias,
        notas_comerciais                 = EXCLUDED.notas_comerciais,
        acoes_sociedades_ativ_fii        = EXCLUDED.acoes_sociedades_ativ_fii,
        cotas_sociedades_ativ_fii        = EXCLUDED.cotas_sociedades_ativ_fii,
        cepac                            = EXCLUDED.cepac,
        cri_cra                          = EXCLUDED.cri_cra,
        letras_hipotecarias              = EXCLUDED.letras_hipotecarias,
        lci_lca                          = EXCLUDED.lci_lca,
        lig                              = EXCLUDED.lig,
        outros_valores_mobiliarios       = EXCLUDED.outros_valores_mobiliarios,
        contas_receber_alugueis          = EXCLUDED.contas_receber_alugueis,
        contas_receber_venda_imov        = EXCLUDED.contas_receber_venda_imov,
        outros_valores_receber           = EXCLUDED.outros_valores_receber,
        alugueis                         = EXCLUDED.alugueis,
        outros_valores                   = EXCLUDED.outros_valores,
        total_investido                  = EXCLUDED.total_investido,
        rendimentos_distribuir           = EXCLUDED.rendimentos_distribuir,
        tx_administracao_pagar           = EXCLUDED.tx_administracao_pagar,
        tx_performance_pagar             = EXCLUDED.tx_performance_pagar,
        obrigacoes_aquisicao_imov        = EXCLUDED.obrigacoes_aquisicao_imov,
        adiantamento_venda_imov          = EXCLUDED.adiantamento_venda_imov,
        adiantamento_alugueis            = EXCLUDED.adiantamento_alugueis,
        obrigacoes_sec_recebiveis        = EXCLUDED.obrigacoes_sec_recebiveis,
        instrumentos_financeiros_deriv   = EXCLUDED.instrumentos_financeiros_deriv,
        provisoes_contingencias          = EXCLUDED.provisoes_contingencias,
        outros_valores_pagar             = EXCLUDED.outros_valores_pagar,
        provisoes_garantias              = EXCLUDED.provisoes_garantias,
        total_passivo                    = EXCLUDED.total_passivo,
        total_imoveis_onus               = EXCLUDED.total_imoveis_onus,
        total_garantias_classe           = EXCLUDED.total_garantias_classe,
        total_garantias_cotistas         = EXCLUDED.total_garantias_cotistas
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
