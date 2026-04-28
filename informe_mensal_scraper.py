"""
informe_mensal_scraper.py — FII Guia (dual-schema bronze→gold pipeline)

Scrapes B3/CVM's Informe Mensal Estruturado API for two fund types:
  - FII    (tipoFundo=1)  — CVM 571/2015 schema, root <DadosEconomicoFinanceiros>
  - FIAGRO (tipoFundo=11) — CVM 175/2022 Anexo VI schema, root <DOC_ARQ>

Pipeline:
  1. Truncate both staging tables (clears previous run's data — "safer truncate")
  2. Scrape FII pass:    grid query with tipoFundo=1, parse with parse_fii(),
     insert into informe_mensal_staging_fii
  3. Scrape FIAGRO pass: grid query with tipoFundo=11, parse with parse_fiagro(),
     insert into informe_mensal_staging_fiagro
  4. Consolidate: read both staging tables, upsert into informe_mensal (gold)
  5. Done. Staging stays populated until next run's step 1 truncate.

Usage:
    # Full scrape (both passes)
    python informe_mensal_scraper.py

    # Only FII pass
    python informe_mensal_scraper.py --fii-only

    # Only FIAGRO pass
    python informe_mensal_scraper.py --fiagro-only

    # Skip the scrape entirely, just re-consolidate from current staging
    # (useful when consolidation failed but staging is intact)
    python informe_mensal_scraper.py --consolidate-only

    # Skip consolidation step (load staging only, useful for debugging)
    python informe_mensal_scraper.py --no-consolidate

    # Reduce CVM hits when only newer docs are needed (incremental mode)
    python informe_mensal_scraper.py --incremental

    # Retry only docs that previously errored (the erros table)
    python informe_mensal_scraper.py --retry-errors

Env:
    DATABASE_URL  required — Postgres connection string
"""

from __future__ import annotations

import argparse
import base64
import json as _json
import logging
import sys
import time
from datetime import datetime, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import requests

from db import connection, execute, init_pool, close_pool, query_all, query_one
from informe_parsers import parse_fii, parse_fiagro

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

BASE_GRID_PARAMS = {
    "idCategoriaDocumento": 6,
    "idTipoDocumento":      40,
    "idEspecieDocumento":   0,
    "situacao":             "A",
    "isSession":            "false",
    "o[0][dataReferencia]": "desc",
}

TIPO_FII    = 1
TIPO_FIAGRO = 11

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(), logging.FileHandler("informe_mensal_scraper.log")],
)
log = logging.getLogger(__name__)

try:
    from tqdm import tqdm
    def wrap(it, **kwargs):
        return tqdm(it, **kwargs)
except ImportError:
    def wrap(it, **kwargs):
        return it


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def retention_cutoff_str() -> str:
    return (datetime.now() - timedelta(days=RETENTION_DAYS)).strftime("%d/%m/%Y")


def _grid_params(tipo_fundo: int, cutoff_str: str) -> dict:
    return {**BASE_GRID_PARAMS, "tipoFundo": tipo_fundo, "dataInicial": cutoff_str}


def is_within_retention(doc: dict, cutoff_str: str) -> bool:
    ref = (doc.get("dataReferencia") or "")
    if not ref:
        return True
    try:
        if "/" in ref:
            parts = ref.split("/")
            if len(parts) == 3:
                ref_dt = datetime.strptime(ref[:10], "%d/%m/%Y")
            elif len(parts) == 2:
                ref_dt = datetime.strptime(f"01/{ref}", "%d/%m/%Y")
            else:
                return True
        else:
            return True
        cutoff_dt = datetime.strptime(cutoff_str, "%d/%m/%Y")
        return ref_dt >= cutoff_dt
    except ValueError:
        return True


# ---------------------------------------------------------------------------
# CVM grid + download
# ---------------------------------------------------------------------------
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
                log.warning(f"  grid retry {attempt}/{MAX_RETRIES} — {e}")
                time.sleep(RETRY_DELAY)
    raise RuntimeError(f"grid fetch exhausted retries: {last_exc}")


def fetch_document_ids(tipo_fundo: int, label: str,
                       resume_ids: set | None = None,
                       max_known_id: int | None = None) -> list[dict]:
    cutoff_str = retention_cutoff_str()
    log.info(f"[{label}] Scanning CVM grid since {cutoff_str}...")

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; FIIGuiaBot/1.0)"})

    docs: list[dict] = []
    offset = 0
    total = None
    grid_bar = None  # initialized after first response (when total is known)

    try:
        while True:
            params = {
                **_grid_params(tipo_fundo, cutoff_str),
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
                # Initialize a manual-update tqdm bar (we update by page size)
                if total > 0:
                    try:
                        from tqdm import tqdm
                        grid_bar = tqdm(total=total, desc=f"{label} grid scan",
                                        unit="doc", leave=False)
                    except ImportError:
                        grid_bar = None

            page = data.get("data", [])
            if not page:
                break

            page_below_max = 0
            for d in page:
                if not is_within_retention(d, cutoff_str):
                    continue
                doc_id = d.get("id")
                if doc_id is None:
                    continue
                if resume_ids and doc_id in resume_ids:
                    continue
                if max_known_id is not None and doc_id <= max_known_id:
                    page_below_max += 1
                    continue
                docs.append({"id": doc_id, "raw": d})

            if grid_bar is not None:
                grid_bar.update(len(page))
                grid_bar.set_postfix(found=len(docs))

            offset += PAGE_SIZE
            if max_known_id is not None and page_below_max == len(page):
                log.info(f"[{label}] All page IDs <= max_known {max_known_id} — stopping")
                break
            if offset >= total:
                break
            time.sleep(REQUEST_DELAY)
    finally:
        if grid_bar is not None:
            grid_bar.close()

    log.info(f"[{label}] {len(docs):,} new docs to fetch")
    return docs


def download_xml(doc_id: int, session: requests.Session) -> str | None:
    """
    Download one CVM document and return its XML text.

    CVM responses come in three flavors:
      1. Raw XML — starts with '<?xml' or '<'
      2. JSON-wrapped base64 — body is a quoted string '"<base64>"' that
         decodes to XML (some endpoints, varies by doc)
      3. Empty / error HTML
    Returns text or None on failure.
    """
    url = f"{DL_ENDPOINT}?id={doc_id}&saveAs=true"
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=120)
            if resp.status_code != 200:
                raise RuntimeError(f"download HTTP {resp.status_code}")

            raw = (resp.text or "").strip()
            if not raw:
                raise ValueError("empty response body")

            # JSON-wrapped base64 (starts with quote)
            if raw.startswith('"'):
                b64 = _json.loads(raw)
                b64 += "=" * (-len(b64) % 4)  # padding
                xml_text = base64.b64decode(b64).decode("utf-8")
            # Raw XML
            elif raw.startswith("<?xml") or raw.startswith("<"):
                xml_text = raw
            else:
                ct = resp.headers.get("Content-Type", "")
                raise ValueError(
                    f"unknown response format (Content-Type={ct!r}, "
                    f"body[:200]={raw[:200]!r})"
                )

            # Strip BOM and leading whitespace
            xml_text = xml_text.lstrip("\ufeff\r\n \t")
            return xml_text

        except Exception as e:
            last_exc = e
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    log.error(f"  doc {doc_id}: download exhausted retries: {last_exc}")
    return None


# ---------------------------------------------------------------------------
# Staging INSERT statements (built dynamically from column lists)
# ---------------------------------------------------------------------------
def _build_insert_sql(table: str, columns: list[str]) -> str:
    cols_csv = ", ".join(columns)
    placeholders = ", ".join(f"%({c})s" for c in columns)
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != "id_documento")
    return f"""
        INSERT INTO {table} ({cols_csv})
        VALUES ({placeholders})
        ON CONFLICT (id_documento) DO UPDATE SET {update_clause}
    """


FII_STAGING_COLS = [
    "id_documento",
    "nome_fundo", "cnpj_fundo", "codigo_isin", "data_funcionamento", "publico_alvo",
    "fundo_exclusivo", "prazo_duracao", "encerramento_exercicio",
    "classificacao", "subclassificacao", "tipo_gestao", "segmento_atuacao",
    "vinculo_familiar_cotistas",
    "bolsa", "bvmf", "cetip", "mb", "mbo",
    "nome_administrador", "cnpj_administrador",
    "logradouro", "numero", "complemento", "bairro", "cidade", "estado", "cep",
    "telefone", "email", "site",
    "competencia",
    "pessoa_fisica", "pj_nao_financeira", "banco_comercial", "corretora_distribuidora",
    "outras_pj_financeiras", "investidores_nao_residentes",
    "entidade_aberta_prev_compl", "entidade_fechada_prev_compl",
    "regime_proprio_prev", "sociedade_seguradora", "sociedade_cap_arrend_mercantil",
    "fundos_inv_imobiliario", "outros_fundos_inv", "cotistas_dist_fundo",
    "outros_tipos_cotistas",
    "ativo", "patrimonio_liquido", "qtd_cotas_emitidas", "num_cotas_emitidas",
    "valor_patr_cotas", "despesas_tx_administracao", "despesas_ag_custodiante",
    "rent_patrimonial_mes", "dividend_yield_mes", "amortiz_acoes_cotas",
    "disponibilidades", "titulos_publicos", "titulos_privados", "fundos_renda_fixa",
    "terrenos", "imoveis_renda_acabados", "imoveis_renda_construcao",
    "imoveis_venda_acabados", "imoveis_venda_construcao", "outros_direitos_reais",
    "acoes", "debentures", "bonus_subscricao", "certificados_dep_val_mob",
    "fia", "fip", "fii", "fdic", "outras_cotas_fi",
    "notas_promissorias", "notas_comerciais",
    "acoes_sociedades_ativ_fii", "cotas_sociedades_ativ_fii", "cepac", "cri_cra",
    "letras_hipotecarias", "lci_lca", "lig", "outros_valores_mobiliarios",
    "contas_receber_alugueis", "contas_receber_venda_imov", "outros_valores_receber",
    "alugueis", "outros_valores", "total_investido",
    "rendimentos_distribuir", "tx_administracao_pagar", "tx_performance_pagar",
    "obrigacoes_aquisicao_imov", "adiantamento_venda_imov", "adiantamento_alugueis",
    "obrigacoes_sec_recebiveis", "instrumentos_financeiros_deriv",
    "provisoes_contingencias", "outros_valores_pagar", "provisoes_garantias",
    "total_passivo",
    "total_imoveis_onus", "total_garantias_classe", "total_garantias_cotistas",
]

FIAGRO_STAGING_COLS = [
    "id_documento",
    "nm_fundo", "nr_cnpj_fundo", "nm_classe", "nr_cnpj_classe",
    "dt_regs_func", "tp_publ_alvo", "cd_isin", "class_unica",
    "classif_auto_regul", "class_regr_outr_anexo",
    "przo_duracao", "dt_encer_exerc_soc", "cotst_vincl_famil",
    "dt_compt", "versao",
    "nm_adm", "nr_cnpj_adm", "nm_gestor", "nr_cnpj_gestor",
    "email_adm", "site", "serv_atend_cotst", "entid_adm_merc_org", "merc_negoc",
    "qtd_tot_cotst", "qtd_pess_natural", "qtd_pess_jurid_exct_financ",
    "qtd_pess_jurid_financ", "qtd_invest_nao_resid",
    "qtd_entid_prev_compl_exct_rpps", "qtd_entid_rpps", "qtd_socied_segur_resegur",
    "qtd_fundos_invst", "qtd_outro_tipo_cotst", "qtd_cotst_distrib_conta_ordem",
    "nr_cot_emitidas", "nr_cot_nm_subclasse", "nr_cot_subclasse",
    "vl_pl_nm_subclasse", "vl_pl_subclasse",
    "vl_ativo", "vl_patrimonio_liquido", "vl_patrimonio_liquido_cotas",
    "vl_desp_tx_admin_rel_pl_mes", "vl_desp_tx_gest_rel_pl_mes",
    "vl_desp_tx_distrib_rel_pl_mes", "vl_rentb_efetiv_mes", "vl_rent_patrim_mes_ref",
    "vl_dividend_yield_mes_ref", "perc_amort_cotst_mes_ref",
    "vl_total_mantido_neces_liq", "vl_ativ_finan", "vl_ativ_finan_lato_sensu",
    "vl_ativ_finan_emis_inst_finan", "vl_outr_ativ_finan", "vl_outr_ativ_emis_intst_finan",
    "vl_total_invest", "vl_direit_cred_agro", "vl_demais_direit_cred",
    "vl_cert_receb_cri", "vl_cert_receb_cra", "vl_cdca", "vl_cda_warrant",
    "vl_cert_dep_agro_cda_warr_wa", "vl_cert_dir_cred_agro_cdca",
    "vl_cpr", "vl_cpr_finan", "vl_cpr_fisica",
    "vl_cred_carbono_agro", "vl_cbio_cred_descarbon",
    "vl_direit_imov_rural", "vl_invest_imov_rural", "vl_outr_tit_cred_agro",
    "vl_lca", "vl_lci",
    "vl_debent", "vl_debent_conv", "vl_debent_nao_conv",
    "vl_nota_comerc", "vl_nota_comerc_curto_przo", "vl_nota_comerc_longo_przo",
    "vl_tit_cred", "vl_tit_div_corp", "vl_outr_tit_div_corp",
    "vl_tit_partic_societ", "vl_outr_tit_partic", "vl_partic_societ_cia_fechada",
    "vl_tit_renda_fixa", "vl_tit_securit", "vl_outr_tit_securit",
    "vl_acao_cert_depos_acao", "vl_mobil", "vl_instrun_finan_deriv_hedge",
    "vl_tot_fdo_invest_renda_fixa",
    "vl_fii", "vl_fiim", "vl_fiagro", "vl_fidc", "vl_fip", "vl_fif", "vl_cot_finvest",
    "vl_tot_ativo_a_vencer", "vl_tot_ativo_vencido", "vl_prazo_venc_liq_ativo",
    "vl_a_vencer_prazo_venc_30", "vl_a_vencer_prazo_venc_31_60",
    "vl_a_vencer_prazo_venc_61_90", "vl_a_vencer_prazo_venc_91_120",
    "vl_a_vencer_prazo_venc_121_180", "vl_a_vencer_prazo_venc_181_360",
    "vl_a_vencer_prazo_venc_361_720", "vl_a_vencer_prazo_venc_721_1080",
    "vl_a_vencer_prazo_venc_1081",
    "vl_vencido_prazo_venc_30", "vl_vencido_prazo_venc_31_60",
    "vl_vencido_prazo_venc_61_90", "vl_vencido_prazo_venc_91_120",
    "vl_vencido_prazo_venc_121_180", "vl_vencido_prazo_venc_181_360",
    "vl_vencido_prazo_venc_361_720", "vl_vencido_prazo_venc_721_1080",
    "vl_vencido_prazo_venc_1081",
    "vl_dev_pess_natu_liq_finan", "vl_dev_pess_natu_liq_fisica",
    "vl_dev_pess_jur_liq_finan", "vl_dev_pess_jur_liq_fisica",
    "vl_tit_cred_liq_finan", "vl_tit_cred_liq_fisica",
    "vl_receber", "vl_tot_passivo", "vl_rend_distrib",
    "vl_tx_admin_pagar", "vl_tx_gestao_pagar", "vl_tx_distrib_pagar",
    "vl_tx_perform_pagar",
    "vl_obrig_arquis_ativo", "vl_adiant_vend_ativo", "vl_adiant_valor_receb",
    "vl_outr_valor_pagar", "vl_provis_conting",
]

INSERT_SQL_FII    = _build_insert_sql("informe_mensal_staging_fii", FII_STAGING_COLS)
INSERT_SQL_FIAGRO = _build_insert_sql("informe_mensal_staging_fiagro", FIAGRO_STAGING_COLS)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def truncate_staging() -> None:
    log.info("Truncating staging tables...")
    execute("TRUNCATE informe_mensal_staging_fii")
    execute("TRUNCATE informe_mensal_staging_fiagro")


def insert_into_fii_staging(doc_id: int, parsed: dict) -> None:
    params = {col: None for col in FII_STAGING_COLS}
    params["id_documento"] = doc_id
    for k, v in parsed.items():
        if k in params:
            params[k] = v
    execute(INSERT_SQL_FII, params)


def insert_into_fiagro_staging(doc_id: int, parsed: dict) -> None:
    params = {col: None for col in FIAGRO_STAGING_COLS}
    params["id_documento"] = doc_id
    for k, v in parsed.items():
        if k in params:
            params[k] = v
    execute(INSERT_SQL_FIAGRO, params)


def record_error(doc_id: int, motivo: str) -> None:
    execute(
        "INSERT INTO erros (id_documento, motivo, registrado_em) VALUES (%s, %s, now())",
        (doc_id, motivo[:1000]),
    )


def clear_error(doc_id: int) -> None:
    execute("DELETE FROM erros WHERE id_documento = %s", (doc_id,))


def loaded_doc_ids() -> set[int]:
    rows = query_all("""
        SELECT id_documento FROM informe_mensal_staging_fii
        UNION
        SELECT id_documento FROM informe_mensal_staging_fiagro
    """)
    return {r["id_documento"] for r in rows}


def errored_doc_ids() -> list[int]:
    rows = query_all("SELECT DISTINCT id_documento FROM erros WHERE id_documento IS NOT NULL")
    return [r["id_documento"] for r in rows]


def max_loaded_id() -> int:
    row = query_one("""
        SELECT GREATEST(
            COALESCE((SELECT MAX(id_documento) FROM informe_mensal_staging_fii), 0),
            COALESCE((SELECT MAX(id_documento) FROM informe_mensal_staging_fiagro), 0),
            COALESCE((SELECT MAX(id_documento) FROM informe_mensal), 0)
        ) AS m
    """)
    return (row["m"] if row and row["m"] else 0) or 0


# ---------------------------------------------------------------------------
# Pass execution
# ---------------------------------------------------------------------------
def run_pass(tipo_fundo: int, label: str, parser_fn, insert_fn,
             resume: bool, retry_errors: bool, incremental: bool) -> dict:
    if retry_errors:
        ids = errored_doc_ids()
        log.info(f"[{label}] Retry-errors mode — {len(ids):,} docs to retry")
        docs = [{"id": i} for i in ids]
    else:
        resume_ids = loaded_doc_ids() if resume else None
        max_id = max_loaded_id() if incremental else None
        docs = fetch_document_ids(tipo_fundo, label,
                                   resume_ids=resume_ids, max_known_id=max_id)

    if not docs:
        log.info(f"[{label}] No new docs to process")
        return {"label": label, "downloaded": 0, "parsed": 0, "skipped": 0, "errors": 0}

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; FIIGuiaBot/1.0)"})

    downloaded = 0
    parsed_ok = 0
    skipped = 0
    errors = 0

    # Use an explicit tqdm so we can update postfix stats in real time
    try:
        from tqdm import tqdm
        pbar = tqdm(total=len(docs), desc=f"{label} download", unit="doc")
    except ImportError:
        pbar = None

    try:
        for doc in docs:
            doc_id = doc["id"]
            try:
                xml_text = download_xml(doc_id, session)
                if xml_text is None:
                    record_error(doc_id, "download failed")
                    errors += 1
                    if pbar:
                        pbar.update(1)
                        pbar.set_postfix(parsed=parsed_ok, skipped=skipped, errors=errors)
                    time.sleep(REQUEST_DELAY)
                    continue
                downloaded += 1

                parsed = parser_fn(xml_text)
                if parsed is None:
                    # Doc doesn't match this parser's expected root tag.
                    # Common at boundary; not an error per se.
                    skipped += 1
                    if pbar:
                        pbar.update(1)
                        pbar.set_postfix(parsed=parsed_ok, skipped=skipped, errors=errors)
                    time.sleep(REQUEST_DELAY)
                    continue

                insert_fn(doc_id, parsed)
                parsed_ok += 1
                clear_error(doc_id)
            except Exception as e:
                log.error(f"  [{label}] doc {doc_id}: {type(e).__name__}: {e}")
                try:
                    record_error(doc_id, f"{type(e).__name__}: {e}")
                except Exception:
                    pass
                errors += 1

            if pbar:
                pbar.update(1)
                pbar.set_postfix(parsed=parsed_ok, skipped=skipped, errors=errors)
            time.sleep(REQUEST_DELAY)
    finally:
        if pbar:
            pbar.close()

    log.info(f"[{label}] downloaded={downloaded} parsed={parsed_ok} "
             f"skipped_other_schema={skipped} errors={errors}")
    return {"label": label, "downloaded": downloaded, "parsed": parsed_ok,
            "skipped": skipped, "errors": errors}


# ---------------------------------------------------------------------------
# Consolidation: staging → gold
# ---------------------------------------------------------------------------
CONSOLIDATE_FII_SQL = """
    INSERT INTO informe_mensal (
        id_documento, tipo_fundo, schema_version,
        nome_fundo, cnpj_fundo, codigo_isin, data_funcionamento, publico_alvo,
        fundo_exclusivo, prazo_duracao, encerramento_exercicio,
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
    )
    SELECT
        s.id_documento, 'FII', 'cvm571',
        s.nome_fundo, s.cnpj_fundo, s.codigo_isin, s.data_funcionamento, s.publico_alvo,
        s.fundo_exclusivo, s.prazo_duracao, s.encerramento_exercicio,
        s.classificacao, s.subclassificacao, s.tipo_gestao, s.segmento_atuacao,
        s.bolsa, s.bvmf, s.cetip, s.mb, s.mbo,
        s.nome_administrador, s.cnpj_administrador,
        s.logradouro, s.numero, s.complemento,
        s.bairro, s.cidade, s.estado,
        s.cep, s.telefone, s.email, s.site,
        s.competencia,
        s.pessoa_fisica, s.pj_nao_financeira, s.banco_comercial, s.corretora_distribuidora,
        s.outras_pj_financeiras, s.investidores_nao_residentes,
        s.entidade_aberta_prev_compl, s.entidade_fechada_prev_compl,
        s.regime_proprio_prev, s.sociedade_seguradora, s.sociedade_cap_arrend_mercantil,
        s.fundos_inv_imobiliario, s.outros_fundos_inv, s.cotistas_dist_fundo,
        s.outros_tipos_cotistas,
        s.ativo, s.patrimonio_liquido, s.qtd_cotas_emitidas, s.num_cotas_emitidas,
        s.valor_patr_cotas, s.despesas_tx_administracao, s.despesas_ag_custodiante,
        s.rent_patrimonial_mes, s.dividend_yield_mes, s.amortiz_acoes_cotas,
        s.disponibilidades, s.titulos_publicos, s.titulos_privados, s.fundos_renda_fixa,
        s.terrenos, s.imoveis_renda_acabados, s.imoveis_renda_construcao,
        s.imoveis_venda_acabados, s.imoveis_venda_construcao, s.outros_direitos_reais,
        s.acoes, s.debentures, s.bonus_subscricao, s.certificados_dep_val_mob,
        s.fia, s.fip, s.fii, s.fdic, s.outras_cotas_fi,
        s.notas_promissorias, s.notas_comerciais,
        s.acoes_sociedades_ativ_fii, s.cotas_sociedades_ativ_fii, s.cepac, s.cri_cra,
        s.letras_hipotecarias, s.lci_lca, s.lig, s.outros_valores_mobiliarios,
        s.contas_receber_alugueis, s.contas_receber_venda_imov, s.outros_valores_receber,
        s.alugueis, s.outros_valores, s.total_investido,
        s.rendimentos_distribuir, s.tx_administracao_pagar, s.tx_performance_pagar,
        s.obrigacoes_aquisicao_imov, s.adiantamento_venda_imov, s.adiantamento_alugueis,
        s.obrigacoes_sec_recebiveis, s.instrumentos_financeiros_deriv,
        s.provisoes_contingencias, s.outros_valores_pagar, s.provisoes_garantias,
        s.total_passivo,
        s.total_imoveis_onus, s.total_garantias_classe, s.total_garantias_cotistas
    FROM informe_mensal_staging_fii s
    ON CONFLICT (id_documento) DO UPDATE SET
        tipo_fundo = EXCLUDED.tipo_fundo,
        schema_version = EXCLUDED.schema_version,
        nome_fundo = EXCLUDED.nome_fundo,
        cnpj_fundo = EXCLUDED.cnpj_fundo,
        codigo_isin = EXCLUDED.codigo_isin,
        data_funcionamento = EXCLUDED.data_funcionamento,
        publico_alvo = EXCLUDED.publico_alvo,
        fundo_exclusivo = EXCLUDED.fundo_exclusivo,
        prazo_duracao = EXCLUDED.prazo_duracao,
        encerramento_exercicio = EXCLUDED.encerramento_exercicio,
        classificacao = EXCLUDED.classificacao,
        subclassificacao = EXCLUDED.subclassificacao,
        tipo_gestao = EXCLUDED.tipo_gestao,
        segmento_atuacao = EXCLUDED.segmento_atuacao,
        mercado_bolsa = EXCLUDED.mercado_bolsa,
        mercado_bvmf = EXCLUDED.mercado_bvmf,
        mercado_cetip = EXCLUDED.mercado_cetip,
        mercado_mb = EXCLUDED.mercado_mb,
        mercado_mbo = EXCLUDED.mercado_mbo,
        nome_administrador = EXCLUDED.nome_administrador,
        cnpj_administrador = EXCLUDED.cnpj_administrador,
        administrador_logradouro = EXCLUDED.administrador_logradouro,
        administrador_numero = EXCLUDED.administrador_numero,
        administrador_complemento = EXCLUDED.administrador_complemento,
        administrador_bairro = EXCLUDED.administrador_bairro,
        administrador_cidade = EXCLUDED.administrador_cidade,
        administrador_estado = EXCLUDED.administrador_estado,
        administrador_cep = EXCLUDED.administrador_cep,
        administrador_telefone = EXCLUDED.administrador_telefone,
        administrador_email = EXCLUDED.administrador_email,
        administrador_site = EXCLUDED.administrador_site,
        competencia = EXCLUDED.competencia,
        pessoa_fisica = EXCLUDED.pessoa_fisica,
        pj_nao_financeira = EXCLUDED.pj_nao_financeira,
        banco_comercial = EXCLUDED.banco_comercial,
        corretora_distribuidora = EXCLUDED.corretora_distribuidora,
        outras_pj_financeiras = EXCLUDED.outras_pj_financeiras,
        investidores_nao_residentes = EXCLUDED.investidores_nao_residentes,
        entidade_aberta_prev_compl = EXCLUDED.entidade_aberta_prev_compl,
        entidade_fechada_prev_compl = EXCLUDED.entidade_fechada_prev_compl,
        regime_proprio_prev = EXCLUDED.regime_proprio_prev,
        sociedade_seguradora = EXCLUDED.sociedade_seguradora,
        sociedade_cap_arrend_mercantil = EXCLUDED.sociedade_cap_arrend_mercantil,
        fundos_inv_imobiliario = EXCLUDED.fundos_inv_imobiliario,
        outros_fundos_inv = EXCLUDED.outros_fundos_inv,
        cotistas_dist_fundo = EXCLUDED.cotistas_dist_fundo,
        outros_tipos_cotistas = EXCLUDED.outros_tipos_cotistas,
        ativo_total = EXCLUDED.ativo_total,
        patrimonio_liquido = EXCLUDED.patrimonio_liquido,
        qtd_cotas_emitidas = EXCLUDED.qtd_cotas_emitidas,
        num_cotas_emitidas = EXCLUDED.num_cotas_emitidas,
        valor_patr_cotas = EXCLUDED.valor_patr_cotas,
        despesas_tx_administracao = EXCLUDED.despesas_tx_administracao,
        despesas_ag_custodiante = EXCLUDED.despesas_ag_custodiante,
        rent_patrimonial_mes = EXCLUDED.rent_patrimonial_mes,
        dividend_yield_mes = EXCLUDED.dividend_yield_mes,
        amortiz_acoes_cotas = EXCLUDED.amortiz_acoes_cotas,
        disponibilidades = EXCLUDED.disponibilidades,
        titulos_publicos = EXCLUDED.titulos_publicos,
        titulos_privados = EXCLUDED.titulos_privados,
        fundos_renda_fixa = EXCLUDED.fundos_renda_fixa,
        terrenos = EXCLUDED.terrenos,
        imoveis_renda_acabados = EXCLUDED.imoveis_renda_acabados,
        imoveis_renda_construcao = EXCLUDED.imoveis_renda_construcao,
        imoveis_venda_acabados = EXCLUDED.imoveis_venda_acabados,
        imoveis_venda_construcao = EXCLUDED.imoveis_venda_construcao,
        outros_direitos_reais = EXCLUDED.outros_direitos_reais,
        acoes = EXCLUDED.acoes, debentures = EXCLUDED.debentures,
        bonus_subscricao = EXCLUDED.bonus_subscricao,
        certificados_dep_val_mob = EXCLUDED.certificados_dep_val_mob,
        fia = EXCLUDED.fia, fip = EXCLUDED.fip, fii = EXCLUDED.fii, fdic = EXCLUDED.fdic,
        outras_cotas_fi = EXCLUDED.outras_cotas_fi,
        notas_promissorias = EXCLUDED.notas_promissorias,
        notas_comerciais = EXCLUDED.notas_comerciais,
        acoes_sociedades_ativ_fii = EXCLUDED.acoes_sociedades_ativ_fii,
        cotas_sociedades_ativ_fii = EXCLUDED.cotas_sociedades_ativ_fii,
        cepac = EXCLUDED.cepac, cri_cra = EXCLUDED.cri_cra,
        letras_hipotecarias = EXCLUDED.letras_hipotecarias,
        lci_lca = EXCLUDED.lci_lca, lig = EXCLUDED.lig,
        outros_valores_mobiliarios = EXCLUDED.outros_valores_mobiliarios,
        contas_receber_alugueis = EXCLUDED.contas_receber_alugueis,
        contas_receber_venda_imov = EXCLUDED.contas_receber_venda_imov,
        outros_valores_receber = EXCLUDED.outros_valores_receber,
        alugueis = EXCLUDED.alugueis, outros_valores = EXCLUDED.outros_valores,
        total_investido = EXCLUDED.total_investido,
        rendimentos_distribuir = EXCLUDED.rendimentos_distribuir,
        tx_administracao_pagar = EXCLUDED.tx_administracao_pagar,
        tx_performance_pagar = EXCLUDED.tx_performance_pagar,
        obrigacoes_aquisicao_imov = EXCLUDED.obrigacoes_aquisicao_imov,
        adiantamento_venda_imov = EXCLUDED.adiantamento_venda_imov,
        adiantamento_alugueis = EXCLUDED.adiantamento_alugueis,
        obrigacoes_sec_recebiveis = EXCLUDED.obrigacoes_sec_recebiveis,
        instrumentos_financeiros_deriv = EXCLUDED.instrumentos_financeiros_deriv,
        provisoes_contingencias = EXCLUDED.provisoes_contingencias,
        outros_valores_pagar = EXCLUDED.outros_valores_pagar,
        provisoes_garantias = EXCLUDED.provisoes_garantias,
        total_passivo = EXCLUDED.total_passivo,
        total_imoveis_onus = EXCLUDED.total_imoveis_onus,
        total_garantias_classe = EXCLUDED.total_garantias_classe,
        total_garantias_cotistas = EXCLUDED.total_garantias_cotistas
"""

CONSOLIDATE_FIAGRO_SQL = """
    INSERT INTO informe_mensal (
        id_documento, tipo_fundo, schema_version,
        nome_fundo, cnpj_fundo, codigo_isin, data_funcionamento, publico_alvo,
        prazo_duracao, encerramento_exercicio,
        classificacao,
        nome_administrador, cnpj_administrador,
        nome_gestor, cnpj_gestor,
        administrador_email, administrador_site,
        competencia,
        pessoa_fisica, pj_nao_financeira,
        outras_pj_financeiras, investidores_nao_residentes,
        sociedade_seguradora, fundos_inv_imobiliario, outros_tipos_cotistas,
        ativo_total, patrimonio_liquido,
        num_cotas_emitidas, valor_patr_cotas,
        despesas_tx_administracao,
        rent_patrimonial_mes, dividend_yield_mes, amortiz_acoes_cotas,
        titulos_privados, fundos_renda_fixa,
        debentures, notas_comerciais, lci_lca,
        cri_cra, cra,
        cdca, cda_warrant, cpr, cbio,
        direitos_imov_rural, invest_imov_rural,
        fii, fip, fdic,
        total_investido,
        rendimentos_distribuir, tx_administracao_pagar, tx_performance_pagar,
        obrigacoes_aquisicao_imov, outros_valores_pagar, provisoes_contingencias,
        total_passivo
    )
    SELECT
        s.id_documento,
        'FIAGRO', 'cvm175',
        s.nm_fundo, s.nr_cnpj_fundo, s.cd_isin, s.dt_regs_func, s.tp_publ_alvo,
        s.przo_duracao, s.dt_encer_exerc_soc,
        s.classif_auto_regul,
        s.nm_adm, s.nr_cnpj_adm,
        s.nm_gestor, s.nr_cnpj_gestor,
        s.email_adm, s.site,
        CASE WHEN s.dt_compt ~ '^[0-9]{2}/[0-9]{4}$'
             THEN TO_DATE('01/' || s.dt_compt, 'DD/MM/YYYY')
             ELSE NULL
        END,
        s.qtd_pess_natural, s.qtd_pess_jurid_exct_financ,
        s.qtd_pess_jurid_financ, s.qtd_invest_nao_resid,
        s.qtd_socied_segur_resegur, s.qtd_fundos_invst, s.qtd_outro_tipo_cotst,
        s.vl_ativo, s.vl_patrimonio_liquido,
        s.nr_cot_emitidas, s.vl_patrimonio_liquido_cotas,
        s.vl_desp_tx_admin_rel_pl_mes,
        s.vl_rent_patrim_mes_ref, s.vl_dividend_yield_mes_ref, s.perc_amort_cotst_mes_ref,
        s.vl_outr_ativ_finan, s.vl_tot_fdo_invest_renda_fixa,
        s.vl_debent, s.vl_nota_comerc, s.vl_lci,
        s.vl_cert_receb_cri, s.vl_cert_receb_cra,
        s.vl_cdca, s.vl_cda_warrant, s.vl_cpr, s.vl_cbio_cred_descarbon,
        s.vl_direit_imov_rural, s.vl_invest_imov_rural,
        s.vl_fii, s.vl_fip, s.vl_fidc,
        s.vl_total_invest,
        s.vl_rend_distrib, s.vl_tx_admin_pagar, s.vl_tx_perform_pagar,
        s.vl_obrig_arquis_ativo, s.vl_outr_valor_pagar, s.vl_provis_conting,
        s.vl_tot_passivo
    FROM informe_mensal_staging_fiagro s
    ON CONFLICT (id_documento) DO UPDATE SET
        tipo_fundo = EXCLUDED.tipo_fundo,
        schema_version = EXCLUDED.schema_version,
        nome_fundo = EXCLUDED.nome_fundo,
        cnpj_fundo = EXCLUDED.cnpj_fundo,
        codigo_isin = EXCLUDED.codigo_isin,
        data_funcionamento = EXCLUDED.data_funcionamento,
        publico_alvo = EXCLUDED.publico_alvo,
        prazo_duracao = EXCLUDED.prazo_duracao,
        encerramento_exercicio = EXCLUDED.encerramento_exercicio,
        classificacao = EXCLUDED.classificacao,
        nome_administrador = EXCLUDED.nome_administrador,
        cnpj_administrador = EXCLUDED.cnpj_administrador,
        nome_gestor = EXCLUDED.nome_gestor,
        cnpj_gestor = EXCLUDED.cnpj_gestor,
        administrador_email = EXCLUDED.administrador_email,
        administrador_site = EXCLUDED.administrador_site,
        competencia = EXCLUDED.competencia,
        pessoa_fisica = EXCLUDED.pessoa_fisica,
        pj_nao_financeira = EXCLUDED.pj_nao_financeira,
        outras_pj_financeiras = EXCLUDED.outras_pj_financeiras,
        investidores_nao_residentes = EXCLUDED.investidores_nao_residentes,
        sociedade_seguradora = EXCLUDED.sociedade_seguradora,
        fundos_inv_imobiliario = EXCLUDED.fundos_inv_imobiliario,
        outros_tipos_cotistas = EXCLUDED.outros_tipos_cotistas,
        ativo_total = EXCLUDED.ativo_total,
        patrimonio_liquido = EXCLUDED.patrimonio_liquido,
        num_cotas_emitidas = EXCLUDED.num_cotas_emitidas,
        valor_patr_cotas = EXCLUDED.valor_patr_cotas,
        despesas_tx_administracao = EXCLUDED.despesas_tx_administracao,
        rent_patrimonial_mes = EXCLUDED.rent_patrimonial_mes,
        dividend_yield_mes = EXCLUDED.dividend_yield_mes,
        amortiz_acoes_cotas = EXCLUDED.amortiz_acoes_cotas,
        titulos_privados = EXCLUDED.titulos_privados,
        fundos_renda_fixa = EXCLUDED.fundos_renda_fixa,
        debentures = EXCLUDED.debentures,
        notas_comerciais = EXCLUDED.notas_comerciais,
        lci_lca = EXCLUDED.lci_lca,
        cri_cra = EXCLUDED.cri_cra,
        cra = EXCLUDED.cra,
        cdca = EXCLUDED.cdca,
        cda_warrant = EXCLUDED.cda_warrant,
        cpr = EXCLUDED.cpr,
        cbio = EXCLUDED.cbio,
        direitos_imov_rural = EXCLUDED.direitos_imov_rural,
        invest_imov_rural = EXCLUDED.invest_imov_rural,
        fii = EXCLUDED.fii, fip = EXCLUDED.fip, fdic = EXCLUDED.fdic,
        total_investido = EXCLUDED.total_investido,
        rendimentos_distribuir = EXCLUDED.rendimentos_distribuir,
        tx_administracao_pagar = EXCLUDED.tx_administracao_pagar,
        tx_performance_pagar = EXCLUDED.tx_performance_pagar,
        obrigacoes_aquisicao_imov = EXCLUDED.obrigacoes_aquisicao_imov,
        outros_valores_pagar = EXCLUDED.outros_valores_pagar,
        provisoes_contingencias = EXCLUDED.provisoes_contingencias,
        total_passivo = EXCLUDED.total_passivo
"""


def consolidate() -> None:
    log.info("Consolidating staging → informe_mensal...")

    fii_count_row = query_one("SELECT COUNT(*) AS n FROM informe_mensal_staging_fii")
    fiagro_count_row = query_one("SELECT COUNT(*) AS n FROM informe_mensal_staging_fiagro")
    fii_count = fii_count_row["n"] if fii_count_row else 0
    fiagro_count = fiagro_count_row["n"] if fiagro_count_row else 0

    log.info(f"  staging_fii rows:    {fii_count:,}")
    log.info(f"  staging_fiagro rows: {fiagro_count:,}")

    if fii_count > 0:
        execute(CONSOLIDATE_FII_SQL)
        log.info(f"  → consolidated {fii_count:,} FII rows")

    if fiagro_count > 0:
        execute(CONSOLIDATE_FIAGRO_SQL)
        log.info(f"  → consolidated {fiagro_count:,} FIAGRO rows")

    final = query_one("SELECT COUNT(*) AS n FROM informe_mensal")
    log.info(f"informe_mensal total: {final['n']:,}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fii-only",         action="store_true")
    parser.add_argument("--fiagro-only",      action="store_true")
    parser.add_argument("--consolidate-only", action="store_true",
                        help="Skip scrape; only run staging→gold consolidation")
    parser.add_argument("--no-consolidate",   action="store_true",
                        help="Run scrape but skip consolidation")
    parser.add_argument("--no-truncate-staging", action="store_true",
                        help="Skip the initial staging truncate")
    parser.add_argument("--resume",           action="store_true",
                        help="Skip docs already in staging from a previous run")
    parser.add_argument("--retry-errors",     action="store_true",
                        help="Only retry docs that previously errored")
    parser.add_argument("--incremental",      action="store_true",
                        help="Only fetch docs newer than max(id) currently loaded")
    args = parser.parse_args()

    init_pool()

    try:
        if not args.consolidate_only and not args.no_truncate_staging:
            truncate_staging()

        if args.consolidate_only:
            log.info("Skipping scrape (--consolidate-only)")
        elif args.fiagro_only:
            log.info("Skipping FII pass (--fiagro-only)")
        else:
            run_pass(TIPO_FII, "FII", parse_fii, insert_into_fii_staging,
                     resume=args.resume, retry_errors=args.retry_errors,
                     incremental=args.incremental)

        if args.consolidate_only:
            pass
        elif args.fii_only:
            log.info("Skipping FIAGRO pass (--fii-only)")
        else:
            run_pass(TIPO_FIAGRO, "FIAGRO", parse_fiagro, insert_into_fiagro_staging,
                     resume=args.resume, retry_errors=args.retry_errors,
                     incremental=args.incremental)

        if args.no_consolidate:
            log.info("Skipping consolidation (--no-consolidate)")
        else:
            consolidate()

        return 0
    finally:
        close_pool()


if __name__ == "__main__":
    sys.exit(main())
