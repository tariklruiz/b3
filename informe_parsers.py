"""
informe_parsers.py — XML parsers for FII and FIAGRO informes mensais.

Each parser receives raw XML text and returns a dict whose keys match the
column names of the corresponding staging table. The scraper is responsible
for routing each document to the right parser based on which CVM grid pass
produced it (tipoFundo=1 → FII, tipoFundo=11 → FIAGRO).

Number formats:
  - FII (CVM 571):    period decimal, e.g. "2336996536.54"
  - FIAGRO (CVM 175): comma decimal,  e.g. "663916617,41"

Both parsers handle their respective format. Helpers below cover the conversion.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime


# ---------------------------------------------------------------------------
# Number / type helpers
# ---------------------------------------------------------------------------
def _get(root, path: str) -> str | None:
    """Find first matching element, return stripped text or None."""
    el = root.find(path)
    if el is None:
        return None
    if el.text is None:
        return None
    text = el.text.strip()
    return text if text else None


def to_float_period(val: str | None) -> float | None:
    """Parse '1234.56' style. Used for FII (CVM 571) numerics."""
    if val is None:
        return None
    try:
        return float(val.replace(" ", "").replace(",", ""))
    except (ValueError, AttributeError):
        return None


def to_float_comma(val: str | None) -> float | None:
    """
    Parse Brazilian '1.234,56' style. Used for FIAGRO (CVM 175) numerics.
    Handles thousands-separator dots and comma decimal.
    """
    if val is None:
        return None
    try:
        # Remove thousands separator (period), then convert comma decimal to period
        cleaned = val.replace(" ", "").replace(".", "").replace(",", ".")
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def to_int(val: str | None) -> int | None:
    """Parse integer, accepting both comma and period as thousands separators."""
    if val is None:
        return None
    try:
        cleaned = val.replace(" ", "").replace(",", "").replace(".", "")
        return int(cleaned)
    except (ValueError, AttributeError):
        return None


def to_bool(val: str | None) -> bool | None:
    """Parse XML boolean strings."""
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in ("true", "1", "sim", "s", "yes"):
        return True
    if s in ("false", "0", "nao", "não", "n", "no"):
        return False
    return None


def parse_iso_date(val: str | None) -> object:
    """Parse 'YYYY-MM-DD' string; return date or None."""
    if not val:
        return None
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_brazilian_date(val: str | None) -> object:
    """Parse 'DD/MM/YYYY' string; return date or None."""
    if not val:
        return None
    try:
        return datetime.strptime(val[:10], "%d/%m/%Y").date()
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# FII parser — CVM 571/2015 schema, root: <DadosEconomicoFinanceiros>
# ---------------------------------------------------------------------------
def parse_fii(xml_text: str) -> dict:
    """
    Parse a FII Informe Mensal Estruturado XML and return a dict suitable for
    insertion into informe_mensal_staging_fii.

    Returns None if the XML doesn't have the expected root tag (caller should
    skip insertion).
    """
    root = ET.fromstring(xml_text)
    if root.tag != "DadosEconomicoFinanceiros":
        return None

    f = to_float_period
    i = to_int

    # TotalInvestido carries an attribute we want
    total_invest_el = root.find(".//TotalInvestido")
    total_investido = f(total_invest_el.get("total")) if total_invest_el is not None else None

    return {
        # Identity
        "nome_fundo":                _get(root, ".//NomeFundo"),
        "cnpj_fundo":                _get(root, ".//CNPJFundo"),
        "codigo_isin":               _get(root, ".//CodigoISIN"),
        "data_funcionamento":        parse_iso_date(_get(root, ".//DataFuncionamento")),
        "publico_alvo":              _get(root, ".//PublicoAlvo"),
        "fundo_exclusivo":           to_bool(_get(root, ".//FundoExclusivo")),
        "prazo_duracao":             _get(root, ".//PrazoDuracao"),
        "encerramento_exercicio":    _get(root, ".//EncerramentoExercicio"),
        "classificacao":             _get(root, ".//Classificacao"),
        "subclassificacao":          _get(root, ".//Subclassificacao"),
        "tipo_gestao":               _get(root, ".//TipoGestao"),
        "segmento_atuacao":          _get(root, ".//SegmentoAtuacao"),
        "vinculo_familiar_cotistas": to_bool(_get(root, ".//VinculoFamiliarCotistas")),

        # Mercado
        "bolsa": to_bool(_get(root, ".//Bolsa")),
        "bvmf":  to_bool(_get(root, ".//BVMF")),
        "cetip": to_bool(_get(root, ".//CETIP")),
        "mb":    to_bool(_get(root, ".//MB")),
        "mbo":   to_bool(_get(root, ".//MBO")),

        # Administrador
        "nome_administrador": _get(root, ".//NomeAdministrador"),
        "cnpj_administrador": _get(root, ".//CNPJAdministrador"),
        "logradouro":  _get(root, ".//Logradouro"),
        "numero":      _get(root, ".//Numero"),
        "complemento": _get(root, ".//Complemento"),
        "bairro":      _get(root, ".//Bairro"),
        "cidade":      _get(root, ".//Cidade"),
        "estado":      _get(root, ".//Estado"),
        "cep":         _get(root, ".//CEP"),
        "telefone":    _get(root, ".//Telefone1"),
        "email":       _get(root, ".//Email"),
        "site":        _get(root, ".//Site"),

        "competencia": parse_iso_date(_get(root, ".//Competencia")),

        # Cotistas
        "pessoa_fisica":                  i(_get(root, ".//PessoaFisica")),
        "pj_nao_financeira":              i(_get(root, ".//PJNaoFinanceira")),
        "banco_comercial":                i(_get(root, ".//BancoComercial")),
        "corretora_distribuidora":        i(_get(root, ".//CorretoraDistribuidora")),
        "outras_pj_financeiras":          i(_get(root, ".//OutrasPJFinanceiras")),
        "investidores_nao_residentes":    i(_get(root, ".//InvestidoresNaoResidentes")),
        "entidade_aberta_prev_compl":     i(_get(root, ".//EntidadeAbertaPrevCompl")),
        "entidade_fechada_prev_compl":    i(_get(root, ".//EntidadeFechadaPrevCompl")),
        "regime_proprio_prev":            i(_get(root, ".//RegimeProprioPrev")),
        "sociedade_seguradora":           i(_get(root, ".//SociedadeSeguradora")),
        "sociedade_cap_arrend_mercantil": i(_get(root, ".//SociedadeCapArrendMercantil")),
        "fundos_inv_imobiliario":         i(_get(root, ".//FundosInvImobiliario")),
        "outros_fundos_inv":              i(_get(root, ".//OutrosFundosInv")),
        "cotistas_dist_fundo":            i(_get(root, ".//CotistasDistFundo")),
        "outros_tipos_cotistas":          i(_get(root, ".//OutrosTiposCotistas")),

        # Items 1-8
        "ativo":                     f(_get(root, ".//Ativo")),
        "patrimonio_liquido":        f(_get(root, ".//PatrimonioLiquido")),
        "qtd_cotas_emitidas":        f(_get(root, ".//QtdCotasEmitidas")),
        "num_cotas_emitidas":        f(_get(root, ".//NumCotasEmitidas")),
        "valor_patr_cotas":          f(_get(root, ".//ValorPatrCotas")),
        "despesas_tx_administracao": f(_get(root, ".//DespesasTxAdministracao")),
        "despesas_ag_custodiante":   f(_get(root, ".//DespesasAgCustodiante")),
        "rent_patrimonial_mes":      f(_get(root, ".//RentPatrimonialMes")),
        "dividend_yield_mes":        f(_get(root, ".//DividendYieldMes")),
        "amortiz_acoes_cotas":       f(_get(root, ".//AmortizAcoesCotas")),

        # Item 9
        "disponibilidades":   f(_get(root, ".//Disponibilidades")),
        "titulos_publicos":   f(_get(root, ".//TitulosPublicos")),
        "titulos_privados":   f(_get(root, ".//TitulosPrivados")),
        "fundos_renda_fixa":  f(_get(root, ".//FundosRendaFixa")),

        # Item 10
        "terrenos":                   f(_get(root, ".//Terrenos")),
        "imoveis_renda_acabados":     f(_get(root, ".//ImoveisRendaAcabados")),
        "imoveis_renda_construcao":   f(_get(root, ".//ImoveisRendaConstrucao")),
        "imoveis_venda_acabados":     f(_get(root, ".//ImoveisVendaAcabados")),
        "imoveis_venda_construcao":   f(_get(root, ".//ImoveisVendaConstrucao")),
        "outros_direitos_reais":      f(_get(root, ".//OutrosDireitosReais")),
        "acoes":                      f(_get(root, ".//Acoes")),
        "debentures":                 f(_get(root, ".//Debentures")),
        "bonus_subscricao":           f(_get(root, ".//BonusSubscricao")),
        "certificados_dep_val_mob":   f(_get(root, ".//CertificadosDepositoValMob")),
        "fia":                        f(_get(root, ".//FIA")),
        "fip":                        f(_get(root, ".//FIP")),
        "fii":                        f(_get(root, ".//FII")),
        "fdic":                       f(_get(root, ".//FDIC")),
        "outras_cotas_fi":            f(_get(root, ".//OutrasCotasFI")),
        "notas_promissorias":         f(_get(root, ".//NotasPromissorias")),
        "notas_comerciais":           f(_get(root, ".//NotasComerciais")),
        "acoes_sociedades_ativ_fii":  f(_get(root, ".//AcoesSociedadesAtivFII")),
        "cotas_sociedades_ativ_fii":  f(_get(root, ".//CotasSociedadesAtivFII")),
        "cepac":                      f(_get(root, ".//CEPAC")),
        "cri_cra":                    f(_get(root, ".//CriCra")),
        "letras_hipotecarias":        f(_get(root, ".//LetrasHipotecarias")),
        "lci_lca":                    f(_get(root, ".//LciLca")),
        "lig":                        f(_get(root, ".//LIG")),
        "outros_valores_mobiliarios": f(_get(root, ".//OutrosValoresMobliarios")),

        # Item 11
        "contas_receber_alugueis":   f(_get(root, ".//ContasReceberAlugueis")),
        "contas_receber_venda_imov": f(_get(root, ".//ContasReceberVendaImov")),
        "outros_valores_receber":    f(_get(root, ".//OutrosValoresReceber")),
        "alugueis":                  f(_get(root, ".//Alugueis")),
        "outros_valores":            f(_get(root, ".//OutrosValores")),
        "total_investido":           total_investido,

        # Items 12-22
        "rendimentos_distribuir":         f(_get(root, ".//RendimentosDistribuir")),
        "tx_administracao_pagar":         f(_get(root, ".//TxAdministracaoPagar")),
        "tx_performance_pagar":           f(_get(root, ".//TxPerformancePagar")),
        "obrigacoes_aquisicao_imov":      f(_get(root, ".//ObrigacoesAquisicaoImov")),
        "adiantamento_venda_imov":        f(_get(root, ".//AdiantamentoVendaImov")),
        "adiantamento_alugueis":          f(_get(root, ".//AdiantamentoAlugueis")),
        "obrigacoes_sec_recebiveis":      f(_get(root, ".//ObrigacoesSecRecebiveis")),
        "instrumentos_financeiros_deriv": f(_get(root, ".//InstrumentosFinanceirosDeriv")),
        "provisoes_contingencias":        f(_get(root, ".//ProvisoesContigencias")),
        "outros_valores_pagar":           f(_get(root, ".//OutrosValoresPagar")),
        "provisoes_garantias":            f(_get(root, ".//ProvisoesGarantias")),
        "total_passivo":                  f(_get(root, ".//TotalPassivo")),

        # Items 23-25
        "total_imoveis_onus":       f(_get(root, ".//TotalImoveisOnus")),
        "total_garantias_classe":   f(_get(root, ".//TotalGarantiasClasse")),
        "total_garantias_cotistas": f(_get(root, ".//TotalGarantiasCotistas")),
    }


# ---------------------------------------------------------------------------
# FIAGRO parser — CVM 175/2022 Anexo VI schema, root: <DOC_ARQ>
# ---------------------------------------------------------------------------
def parse_fiagro(xml_text: str) -> dict:
    """
    Parse a FIAGRO Informe Mensal XML (CVM 175/2022 Anexo VI) and return
    a dict suitable for insertion into informe_mensal_staging_fiagro.

    Returns None if the XML doesn't have the expected root tag.
    """
    root = ET.fromstring(xml_text)
    if root.tag != "DOC_ARQ":
        return None

    f = to_float_comma
    i = to_int

    return {
        # Cabeçalho
        "nm_fundo":              _get(root, ".//NM_FUNDO"),
        "nr_cnpj_fundo":         _get(root, ".//NR_CNPJ_FUNDO"),
        "nm_classe":             _get(root, ".//NM_CLASSE"),
        "nr_cnpj_classe":        _get(root, ".//NR_CNPJ_CLASSE"),
        "dt_regs_func":          parse_brazilian_date(_get(root, ".//DT_REGS_FUNC")),
        "tp_publ_alvo":          _get(root, ".//TP_PUBL_ALVO"),
        "cd_isin":               _get(root, ".//CD_ISIN"),
        "class_unica":           _get(root, ".//CLASS_UNICA"),
        "classif_auto_regul":    _get(root, ".//CLASSIF_AUTO_REGUL"),
        "class_regr_outr_anexo": _get(root, ".//CLASS_REGR_OUTR_ANEXO"),
        "przo_duracao":          _get(root, ".//PRZO_DURACAO"),
        "dt_encer_exerc_soc":    _get(root, ".//DT_ENCER_EXERC_SOC"),
        "cotst_vincl_famil":     _get(root, ".//COTST_VINCL_FAMIL"),
        "dt_compt":              _get(root, ".//DT_COMPT"),    # 'MM/YYYY' kept as string
        "versao":                _get(root, ".//VERSAO"),

        # Administrador / Gestor
        "nm_adm":             _get(root, ".//NM_ADM"),
        "nr_cnpj_adm":        _get(root, ".//NR_CNPJ_ADM"),
        "nm_gestor":          _get(root, ".//NM_GESTOR"),
        "nr_cnpj_gestor":     _get(root, ".//NR_CNPJ_GESTOR"),
        "email_adm":          _get(root, ".//EMAIL_ADM"),
        "site":               _get(root, ".//SITE"),
        "serv_atend_cotst":   _get(root, ".//SERV_ATEND_COTST"),
        "entid_adm_merc_org": _get(root, ".//ENTID_ADM_MERC_ORG"),
        "merc_negoc":         _get(root, ".//MERC_NEGOC"),

        # Cotistas
        "qtd_tot_cotst":                  i(_get(root, ".//QTD_TOT_COTST")),
        "qtd_pess_natural":               i(_get(root, ".//QTD_PESS_NATURAL")),
        "qtd_pess_jurid_exct_financ":     i(_get(root, ".//QTD_PESS_JURID_EXCT_FINANC")),
        "qtd_pess_jurid_financ":          i(_get(root, ".//QTD_PESS_JURID_FINANC")),
        "qtd_invest_nao_resid":           i(_get(root, ".//QTD_INVEST_NAO_RESID")),
        "qtd_entid_prev_compl_exct_rpps": i(_get(root, ".//QTD_ENTID_PREV_COMPL_EXCT_RPPS")),
        "qtd_entid_rpps":                 i(_get(root, ".//QTD_ENTID_RPPS")),
        "qtd_socied_segur_resegur":       i(_get(root, ".//QTD_SOCIED_SEGUR_RESEGUR")),
        "qtd_fundos_invst":               i(_get(root, ".//QTD_FUNDOS_INVST")),
        "qtd_outro_tipo_cotst":           i(_get(root, ".//QTD_OUTRO_TIPO_COTST")),
        "qtd_cotst_distrib_conta_ordem":  i(_get(root, ".//QTD_COTST_DISTRIB_CONTA_ORDEM")),

        # Subclasses
        "nr_cot_emitidas":     f(_get(root, ".//NR_COT_EMITIDAS")),
        "nr_cot_nm_subclasse": _get(root, ".//NR_COT_NM_SUBCLASSE"),
        "nr_cot_subclasse":    f(_get(root, ".//NR_COT_SUBCLASSE")),
        "vl_pl_nm_subclasse":  _get(root, ".//VL_PL_NM_SUBCLASSE"),
        "vl_pl_subclasse":     f(_get(root, ".//VL_PL_SUBCLASSE")),

        # Patrimônio e despesas
        "vl_ativo":                      f(_get(root, ".//VL_ATIVO")),
        "vl_patrimonio_liquido":         f(_get(root, ".//VL_PATRIMONIO_LIQUIDO")),
        "vl_patrimonio_liquido_cotas":   f(_get(root, ".//VL_PATRIMONIO_LIQUIDO_COTAS")),
        "vl_desp_tx_admin_rel_pl_mes":   f(_get(root, ".//VL_DESP_TX_ADMIN_REL_PL_MES")),
        "vl_desp_tx_gest_rel_pl_mes":    f(_get(root, ".//VL_DESP_TX_GEST_REL_PL_MES")),
        "vl_desp_tx_distrib_rel_pl_mes": f(_get(root, ".//VL_DESP_TX_DISTRIB_REL_PL_MES")),
        "vl_rentb_efetiv_mes":           f(_get(root, ".//VL_RENTB_EFETIV_MES")),
        "vl_rent_patrim_mes_ref":        f(_get(root, ".//VL_RENT_PATRIM_MES_REF")),
        "vl_dividend_yield_mes_ref":     f(_get(root, ".//VL_DIVIDEND_YIELD_MES_REF")),
        "perc_amort_cotst_mes_ref":      f(_get(root, ".//PERC_AMORT_COTST_MES_REF")),

        # Liquidez
        "vl_total_mantido_neces_liq":    f(_get(root, ".//VL_TOTAL_MANTIDO_NECES_LIQ")),
        "vl_ativ_finan":                 f(_get(root, ".//VL_ATIV_FINAN")),
        "vl_ativ_finan_lato_sensu":      f(_get(root, ".//VL_ATIV_FINAN_LATO_SENSU")),
        "vl_ativ_finan_emis_inst_finan": f(_get(root, ".//VL_ATIV_FINAN_EMIS_INST_FINAN")),
        "vl_outr_ativ_finan":            f(_get(root, ".//VL_OUTR_ATIV_FINAN")),
        "vl_outr_ativ_emis_intst_finan": f(_get(root, ".//VL_OUTR_ATIV_EMIS_INTST_FINAN")),

        # Investimentos — agro-specific
        "vl_total_invest":              f(_get(root, ".//VL_TOTAL_INVEST")),
        "vl_direit_cred_agro":          f(_get(root, ".//VL_DIREIT_CRED_AGRO")),
        "vl_demais_direit_cred":        f(_get(root, ".//VL_DEMAIS_DIREIT_CRED")),
        "vl_cert_receb_cri":            f(_get(root, ".//VL_CERT_RECEB_CRI")),
        "vl_cert_receb_cra":            f(_get(root, ".//VL_CERT_RECEB_CRA")),
        "vl_cdca":                      f(_get(root, ".//VL_CDCA")),
        "vl_cda_warrant":               f(_get(root, ".//VL_CDA_WARRANT")),
        "vl_cert_dep_agro_cda_warr_wa": f(_get(root, ".//VL_CERT_DEP_AGRO_CDA_WARR_WA")),
        "vl_cert_dir_cred_agro_cdca":   f(_get(root, ".//VL_CERT_DIR_CRED_AGRO_CDCA")),
        "vl_cpr":                       f(_get(root, ".//VL_CPR")),
        "vl_cpr_finan":                 f(_get(root, ".//VL_CPR_FINAN")),
        "vl_cpr_fisica":                f(_get(root, ".//VL_CPR_FISICA")),
        "vl_cred_carbono_agro":         f(_get(root, ".//VL_CRED_CARBONO_AGRO")),
        "vl_cbio_cred_descarbon":       f(_get(root, ".//VL_CBIO_CRED_DESCARBON")),
        "vl_direit_imov_rural":         f(_get(root, ".//VL_DIREIT_IMOV_RURAL")),
        "vl_invest_imov_rural":         f(_get(root, ".//VL_INVEST_IMOV_RURAL")),
        "vl_outr_tit_cred_agro":        f(_get(root, ".//VL_OUTR_TIT_CRED_AGRO")),
        "vl_lca":                       f(_get(root, ".//VL_LCA")),
        "vl_lci":                       f(_get(root, ".//VL_LCI")),
        "vl_debent":                    f(_get(root, ".//VL_DEBENT")),
        "vl_debent_conv":               f(_get(root, ".//VL_DEBENT_CONV")),
        "vl_debent_nao_conv":           f(_get(root, ".//VL_DEBENT_NAO_CONV")),
        "vl_nota_comerc":               f(_get(root, ".//VL_NOTA_COMERC")),
        "vl_nota_comerc_curto_przo":    f(_get(root, ".//VL_NOTA_COMERC_CURTO_PRZO")),
        "vl_nota_comerc_longo_przo":    f(_get(root, ".//VL_NOTA_COMERC_LONGO_PRZO")),
        "vl_tit_cred":                  f(_get(root, ".//VL_TIT_CRED")),
        "vl_tit_div_corp":              f(_get(root, ".//VL_TIT_DIV_CORP")),
        "vl_outr_tit_div_corp":         f(_get(root, ".//VL_OUTR_TIT_DIV_CORP")),
        "vl_tit_partic_societ":         f(_get(root, ".//VL_TIT_PARTIC_SOCIET")),
        "vl_outr_tit_partic":           f(_get(root, ".//VL_OUTR_TIT_PARTIC")),
        "vl_partic_societ_cia_fechada": f(_get(root, ".//VL_PARTIC_SOCIET_CIA_FECHADA")),
        "vl_tit_renda_fixa":            f(_get(root, ".//VL_TIT_RENDA_FIXA")),
        "vl_tit_securit":               f(_get(root, ".//VL_TIT_SECURIT")),
        "vl_outr_tit_securit":          f(_get(root, ".//VL_OUTR_TIT_SECURIT")),
        "vl_acao_cert_depos_acao":      f(_get(root, ".//VL_ACAO_CERT_DEPOS_ACAO")),
        "vl_mobil":                     f(_get(root, ".//VL_MOBIL")),
        "vl_instrun_finan_deriv_hedge": f(_get(root, ".//VL_INSTRUN_FINAN_DERIV_HEDGE")),
        "vl_tot_fdo_invest_renda_fixa": f(_get(root, ".//VL_TOT_FDO_INVEST_RENDA_FIXA")),
        "vl_fii":                       f(_get(root, ".//VL_FII")),
        "vl_fiim":                      f(_get(root, ".//VL_FIIM")),
        "vl_fiagro":                    f(_get(root, ".//VL_FIAGRO")),
        "vl_fidc":                      f(_get(root, ".//VL_FIDC")),
        "vl_fip":                       f(_get(root, ".//VL_FIP")),
        "vl_fif":                       f(_get(root, ".//VL_FIF")),
        "vl_cot_finvest":               f(_get(root, ".//VL_COT_FINVEST")),

        # Aging buckets
        "vl_tot_ativo_a_vencer":           f(_get(root, ".//VL_TOT_ATIVO_A_VENCER")),
        "vl_tot_ativo_vencido":            f(_get(root, ".//VL_TOT_ATIVO_VENCIDO")),
        "vl_prazo_venc_liq_ativo":         f(_get(root, ".//VL_PRAZO_VENC_LIQ_ATIVO")),
        "vl_a_vencer_prazo_venc_30":       f(_get(root, ".//VL_A_VENCER_PRAZO_VENC_30")),
        "vl_a_vencer_prazo_venc_31_60":    f(_get(root, ".//VL_A_VENCER_PRAZO_VENC_31_60")),
        "vl_a_vencer_prazo_venc_61_90":    f(_get(root, ".//VL_A_VENCER_PRAZO_VENC_61_90")),
        "vl_a_vencer_prazo_venc_91_120":   f(_get(root, ".//VL_A_VENCER_PRAZO_VENC_91_120")),
        "vl_a_vencer_prazo_venc_121_180":  f(_get(root, ".//VL_A_VENCER_PRAZO_VENC_121_180")),
        "vl_a_vencer_prazo_venc_181_360":  f(_get(root, ".//VL_A_VENCER_PRAZO_VENC_181_360")),
        "vl_a_vencer_prazo_venc_361_720":  f(_get(root, ".//VL_A_VENCER_PRAZO_VENC_361_720")),
        "vl_a_vencer_prazo_venc_721_1080": f(_get(root, ".//VL_A_VENCER_PRAZO_VENC_721_1080")),
        "vl_a_vencer_prazo_venc_1081":     f(_get(root, ".//VL_A_VENCER_PRAZO_VENC_1081")),
        "vl_vencido_prazo_venc_30":        f(_get(root, ".//VL_VENCIDO_PRAZO_VENC_30")),
        "vl_vencido_prazo_venc_31_60":     f(_get(root, ".//VL_VENCIDO_PRAZO_VENC_31_60")),
        "vl_vencido_prazo_venc_61_90":     f(_get(root, ".//VL_VENCIDO_PRAZO_VENC_61_90")),
        "vl_vencido_prazo_venc_91_120":    f(_get(root, ".//VL_VENCIDO_PRAZO_VENC_91_120")),
        "vl_vencido_prazo_venc_121_180":   f(_get(root, ".//VL_VENCIDO_PRAZO_VENC_121_180")),
        "vl_vencido_prazo_venc_181_360":   f(_get(root, ".//VL_VENCIDO_PRAZO_VENC_181_360")),
        "vl_vencido_prazo_venc_361_720":   f(_get(root, ".//VL_VENCIDO_PRAZO_VENC_361_720")),
        "vl_vencido_prazo_venc_721_1080":  f(_get(root, ".//VL_VENCIDO_PRAZO_VENC_721_1080")),
        "vl_vencido_prazo_venc_1081":      f(_get(root, ".//VL_VENCIDO_PRAZO_VENC_1081")),
        "vl_dev_pess_natu_liq_finan":      f(_get(root, ".//VL_DEV_PESS_NATU_LIQ_FINAN")),
        "vl_dev_pess_natu_liq_fisica":     f(_get(root, ".//VL_DEV_PESS_NATU_LIQ_FISICA")),
        "vl_dev_pess_jur_liq_finan":       f(_get(root, ".//VL_DEV_PESS_JUR_LIQ_FINAN")),
        "vl_dev_pess_jur_liq_fisica":      f(_get(root, ".//VL_DEV_PESS_JUR_LIQ_FISICA")),
        "vl_tit_cred_liq_finan":           f(_get(root, ".//VL_TIT_CRED_LIQ_FINAN")),
        "vl_tit_cred_liq_fisica":          f(_get(root, ".//VL_TIT_CRED_LIQ_FISICA")),

        # Receber / Passivo
        "vl_receber":             f(_get(root, ".//VL_RECEBER")),
        "vl_tot_passivo":         f(_get(root, ".//VL_TOT_PASSIVO")),
        "vl_rend_distrib":        f(_get(root, ".//VL_REND_DISTRIB")),
        "vl_tx_admin_pagar":      f(_get(root, ".//VL_TX_ADMIN_PAGAR")),
        "vl_tx_gestao_pagar":     f(_get(root, ".//VL_TX_GESTAO_PAGAR")),
        "vl_tx_distrib_pagar":    f(_get(root, ".//VL_TX_DISTRIB_PAGAR")),
        "vl_tx_perform_pagar":    f(_get(root, ".//VL_TX_PERFORM_PAGAR")),
        "vl_obrig_arquis_ativo":  f(_get(root, ".//VL_OBRIG_ARQUIS_ATIVO")),
        "vl_adiant_vend_ativo":   f(_get(root, ".//VL_ADIANT_VEND_ATIVO")),
        "vl_adiant_valor_receb":  f(_get(root, ".//VL_ADIANT_VALOR_RECEB")),
        "vl_outr_valor_pagar":    f(_get(root, ".//VL_OUTR_VALOR_PAGAR")),
        "vl_provis_conting":      f(_get(root, ".//VL_PROVIS_CONTING")),
    }


# ---------------------------------------------------------------------------
# Test against sample XMLs
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python informe_parsers.py <xml_file>")
        sys.exit(1)

    with open(sys.argv[1], "r", encoding="utf-8") as fh:
        text = fh.read()

    # Try FII first
    try:
        result = parse_fii(text)
        if result:
            print("Detected as FII; sample fields:")
            for k in ("nome_fundo", "cnpj_fundo", "ativo", "patrimonio_liquido",
                      "cri_cra", "fii", "competencia"):
                print(f"  {k}: {result.get(k)}")
            sys.exit(0)
    except Exception:
        pass

    try:
        result = parse_fiagro(text)
        if result:
            print("Detected as FIAGRO; sample fields:")
            for k in ("nm_fundo", "nr_cnpj_fundo", "vl_ativo", "vl_patrimonio_liquido",
                      "vl_cert_receb_cri", "vl_cert_receb_cra", "dt_compt"):
                print(f"  {k}: {result.get(k)}")
            sys.exit(0)
    except Exception as e:
        print(f"FIAGRO parse error: {e}")
        sys.exit(1)

    print("Could not parse — unknown root tag")
    sys.exit(1)
