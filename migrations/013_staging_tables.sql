-- ============================================================================
-- FII Guia — Migration 013
-- ----------------------------------------------------------------------------
-- Creates staging tables for FII and FIAGRO informes mensais. These hold
-- parsed XML in its native shape (one column per leaf XML tag), serving
-- as the bronze layer in the bronze→gold pipeline.
--
-- Column names are snake_case versions of the XML tag names — preserves the
-- 1:1 mapping for diagnosis without requiring quoted identifiers.
--
-- Lifecycle (managed by informe_mensal_scraper.py):
--   1. At start of each scraper run: TRUNCATE both staging tables
--      (NB: this is the "safer truncate" — staging from previous run stays
--      until next run starts. Allows manual re-run of consolidation if it
--      fails the first time.)
--   2. Scraper INSERTs each parsed doc into the appropriate staging table
--   3. After all docs parsed, consolidation reads staging → upserts into
--      informe_mensal (gold)
--   4. Staging stays populated until next run's truncate.
--
-- Run after migration 012:
--     psql "$DATABASE_URL" -f migrations/013_staging_tables.sql
-- Idempotent.
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- FII staging — CVM 571/2015 schema (root: <DadosEconomicoFinanceiros>)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS informe_mensal_staging_fii;
CREATE TABLE informe_mensal_staging_fii (
    id_documento                BIGINT      PRIMARY KEY,

    -- Identity
    nome_fundo                  TEXT,
    cnpj_fundo                  TEXT,
    codigo_isin                 TEXT,
    data_funcionamento          DATE,
    publico_alvo                TEXT,
    fundo_exclusivo             BOOLEAN,
    prazo_duracao               TEXT,
    encerramento_exercicio      TEXT,
    classificacao               TEXT,
    subclassificacao            TEXT,
    tipo_gestao                 TEXT,
    segmento_atuacao            TEXT,
    vinculo_familiar_cotistas   BOOLEAN,

    -- Mercado
    bolsa                       BOOLEAN,
    bvmf                        BOOLEAN,
    cetip                       BOOLEAN,
    mb                          BOOLEAN,
    mbo                         BOOLEAN,

    -- Administrador
    nome_administrador          TEXT,
    cnpj_administrador          TEXT,
    logradouro                  TEXT,
    numero                      TEXT,
    complemento                 TEXT,
    bairro                      TEXT,
    cidade                      TEXT,
    estado                      TEXT,
    cep                         TEXT,
    telefone                    TEXT,
    email                       TEXT,
    site                        TEXT,

    -- Competencia
    competencia                 DATE,

    -- Cotistas
    pessoa_fisica                       INTEGER,
    pj_nao_financeira                   INTEGER,
    banco_comercial                     INTEGER,
    corretora_distribuidora             INTEGER,
    outras_pj_financeiras               INTEGER,
    investidores_nao_residentes         INTEGER,
    entidade_aberta_prev_compl          INTEGER,
    entidade_fechada_prev_compl         INTEGER,
    regime_proprio_prev                 INTEGER,
    sociedade_seguradora                INTEGER,
    sociedade_cap_arrend_mercantil      INTEGER,
    fundos_inv_imobiliario              INTEGER,
    outros_fundos_inv                   INTEGER,
    cotistas_dist_fundo                 INTEGER,
    outros_tipos_cotistas               INTEGER,

    -- Items 1-8
    ativo                       NUMERIC,
    patrimonio_liquido          NUMERIC,
    qtd_cotas_emitidas          NUMERIC,
    num_cotas_emitidas          NUMERIC,
    valor_patr_cotas            NUMERIC,
    despesas_tx_administracao   NUMERIC,
    despesas_ag_custodiante     NUMERIC,
    rent_patrimonial_mes        NUMERIC,
    dividend_yield_mes          NUMERIC,
    amortiz_acoes_cotas         NUMERIC,

    -- Item 9 — liquidez
    disponibilidades            NUMERIC,
    titulos_publicos            NUMERIC,
    titulos_privados            NUMERIC,
    fundos_renda_fixa           NUMERIC,

    -- Item 10 — investimentos
    terrenos                    NUMERIC,
    imoveis_renda_acabados      NUMERIC,
    imoveis_renda_construcao    NUMERIC,
    imoveis_venda_acabados      NUMERIC,
    imoveis_venda_construcao    NUMERIC,
    outros_direitos_reais       NUMERIC,
    acoes                       NUMERIC,
    debentures                  NUMERIC,
    bonus_subscricao            NUMERIC,
    certificados_dep_val_mob    NUMERIC,
    fia                         NUMERIC,
    fip                         NUMERIC,
    fii                         NUMERIC,
    fdic                        NUMERIC,
    outras_cotas_fi             NUMERIC,
    notas_promissorias          NUMERIC,
    notas_comerciais            NUMERIC,
    acoes_sociedades_ativ_fii   NUMERIC,
    cotas_sociedades_ativ_fii   NUMERIC,
    cepac                       NUMERIC,
    cri_cra                     NUMERIC,
    letras_hipotecarias         NUMERIC,
    lci_lca                     NUMERIC,
    lig                         NUMERIC,
    outros_valores_mobiliarios  NUMERIC,

    -- Item 11
    contas_receber_alugueis     NUMERIC,
    contas_receber_venda_imov   NUMERIC,
    outros_valores_receber      NUMERIC,
    alugueis                    NUMERIC,
    outros_valores              NUMERIC,
    total_investido             NUMERIC,

    -- Items 12-22 — passivo
    rendimentos_distribuir          NUMERIC,
    tx_administracao_pagar          NUMERIC,
    tx_performance_pagar            NUMERIC,
    obrigacoes_aquisicao_imov       NUMERIC,
    adiantamento_venda_imov         NUMERIC,
    adiantamento_alugueis           NUMERIC,
    obrigacoes_sec_recebiveis       NUMERIC,
    instrumentos_financeiros_deriv  NUMERIC,
    provisoes_contingencias         NUMERIC,
    outros_valores_pagar            NUMERIC,
    provisoes_garantias             NUMERIC,
    total_passivo                   NUMERIC,

    -- Items 23-25
    total_imoveis_onus              NUMERIC,
    total_garantias_classe          NUMERIC,
    total_garantias_cotistas        NUMERIC,

    inserido_em                 TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_staging_fii_cnpj
    ON informe_mensal_staging_fii (cnpj_fundo);

COMMENT ON TABLE informe_mensal_staging_fii IS
'Bronze layer for FII Informe Mensal Estruturado (CVM 571/2015 schema, root: DadosEconomicoFinanceiros). Truncated at start of each scraper run.';


-- ----------------------------------------------------------------------------
-- FIAGRO staging — CVM 175/2022 Anexo VI schema (root: <DOC_ARQ>)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS informe_mensal_staging_fiagro;
CREATE TABLE informe_mensal_staging_fiagro (
    id_documento                        BIGINT      PRIMARY KEY,

    -- Cabeçalho (CAB_INFORM)
    nm_fundo                            TEXT,
    nr_cnpj_fundo                       TEXT,
    nm_classe                           TEXT,
    nr_cnpj_classe                      TEXT,
    dt_regs_func                        DATE,
    tp_publ_alvo                        TEXT,
    cd_isin                             TEXT,
    class_unica                         TEXT,
    classif_auto_regul                  TEXT,
    class_regr_outr_anexo               TEXT,
    przo_duracao                        TEXT,
    dt_encer_exerc_soc                  TEXT,
    cotst_vincl_famil                   TEXT,
    dt_compt                            TEXT,    -- 'MM/YYYY' format
    versao                              TEXT,

    -- Administrador / Gestor
    nm_adm                              TEXT,
    nr_cnpj_adm                         TEXT,
    nm_gestor                           TEXT,
    nr_cnpj_gestor                      TEXT,
    email_adm                           TEXT,
    site                                TEXT,
    serv_atend_cotst                    TEXT,
    entid_adm_merc_org                  TEXT,
    merc_negoc                          TEXT,

    -- Cotistas
    qtd_tot_cotst                       INTEGER,
    qtd_pess_natural                    INTEGER,
    qtd_pess_jurid_exct_financ          INTEGER,
    qtd_pess_jurid_financ               INTEGER,
    qtd_invest_nao_resid                INTEGER,
    qtd_entid_prev_compl_exct_rpps      INTEGER,
    qtd_entid_rpps                      INTEGER,
    qtd_socied_segur_resegur            INTEGER,
    qtd_fundos_invst                    INTEGER,
    qtd_outro_tipo_cotst                INTEGER,
    qtd_cotst_distrib_conta_ordem       INTEGER,

    -- Subclasses
    nr_cot_emitidas                     NUMERIC,
    nr_cot_nm_subclasse                 TEXT,
    nr_cot_subclasse                    NUMERIC,
    vl_pl_nm_subclasse                  TEXT,
    vl_pl_subclasse                     NUMERIC,

    -- Patrimônio e despesas
    vl_ativo                            NUMERIC,
    vl_patrimonio_liquido               NUMERIC,
    vl_patrimonio_liquido_cotas         NUMERIC,
    vl_desp_tx_admin_rel_pl_mes         NUMERIC,
    vl_desp_tx_gest_rel_pl_mes          NUMERIC,
    vl_desp_tx_distrib_rel_pl_mes       NUMERIC,
    vl_rentb_efetiv_mes                 NUMERIC,
    vl_rent_patrim_mes_ref              NUMERIC,
    vl_dividend_yield_mes_ref           NUMERIC,
    perc_amort_cotst_mes_ref            NUMERIC,

    -- Liquidez
    vl_total_mantido_neces_liq          NUMERIC,
    vl_ativ_finan                       NUMERIC,
    vl_ativ_finan_lato_sensu            NUMERIC,
    vl_ativ_finan_emis_inst_finan       NUMERIC,
    vl_outr_ativ_finan                  NUMERIC,
    vl_outr_ativ_emis_intst_finan       NUMERIC,

    -- Investimentos — agro-specific
    vl_total_invest                     NUMERIC,
    vl_direit_cred_agro                 NUMERIC,
    vl_demais_direit_cred               NUMERIC,
    vl_cert_receb_cri                   NUMERIC,
    vl_cert_receb_cra                   NUMERIC,
    vl_cdca                             NUMERIC,
    vl_cda_warrant                      NUMERIC,
    vl_cert_dep_agro_cda_warr_wa        NUMERIC,
    vl_cert_dir_cred_agro_cdca          NUMERIC,
    vl_cpr                              NUMERIC,
    vl_cpr_finan                        NUMERIC,
    vl_cpr_fisica                       NUMERIC,
    vl_cred_carbono_agro                NUMERIC,
    vl_cbio_cred_descarbon              NUMERIC,
    vl_direit_imov_rural                NUMERIC,
    vl_invest_imov_rural                NUMERIC,
    vl_outr_tit_cred_agro               NUMERIC,
    vl_lca                              NUMERIC,
    vl_lci                              NUMERIC,
    vl_debent                           NUMERIC,
    vl_debent_conv                      NUMERIC,
    vl_debent_nao_conv                  NUMERIC,
    vl_nota_comerc                      NUMERIC,
    vl_nota_comerc_curto_przo           NUMERIC,
    vl_nota_comerc_longo_przo           NUMERIC,
    vl_tit_cred                         NUMERIC,
    vl_tit_div_corp                     NUMERIC,
    vl_outr_tit_div_corp                NUMERIC,
    vl_tit_partic_societ                NUMERIC,
    vl_outr_tit_partic                  NUMERIC,
    vl_partic_societ_cia_fechada        NUMERIC,
    vl_tit_renda_fixa                   NUMERIC,
    vl_tit_securit                      NUMERIC,
    vl_outr_tit_securit                 NUMERIC,
    vl_acao_cert_depos_acao             NUMERIC,
    vl_mobil                            NUMERIC,
    vl_instrun_finan_deriv_hedge        NUMERIC,
    vl_tot_fdo_invest_renda_fixa        NUMERIC,
    vl_fii                              NUMERIC,
    vl_fiim                             NUMERIC,
    vl_fiagro                           NUMERIC,
    vl_fidc                             NUMERIC,
    vl_fip                              NUMERIC,
    vl_fif                              NUMERIC,
    vl_cot_finvest                      NUMERIC,

    -- Aging buckets
    vl_tot_ativo_a_vencer               NUMERIC,
    vl_tot_ativo_vencido                NUMERIC,
    vl_prazo_venc_liq_ativo             NUMERIC,
    vl_a_vencer_prazo_venc_30           NUMERIC,
    vl_a_vencer_prazo_venc_31_60        NUMERIC,
    vl_a_vencer_prazo_venc_61_90        NUMERIC,
    vl_a_vencer_prazo_venc_91_120       NUMERIC,
    vl_a_vencer_prazo_venc_121_180      NUMERIC,
    vl_a_vencer_prazo_venc_181_360      NUMERIC,
    vl_a_vencer_prazo_venc_361_720      NUMERIC,
    vl_a_vencer_prazo_venc_721_1080     NUMERIC,
    vl_a_vencer_prazo_venc_1081         NUMERIC,
    vl_vencido_prazo_venc_30            NUMERIC,
    vl_vencido_prazo_venc_31_60         NUMERIC,
    vl_vencido_prazo_venc_61_90         NUMERIC,
    vl_vencido_prazo_venc_91_120        NUMERIC,
    vl_vencido_prazo_venc_121_180       NUMERIC,
    vl_vencido_prazo_venc_181_360       NUMERIC,
    vl_vencido_prazo_venc_361_720       NUMERIC,
    vl_vencido_prazo_venc_721_1080      NUMERIC,
    vl_vencido_prazo_venc_1081          NUMERIC,
    vl_dev_pess_natu_liq_finan          NUMERIC,
    vl_dev_pess_natu_liq_fisica         NUMERIC,
    vl_dev_pess_jur_liq_finan           NUMERIC,
    vl_dev_pess_jur_liq_fisica          NUMERIC,
    vl_tit_cred_liq_finan               NUMERIC,
    vl_tit_cred_liq_fisica              NUMERIC,

    -- Receber / Passivo
    vl_receber                          NUMERIC,
    vl_tot_passivo                      NUMERIC,
    vl_rend_distrib                     NUMERIC,
    vl_tx_admin_pagar                   NUMERIC,
    vl_tx_gestao_pagar                  NUMERIC,
    vl_tx_distrib_pagar                 NUMERIC,
    vl_tx_perform_pagar                 NUMERIC,
    vl_obrig_arquis_ativo               NUMERIC,
    vl_adiant_vend_ativo                NUMERIC,
    vl_adiant_valor_receb               NUMERIC,
    vl_outr_valor_pagar                 NUMERIC,
    vl_provis_conting                   NUMERIC,

    inserido_em                         TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_staging_fiagro_cnpj
    ON informe_mensal_staging_fiagro (nr_cnpj_fundo);

COMMENT ON TABLE informe_mensal_staging_fiagro IS
'Bronze layer for FIAGRO Informe Mensal (CVM 175/2022 Anexo VI schema, root: DOC_ARQ). Truncated at start of each scraper run.';

COMMIT;
