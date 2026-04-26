-- ============================================================================
-- FII Guia — Migration 007
-- ----------------------------------------------------------------------------
-- Redesigns informe_mensal to capture all leaf-level XML fields from CVM's
-- "Informe Mensal Estruturado" filings. Drops historical data; the existing
-- data quality is mixed (some columns systematically NULL across years) and
-- the historical depth isn't currently used by any product feature.
--
-- Design decisions (from explicit user direction):
--   - One row per document (id_documento as PK, no fund-level uniqueness)
--   - All XML leaf fields captured, even rare-zero ones
--   - Historical informes aren't backfilled; daily scraper accumulates over time
--
-- Naming convention: snake_case based on XML tag, with category prefix where
-- it improves readability. Comments document the mapping back to CVM items
-- 9, 10, 11, 12-22 (numeric items in the PDF view of the filing).
--
-- Run after migrations 001-006:
--     psql "$DATABASE_URL" -f migrations/007_redesign_informe_mensal.sql
-- ============================================================================

BEGIN;

-- The previous fund_profile view depends on informe_mensal columns. Drop it
-- before recreating the table; we rebuild the view in migration 008.
DROP VIEW IF EXISTS fund_profile;

-- Drop and recreate the table from scratch (faster than ALTER for this many
-- columns, and simpler to read). Existing rows are dropped — historical depth
-- isn't currently used.
DROP TABLE IF EXISTS informe_mensal;

CREATE TABLE informe_mensal (
    -- ----------------------------------------------------------------- IDENTITY
    id_documento                BIGINT      PRIMARY KEY,

    nome_fundo                  TEXT,
    cnpj_fundo                  TEXT,
    codigo_isin                 TEXT,
    data_funcionamento          DATE,
    publico_alvo                TEXT,
    fundo_exclusivo             BOOLEAN,
    prazo_duracao               TEXT,
    encerramento_exercicio      TEXT,        -- e.g. "31/12"

    classificacao               TEXT,        -- Papel / Tijolo / etc.
    subclassificacao            TEXT,        -- Renda / Híbrido / etc.
    tipo_gestao                 TEXT,        -- Ativa / Passiva
    segmento_atuacao            TEXT,        -- Logística / Multi / etc.

    -- Mercado de negociação
    mercado_bolsa               BOOLEAN,
    mercado_bvmf                BOOLEAN,
    mercado_cetip               BOOLEAN,
    mercado_mb                  BOOLEAN,
    mercado_mbo                 BOOLEAN,

    -- ----------------------------------------------------------------- ADMIN
    nome_administrador          TEXT,
    cnpj_administrador          TEXT,
    administrador_logradouro    TEXT,
    administrador_numero        TEXT,
    administrador_complemento   TEXT,
    administrador_bairro        TEXT,
    administrador_cidade        TEXT,
    administrador_estado        TEXT,
    administrador_cep           TEXT,
    administrador_telefone      TEXT,
    administrador_email         TEXT,
    administrador_site          TEXT,

    -- ----------------------------------------------------------------- COMPETÊNCIA
    competencia                 DATE,

    -- ----------------------------------------------------------------- COTISTAS
    -- Number of unitholders by type (item: detalhamento do número de cotistas)
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

    -- ----------------------------------------------------------------- ITEMS 1-8 (financial summary)
    ativo_total                 NUMERIC,    -- item 1
    patrimonio_liquido          NUMERIC,    -- item 2
    qtd_cotas_emitidas          NUMERIC,    -- item 3 (integer count)
    num_cotas_emitidas          NUMERIC,    -- item 3 (decimal — sometimes != qtd, includes fractional)
    valor_patr_cotas            NUMERIC,    -- item 4 (R$ per cota)
    despesas_tx_administracao   NUMERIC,    -- item 5 (% PL)
    despesas_ag_custodiante     NUMERIC,    -- item 6 (% PL)
    rent_patrimonial_mes        NUMERIC,    -- item 7.1 (%)
    dividend_yield_mes          NUMERIC,    -- item 7.2 (%)
    amortiz_acoes_cotas         NUMERIC,    -- item 8 (%)

    -- ----------------------------------------------------------------- ITEM 9 (liquidez)
    -- Total mantido para necessidades de liquidez; sub-items 9.1-9.4
    disponibilidades            NUMERIC,    -- 9.1
    titulos_publicos            NUMERIC,    -- 9.2
    titulos_privados            NUMERIC,    -- 9.3
    fundos_renda_fixa           NUMERIC,    -- 9.4

    -- ----------------------------------------------------------------- ITEM 10 (investimentos)
    -- Item 10.1 has sub-items for direitos reais sobre imóveis
    terrenos                    NUMERIC,    -- 10.1.1
    imoveis_renda_acabados      NUMERIC,    -- 10.1.2
    imoveis_renda_construcao    NUMERIC,    -- 10.1.3
    imoveis_venda_acabados      NUMERIC,    -- 10.1.4
    imoveis_venda_construcao    NUMERIC,    -- 10.1.5
    outros_direitos_reais       NUMERIC,    -- 10.1.6

    acoes                       NUMERIC,    -- 10.2
    debentures                  NUMERIC,    -- 10.3
    bonus_subscricao            NUMERIC,    -- 10.4
    certificados_dep_val_mob    NUMERIC,    -- 10.5
    fia                         NUMERIC,    -- 10.6 — fundo de ações
    fip                         NUMERIC,    -- 10.7 — FIP
    fii                         NUMERIC,    -- 10.8 — outras cotas de FII
    fdic                        NUMERIC,    -- 10.9 — FIDC
    outras_cotas_fi             NUMERIC,    -- 10.10
    notas_promissorias          NUMERIC,    -- 10.11
    notas_comerciais            NUMERIC,    -- 10.12
    acoes_sociedades_ativ_fii   NUMERIC,    -- 10.13 — ações de sociedades ativ. permitidas a FII
    cotas_sociedades_ativ_fii   NUMERIC,    -- 10.14
    cepac                       NUMERIC,    -- 10.15
    cri_cra                     NUMERIC,    -- 10.16
    letras_hipotecarias         NUMERIC,    -- 10.17
    lci_lca                     NUMERIC,    -- 10.18
    lig                         NUMERIC,    -- 10.19
    outros_valores_mobiliarios  NUMERIC,    -- 10.20

    -- ----------------------------------------------------------------- ITEM 11 (valores a receber)
    contas_receber_alugueis     NUMERIC,    -- 11.1
    contas_receber_venda_imov   NUMERIC,    -- 11.2
    outros_valores_receber      NUMERIC,    -- 11.3

    -- Cross-cutting aggregates also reported:
    alugueis                    NUMERIC,    -- aggregate rent income (different from 11.1 receivables)
    outros_valores              NUMERIC,    -- residual reporting bucket
    total_investido             NUMERIC,    -- item 10 aggregate

    -- ----------------------------------------------------------------- ITEMS 12-22 (passivo)
    rendimentos_distribuir          NUMERIC,    -- 12 (declared but not paid)
    tx_administracao_pagar          NUMERIC,    -- 13
    tx_performance_pagar            NUMERIC,    -- 14
    obrigacoes_aquisicao_imov       NUMERIC,    -- 15
    adiantamento_venda_imov         NUMERIC,    -- 16
    adiantamento_alugueis           NUMERIC,    -- 17
    obrigacoes_sec_recebiveis       NUMERIC,    -- 18
    instrumentos_financeiros_deriv  NUMERIC,    -- 19
    provisoes_contingencias         NUMERIC,    -- 20
    outros_valores_pagar            NUMERIC,    -- 21
    provisoes_garantias             NUMERIC,    -- 22
    total_passivo                   NUMERIC,    -- aggregate of 12-22

    -- ----------------------------------------------------------------- ITEMS 23-25 (informações adicionais)
    total_imoveis_onus              NUMERIC,    -- 23
    total_garantias_classe          NUMERIC,    -- 24
    total_garantias_cotistas        NUMERIC,    -- 25

    -- ----------------------------------------------------------------- METADATA
    inserido_em                 TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_informe_cnpj_competencia
    ON informe_mensal (cnpj_fundo, competencia DESC);

CREATE INDEX idx_informe_competencia
    ON informe_mensal (competencia DESC);

COMMENT ON TABLE informe_mensal IS
'CVM Informe Mensal Estruturado (FII). One row per filing (id_documento). Schema captures all leaf-level XML fields for comprehensive analytics.';

COMMIT;
