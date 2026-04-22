-- ============================================================================
-- FII Guia — Initial Postgres Schema
-- ----------------------------------------------------------------------------
-- Migration from 4 SQLite databases + fund_types.json to a single Postgres DB.
-- All columns renamed to snake_case. Dates stored as native DATE / TIMESTAMPTZ.
-- Run against a fresh Railway Postgres database:
--     psql "$DATABASE_URL" -f migrations/001_initial.sql
-- Idempotent via IF NOT EXISTS — safe to re-run.
-- ============================================================================

BEGIN;

-- ============================================================================
-- Schema evolution fixes (safe to run repeatedly)
-- ----------------------------------------------------------------------------
-- Some legacy dividendo rows in the SQLite source lack a ticker (cod_negociacao)
-- even though they have a valid CNPJ. SQLite allowed NULL; we relax the
-- constraint here so those historical rows can migrate successfully.
-- ============================================================================
ALTER TABLE IF EXISTS dividendos ALTER COLUMN cod_negociacao DROP NOT NULL;

-- ----------------------------------------------------------------------------
-- Widen NUMERIC(10,6) percentage/ratio columns to NUMERIC(18,6). SQLite is
-- untyped and some source rows contain values that overflow precision 10
-- (e.g. outlier monthly returns, data-quality issues). Widening preserves
-- all source data without truncation.
-- ----------------------------------------------------------------------------
ALTER TABLE IF EXISTS informe_mensal ALTER COLUMN dividend_yield_mes TYPE NUMERIC(18, 6);
ALTER TABLE IF EXISTS informe_mensal ALTER COLUMN rent_patr_mensal   TYPE NUMERIC(18, 6);
ALTER TABLE IF EXISTS gestores       ALTER COLUMN ltv_medio          TYPE NUMERIC(18, 6);
ALTER TABLE IF EXISTS gestores       ALTER COLUMN vacancia_pct       TYPE NUMERIC(18, 6);
ALTER TABLE IF EXISTS gestores       ALTER COLUMN contratos_vencer_12m_pct TYPE NUMERIC(18, 6);
ALTER TABLE IF EXISTS gestores       ALTER COLUMN cap_rate           TYPE NUMERIC(18, 6);


-- ============================================================================
-- 1. cotahist — B3 historical quotes (largest table, ~1GB in SQLite)
-- ----------------------------------------------------------------------------
-- PK (cod_neg, dt_pregao) is genuinely unique per ticker-per-day and gives
-- us the hot-path index for free. tp_merc=10 is the FII spot market filter;
-- we add a partial index on it since that's the 99% query pattern.
-- ============================================================================
CREATE TABLE IF NOT EXISTS cotahist (
    cod_neg          TEXT    NOT NULL,
    dt_pregao        DATE    NOT NULL,
    preco_ultimo     NUMERIC(12, 4),
    preco_abertura   NUMERIC(12, 4),
    preco_maximo     NUMERIC(12, 4),
    preco_minimo     NUMERIC(12, 4),
    vol_negocios     NUMERIC(20, 2),
    num_negocios     INTEGER,
    nom_resumido     TEXT,
    tp_merc          INTEGER NOT NULL,
    PRIMARY KEY (cod_neg, dt_pregao)
);

CREATE INDEX IF NOT EXISTS idx_cotahist_fii_spot
    ON cotahist (cod_neg, dt_pregao DESC)
    WHERE tp_merc = 10;

CREATE INDEX IF NOT EXISTS idx_cotahist_dt_pregao
    ON cotahist (dt_pregao DESC);


-- ============================================================================
-- 2. dividendos — dividend announcements
-- ----------------------------------------------------------------------------
-- id_documento as PK (was UNIQUE + separate rowid in SQLite — cleaner as PK).
-- Index on cod_negociacao for ticker lookups, on data_base for chronology.
-- ============================================================================
CREATE TABLE IF NOT EXISTS dividendos (
    id_documento     BIGINT  PRIMARY KEY,
    cod_negociacao   TEXT,
    cnpj_fundo       TEXT,
    data_base        DATE,
    valor_provento   NUMERIC(12, 6),
    data_pagamento   DATE,
    data_informacao  DATE,
    inserido_em      TIMESTAMPTZ DEFAULT now(),
    isento_ir        BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_dividendos_ticker_data
    ON dividendos (cod_negociacao, data_base DESC);

CREATE INDEX IF NOT EXISTS idx_dividendos_cnpj
    ON dividendos (cnpj_fundo);


-- ============================================================================
-- 3. erros — failed dividend scrape attempts (simple audit log)
-- ============================================================================
CREATE TABLE IF NOT EXISTS erros (
    id               BIGSERIAL PRIMARY KEY,
    id_documento     BIGINT,
    cod_negociacao   TEXT,
    motivo           TEXT,
    registrado_em    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_erros_registrado_em
    ON erros (registrado_em DESC);


-- ============================================================================
-- 4. informe_mensal — CVM monthly fund reports
-- ----------------------------------------------------------------------------
-- Composite PK on (cnpj_fundo, competencia) — one report per fund per month.
-- ============================================================================
CREATE TABLE IF NOT EXISTS informe_mensal (
    cnpj_fundo              TEXT    NOT NULL,
    competencia             DATE    NOT NULL,
    nome_fundo              TEXT,
    classificacao           TEXT,
    subclassificacao        TEXT,
    tipo_gestao             TEXT,
    nome_administrador      TEXT,
    total_cotistas          INTEGER,
    patrimonio_liquido      NUMERIC(20, 2),
    num_cotas_emitidas      NUMERIC(20, 6),
    valor_patr_cotas        NUMERIC(14, 6),
    despesas_tx_adm         NUMERIC(18, 2),
    dividend_yield_mes      NUMERIC(18, 6),
    rent_patr_mensal        NUMERIC(18, 6),
    rendimentos_distribuir  NUMERIC(18, 2),
    PRIMARY KEY (cnpj_fundo, competencia)
);

CREATE INDEX IF NOT EXISTS idx_informe_competencia
    ON informe_mensal (competencia DESC);


-- ============================================================================
-- 5. gestores — management reports (extracted from PDFs via Claude)
-- ----------------------------------------------------------------------------
-- JSONB for the 3 structured fields so we can query inside them later if
-- needed (e.g., filter by fundo inside alocacao_fundos). Surrogate PK +
-- UNIQUE (ticker, competencia) preserves the SQLite model exactly.
-- ============================================================================
CREATE TABLE IF NOT EXISTS gestores (
    id                          BIGSERIAL   PRIMARY KEY,
    ticker                      TEXT        NOT NULL,
    competencia                 DATE        NOT NULL,
    classificacao               TEXT,
    tom_gestor                  TEXT,
    pl_total_brl                NUMERIC(20, 2),
    cota_mercado                NUMERIC(14, 6),
    cota_patrimonial            NUMERIC(14, 6),
    spread_credito_bps          NUMERIC(10, 2),
    ltv_medio                   NUMERIC(18, 6),
    resultado_por_cota          NUMERIC(14, 6),
    distribuicao_por_cota       NUMERIC(14, 6),
    reserva_monetaria_brl       NUMERIC(20, 2),
    vacancia_pct                NUMERIC(18, 6),
    contratos_vencer_12m_pct    NUMERIC(18, 6),
    cap_rate                    NUMERIC(18, 6),
    contexto_meses              JSONB,
    cris_em_observacao          JSONB,
    alocacao_fundos             JSONB,
    mudancas_portfolio          TEXT,
    resumo                      TEXT,
    alertas_dados               TEXT,
    processado_em               TIMESTAMPTZ DEFAULT now(),
    UNIQUE (ticker, competencia)
);

CREATE INDEX IF NOT EXISTS idx_gestores_ticker
    ON gestores (ticker);

CREATE INDEX IF NOT EXISTS idx_gestores_competencia
    ON gestores (competencia DESC);


-- ============================================================================
-- 6. fund_types — classification (replaces fund_types.json)
-- ----------------------------------------------------------------------------
-- ticker as PK. `fonte` column preserves the metadata that used to live in
-- the JSON file header.
-- ============================================================================
CREATE TABLE IF NOT EXISTS fund_types (
    ticker           TEXT        PRIMARY KEY,
    classificacao    TEXT        NOT NULL,
    fonte            TEXT,
    atualizado       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fund_types_classificacao
    ON fund_types (classificacao);


COMMIT;

-- ============================================================================
-- Post-migration notes:
-- - After the one-shot load, run ANALYZE on every table so the query
--   planner has fresh statistics:
--     ANALYZE cotahist;
--     ANALYZE dividendos;
--     ANALYZE informe_mensal;
--     ANALYZE gestores;
--     ANALYZE fund_types;
-- - Postgres auto-vacuums; no manual VACUUM needed.
-- ============================================================================
