-- ============================================================================
-- FII Guia — Migration 012
-- ----------------------------------------------------------------------------
-- Extends informe_mensal to support FIAGRO data.
--
--   - tipo_fundo: discriminator ('FII' / 'FIAGRO' / 'FII-INFRA' future)
--   - cra: CRA receivables (FIAGRO has them as a separate field; FII XMLs
--          collapse CRI+CRA into CriCra, so for FII rows this stays NULL
--          and cri_cra holds the combined value)
--   - nome_gestor / cnpj_gestor: FIAGRO XML provides gestor as a separate
--          entity from administrador; useful identity field
--   - direitos_imov_rural / invest_imov_rural: FIAGRO-specific real estate
--   - cdca / cda_warrant / cpr / cbio: agro-specific receivables/instruments
--          (NULL for FII rows by definition — FIIs don't hold these)
--
-- Run after migration 011:
--     psql "$DATABASE_URL" -f migrations/012_add_fiagro_columns.sql
-- Idempotent.
-- ============================================================================

BEGIN;

ALTER TABLE informe_mensal
    ADD COLUMN IF NOT EXISTS tipo_fundo            TEXT,
    ADD COLUMN IF NOT EXISTS cra                   NUMERIC,
    ADD COLUMN IF NOT EXISTS nome_gestor           TEXT,
    ADD COLUMN IF NOT EXISTS cnpj_gestor           TEXT,
    ADD COLUMN IF NOT EXISTS direitos_imov_rural   NUMERIC,
    ADD COLUMN IF NOT EXISTS invest_imov_rural     NUMERIC,
    ADD COLUMN IF NOT EXISTS cdca                  NUMERIC,
    ADD COLUMN IF NOT EXISTS cda_warrant           NUMERIC,
    ADD COLUMN IF NOT EXISTS cpr                   NUMERIC,
    ADD COLUMN IF NOT EXISTS cbio                  NUMERIC,
    ADD COLUMN IF NOT EXISTS schema_version        TEXT;

COMMENT ON COLUMN informe_mensal.tipo_fundo IS
'Discriminator: FII | FIAGRO | (future) FII-INFRA. Set by scraper based on which CVM grid pass produced the doc.';

COMMENT ON COLUMN informe_mensal.cra IS
'CRA receivables (FIAGRO-specific field VL_CERT_RECEB_CRA). NULL for FII rows. For FIAGROs, cri_cra column gets VL_CERT_RECEB_CRI; this column gets VL_CERT_RECEB_CRA.';

COMMENT ON COLUMN informe_mensal.schema_version IS
'Source XML schema: cvm571 (FII Anexo 39-I) or cvm175 (FIAGRO Anexo VI).';

CREATE INDEX IF NOT EXISTS idx_informe_tipo_fundo
    ON informe_mensal (tipo_fundo);

COMMIT;
