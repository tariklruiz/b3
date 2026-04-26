-- ============================================================================
-- FII Guia — Migration 005
-- ----------------------------------------------------------------------------
-- Adds two composition columns missed in the original schema:
--   - fii: investment in OTHER FIIs (CVM informe item 10.8 / <FII> in XML)
--   - acoes_sociedades_ativ_fii: shares of real-estate operating companies
--
-- Both are NUMERIC (BRL) and nullable. Existing rows get NULL for these
-- fields; the scraper populates them on next refresh. After re-scraping,
-- the "outros" residual in the composition pie shrinks dramatically for
-- FOF/hybrid funds.
--
-- Idempotent via IF NOT EXISTS — safe to re-run.
-- ============================================================================

BEGIN;

ALTER TABLE informe_mensal
    ADD COLUMN IF NOT EXISTS fii                       NUMERIC,
    ADD COLUMN IF NOT EXISTS acoes_sociedades_ativ_fii NUMERIC;

COMMIT;
