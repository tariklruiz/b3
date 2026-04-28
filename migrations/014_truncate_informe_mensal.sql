-- ============================================================================
-- FII Guia — Migration 014
-- ----------------------------------------------------------------------------
-- Truncates informe_mensal to clear ~9000 rows that the old single-schema
-- parser inserted with all-NULL fields (FIAGRO docs that the FII parser
-- couldn't read).
--
-- After running this, the next informe_mensal_scraper.py run will repopulate
-- the table correctly via the bronze→gold pipeline.
--
-- Run after migration 013:
--     psql "$DATABASE_URL" -f migrations/014_truncate_informe_mensal.sql
-- ============================================================================

BEGIN;

TRUNCATE informe_mensal;

COMMIT;
