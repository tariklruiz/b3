-- ============================================================================
-- FII Guia — Migration 015
-- ----------------------------------------------------------------------------
-- Normalizes all CNPJ columns to digits-only ('14digits') across the schema.
--
-- Background: CVM XMLs sometimes use the formatted '00.000.000/0000-00' form
-- and sometimes the raw '00000000000000' form. Mixed formats in the same
-- column break JOINs and WHERE-clauses (e.g., HABT11 stored as formatted
-- but queried as digits-only → no row found).
--
-- Going forward: parsers (informe_parsers.py clean_cnpj()) normalize at
-- ingestion. This migration cleans existing rows so old data matches.
--
-- Tables touched:
--   - dividendos.cnpj_fundo
--   - informe_mensal.cnpj_fundo, cnpj_administrador, cnpj_gestor
--   - split_grouping.cnpj_fundo
--
-- Views over informe_mensal (fund_profile) are unaffected — they query the
-- underlying table dynamically, so they auto-fix once the table is clean.
--
-- Run:
--     psql "$DATABASE_URL" -f migrations/015_normalize_cnpj.sql
-- Idempotent (rows already clean stay clean).
-- ============================================================================

BEGIN;

-- dividendos
UPDATE dividendos
   SET cnpj_fundo = REGEXP_REPLACE(cnpj_fundo, '[^0-9]', '', 'g')
 WHERE cnpj_fundo IS NOT NULL
   AND cnpj_fundo ~ '[^0-9]';

-- informe_mensal
UPDATE informe_mensal
   SET cnpj_fundo = REGEXP_REPLACE(cnpj_fundo, '[^0-9]', '', 'g')
 WHERE cnpj_fundo IS NOT NULL
   AND cnpj_fundo ~ '[^0-9]';

UPDATE informe_mensal
   SET cnpj_administrador = REGEXP_REPLACE(cnpj_administrador, '[^0-9]', '', 'g')
 WHERE cnpj_administrador IS NOT NULL
   AND cnpj_administrador ~ '[^0-9]';

UPDATE informe_mensal
   SET cnpj_gestor = REGEXP_REPLACE(cnpj_gestor, '[^0-9]', '', 'g')
 WHERE cnpj_gestor IS NOT NULL
   AND cnpj_gestor ~ '[^0-9]';

-- split_grouping
UPDATE split_grouping
   SET cnpj_fundo = REGEXP_REPLACE(cnpj_fundo, '[^0-9]', '', 'g')
 WHERE cnpj_fundo IS NOT NULL
   AND cnpj_fundo ~ '[^0-9]';

COMMIT;
