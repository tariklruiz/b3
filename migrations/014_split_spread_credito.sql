-- Migration 014: Split spread_credito into CDI and IPCA variants
-- ============================================================================
-- The original gestores schema had a single `spread_credito_bps` column. This
-- forced agents to choose one indexer for funds with mixed CDI/IPCA carteiras
-- and lost information for funds whose primary indexer was IPCA.
--
-- We split into two nullable columns and keep the original for backward
-- compatibility with the 5 manually-extracted rows. Going forward, the agent
-- writes only to the new columns; the old `spread_credito_bps` stays NULL on
-- new rows. We can drop it in a future migration once we're confident.
-- ============================================================================

ALTER TABLE gestores
    ADD COLUMN IF NOT EXISTS spread_credito_cdi_bps  NUMERIC,
    ADD COLUMN IF NOT EXISTS spread_credito_ipca_bps NUMERIC;

COMMENT ON COLUMN gestores.spread_credito_cdi_bps IS
    'Spread médio sobre CDI em basis points (CDI+3,66% → 366). NULL if fund is IPCA-only or pre-migration.';

COMMENT ON COLUMN gestores.spread_credito_ipca_bps IS
    'Spread médio sobre IPCA em basis points (IPCA+9% → 900). NULL if fund is CDI-only or pre-migration.';

COMMENT ON COLUMN gestores.spread_credito_bps IS
    'DEPRECATED — kept for the 5 pre-agent manual rows. New agent rows leave this NULL and use the CDI/IPCA-split columns instead.';
