-- ============================================================================
-- FII Guia — Migration 003
-- ----------------------------------------------------------------------------
-- Adds the `fii_quotes_latest` table used by the quote-crawler service.
-- One row per ticker, price overwritten on each crawl. No historical data.
--
-- Run against Railway Postgres:
--     psql "$DATABASE_URL" -f migrations/003_add_fii_quotes_latest.sql
-- Idempotent via CREATE TABLE IF NOT EXISTS.
-- ============================================================================

CREATE TABLE IF NOT EXISTS fii_quotes_latest (
    cod_neg    TEXT        PRIMARY KEY,
    preco      NUMERIC,
    atualizado TIMESTAMPTZ DEFAULT now()
);
