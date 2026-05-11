-- Migration 015: B3 canonical listing of all FIIs and FIAGROs
-- ============================================================================
-- The CVM grid endpoint returns `nomePregao` and `descricaoFundo` for each
-- relatório, but does NOT return CNPJ. Cloudflare also blocks per-CNPJ grid
-- queries from data-center IPs (Railway in particular).
--
-- The bulk grid pattern (no CNPJ filter, paginated tipoFundo=1 or 11) does
-- work, but we need a way to map grid rows back to tickers. This table is
-- that bridge.
--
-- Source: B3's public "Fundos Listados" downloads — they publish the full
-- canonical list of all FIIs and FIAGROs as CSV with columns:
--   - Razão Social    (legal name, matches grid's descricaoFundo)
--   - Fundo           (short name, matches grid's nomePregao)
--   - Código          (4-letter trading code stem; ticker = codigo + '11')
-- ============================================================================

CREATE TABLE IF NOT EXISTS fund_listing (
    codigo         TEXT NOT NULL,                       -- 4-letter trading stem (MXRF, KNCR, etc.)
    ticker         TEXT GENERATED ALWAYS AS (codigo || '11') STORED,
    razao_social   TEXT NOT NULL,                       -- legal name from B3
    fundo          TEXT,                                -- short name (matches nomePregao on grid)
    tipo_fundo     TEXT NOT NULL CHECK (tipo_fundo IN ('FII', 'FIAGRO')),
    updated_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (codigo)
);

CREATE INDEX IF NOT EXISTS idx_fund_listing_fundo ON fund_listing(fundo);
CREATE INDEX IF NOT EXISTS idx_fund_listing_ticker ON fund_listing(ticker);
CREATE INDEX IF NOT EXISTS idx_fund_listing_tipo ON fund_listing(tipo_fundo);

COMMENT ON TABLE fund_listing IS
    'Canonical B3 listing of all FIIs and FIAGROs. Updated from B3 CSV downloads. Used to map CVM grid responses back to tickers.';
