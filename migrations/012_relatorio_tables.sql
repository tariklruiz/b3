-- Migration 012: Relatório Gerencial pipeline tables
-- Two tables:
--   1. relatorio_universe — top-50 FII + top-10 FIAGRO by 30-day trade count.
--      Populated by build_universe.py, refreshed monthly. The scraper uses this
--      as a CNPJ filter when processing the CVM grid response.
--   2. relatorios_gerenciais — metadata for every PDF the scraper downloads.
--      The agent queries this table to find (latest, m-1, m-12) PDFs per ticker.
-- ============================================================================

CREATE TABLE IF NOT EXISTS relatorio_universe (
    cnpj_fundo      TEXT PRIMARY KEY,
    ticker          TEXT NOT NULL,
    tipo_fundo      TEXT NOT NULL CHECK (tipo_fundo IN ('FII', 'FIAGRO')),
    classificacao   TEXT,                       -- 'Tijolo', 'Papel', 'Híbrido', 'FOF', 'Fiagro'
    ranking         INT NOT NULL,               -- 1..50 within FII, 1..10 within FIAGRO
    trade_count_30d BIGINT,                     -- sum of num_negocios over last 30 trading days
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_universe_active_tipo
    ON relatorio_universe(active, tipo_fundo);

CREATE INDEX IF NOT EXISTS idx_universe_ticker
    ON relatorio_universe(ticker);


CREATE TABLE IF NOT EXISTS relatorios_gerenciais (
    doc_id          BIGINT PRIMARY KEY,         -- CVM document id from the grid
    cnpj_fundo      TEXT NOT NULL,              -- normalized to digits-only via clean_cnpj()
    ticker          TEXT,                       -- resolved from relatorio_universe at scrape time
    tipo_fundo      TEXT CHECK (tipo_fundo IN ('FII', 'FIAGRO')),
    data_referencia DATE NOT NULL,              -- competência do relatório (period covered)
    data_entrega    TIMESTAMP,                  -- when the gestor protocoled the doc
    versao          INT,                        -- CVM version number (1, 2, ...)
    nome_arquivo    TEXT,                       -- original CVM filename, for traceability
    pdf_path        TEXT NOT NULL,              -- /mnt/volumes/relatorios/{ticker}/{doc_id}.pdf
    file_size_bytes BIGINT,
    sha256          TEXT,                       -- for integrity checks and dedup
    downloaded_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMP NULL,             -- set by agent on successful extraction;
                                                -- NULL means the PDF is pending processing
    pdf_deleted_at  TIMESTAMP NULL              -- set when the PDF file is removed from disk;
                                                -- decoupled from processed_at so we can audit
                                                -- when cleanup happened separately from extraction
);

-- Prevent duplicate versions of the same monthly report
CREATE UNIQUE INDEX IF NOT EXISTS uq_rel_cnpj_data_versao
    ON relatorios_gerenciais(cnpj_fundo, data_referencia, COALESCE(versao, 1));

CREATE INDEX IF NOT EXISTS idx_rel_cnpj_data_desc
    ON relatorios_gerenciais(cnpj_fundo, data_referencia DESC);

CREATE INDEX IF NOT EXISTS idx_rel_ticker_data_desc
    ON relatorios_gerenciais(ticker, data_referencia DESC);

CREATE INDEX IF NOT EXISTS idx_rel_data
    ON relatorios_gerenciais(data_referencia DESC);

-- Agent queries this index to find unprocessed PDFs efficiently
CREATE INDEX IF NOT EXISTS idx_rel_pending
    ON relatorios_gerenciais(downloaded_at) WHERE processed_at IS NULL;

COMMENT ON TABLE relatorio_universe IS
    'Top-50 FII + top-10 FIAGRO by 30-day trade count. Refreshed monthly by build_universe.py.';

COMMENT ON TABLE relatorios_gerenciais IS
    'Metadata for downloaded relatório gerencial PDFs. PDFs themselves live on Railway volume.';
