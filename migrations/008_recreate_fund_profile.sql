-- ============================================================================
-- FII Guia — Migration 008
-- ----------------------------------------------------------------------------
-- Recreates fund_profile view after the informe_mensal redesign in 007.
-- Same logic as before — composition percentages and declared classification —
-- just adjusted for new column names.
--
-- Run after migration 007:
--     psql "$DATABASE_URL" -f migrations/008_recreate_fund_profile.sql
-- ============================================================================

CREATE OR REPLACE VIEW fund_profile AS
WITH latest_informe AS (
    -- Most recent filing per fund. Ties broken by id_documento (later
    -- filings = later id, including amendments to same competência).
    SELECT DISTINCT ON (cnpj_fundo)
        cnpj_fundo,
        competencia,
        classificacao,
        subclassificacao,
        ativo_total,
        cri_cra,
        titulos_privados,
        fundos_renda_fixa,
        imoveis_renda_acabados,
        fii,
        acoes_sociedades_ativ_fii
    FROM informe_mensal
    WHERE cnpj_fundo IS NOT NULL
      AND ativo_total IS NOT NULL
      AND ativo_total > 0
    ORDER BY cnpj_fundo, competencia DESC, id_documento DESC
)
SELECT
    cnpj_fundo,
    competencia,
    classificacao    AS classificacao_declarada,
    subclassificacao AS subclassificacao_declarada,

    cri_cra                    / NULLIF(ativo_total, 0) AS cri_cra_pct,
    titulos_privados           / NULLIF(ativo_total, 0) AS titulos_privados_pct,
    fundos_renda_fixa          / NULLIF(ativo_total, 0) AS fundos_renda_fixa_pct,
    imoveis_renda_acabados     / NULLIF(ativo_total, 0) AS imoveis_renda_pct,
    fii                        / NULLIF(ativo_total, 0) AS fii_pct,
    acoes_sociedades_ativ_fii  / NULLIF(ativo_total, 0) AS acoes_sociedades_ativ_fii_pct,

    GREATEST(
        0,
        1 - COALESCE(cri_cra                    / NULLIF(ativo_total, 0), 0)
          - COALESCE(titulos_privados           / NULLIF(ativo_total, 0), 0)
          - COALESCE(fundos_renda_fixa          / NULLIF(ativo_total, 0), 0)
          - COALESCE(imoveis_renda_acabados     / NULLIF(ativo_total, 0), 0)
          - COALESCE(fii                        / NULLIF(ativo_total, 0), 0)
          - COALESCE(acoes_sociedades_ativ_fii  / NULLIF(ativo_total, 0), 0)
    ) AS outros_pct

FROM latest_informe;

COMMENT ON VIEW fund_profile IS
'Per-fund composition snapshot from latest informe filing. Picks newest by competencia, ties broken by id_documento.';
