-- ============================================================================
-- FII Guia — Migration 006
-- ----------------------------------------------------------------------------
-- Replaces fund_health_score (alavancagem + cobertura scoring) with
-- fund_profile (composition only). The previous metrics had data quality
-- issues that made them misleading:
--
--   - alavancagem (passivo/ativo) lumped operational items (declared
--     dividends, admin fees) with actual debt; almost all funds scored green.
--   - cobertura de dividendos penalized pass-through funds (the majority)
--     because they distribute everything monthly without accumulating reserve.
--
-- The new view exposes only what informe_mensal can describe reliably:
-- the fund's actual portfolio composition vs declared classification.
-- No scoring, no judgment.
--
-- Run after migrations 001-005:
--     psql "$DATABASE_URL" -f migrations/006_replace_with_fund_profile.sql
-- Idempotent: drops old view if exists, creates new one.
-- ============================================================================

DROP VIEW IF EXISTS fund_health_score;

CREATE OR REPLACE VIEW fund_profile AS
WITH latest_informe AS (
    -- Most recent informe filing per fund
    SELECT DISTINCT ON (cnpj_fundo)
        cnpj_fundo,
        competencia,
        classificacao,
        subclassificacao,
        ativo_total,
        imoveis_renda,
        titulos_privados,
        fundos_renda_fixa,
        cri_cra,
        fii,
        acoes_sociedades_ativ_fii
    FROM informe_mensal
    WHERE cnpj_fundo IS NOT NULL
      AND ativo_total IS NOT NULL
      AND ativo_total > 0
    ORDER BY cnpj_fundo, competencia DESC
)
SELECT
    cnpj_fundo,
    competencia,
    classificacao    AS classificacao_declarada,
    subclassificacao AS subclassificacao_declarada,

    -- Composition as fractions of ativo_total. NULL when source value is NULL
    -- (missing data); 0 when the fund genuinely has zero in that category.
    cri_cra                    / NULLIF(ativo_total, 0) AS cri_cra_pct,
    titulos_privados           / NULLIF(ativo_total, 0) AS titulos_privados_pct,
    fundos_renda_fixa          / NULLIF(ativo_total, 0) AS fundos_renda_fixa_pct,
    imoveis_renda              / NULLIF(ativo_total, 0) AS imoveis_renda_pct,
    fii                        / NULLIF(ativo_total, 0) AS fii_pct,
    acoes_sociedades_ativ_fii  / NULLIF(ativo_total, 0) AS acoes_sociedades_ativ_fii_pct,

    -- "outros" = whatever's left after the named categories. Clamped to 0
    -- in case rounding pushes the sum slightly above ativo.
    GREATEST(
        0,
        1 - COALESCE(cri_cra                    / NULLIF(ativo_total, 0), 0)
          - COALESCE(titulos_privados           / NULLIF(ativo_total, 0), 0)
          - COALESCE(fundos_renda_fixa          / NULLIF(ativo_total, 0), 0)
          - COALESCE(imoveis_renda              / NULLIF(ativo_total, 0), 0)
          - COALESCE(fii                        / NULLIF(ativo_total, 0), 0)
          - COALESCE(acoes_sociedades_ativ_fii  / NULLIF(ativo_total, 0), 0)
    ) AS outros_pct

FROM latest_informe;

COMMENT ON VIEW fund_profile IS
'Per-fund composition snapshot from latest informe_mensal. Returns asset class percentages and declared CVM classification. No scoring or judgment.';
