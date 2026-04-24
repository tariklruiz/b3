-- ============================================================================
-- FII Guia — Migration 004
-- ----------------------------------------------------------------------------
-- Adds the `fund_health_score` view, which computes per-fund health metrics
-- from informe_mensal + dividendos at query time:
--   - alavancagem (passivo / ativo)
--   - cobertura de dividendos (rendimentos_distribuir / avg monthly payout 12m)
--   - health score (0-4), tier, narrative
--
-- Non-materialized view. Queries are cheap because /fundo/informe only fetches
-- one row at a time (WHERE cnpj_fundo = ?), and the underlying indexes cover
-- the join paths.
--
-- Run against Railway Postgres:
--     psql "$DATABASE_URL" -f migrations/004_add_fund_health_score_view.sql
-- Idempotent via CREATE OR REPLACE VIEW.
-- ============================================================================

CREATE OR REPLACE VIEW fund_health_score AS
WITH latest_informe AS (
    -- Most recent informe filing per fund
    SELECT DISTINCT ON (cnpj_fundo)
        cnpj_fundo,
        competencia,
        classificacao,
        ativo_total,
        patrimonio_liquido,
        total_passivo,
        num_cotas_emitidas,
        rendimentos_distribuir,
        imoveis_renda,
        titulos_privados,
        fundos_renda_fixa,
        cri_cra
    FROM informe_mensal
    WHERE cnpj_fundo IS NOT NULL
      AND ativo_total IS NOT NULL
      AND ativo_total > 0
    ORDER BY cnpj_fundo, competencia DESC
),
div_12m AS (
    -- Avg monthly dividend payout per ticker over last 12 months
    -- (uses ticker because dividendos.cnpj_fundo can be inconsistent
    -- for some historical rows; ticker is the reliable join key)
    SELECT
        d.cod_negociacao,
        d.cnpj_fundo,
        AVG(d.valor_provento) AS avg_valor_provento,
        COUNT(*) AS n_months
    FROM dividendos d
    WHERE d.data_base > CURRENT_DATE - INTERVAL '12 months'
      AND d.valor_provento IS NOT NULL
      AND d.valor_provento > 0
      AND d.cnpj_fundo IS NOT NULL
    GROUP BY d.cod_negociacao, d.cnpj_fundo
),
div_3m_fallback AS (
    -- Fallback: average over 3 months for funds without 12m history
    SELECT
        d.cod_negociacao,
        d.cnpj_fundo,
        AVG(d.valor_provento) AS avg_valor_provento,
        COUNT(*) AS n_months
    FROM dividendos d
    WHERE d.data_base > CURRENT_DATE - INTERVAL '3 months'
      AND d.valor_provento IS NOT NULL
      AND d.valor_provento > 0
      AND d.cnpj_fundo IS NOT NULL
    GROUP BY d.cod_negociacao, d.cnpj_fundo
),
metrics AS (
    SELECT
        li.cnpj_fundo,
        li.competencia,
        li.classificacao,
        li.ativo_total,
        li.patrimonio_liquido,
        li.total_passivo,
        li.num_cotas_emitidas,
        li.rendimentos_distribuir,
        li.imoveis_renda,
        li.titulos_privados,
        li.fundos_renda_fixa,
        li.cri_cra,

        -- alavancagem = passivo / ativo
        CASE
            WHEN li.total_passivo IS NULL THEN NULL
            ELSE li.total_passivo / NULLIF(li.ativo_total, 0)
        END AS alavancagem_ratio,

        -- Use 12m average if available, else 3m fallback
        COALESCE(d12.avg_valor_provento, d3.avg_valor_provento) AS avg_valor_provento,
        COALESCE(d12.n_months, d3.n_months, 0) AS div_n_months,
        CASE
            WHEN d12.avg_valor_provento IS NOT NULL THEN '12m_avg'
            WHEN d3.avg_valor_provento  IS NOT NULL THEN '3m_fallback'
            ELSE 'no_data'
        END AS cobertura_method

    FROM latest_informe li
    LEFT JOIN div_12m d12 ON d12.cnpj_fundo = li.cnpj_fundo
    LEFT JOIN div_3m_fallback d3 ON d3.cnpj_fundo = li.cnpj_fundo
),
computed AS (
    SELECT
        m.*,

        -- cobertura in months:
        --   (rendimentos_distribuir in BRL) /
        --   (avg_valor_provento per cota × num_cotas_emitidas = total payout per month in BRL)
        CASE
            WHEN m.avg_valor_provento IS NULL OR m.avg_valor_provento <= 0 THEN NULL
            WHEN m.num_cotas_emitidas IS NULL OR m.num_cotas_emitidas <= 0 THEN NULL
            WHEN m.rendimentos_distribuir IS NULL THEN NULL
            ELSE m.rendimentos_distribuir /
                 (m.avg_valor_provento * m.num_cotas_emitidas)
        END AS cobertura_meses

    FROM metrics m
),
scored AS (
    SELECT
        c.*,

        -- alavancagem scoring: <15% = 2, 15-30% = 1, >30% = 0
        CASE
            WHEN c.alavancagem_ratio IS NULL THEN NULL
            WHEN c.alavancagem_ratio < 0.15 THEN 2
            WHEN c.alavancagem_ratio < 0.30 THEN 1
            ELSE 0
        END AS alav_pts,

        CASE
            WHEN c.alavancagem_ratio IS NULL THEN NULL
            WHEN c.alavancagem_ratio < 0.15 THEN 'saudavel'
            WHEN c.alavancagem_ratio < 0.30 THEN 'atencao'
            ELSE 'risco'
        END AS alav_tier,

        CASE
            WHEN c.alavancagem_ratio IS NULL THEN NULL
            WHEN c.alavancagem_ratio < 0.15 THEN 'baixa alavancagem'
            WHEN c.alavancagem_ratio < 0.30 THEN 'alavancagem moderada'
            ELSE 'alta alavancagem'
        END AS alav_label,

        -- cobertura scoring: >=2 = 2, 1-2 = 1, <1 = 0
        -- If cobertura_meses can't be computed (missing dividend history or
        -- zero num_cotas), return NULL — we can't judge what we can't measure.
        CASE
            WHEN c.cobertura_method = 'no_data' THEN NULL
            WHEN c.rendimentos_distribuir IS NULL THEN NULL
            WHEN c.cobertura_meses IS NULL THEN NULL
            WHEN c.cobertura_meses >= 2.0 THEN 2
            WHEN c.cobertura_meses >= 1.0 THEN 1
            ELSE 0
        END AS cobert_pts,

        CASE
            WHEN c.cobertura_method = 'no_data' THEN NULL
            WHEN c.rendimentos_distribuir IS NULL THEN NULL
            WHEN c.cobertura_meses IS NULL THEN NULL
            WHEN c.cobertura_meses >= 2.0 THEN 'saudavel'
            WHEN c.cobertura_meses >= 1.0 THEN 'atencao'
            ELSE 'risco'
        END AS cobert_tier,

        CASE
            WHEN c.cobertura_method = 'no_data' THEN NULL
            WHEN c.rendimentos_distribuir IS NULL THEN NULL
            WHEN c.cobertura_meses IS NULL THEN NULL
            WHEN c.cobertura_meses >= 2.0 THEN 'cobertura confortável'
            WHEN c.cobertura_meses >= 1.0 THEN 'cobertura moderada'
            ELSE 'cobertura apertada'
        END AS cobert_label

    FROM computed c
)
SELECT
    s.cnpj_fundo,
    s.competencia,
    s.classificacao AS classificacao_declarada,

    -- Raw metric values (frontend formats them)
    s.alavancagem_ratio,
    s.alav_pts,
    s.alav_tier,
    s.alav_label,

    s.cobertura_meses,
    s.cobert_pts,
    s.cobert_tier,
    s.cobert_label,
    s.cobertura_method,

    -- Composition as fractions of ativo_total. NULL only when source value is
    -- NULL (missing data); 0 means the fund genuinely has zero in that category.
    s.cri_cra          / NULLIF(s.ativo_total, 0) AS cri_cra_pct,
    s.titulos_privados / NULLIF(s.ativo_total, 0) AS titulos_privados_pct,
    s.fundos_renda_fixa / NULLIF(s.ativo_total, 0) AS fundos_renda_fixa_pct,
    s.imoveis_renda    / NULLIF(s.ativo_total, 0) AS imoveis_renda_pct,
    -- "outros" = whatever's left; clamped to 0 if the named categories sum to > ativo
    GREATEST(
        0,
        1 - COALESCE(s.cri_cra / NULLIF(s.ativo_total, 0), 0)
          - COALESCE(s.titulos_privados / NULLIF(s.ativo_total, 0), 0)
          - COALESCE(s.fundos_renda_fixa / NULLIF(s.ativo_total, 0), 0)
          - COALESCE(s.imoveis_renda / NULLIF(s.ativo_total, 0), 0)
    ) AS outros_pct,

    -- Overall score (sum of component points)
    CASE
        WHEN s.alav_pts IS NULL AND s.cobert_pts IS NULL THEN NULL
        ELSE COALESCE(s.alav_pts, 0) + COALESCE(s.cobert_pts, 0)
    END AS score,

    -- max_score: 4 normally, 2 if one component is missing
    CASE
        WHEN s.alav_pts IS NULL AND s.cobert_pts IS NULL THEN NULL
        WHEN s.alav_pts IS NULL OR s.cobert_pts IS NULL THEN 2
        ELSE 4
    END AS max_score,

    -- Tier from score (full 4-point scale)
    CASE
        WHEN s.alav_pts IS NULL AND s.cobert_pts IS NULL THEN NULL
        WHEN s.alav_pts IS NULL OR s.cobert_pts IS NULL THEN
            -- Reduced to 2-point scale: 2 = saudavel, 1 = atencao, 0 = risco
            CASE (COALESCE(s.alav_pts, 0) + COALESCE(s.cobert_pts, 0))
                WHEN 2 THEN 'saudavel'
                WHEN 1 THEN 'atencao'
                ELSE 'risco'
            END
        ELSE
            -- Full 4-point scale
            CASE (s.alav_pts + s.cobert_pts)
                WHEN 4 THEN 'saudavel'
                WHEN 3 THEN 'saudavel'
                WHEN 2 THEN 'atencao'
                WHEN 1 THEN 'risco'
                ELSE 'risco'
            END
    END AS tier,

    -- Narrative: one of the 9 canonical strings, computed from tier combination
    -- Full-information rows (both components present)
    CASE
        WHEN s.alav_pts IS NULL OR s.cobert_pts IS NULL THEN NULL  -- handled below
        WHEN s.alav_pts = 2 AND s.cobert_pts = 2 THEN 'baixa alavancagem e cobertura confortável.'
        WHEN s.alav_pts = 2 AND s.cobert_pts = 1 THEN 'baixa alavancagem, cobertura de dividendos em nível moderado.'
        WHEN s.alav_pts = 2 AND s.cobert_pts = 0 THEN 'baixa alavancagem, mas reserva para dividendos apertada.'
        WHEN s.alav_pts = 1 AND s.cobert_pts = 2 THEN 'cobertura de dividendos confortável, alavancagem moderada.'
        WHEN s.alav_pts = 1 AND s.cobert_pts = 1 THEN 'alavancagem moderada e cobertura também moderada.'
        WHEN s.alav_pts = 1 AND s.cobert_pts = 0 THEN 'alavancagem moderada e reserva de dividendos apertada.'
        WHEN s.alav_pts = 0 AND s.cobert_pts = 2 THEN 'cobertura confortável, mas alta alavancagem.'
        WHEN s.alav_pts = 0 AND s.cobert_pts = 1 THEN 'alta alavancagem e cobertura moderada.'
        WHEN s.alav_pts = 0 AND s.cobert_pts = 0 THEN 'alta alavancagem e sem reserva para dividendos.'
        ELSE NULL
    END AS narrative_full,

    -- Partial data: one component missing
    CASE
        WHEN s.alav_pts IS NOT NULL AND s.cobert_pts IS NULL THEN
            'dados parciais — ' || s.alav_label || '.'
        WHEN s.alav_pts IS NULL AND s.cobert_pts IS NOT NULL THEN
            'dados parciais — ' || s.cobert_label || '.'
        ELSE NULL
    END AS narrative_partial

FROM scored s;

COMMENT ON VIEW fund_health_score IS
'Per-fund health metrics computed from informe_mensal + dividendos. Use narrative_full when both alav_pts and cobert_pts are non-null; use narrative_partial otherwise. tier is saudavel/atencao/risco.';
