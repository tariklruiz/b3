"""
Guia FII — API (Postgres edition)

Endpoints:
    GET /prices?tickers=MXRF11&date=2026-03-31
    GET /tickers?search=MXRF
    GET /dates/all
    GET /dates/available?ticker=MXRF11
    GET /fundo/preco?ticker=MXRF11         — price history for health check page
    GET /fundo/dividendos?ticker=MXRF11    — last 13 dividends + stats
    GET /fundo/informe?ticker=MXRF11       — latest monthly report
    GET /fundo/gestor?ticker=MXRF11        — latest management report summary
    GET /benchmarks                        — median DY per classificação
    GET /cdi                               — current CDI rate from BCB
    GET /debug                             — deploy diagnostics

This file is the Postgres rewrite of the original SQLite-based API. Endpoint
shapes, query params, and response JSON are IDENTICAL to the previous version.
Only the data layer changed: sqlite3 -> psycopg2 via db.py (pooled connections).
"""

from __future__ import annotations

import math
import os
import re
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from db import (
    close_pool,
    get_all_fund_types,
    get_fund_type,
    init_pool,
    query_all,
    query_one,
)

# ---------------------------------------------------------------------------
# App lifecycle: init and close the connection pool
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_pool()
    try:
        yield
    finally:
        close_pool()


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])

app = FastAPI(
    title="Guia FII API",
    description="B3 price and dividend data for FIIs (Postgres)",
    version="3.0.0",
    lifespan=lifespan,
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

TICKER_RE = re.compile(r"^[A-Z0-9]{1,12}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_ticker(t: str) -> str:
    t = t.strip().upper()
    if not TICKER_RE.match(t):
        raise HTTPException(status_code=400, detail=f"Ticker inválido: '{t}'")
    return t


def validate_tickers(raw: str) -> list[str]:
    tickers = [t.strip().upper() for t in raw.split(",") if t.strip()]
    if not tickers:
        raise HTTPException(status_code=400, detail="Nenhum ativo informado.")
    if len(tickers) > 50:
        raise HTTPException(status_code=400, detail="Máximo de 50 ativos por consulta.")
    for t in tickers:
        if not TICKER_RE.match(t):
            raise HTTPException(status_code=400, detail=f"Ativo inválido: '{t}'")
    return tickers


def validate_date(d: str) -> str:
    if not DATE_RE.match(d):
        raise HTTPException(status_code=400, detail="Data inválida. Use YYYY-MM-DD.")
    try:
        datetime.strptime(d, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Data inválida: '{d}'")
    return d


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def iso(d: Any) -> str | None:
    """
    Convert a Python date/datetime to ISO string, preserving backward
    compatibility with the previous SQLite API where dates were TEXT.
    Returns None for None, unchanged for strings, isoformat for date/datetime.
    """
    if d is None:
        return None
    if isinstance(d, (date, datetime)):
        return d.isoformat()[:10] if isinstance(d, date) and not isinstance(d, datetime) else d.isoformat()
    return str(d)


def calc_volatility(prices: list) -> float:
    """Annualised volatility from a list of closing prices (oldest first)."""
    if len(prices) < 2:
        return 0.0
    returns = []
    for i in range(1, len(prices)):
        if prices[i - 1] and float(prices[i - 1]) > 0:
            returns.append(math.log(float(prices[i]) / float(prices[i - 1])))
    if not returns:
        return 0.0
    n = len(returns)
    mean = sum(returns) / n
    variance = sum((r - mean) ** 2 for r in returns) / (n - 1)
    return math.sqrt(variance) * math.sqrt(252)


def dates_ago(n: int) -> str:
    return (date.today() - timedelta(days=n)).strftime("%Y-%m-%d")


def f(x: Any) -> float | None:
    """Cast Postgres NUMERIC (Decimal) to float for JSON. Keeps None as None."""
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Endpoints — root / debug
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"status": "ok", "message": "Guia FII API is running"}


@app.get("/debug")
def debug_info():
    """Health diagnostic: verifies DB connection and fund_types population."""
    try:
        cotahist_count = query_one("SELECT COUNT(*) AS n FROM cotahist")
        fund_types_count = query_one("SELECT COUNT(*) AS n FROM fund_types")
        return {
            "status": "ok",
            "database": "postgres",
            "cotahist_rows": cotahist_count["n"] if cotahist_count else 0,
            "fund_types_rows": fund_types_count["n"] if fund_types_count else 0,
            "mxrf11_classification": get_fund_type("MXRF11"),
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# ---------------------------------------------------------------------------
# Endpoints — "legacy" (prices page)
# ---------------------------------------------------------------------------

@app.get("/prices")
@limiter.limit("30/minute")
def get_prices(
    request: Request,
    tickers: str = Query(...),
    date: str = Query(...),
):
    ticker_list = validate_tickers(tickers)
    validate_date(date)

    # Postgres uses %s placeholders; build a placeholder list for IN clause
    placeholders = ",".join(["%s"] * len(ticker_list))
    params = tuple(ticker_list) + (date,)
    query = f"""
        SELECT cod_neg         AS ticker,
               dt_pregao        AS date,
               preco_abertura   AS open,
               preco_maximo     AS high,
               preco_minimo     AS low,
               preco_ultimo     AS close,
               vol_negocios     AS volume,
               num_negocios     AS trades
        FROM cotahist
        WHERE cod_neg IN ({placeholders}) AND dt_pregao = %s
        ORDER BY cod_neg
    """
    rows = query_all(query, params)

    found = {}
    for row in rows:
        found[row["ticker"]] = {
            "ticker": row["ticker"],
            "date": iso(row["date"]),
            "open":   f(row["open"]),
            "high":   f(row["high"]),
            "low":    f(row["low"]),
            "close":  f(row["close"]),
            "volume": f(row["volume"]),
            "trades": row["trades"],
        }

    results = []
    for t in ticker_list:
        results.append(found.get(t, {
            "ticker": t, "date": date,
            "open": None, "high": None, "low": None,
            "close": None, "volume": None, "trades": None,
            "note": "No data for this ticker on this date",
        }))
    return {"date": date, "results": results}


@app.get("/tickers")
@limiter.limit("20/minute")
def get_tickers(request: Request, search: str = Query(None)):
    if search:
        if len(search) > 12 or not re.match(r"^[A-Z0-9]+$", search.upper()):
            raise HTTPException(status_code=400, detail="Filtro inválido.")
        rows = query_all(
            "SELECT DISTINCT cod_neg AS ticker, nom_resumido AS name "
            "FROM cotahist WHERE cod_neg LIKE %s ORDER BY cod_neg",
            (f"%{search.upper()}%",),
        )
    else:
        rows = query_all(
            "SELECT DISTINCT cod_neg AS ticker, nom_resumido AS name "
            "FROM cotahist ORDER BY cod_neg"
        )
    return {"count": len(rows), "tickers": [dict(r) for r in rows]}


@app.get("/dates/all")
def get_all_dates():
    rows = query_all(
        "SELECT DISTINCT dt_pregao AS date FROM cotahist ORDER BY dt_pregao DESC"
    )
    dates = [iso(r["date"]) for r in rows]
    return {
        "count": len(dates),
        "latest": dates[0] if dates else None,
        "earliest": dates[-1] if dates else None,
        "dates": dates,
    }


@app.get("/dates/available")
def get_available_dates(ticker: str = Query(...)):
    t = validate_ticker(ticker)
    rows = query_all(
        "SELECT DISTINCT dt_pregao AS date FROM cotahist "
        "WHERE cod_neg = %s ORDER BY dt_pregao DESC",
        (t,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Ticker {t} not found")
    dates = [iso(r["date"]) for r in rows]
    return {
        "ticker": t,
        "count": len(dates),
        "latest": dates[0],
        "earliest": dates[-1],
        "dates": dates,
    }


# ---------------------------------------------------------------------------
# Endpoints — health check page
# ---------------------------------------------------------------------------

@app.get("/fundo/preco")
@limiter.limit("30/minute")
def get_fundo_preco(request: Request, ticker: str = Query(...)):
    """
    Returns price history metrics for the health check page:
    - current price + date
    - % change vs D-1, D-7, D-30, D-90, D-180, 12M, 24M
    - volatility (annualised stddev) for 7d, 30d, 90d, 12M windows
    - liquidity (num trades/day avg) for 7d, 30d, 90d, 12M windows
    """
    t = validate_ticker(ticker)
    rows = query_all(
        """
        SELECT dt_pregao     AS date,
               preco_ultimo  AS close,
               num_negocios  AS trades
        FROM cotahist
        WHERE cod_neg = %s AND tp_merc = 10 AND preco_ultimo > 0
        ORDER BY dt_pregao DESC
        LIMIT 520
        """,
        (t,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Ticker {t} not found")

    prices = [float(r["close"]) for r in rows]  # newest first
    trades = [r["trades"] or 0 for r in rows]
    dates = [iso(r["date"]) for r in rows]

    def pct_change(n: int) -> float | None:
        if len(prices) <= n or prices[n] == 0:
            return None
        return (prices[0] - prices[n]) / prices[n]

    def avg_trades(n: int) -> float | None:
        if len(trades) < n:
            return None
        return sum(trades[:n]) / n

    def vol(n: int) -> float | None:
        if len(prices) < n + 1:
            return None
        return calc_volatility(list(reversed(prices[:n + 1])))

    return {
        "ticker": t,
        "preco": prices[0],
        "preco_data": dates[0],
        "variacao": {
            "d1":   pct_change(1),
            "d7":   pct_change(7),
            "d30":  pct_change(30),
            "d90":  pct_change(90),
            "d180": pct_change(180),
            "12m":  pct_change(252),
            "24m":  pct_change(504),
        },
        "volatilidade": {
            "7d":  vol(7),
            "30d": vol(30),
            "90d": vol(90),
            "12m": vol(252),
        },
        "liquidez": {
            "7d":  avg_trades(7),
            "30d": avg_trades(30),
            "90d": avg_trades(90),
            "12m": avg_trades(252),
        },
    }


@app.get("/fundo/dividendos")
@limiter.limit("30/minute")
def get_fundo_dividendos(request: Request, ticker: str = Query(...)):
    """
    Returns last 13 dividends + consistency stats from dividendos.
    Used for the waffle chart and DY section.
    """
    t = validate_ticker(ticker)

    rows = query_all(
        """
        SELECT data_base, data_pagamento, valor_provento, isento_ir
        FROM dividendos
        WHERE cod_negociacao = %s
          AND valor_provento IS NOT NULL
          AND valor_provento > 0
        ORDER BY data_base DESC
        LIMIT 13
        """,
        (t,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Nenhum dividendo encontrado para {t}")

    # Serialize dates and cast numerics up front
    dividendos = [{
        "data_base":      iso(r["data_base"]),
        "data_pagamento": iso(r["data_pagamento"]),
        "valor_provento": f(r["valor_provento"]),
        "isento_ir":      r["isento_ir"],
    } for r in rows]
    dividendos.reverse()  # oldest first for waffle chart

    valores = [d["valor_provento"] for d in dividendos if d["valor_provento"] is not None]
    if not valores:
        raise HTTPException(status_code=404, detail=f"Dividendos sem valor para {t}")
    ultimo = dividendos[-1]

    # DY pills — using last known price
    preco_row = query_one(
        """
        SELECT preco_ultimo AS preco
        FROM cotahist
        WHERE cod_neg = %s AND tp_merc = 10 AND preco_ultimo > 0
        ORDER BY dt_pregao DESC
        LIMIT 1
        """,
        (t,),
    )
    preco = f(preco_row["preco"]) if preco_row else None

    def dy(v):
        if preco and preco > 0:
            return round(v / preco, 6)
        return None

    n = len(valores)
    dy_m1   = dy(valores[-1])
    dy_m3   = dy(sum(valores[-3:]) / min(3, n)) if n >= 1 else None
    dy_m12  = dy(sum(valores) / n) if n >= 1 else None
    dy_yoy  = round((valores[-1] - valores[0]) / valores[0], 6) if valores[0] > 0 else None
    dy_anual = dy(sum(valores[-12:]) * (12 / min(12, n))) if n >= 1 else None

    return {
        "ticker": t,
        "ultimo": {
            "valor":     ultimo["valor_provento"],
            "data_base": ultimo["data_base"],
            "data_pago": ultimo["data_pagamento"],
            "isento_ir": ultimo["isento_ir"],
        },
        "historico": dividendos,
        "consistencia": {
            "pagos": n,
            "total": 13,
            "pct":   round(n / 13, 4),
        },
        "dy": {
            "anual": dy_anual,
            "m1":    dy_m1,
            "m3":    dy_m3,
            "m12":   dy_m12,
            "yoy":   dy_yoy,
        },
    }


@app.get("/fundo/informe")
@limiter.limit("30/minute")
def get_fundo_informe(request: Request, ticker: str = Query(...)):
    """
    Returns the latest informe mensal for a given ticker. Full 27-field dump plus
    a computed `health` object with alavancagem + cobertura de dividendos scoring
    (0-4 total) and composition percentages. `health` is None if the fund lacks
    enough data (e.g., new fund without dividend history).
    """
    t = validate_ticker(ticker)

    # informe_mensal uses cnpj_fundo; resolve ticker -> cnpj via dividendos
    cnpj_row = query_one(
        """
        SELECT cnpj_fundo
        FROM dividendos
        WHERE cod_negociacao = %s AND cnpj_fundo IS NOT NULL
        LIMIT 1
        """,
        (t,),
    )
    if not cnpj_row:
        raise HTTPException(status_code=404, detail=f"CNPJ não encontrado para {t}")
    cnpj = cnpj_row["cnpj_fundo"]

    row = query_one(
        """
        SELECT id_documento,
               nome_fundo,
               cnpj_fundo,
               data_funcionamento,
               publico_alvo,
               classificacao,
               subclassificacao,
               tipo_gestao,
               nome_administrador,
               cnpj_administrador,
               competencia,
               total_cotistas,
               pessoa_fisica,
               ativo_total,
               patrimonio_liquido,
               num_cotas_emitidas,
               valor_patr_cotas,
               despesas_tx_adm,
               rent_patr_mensal,
               dividend_yield_mes,
               total_investido,
               imoveis_renda,
               titulos_privados,
               fundos_renda_fixa,
               cri_cra,
               total_passivo,
               rendimentos_distribuir
        FROM informe_mensal
        WHERE cnpj_fundo = %s
        ORDER BY competencia DESC
        LIMIT 1
        """,
        (cnpj,),
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"Informe mensal não encontrado para {t}")

    # Pull computed health metrics from the view (cheap single-row lookup)
    health_row = query_one(
        """
        SELECT alavancagem_ratio, alav_pts, alav_tier, alav_label,
               cobertura_meses, cobert_pts, cobert_tier, cobert_label, cobertura_method,
               score, max_score, tier,
               narrative_full, narrative_partial,
               classificacao_declarada,
               cri_cra_pct, titulos_privados_pct,
               fundos_renda_fixa_pct, imoveis_renda_pct, outros_pct
        FROM fund_health_score
        WHERE cnpj_fundo = %s
        """,
        (cnpj,),
    )

    # Build the nested health object. None if we have no usable data at all.
    health = None
    if health_row and (health_row["alav_pts"] is not None or health_row["cobert_pts"] is not None):
        narrative = health_row["narrative_full"] or health_row["narrative_partial"]

        health = {
            "score": health_row["score"],
            "max_score": health_row["max_score"],
            "tier": health_row["tier"],
            "tier_label": {
                "saudavel": "Saudável",
                "atencao": "Atenção",
                "risco": "Risco",
            }.get(health_row["tier"]),
            "narrative": narrative,
            "components": {
                "alavancagem": None if health_row["alav_pts"] is None else {
                    "value": f(health_row["alavancagem_ratio"]),
                    "points": health_row["alav_pts"],
                    "tier": health_row["alav_tier"],
                    "label": health_row["alav_label"],
                },
                "cobertura_dividendos": None if health_row["cobert_pts"] is None else {
                    "value": f(health_row["cobertura_meses"]),
                    "points": health_row["cobert_pts"],
                    "tier": health_row["cobert_tier"],
                    "label": health_row["cobert_label"],
                    "method": health_row["cobertura_method"],
                },
            },
            "composicao": {
                "classificacao_declarada": health_row["classificacao_declarada"],
                "breakdown": {
                    "cri_cra_pct":           f(health_row["cri_cra_pct"]),
                    "titulos_privados_pct":  f(health_row["titulos_privados_pct"]),
                    "fundos_renda_fixa_pct": f(health_row["fundos_renda_fixa_pct"]),
                    "imoveis_renda_pct":     f(health_row["imoveis_renda_pct"]),
                    "outros_pct":            f(health_row["outros_pct"]),
                },
            },
        }

    return {
        # Identity
        "ticker": t,
        "cnpj": cnpj,
        "id_documento": row["id_documento"],
        "nome": row["nome_fundo"],

        # Classification
        "classificacao": row["classificacao"],
        "classificacao_market": get_fund_type(t),  # Papel/Tijolo/FOF/Híbrido from fund_types table
        "subclassificacao": row["subclassificacao"],
        "tipo_gestao": row["tipo_gestao"],
        "publico_alvo": row["publico_alvo"],

        # Administrator
        "administrador": row["nome_administrador"],
        "cnpj_administrador": row["cnpj_administrador"],

        # Dates
        "competencia": iso(row["competencia"]),
        "data_funcionamento": iso(row["data_funcionamento"]),

        # Investors
        "cotistas": row["total_cotistas"],
        "cotistas_pf": row["pessoa_fisica"],

        # Balance sheet
        "ativo_total": f(row["ativo_total"]),
        "pl": f(row["patrimonio_liquido"]),
        "total_passivo": f(row["total_passivo"]),

        # Quotas & admin
        "cotas_emitidas": f(row["num_cotas_emitidas"]),
        "nav_cota": f(row["valor_patr_cotas"]),
        "tx_adm": f(row["despesas_tx_adm"]),

        # Returns
        "dy_mes": f(row["dividend_yield_mes"]),
        "rent_mensal": f(row["rent_patr_mensal"]),

        # Portfolio composition (raw values in BRL)
        "total_investido": f(row["total_investido"]),
        "imoveis_renda": f(row["imoveis_renda"]),
        "titulos_privados": f(row["titulos_privados"]),
        "fundos_renda_fixa": f(row["fundos_renda_fixa"]),
        "cri_cra": f(row["cri_cra"]),

        # Distributable income
        "rendimentos_distribuir": f(row["rendimentos_distribuir"]),

        # Computed health score (nested object, None if insufficient data)
        "health": health,
    }


@app.get("/fundo/gestor")
@limiter.limit("30/minute")
def get_fundo_gestor(request: Request, ticker: str = Query(...)):
    """
    Returns the latest management report summary for a given ticker.
    JSONB columns (contexto_meses, cris_em_observacao, alocacao_fundos) are
    already deserialized by psycopg2, no json.loads needed.
    """
    t = validate_ticker(ticker)
    row = query_one(
        """
        SELECT ticker, competencia, classificacao, tom_gestor,
               pl_total_brl, cota_mercado, cota_patrimonial,
               spread_credito_bps, ltv_medio, resultado_por_cota,
               distribuicao_por_cota, reserva_monetaria_brl,
               vacancia_pct, contratos_vencer_12m_pct, cap_rate,
               contexto_meses, cris_em_observacao, alocacao_fundos,
               mudancas_portfolio, resumo, alertas_dados, processado_em
        FROM gestores
        WHERE ticker = %s
        ORDER BY competencia DESC
        LIMIT 1
        """,
        (t,),
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"Nenhum relatório gerencial encontrado para {t}")

    return {
        "ticker": row["ticker"],
        "competencia": iso(row["competencia"]),
        "classificacao": row["classificacao"],
        "tom_gestor": row["tom_gestor"],
        "pl_total_brl": f(row["pl_total_brl"]),
        "cota_mercado": f(row["cota_mercado"]),
        "cota_patrimonial": f(row["cota_patrimonial"]),
        "spread_credito_bps": f(row["spread_credito_bps"]),
        "ltv_medio": f(row["ltv_medio"]),
        "resultado_por_cota": f(row["resultado_por_cota"]),
        "distribuicao_por_cota": f(row["distribuicao_por_cota"]),
        "reserva_monetaria_brl": f(row["reserva_monetaria_brl"]),
        "vacancia_pct": f(row["vacancia_pct"]),
        "contratos_vencer_12m_pct": f(row["contratos_vencer_12m_pct"]),
        "cap_rate": f(row["cap_rate"]),
        # JSONB arrives as native dict/list from psycopg2; preserve shape exactly
        "contexto_meses":       row["contexto_meses"] if row["contexto_meses"] is not None else [],
        "cris_em_observacao":   row["cris_em_observacao"] if row["cris_em_observacao"] is not None else [],
        "alocacao_fundos":      row["alocacao_fundos"],
        "mudancas_portfolio":   row["mudancas_portfolio"],
        "resumo":               row["resumo"],
        "alertas_dados":        row["alertas_dados"],
        "processado_em":        iso(row["processado_em"]),
    }


# ---------------------------------------------------------------------------
# Benchmarks cache — recalculated at most once per hour
# ---------------------------------------------------------------------------

_benchmarks_cache: dict = {"data": None, "ts": 0}


def _calc_benchmarks() -> dict:
    """
    Median monthly DY per classificação (Papel/Tijolo/FOF/Híbrido).
    Heavy query: joins latest-price-per-ticker with sum of last 12 dividends,
    grouped by market classification from fund_types table.
    """
    from statistics import median as _median

    # 1. Latest price per ticker — DISTINCT ON is Postgres' idiomatic
    #    "most recent row per group" form (faster than MAX() subquery)
    price_rows = query_all(
        """
        SELECT DISTINCT ON (cod_neg)
               cod_neg      AS ticker,
               preco_ultimo AS preco
        FROM cotahist
        WHERE tp_merc = 10 AND preco_ultimo > 0
        ORDER BY cod_neg, dt_pregao DESC
        """
    )
    prices = {r["ticker"]: float(r["preco"]) for r in price_rows}

    # 2. Sum of last 12 dividends per ticker
    div_rows = query_all(
        """
        SELECT cod_negociacao AS ticker,
               cnpj_fundo,
               SUM(valor_provento) AS dy12_abs,
               COUNT(*)            AS n
        FROM (
            SELECT cod_negociacao,
                   cnpj_fundo,
                   valor_provento,
                   ROW_NUMBER() OVER (
                       PARTITION BY cod_negociacao
                       ORDER BY data_base DESC
                   ) AS rn
            FROM dividendos
            WHERE valor_provento > 0 AND cod_negociacao IS NOT NULL
        ) sub
        WHERE rn <= 12
        GROUP BY cod_negociacao, cnpj_fundo
        HAVING COUNT(*) >= 6
        """
    )
    div_map = {
        r["ticker"]: {"cnpj": r["cnpj_fundo"], "dy12": float(r["dy12_abs"])}
        for r in div_rows
    }

    # 3. Group DY by classification from fund_types table
    groups: dict = {}
    for ticker, d in div_map.items():
        preco = prices.get(ticker)
        if not preco or preco <= 0:
            continue
        cls = get_fund_type(ticker)
        if not cls or cls == "Outros":
            continue
        dy_mensal = (d["dy12"] / preco) / 12
        groups.setdefault(cls, []).append(dy_mensal)

    # 4. Median per group (min 5 funds)
    result = {}
    for cls, values in groups.items():
        if len(values) >= 5:
            result[cls] = {
                "mediana_dy_mensal": round(_median(values), 6),
                "mediana_dy_anual":  round(_median(values) * 12, 6),
                "n_fundos":          len(values),
            }
    return result


@app.get("/benchmarks")
@limiter.limit("30/minute")
def get_benchmarks(request: Request):
    """
    Returns median monthly DY per classification. Cached for 1 hour in memory.
    """
    import time as _time
    now = _time.time()
    if _benchmarks_cache["data"] is None or now - _benchmarks_cache["ts"] > 3600:
        _benchmarks_cache["data"] = _calc_benchmarks()
        _benchmarks_cache["ts"] = now
    return {
        "benchmarks": _benchmarks_cache["data"],
        "calculado_em": datetime.fromtimestamp(_benchmarks_cache["ts"]).strftime("%Y-%m-%d %H:%M"),
    }


# ---------------------------------------------------------------------------
# CDI cache — refreshed once per day from BCB API (external HTTP, not DB)
# ---------------------------------------------------------------------------

_cdi_cache: dict = {"data": None, "ts": 0}


@app.get("/cdi")
@limiter.limit("30/minute")
def get_cdi(request: Request):
    """
    Returns current CDI rate from the Brazilian Central Bank API.
    Series 4389 = CDI daily rate (% a.a.). Cached for 24 hours.
    """
    import time as _time
    import urllib.request as _req
    import json as _json

    now = _time.time()
    if _cdi_cache["data"] is not None and now - _cdi_cache["ts"] < 86400:
        return _cdi_cache["data"]

    try:
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.4389/dados/ultimos/1?formato=json"
        with _req.urlopen(url, timeout=10) as resp:
            rows = _json.loads(resp.read())

        cdi_anual_pct = float(rows[0]["valor"])
        data_ref = rows[0]["data"]  # DD/MM/YYYY from BCB

        cdi_anual = cdi_anual_pct / 100
        cdi_mensal = (1 + cdi_anual) ** (1 / 12) - 1

        result = {
            "cdi_anual":  round(cdi_anual, 6),
            "cdi_mensal": round(cdi_mensal, 6),
            "data_ref":   data_ref,
            "fonte":      "Banco Central do Brasil — série 4389",
        }
        _cdi_cache["data"] = result
        _cdi_cache["ts"] = now
        return result

    except Exception as e:
        if _cdi_cache["data"] is not None:
            return {**_cdi_cache["data"], "aviso": "usando cache — BCB indisponível"}
        raise HTTPException(status_code=503, detail=f"BCB API unavailable: {str(e)}")
