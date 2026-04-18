"""
Guia FII — API
Endpoints:
    GET /prices?tickers=MXRF11&date=2026-03-31
    GET /tickers?search=MXRF
    GET /dates/all
    GET /dates/available?ticker=MXRF11
    GET /fundo/preco?ticker=MXRF11        — price history for health check page
    GET /fundo/dividendos?ticker=MXRF11   — last 13 dividends + stats
"""

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import sqlite3
import os
import re
import math
from pathlib import Path
from datetime import datetime, date, timedelta

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
DB_PATH      = os.getenv("DB_PATH",      "data/b3.db")
DIV_PATH     = os.getenv("DIV_PATH",     "data/dividendos.db")
INFORME_PATH = os.getenv("INFORME_PATH", "data/informe_mensal.db")
FUND_TYPES_PATH = os.getenv("FUND_TYPES_PATH", "data/fund_types.json")

# ---------------------------------------------------------------------------
# Fund classification — loaded from fund_types.json
# Refreshed from disk at most once per hour (hot-reload without restart)
# ---------------------------------------------------------------------------
import json as _json_mod

_fund_types_cache: dict = {"data": {}, "ts": 0}

def get_fund_type(ticker: str) -> str | None:
    """Return market classification for a ticker from fund_types.json."""
    import time as _t
    now = _t.time()
    if now - _fund_types_cache["ts"] > 3600:
        path = Path(FUND_TYPES_PATH)
        if path.exists():
            try:
                raw = _json_mod.loads(path.read_text(encoding="utf-8"))
                _fund_types_cache["data"] = raw.get("fundos", {})
                _fund_types_cache["ts"] = now
            except Exception:
                pass
    return _fund_types_cache["data"].get(ticker.upper())

# ---------------------------------------------------------------------------
# RATE LIMITER
# ---------------------------------------------------------------------------
limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])

app = FastAPI(
    title="Guia FII API",
    description="B3 price and dividend data for FIIs",
    version="2.0.0",
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
# DB helpers
# ---------------------------------------------------------------------------
def get_b3():
    db = Path(DB_PATH)
    if not db.exists():
        raise HTTPException(status_code=503, detail=f"b3.db not found at {DB_PATH}")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    return conn

def get_div():
    db = Path(DIV_PATH)
    if not db.exists():
        raise HTTPException(status_code=503, detail=f"dividendos.db not found at {DIV_PATH}")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    return conn

def get_informe():
    db = Path(INFORME_PATH)
    if not db.exists():
        raise HTTPException(status_code=503, detail=f"informe_mensal.db not found at {INFORME_PATH}")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    return conn

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
TICKER_RE = re.compile(r'^[A-Z0-9]{1,12}$')
DATE_RE   = re.compile(r'^\d{4}-\d{2}-\d{2}$')

def validate_ticker(t: str) -> str:
    t = t.strip().upper()
    if not TICKER_RE.match(t):
        raise HTTPException(status_code=400, detail=f"Ticker inválido: '{t}'")
    return t

def validate_tickers(raw: str) -> list:
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
def calc_volatility(prices: list) -> float:
    """Annualised volatility from a list of closing prices (oldest first)."""
    if len(prices) < 2:
        return 0.0
    returns = []
    for i in range(1, len(prices)):
        if prices[i-1] > 0:
            returns.append(math.log(prices[i] / prices[i-1]))
    if not returns:
        return 0.0
    n = len(returns)
    mean = sum(returns) / n
    variance = sum((r - mean) ** 2 for r in returns) / (n - 1)
    return math.sqrt(variance) * math.sqrt(252)

def dates_ago(n: int) -> str:
    return (date.today() - timedelta(days=n)).strftime("%Y-%m-%d")

# ---------------------------------------------------------------------------
# Endpoints — legacy (prices page)
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"status": "ok", "message": "Guia FII API is running"}


@app.get("/debug")
def debug_info():
    import os, glob
    path = Path(FUND_TYPES_PATH)
    files_in_root = glob.glob("*.json")
    files_in_data = glob.glob("data/*.json") + glob.glob("backend/data/*.json")
    return {
        "fund_types_path_setting": FUND_TYPES_PATH,
        "fund_types_path_absolute": str(path.absolute()),
        "fund_types_exists": path.exists(),
        "cwd": os.getcwd(),
        "json_files_found": files_in_root + files_in_data,
        "fund_types_loaded": len(_fund_types_cache["data"]),
        "mxrf11": get_fund_type("MXRF11"),
    }


@app.get("/prices")
@limiter.limit("30/minute")
def get_prices(
    request: Request,
    tickers: str = Query(...),
    date: str = Query(...),
):
    ticker_list = validate_tickers(tickers)
    validate_date(date)
    placeholders = ",".join("?" * len(ticker_list))
    query = f"""
        SELECT CodNeg AS ticker, DtPregao AS date,
               PrecoAbertura AS open, PrecoMaximo AS high,
               PrecoMinimo AS low, PrecoUltimo AS close,
               VolNegocios AS volume, NumNegocios AS trades
        FROM cotahist
        WHERE CodNeg IN ({placeholders}) AND DtPregao = ?
        ORDER BY CodNeg
    """
    conn = get_b3()
    rows = conn.execute(query, ticker_list + [date]).fetchall()
    conn.close()
    found = {row["ticker"]: dict(row) for row in rows}
    results = []
    for t in ticker_list:
        results.append(found.get(t, {
            "ticker": t, "date": date,
            "open": None, "high": None, "low": None,
            "close": None, "volume": None, "trades": None,
            "note": "No data for this ticker on this date"
        }))
    return {"date": date, "results": results}


@app.get("/tickers")
@limiter.limit("20/minute")
def get_tickers(request: Request, search: str = Query(None)):
    conn = get_b3()
    if search:
        if len(search) > 12 or not re.match(r'^[A-Z0-9]+$', search.upper()):
            raise HTTPException(status_code=400, detail="Filtro inválido.")
        rows = conn.execute(
            "SELECT DISTINCT CodNeg AS ticker, NomResumido AS name FROM cotahist WHERE CodNeg LIKE ? ORDER BY CodNeg",
            (f"%{search.upper()}%",)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT DISTINCT CodNeg AS ticker, NomResumido AS name FROM cotahist ORDER BY CodNeg"
        ).fetchall()
    conn.close()
    return {"count": len(rows), "tickers": [dict(r) for r in rows]}


@app.get("/dates/all")
def get_all_dates():
    conn = get_b3()
    rows = conn.execute(
        "SELECT DISTINCT DtPregao AS date FROM cotahist ORDER BY DtPregao DESC"
    ).fetchall()
    conn.close()
    dates = [r["date"] for r in rows]
    return {"count": len(dates), "latest": dates[0] if dates else None,
            "earliest": dates[-1] if dates else None, "dates": dates}


@app.get("/dates/available")
def get_available_dates(ticker: str = Query(...)):
    t = validate_ticker(ticker)
    conn = get_b3()
    rows = conn.execute(
        "SELECT DISTINCT DtPregao AS date FROM cotahist WHERE CodNeg = ? ORDER BY DtPregao DESC",
        (t,)
    ).fetchall()
    conn.close()
    if not rows:
        raise HTTPException(status_code=404, detail=f"Ticker {t} not found")
    dates = [r["date"] for r in rows]
    return {"ticker": t, "count": len(dates),
            "latest": dates[0], "earliest": dates[-1], "dates": dates}


# ---------------------------------------------------------------------------
# Endpoints — health check page
# ---------------------------------------------------------------------------

@app.get("/fundo/preco")
@limiter.limit("30/minute")
def get_fundo_preco(request: Request, ticker: str = Query(...)):
    """
    Returns price history metrics for the health check page:
    - current price + date
    - % change vs D-1, D-7, D-30, D-90, D-180, 12M
    - volatility (annualised stddev) for 7d, 30d, 90d, 12M windows
    - liquidity (num trades/day avg) for 7d, 30d, 90d, 12M windows
    """
    t = validate_ticker(ticker)
    conn = get_b3()

    # Get last 365 trading days for this ticker
    rows = conn.execute("""
        SELECT DtPregao AS date, PrecoUltimo AS close, NumNegocios AS trades
        FROM cotahist
        WHERE CodNeg = ? AND TpMerc = 10 AND PrecoUltimo > 0
        ORDER BY DtPregao DESC
        LIMIT 520
    """, (t,)).fetchall()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail=f"Ticker {t} not found")

    prices  = [r["close"]  for r in rows]  # newest first
    trades  = [r["trades"] for r in rows]
    dates   = [r["date"]   for r in rows]

    def pct_change(n: int) -> float | None:
        if len(prices) <= n:
            return None
        if prices[n] == 0:
            return None
        return (prices[0] - prices[n]) / prices[n]

    def avg_trades(n: int) -> float | None:
        if len(trades) < n:
            return None
        return sum(trades[:n]) / n

    def vol(n: int) -> float | None:
        if len(prices) < n + 1:
            return None
        return calc_volatility(list(reversed(prices[:n+1])))

    return {
        "ticker":     t,
        "preco":      prices[0],
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
    Returns last 13 dividends + consistency stats from dividendos.db.
    Used for the waffle chart and DY section.
    """
    t = validate_ticker(ticker)
    conn = get_div()

    rows = conn.execute("""
        SELECT data_base, data_pagamento, valor_provento, isento_ir
        FROM dividendos
        WHERE cod_negociacao = ?
          AND valor_provento IS NOT NULL
          AND valor_provento > 0
        ORDER BY data_base DESC
        LIMIT 13
    """, (t,)).fetchall()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail=f"Nenhum dividendo encontrado para {t}")

    dividendos = [dict(r) for r in rows]
    dividendos.reverse()  # oldest first for waffle chart

    valores = [d["valor_provento"] for d in dividendos]
    ultimo  = dividendos[-1]

    # DY pills — using last known price from b3.db
    conn_b3 = get_b3()
    preco_row = conn_b3.execute("""
        SELECT PrecoUltimo FROM cotahist
        WHERE CodNeg = ? AND TpMerc = 10 AND PrecoUltimo > 0
        ORDER BY DtPregao DESC LIMIT 1
    """, (t,)).fetchone()
    conn_b3.close()

    preco = preco_row["PrecoUltimo"] if preco_row else None

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
        "ticker":       t,
        "ultimo": {
            "valor":        ultimo["valor_provento"],
            "data_base":    ultimo["data_base"],
            "data_pago":    ultimo["data_pagamento"],
            "isento_ir":    ultimo["isento_ir"],
        },
        "historico":    dividendos,   # list of 13 months, oldest first
        "consistencia": {
            "pagos":  n,
            "total":  13,
            "pct":    round(n / 13, 4),
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
    Returns the latest informe mensal for a given ticker:
    nome, segmento, PL, NAV/cota, cotistas, tx_adm, DY mensal
    """
    t = validate_ticker(ticker)

    # informe_mensal uses cnpj_fundo, not cod_negociacao directly
    # join via dividendos.db to resolve ticker → cnpj
    conn_div = get_div()
    cnpj_row = conn_div.execute("""
        SELECT cnpj_fundo FROM dividendos
        WHERE cod_negociacao = ? AND cnpj_fundo IS NOT NULL
        LIMIT 1
    """, (t,)).fetchone()
    conn_div.close()

    if not cnpj_row:
        raise HTTPException(status_code=404, detail=f"CNPJ não encontrado para {t}")

    cnpj = cnpj_row["cnpj_fundo"]

    conn = get_informe()
    row = conn.execute("""
        SELECT
            nome_fundo,
            cnpj_fundo,
            classificacao,
            subclassificacao,
            tipo_gestao,
            nome_administrador,
            competencia,
            total_cotistas,
            patrimonio_liquido,
            num_cotas_emitidas,
            valor_patr_cotas,
            despesas_tx_adm,
            dividend_yield_mes,
            rent_patr_mensal,
            rendimentos_distribuir
        FROM informe_mensal
        WHERE cnpj_fundo = ?
        ORDER BY competencia DESC
        LIMIT 1
    """, (cnpj,)).fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail=f"Informe mensal não encontrado para {t}")

    r = dict(row)
    return {
        "ticker":                 t,
        "cnpj":                   cnpj,
        "nome":                   r["nome_fundo"],
        "classificacao":          r["classificacao"],
        "classificacao_market":   get_fund_type(t),   # Papel/Tijolo/FOF/Híbrido from fund_types.json
        "subclassificacao":       r["subclassificacao"],
        "tipo_gestao":            r["tipo_gestao"],
        "administrador":          r["nome_administrador"],
        "competencia":            r["competencia"],
        "cotistas":               r["total_cotistas"],
        "pl":                     r["patrimonio_liquido"],
        "cotas_emitidas":         r["num_cotas_emitidas"],
        "nav_cota":               r["valor_patr_cotas"],
        "tx_adm":                 r["despesas_tx_adm"],
        "dy_mes":                 r["dividend_yield_mes"],
        "rent_mensal":            r["rent_patr_mensal"],
        "rendimentos_distribuir": r["rendimentos_distribuir"],
    }


# ---------------------------------------------------------------------------
# Benchmarks cache — recalculated at most once per hour
# ---------------------------------------------------------------------------
_benchmarks_cache: dict = {"data": None, "ts": 0}

def _calc_benchmarks() -> dict:
    from statistics import median as _median

    # 1. Latest price per ticker — use MAX(DtPregao) in a subquery join
    #    instead of a correlated subquery (much faster on large tables)
    conn_b3 = get_b3()
    price_rows = conn_b3.execute("""
        SELECT c.CodNeg AS ticker, c.PrecoUltimo AS preco
        FROM cotahist c
        INNER JOIN (
            SELECT CodNeg, MAX(DtPregao) AS max_dt
            FROM cotahist
            WHERE TpMerc = 10 AND PrecoUltimo > 0
            GROUP BY CodNeg
        ) latest ON c.CodNeg = latest.CodNeg AND c.DtPregao = latest.max_dt
        WHERE c.TpMerc = 10 AND c.PrecoUltimo > 0
    """).fetchall()
    conn_b3.close()
    prices = {r["ticker"]: r["preco"] for r in price_rows}

    # 2. Sum of last 12 dividends per ticker
    conn_div = get_div()
    div_rows = conn_div.execute("""
        SELECT cod_negociacao AS ticker,
               cnpj_fundo,
               SUM(valor_provento) AS dy12_abs,
               COUNT(*)           AS n
        FROM (
            SELECT cod_negociacao, cnpj_fundo, valor_provento,
                   ROW_NUMBER() OVER (PARTITION BY cod_negociacao ORDER BY data_base DESC) AS rn
            FROM dividendos
            WHERE valor_provento > 0 AND cod_negociacao IS NOT NULL
        )
        WHERE rn <= 12
        GROUP BY cod_negociacao
        HAVING n >= 6
    """).fetchall()
    conn_div.close()
    div_map = {r["ticker"]: {"cnpj": r["cnpj_fundo"], "dy12": r["dy12_abs"]} for r in div_rows}

    # 3. Classification from fund_types.json (market standard: Papel/Tijolo/FOF/Híbrido)
    #    get_fund_type() handles caching and hot-reload automatically

    # 4. Group DY by classificacao
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

    # 5. Median per group (min 5 funds)
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
    Returns median monthly DY per classificacao.
    Result is cached in memory for 1 hour.
    """
    import time as _time
    now = _time.time()
    if _benchmarks_cache["data"] is None or now - _benchmarks_cache["ts"] > 3600:
        _benchmarks_cache["data"] = _calc_benchmarks()
        _benchmarks_cache["ts"]   = now
    return {
        "benchmarks":   _benchmarks_cache["data"],
        "calculado_em": datetime.fromtimestamp(_benchmarks_cache["ts"]).strftime("%Y-%m-%d %H:%M"),
    }


# ---------------------------------------------------------------------------
# CDI cache — refreshed once per day
# ---------------------------------------------------------------------------
_cdi_cache: dict = {"data": None, "ts": 0}

@app.get("/cdi")
@limiter.limit("30/minute")
def get_cdi(request: Request):
    """
    Returns the current CDI rate fetched from the Brazilian Central Bank API.
    Cached for 24 hours.
    Series 4391 = CDI daily rate (% a.d.)
    """
    import time as _time
    import urllib.request as _req
    import json as _json

    now = _time.time()
    if _cdi_cache["data"] is not None and now - _cdi_cache["ts"] < 86400:
        return _cdi_cache["data"]

    try:
        # Series 4389 = CDI diário em % a.a. (annualised daily rate)
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.4389/dados/ultimos/1?formato=json"
        with _req.urlopen(url, timeout=10) as resp:
            rows = _json.loads(resp.read())

        # BCB returns CDI as % a.a. e.g. "14.65"
        cdi_anual_pct = float(rows[0]["valor"])
        data_ref      = rows[0]["data"]  # DD/MM/YYYY

        cdi_anual  = cdi_anual_pct / 100
        # Convert annual to monthly: (1 + anual)^(1/12) - 1
        cdi_mensal = (1 + cdi_anual) ** (1/12) - 1

        result = {
            "cdi_anual":   round(cdi_anual, 6),
            "cdi_mensal":  round(cdi_mensal, 6),
            "data_ref":    data_ref,
            "fonte":       "Banco Central do Brasil — série 4389",
        }
        _cdi_cache["data"] = result
        _cdi_cache["ts"]   = now
        return result

    except Exception as e:
        # Return cache if available, even if stale
        if _cdi_cache["data"] is not None:
            return {**_cdi_cache["data"], "aviso": "usando cache — BCB indisponível"}
        raise HTTPException(status_code=503, detail=f"BCB API unavailable: {str(e)}")

