"""
agent.py — Run the LLM extractor over relatórios that are pending processing.

Pipeline per fund:
  1. Query relatorios_gerenciais for unprocessed rows (processed_at IS NULL),
     filtered to a single subtype's classificacao (e.g., 'Papel').
  2. Group by ticker. Each ticker should have 3 rows: M (latest), M-1, M-12.
     Funds with fewer than 3 are processed with what's available.
  3. For each ticker:
     a. Load all 3 PDFs from disk via pdf_path.
     b. Preprocess each via pdf_preprocessor.preprocess_pdf().
     c. Call Claude API with the subtype's system prompt + concatenated PDFs.
     d. Validate response is parseable JSON, contains required keys.
     e. INSERT into gestores (ON CONFLICT DO NOTHING by default;
        --overwrite-manual flips to DO UPDATE).
     f. Mark all 3 relatorios_gerenciais rows with processed_at=NOW().
     g. Delete the 3 PDFs from disk, mark pdf_deleted_at.
  4. On any failure: log to erros, leave processed_at=NULL, leave PDFs in place.

Budget enforcement:
  - Per-fund cap is implicit: max_tokens=4000 caps output, ~50k input cap caps
    input. Worst case ~$0.21/fund, well under $0.50.
  - Monthly cap: queries SUM(cost_usd) from gestores in current calendar month
    before each fund. Warns at $20, hard-stops at $25.

Usage:
    python agent.py                              # process all FII Papel
    python agent.py --ticker MXRF11              # process one ticker
    python agent.py --limit 3                    # process at most N funds
    python agent.py --dry-run                    # parse but don't call API
    python agent.py --overwrite-manual           # allow overwriting existing rows
    python agent.py --subtype fii_papel          # explicit subtype (default fii_papel for v1)

Env:
    DATABASE_URL          required — Postgres connection string
    ANTHROPIC_API_KEY     required — Anthropic API key
    PROMPTS_DIR           optional — path to prompts/ folder (default: ./prompts)
    MONTHLY_BUDGET_USD    optional — default 30
    MONTHLY_BUDGET_WARN_USD optional — default 20
    MONTHLY_BUDGET_STOP_USD optional — default 25
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import anthropic

from db import close_pool, connection, execute, init_pool, query_all, query_one
from pdf_preprocessor import preprocess_pdf, PDFHasNoTextError

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
EXTRACTOR_MODEL = "claude-sonnet-4-5-20250929"   # Sonnet 4.6 alias
MAX_TOKENS_OUTPUT = 4000
PROMPT_VERSION = "fii_papel_v2"

# Anthropic Sonnet pricing as of late 2025 / early 2026:
#   input:        $3.00 / 1M tokens
#   output:      $15.00 / 1M tokens
#   cache write: $3.75 / 1M tokens
#   cache read:  $0.30 / 1M tokens
PRICE_INPUT_PER_TOKEN       = 3.00 / 1_000_000
PRICE_OUTPUT_PER_TOKEN      = 15.00 / 1_000_000
PRICE_CACHE_WRITE_PER_TOKEN = 3.75 / 1_000_000
PRICE_CACHE_READ_PER_TOKEN  = 0.30 / 1_000_000

# Monthly budget controls (env-overridable for ops flexibility)
BUDGET_TOTAL = float(os.environ.get("MONTHLY_BUDGET_USD", "30"))
BUDGET_WARN  = float(os.environ.get("MONTHLY_BUDGET_WARN_USD", "20"))
BUDGET_STOP  = float(os.environ.get("MONTHLY_BUDGET_STOP_USD", "25"))

# Pacing between funds to stay under Anthropic's per-minute rate limit. On
# entry tier (30k input TPM on Sonnet), a single fund's extraction uses
# 15-21k tokens. Even 70s wasn't enough: empirically, the rolling-window
# counter seems to use request-start (not response-end) timestamps, so a
# fund finishing at T can still have its tokens counted toward the window
# at T+70. 120s gives comfortable headroom. When the account auto-tiers
# up (Tier 2 50k+ TPM, Tier 3 100k+), this delay can be reduced via env
# var or removed entirely.
INTER_FUND_DELAY_S = int(os.environ.get("INTER_FUND_DELAY_S", "120"))

PROMPTS_DIR = Path(os.environ.get("PROMPTS_DIR", "prompts"))

# Required JSON keys per subtype (validates extractor output before DB write).
# A missing key means the extractor's response is malformed; we log to erros
# rather than guessing what the value should have been.
REQUIRED_KEYS_FII_PAPEL = {
    "ticker", "competencia", "tom_gestor",
    "pl_total_brl", "cota_mercado", "cota_patrimonial",
    "spread_credito_cdi_bps", "spread_credito_ipca_bps", "ltv_medio",
    "resultado_por_cota", "distribuicao_por_cota", "reserva_monetaria_brl",
    "contexto_meses", "cris_em_observacao",
    "vacancia_pct", "contratos_vencer_12m_pct", "cap_rate", "alocacao_fundos",
    "mudancas_portfolio", "resumo", "alertas_dados",
}

VALID_TOM_GESTOR = {"conservador", "neutro", "otimista"}

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s -- %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("agent")


# ---------------------------------------------------------------------------
# Prompt loading
# ---------------------------------------------------------------------------
def load_prompt(subtype: str) -> str:
    """Load the system prompt for a subtype from prompts/{subtype}.txt"""
    path = PROMPTS_DIR / f"{subtype}.txt"
    if not path.exists():
        raise FileNotFoundError(
            f"Prompt file not found: {path}. "
            f"Expected layout: {PROMPTS_DIR}/<subtype>.txt"
        )
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Worklist construction
# ---------------------------------------------------------------------------
def get_worklist(classificacao: str, ticker_filter: str | None = None) -> dict[str, list[dict]]:
    """
    Returns {ticker: [list of doc rows ordered by data_referencia DESC]}.
    Each fund's list contains 1-3 rows: M (newest), M-1, M-12.

    Filters:
      - classificacao on fund_types (e.g., 'Papel')
      - ticker_filter (optional, for processing one fund at a time)
      - processed_at IS NULL (only pending PDFs)
    """
    sql = """
        SELECT r.doc_id, r.ticker, r.data_referencia, r.pdf_path,
               r.tipo_fundo, r.cnpj_fundo
        FROM relatorios_gerenciais r
        JOIN fund_types ft ON ft.ticker = r.ticker
        WHERE r.processed_at IS NULL
          AND r.pdf_deleted_at IS NULL
          AND ft.classificacao = %s
    """
    params: list = [classificacao]
    if ticker_filter:
        sql += " AND r.ticker = %s"
        params.append(ticker_filter)
    sql += " ORDER BY r.ticker, r.data_referencia DESC"

    rows = query_all(sql, tuple(params))
    grouped: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        grouped[r["ticker"]].append(r)
    return dict(grouped)


def select_target_docs(docs: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    From a fund's unprocessed relatórios, pick at most 3 (M, M-1, M-12) by
    data_referencia. Returns (selected, skipped). Skipped rows should be
    marked processed_at=NOW() so they don't keep appearing in the worklist.

    Selection rules (same as the scraper's select_target_reports):
      - M:    most recent
      - M-1:  immediately previous (by data_referencia)
      - M-12: closest by day count to (M's data_referencia - 365 days)

    docs comes in already sorted by data_referencia DESC from get_worklist().
    Only 1 or 2 docs available → return what's there as M and M-1, no M-12.
    """
    if len(docs) <= 1:
        return docs, []

    parsed: list[tuple] = []  # (data_ref_as_date, row)
    for d in docs:
        ref = d["data_referencia"]
        if not isinstance(ref, date):
            continue
        parsed.append((ref, d))

    if not parsed:
        return [], list(docs)

    parsed.sort(key=lambda t: t[0], reverse=True)

    selected: list[dict] = []
    selected.append(parsed[0][1])  # M (latest)

    if len(parsed) >= 2:
        selected.append(parsed[1][1])  # M-1 (immediately previous)

    if len(parsed) >= 3:
        from datetime import timedelta as _td
        latest_date = parsed[0][0]
        target = latest_date - _td(days=365)
        candidates = parsed[2:]  # already-picked ones excluded
        best = min(candidates, key=lambda t: abs((t[0] - target).days))
        selected.append(best[1])

    selected_ids = {d["doc_id"] for d in selected}
    skipped = [d for d in docs if d["doc_id"] not in selected_ids]
    return selected, skipped


# ---------------------------------------------------------------------------
# Budget tracking
# ---------------------------------------------------------------------------
def month_to_date_spend() -> float:
    """Sum cost_usd for gestores rows created in the current calendar month."""
    row = query_one(
        """
        SELECT COALESCE(SUM(cost_usd), 0) AS spent
        FROM gestores
        WHERE processado_em >= date_trunc('month', NOW())
        """
    )
    return float(row["spent"]) if row else 0.0


def check_budget_or_abort() -> float:
    """
    Returns current month-to-date spend. Logs a warning at >= BUDGET_WARN.
    Raises BudgetExceeded if >= BUDGET_STOP.
    """
    spent = month_to_date_spend()
    if spent >= BUDGET_STOP:
        msg = (
            f"BUDGET STOP: month-to-date spend ${spent:.2f} >= ${BUDGET_STOP:.2f}. "
            f"Halting all extractions until next month or budget reset."
        )
        log.error(msg)
        record_error(None, None, "budget_stop", msg)
        raise BudgetExceeded(msg)
    if spent >= BUDGET_WARN:
        log.warning(
            f"BUDGET WARN: month-to-date spend ${spent:.2f} >= ${BUDGET_WARN:.2f} "
            f"(stop threshold ${BUDGET_STOP:.2f})"
        )
    return spent


class BudgetExceeded(Exception):
    """Raised when monthly budget cap is hit. Halts further processing."""


# ---------------------------------------------------------------------------
# Cost calculation from API response
# ---------------------------------------------------------------------------
def calculate_cost_usd(usage: Any) -> float:
    """
    Compute cost from the API response's usage block. Anthropic returns
    separate counters for cache reads/writes when prompt caching is in play.
    """
    input_tokens = getattr(usage, "input_tokens", 0) or 0
    output_tokens = getattr(usage, "output_tokens", 0) or 0
    cache_creation = getattr(usage, "cache_creation_input_tokens", 0) or 0
    cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0

    return (
        input_tokens   * PRICE_INPUT_PER_TOKEN +
        output_tokens  * PRICE_OUTPUT_PER_TOKEN +
        cache_creation * PRICE_CACHE_WRITE_PER_TOKEN +
        cache_read     * PRICE_CACHE_READ_PER_TOKEN
    )


# ---------------------------------------------------------------------------
# JSON validation
# ---------------------------------------------------------------------------
def parse_and_validate_json(raw: str, expected_keys: set[str]) -> dict:
    """
    Strip incidental wrappers (markdown fences) and parse JSON. Validates
    that all required keys are present. Raises ExtractionError on any issue
    so caller logs to erros and skips the fund.
    """
    s = raw.strip()
    # Strip ```json ... ``` if model added them despite being told not to
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```\s*$", "", s)
    if not (s.startswith("{") and s.endswith("}")):
        raise ExtractionError(
            f"Response does not start with '{{' and end with '}}'. "
            f"First 100 chars: {s[:100]!r}"
        )
    try:
        data = json.loads(s)
    except json.JSONDecodeError as e:
        raise ExtractionError(f"JSON parse failed at line {e.lineno} col {e.colno}: {e.msg}") from e

    missing = expected_keys - set(data.keys())
    if missing:
        raise ExtractionError(f"Response missing required keys: {sorted(missing)}")

    if data.get("tom_gestor") not in VALID_TOM_GESTOR:
        raise ExtractionError(
            f"tom_gestor '{data.get('tom_gestor')}' not in {VALID_TOM_GESTOR}"
        )

    return data


class ExtractionError(Exception):
    """Raised when the LLM response is malformed or fails schema validation."""


# ---------------------------------------------------------------------------
# DB writes
# ---------------------------------------------------------------------------
def insert_gestor_row(payload: dict, classificacao: str, doc_ids: dict,
                      cost_usd: float, overwrite: bool) -> bool:
    """
    Insert (or update if --overwrite-manual) a row into gestores.
    Returns True if a row was written, False if skipped due to ON CONFLICT.

    competencia comes in as 'YYYY-MM' from the LLM; we convert to a DATE
    (first of month) for storage, since the column is DATE.
    """
    yyyy_mm = payload["competencia"]
    if not re.match(r"^\d{4}-\d{2}$", yyyy_mm):
        raise ExtractionError(f"competencia must be 'YYYY-MM', got {yyyy_mm!r}")
    competencia_date = datetime.strptime(f"{yyyy_mm}-01", "%Y-%m-%d").date()

    cols = [
        "ticker", "competencia", "classificacao", "tom_gestor",
        "pl_total_brl", "cota_mercado", "cota_patrimonial",
        "spread_credito_cdi_bps", "spread_credito_ipca_bps", "ltv_medio",
        "resultado_por_cota", "distribuicao_por_cota", "reserva_monetaria_brl",
        "vacancia_pct", "contratos_vencer_12m_pct", "cap_rate",
        "contexto_meses", "cris_em_observacao", "alocacao_fundos",
        "mudancas_portfolio", "resumo", "alertas_dados",
        "doc_id_m", "doc_id_m1", "doc_id_m12",
        "cost_usd", "extractor_model", "prompt_version",
        "processado_em",
    ]
    values = [
        payload["ticker"], competencia_date, classificacao, payload["tom_gestor"],
        payload.get("pl_total_brl"), payload.get("cota_mercado"), payload.get("cota_patrimonial"),
        payload.get("spread_credito_cdi_bps"), payload.get("spread_credito_ipca_bps"),
        payload.get("ltv_medio"),
        payload.get("resultado_por_cota"), payload.get("distribuicao_por_cota"),
        payload.get("reserva_monetaria_brl"),
        payload.get("vacancia_pct"), payload.get("contratos_vencer_12m_pct"),
        payload.get("cap_rate"),
        json.dumps(payload.get("contexto_meses") or []),
        json.dumps(payload.get("cris_em_observacao") or []),
        json.dumps(payload.get("alocacao_fundos")) if payload.get("alocacao_fundos") is not None else None,
        payload.get("mudancas_portfolio"), payload.get("resumo"), payload.get("alertas_dados"),
        doc_ids.get("m"), doc_ids.get("m_1"), doc_ids.get("m_12"),
        cost_usd, EXTRACTOR_MODEL, PROMPT_VERSION,
        datetime.now(),
    ]

    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)

    if overwrite:
        # Build UPDATE SET clause for all columns except the unique key
        update_cols = [c for c in cols if c not in ("ticker", "competencia")]
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        sql = f"""
            INSERT INTO gestores ({col_list})
            VALUES ({placeholders})
            ON CONFLICT (ticker, competencia) DO UPDATE SET {set_clause}
        """
    else:
        sql = f"""
            INSERT INTO gestores ({col_list})
            VALUES ({placeholders})
            ON CONFLICT (ticker, competencia) DO NOTHING
        """

    rows_affected = execute(sql, tuple(values))
    return rows_affected > 0


def mark_processed(doc_ids: list[int]) -> None:
    """Mark relatórios as processed AFTER successful gestores INSERT."""
    if not doc_ids:
        return
    placeholders = ", ".join(["%s"] * len(doc_ids))
    execute(
        f"UPDATE relatorios_gerenciais SET processed_at = NOW() "
        f"WHERE doc_id IN ({placeholders})",
        tuple(doc_ids),
    )


def delete_pdfs_and_mark(doc_rows: list[dict]) -> None:
    """Best-effort PDF deletion. Marks pdf_deleted_at even if file missing."""
    for row in doc_rows:
        path = Path(row["pdf_path"])
        try:
            if path.exists():
                path.unlink()
        except OSError as e:
            log.warning(f"Could not delete {path}: {e}. Cleanup pass will retry.")
        execute(
            "UPDATE relatorios_gerenciais SET pdf_deleted_at = NOW() WHERE doc_id = %s",
            (row["doc_id"],),
        )


def record_error(doc_id: int | None, ticker: str | None, code: str, message: str) -> None:
    """
    Insert into the erros table. Real schema is:
        (id, id_documento, cod_negociacao, motivo, registrado_em)
    No `origem` column, so we prefix motivo with [agent] for filterability.
    """
    full_msg = f"[agent:{code}] {message[:900]}"
    try:
        execute(
            """
            INSERT INTO erros (id_documento, cod_negociacao, motivo, registrado_em)
            VALUES (%s, %s, %s, NOW())
            """,
            (doc_id, ticker, full_msg),
        )
    except Exception as e:
        log.warning(f"Could not record error: {e}")


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------
def build_user_message(ticker: str, docs: list[dict], markdowns: list[str]) -> str:
    """
    Compose the user turn: ticker context + the three preprocessed PDFs labeled
    by their data_referencia. This is what gets sent (the system prompt is
    cached separately).
    """
    parts = [f"Ticker: {ticker}", ""]
    for doc, md in zip(docs, markdowns):
        ref = doc["data_referencia"]
        # data_referencia is a date object from postgres
        ref_str = ref.strftime("%Y-%m") if isinstance(ref, date) else str(ref)
        parts.append(f"=== Relatório {ref_str} (doc_id={doc['doc_id']}) ===")
        parts.append(md)
        parts.append("")
    return "\n".join(parts)


def call_extractor(client: anthropic.Anthropic, system_prompt: str,
                   user_message: str) -> tuple[dict, float]:
    """
    Call Claude with the system prompt cached. Returns (payload, cost_usd).
    Raises ExtractionError if the response is unparseable.
    """
    response = client.messages.create(
        model=EXTRACTOR_MODEL,
        max_tokens=MAX_TOKENS_OUTPUT,
        system=[
            {
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[
            {"role": "user", "content": user_message},
        ],
    )

    cost = calculate_cost_usd(response.usage)

    # Concatenate all text blocks (Sonnet typically returns one block but
    # tool use or extended thinking could add more)
    text_parts = [b.text for b in response.content if getattr(b, "type", None) == "text"]
    raw = "".join(text_parts)
    payload = parse_and_validate_json(raw, REQUIRED_KEYS_FII_PAPEL)
    return payload, cost


# ---------------------------------------------------------------------------
# Per-fund processing
# ---------------------------------------------------------------------------
def process_fund(client: anthropic.Anthropic, ticker: str, docs: list[dict],
                 system_prompt: str, classificacao: str, overwrite: bool,
                 dry_run: bool) -> dict:
    """
    Run the full pipeline for one fund. Returns a result dict with cost
    and outcome. Never raises BudgetExceeded — caller checks budget
    between funds.
    """
    # Select 3 (M, M-1, M-12) from however many are available; mark extras
    # as processed so they don't keep appearing in future worklists.
    selected, skipped = select_target_docs(docs)
    if skipped:
        log.info(
            f"[{ticker}] {len(docs)} unprocessed docs found, selecting {len(selected)} "
            f"(M/M-1/M-12), marking {len(skipped)} extras as processed"
        )
        if not dry_run:
            mark_processed([d["doc_id"] for d in skipped])

    docs = selected
    log.info(f"[{ticker}] {len(docs)} doc(s) to process — {[d['data_referencia'] for d in docs]}")

    # Identify which doc is M, M-1, M-12 (already sorted DESC by select)
    doc_id_map: dict = {}
    if len(docs) >= 1: doc_id_map["m"] = docs[0]["doc_id"]
    if len(docs) >= 2: doc_id_map["m_1"] = docs[1]["doc_id"]
    if len(docs) >= 3: doc_id_map["m_12"] = docs[2]["doc_id"]

    # Preprocess all PDFs first; if any fail, skip the fund entirely
    markdowns = []
    try:
        for d in docs:
            md = preprocess_pdf(Path(d["pdf_path"]))
            markdowns.append(md)
    except (PDFHasNoTextError, FileNotFoundError) as e:
        msg = f"PDF preprocessing failed: {e}"
        log.error(f"[{ticker}] {msg}")
        record_error(docs[0]["doc_id"], ticker, "preprocess_failed", msg)
        return {"ticker": ticker, "status": "skipped_preprocess", "cost": 0.0}

    if dry_run:
        total_chars = sum(len(m) for m in markdowns)
        log.info(f"[{ticker}] DRY RUN — {total_chars:,} chars across {len(markdowns)} PDFs (would call API)")
        return {"ticker": ticker, "status": "dry_run", "cost": 0.0}

    user_message = build_user_message(ticker, docs, markdowns)

    # Call the LLM
    try:
        payload, cost = call_extractor(client, system_prompt, user_message)
    except anthropic.APIError as e:
        msg = f"Anthropic API error: {e}"
        log.error(f"[{ticker}] {msg}")
        record_error(docs[0]["doc_id"], ticker, "api_error", msg)
        return {"ticker": ticker, "status": "api_error", "cost": 0.0}
    except ExtractionError as e:
        msg = f"Extraction validation failed: {e}"
        log.error(f"[{ticker}] {msg}")
        record_error(docs[0]["doc_id"], ticker, "validation_failed", msg)
        return {"ticker": ticker, "status": "validation_failed", "cost": 0.0}

    log.info(f"[{ticker}] Extraction OK — cost ${cost:.4f}, tom={payload.get('tom_gestor')}")

    # Sanity: payload's ticker should match the row we processed
    if payload.get("ticker") != ticker:
        log.warning(
            f"[{ticker}] Payload ticker mismatch: got {payload.get('ticker')!r}, "
            f"forcing to {ticker!r}"
        )
        payload["ticker"] = ticker

    # DB write
    try:
        wrote = insert_gestor_row(payload, classificacao, doc_id_map, cost, overwrite)
    except Exception as e:
        msg = f"gestores INSERT failed: {e}"
        log.error(f"[{ticker}] {msg}")
        record_error(docs[0]["doc_id"], ticker, "db_insert_failed", msg)
        return {"ticker": ticker, "status": "db_failed", "cost": cost}

    if not wrote:
        log.info(f"[{ticker}] gestores row already exists (ON CONFLICT DO NOTHING) — keeping manual data")
        # Still mark relatórios as processed so we don't keep retrying this fund.
        # The PDFs are still useful for re-runs with --overwrite-manual, so leave them.
        # Actually: not marking processed_at here is the safer call. Without it,
        # the agent would reprocess every run and waste money. Mark processed.
        mark_processed([d["doc_id"] for d in docs])
        return {"ticker": ticker, "status": "skipped_existing", "cost": cost}

    # Success: mark processed, delete PDFs
    mark_processed([d["doc_id"] for d in docs])
    delete_pdfs_and_mark(docs)
    return {"ticker": ticker, "status": "success", "cost": cost}


# ---------------------------------------------------------------------------
# CLI driver
# ---------------------------------------------------------------------------
SUBTYPE_TO_CLASSIFICACAO = {
    "fii_papel":   "Papel",
    "fii_tijolo":  "Tijolo",
    "fii_hibrido": "Híbrido",
    "fii_fof":     "FOF",
    # FIAGRO subtypes will be added when their prompts exist
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run LLM extractor over pending relatórios.")
    parser.add_argument("--subtype", default="fii_papel",
                        choices=list(SUBTYPE_TO_CLASSIFICACAO.keys()),
                        help="Which subtype to process (default: fii_papel)")
    parser.add_argument("--ticker", help="Process a single ticker only")
    parser.add_argument("--limit", type=int, help="Process at most N funds")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preprocess PDFs but skip the API call")
    parser.add_argument("--overwrite-manual", action="store_true",
                        help="Overwrite existing gestores rows (default: preserve)")
    args = parser.parse_args()

    classificacao = SUBTYPE_TO_CLASSIFICACAO[args.subtype]

    if not os.environ.get("ANTHROPIC_API_KEY") and not args.dry_run:
        log.error("ANTHROPIC_API_KEY env var is not set")
        return 2

    init_pool()
    try:
        # Initial budget check
        if not args.dry_run:
            try:
                spent = check_budget_or_abort()
                log.info(f"Month-to-date spend: ${spent:.2f} (warn ${BUDGET_WARN}, stop ${BUDGET_STOP})")
            except BudgetExceeded:
                return 1

        # Load prompt
        try:
            system_prompt = load_prompt(args.subtype)
        except FileNotFoundError as e:
            log.error(str(e))
            return 2

        # Build worklist
        worklist = get_worklist(classificacao, ticker_filter=args.ticker)
        if not worklist:
            log.info(
                f"No pending relatórios for classificacao={classificacao!r}"
                + (f" ticker={args.ticker!r}" if args.ticker else "")
            )
            return 0
        log.info(f"Worklist: {len(worklist)} fund(s) — {sorted(worklist)}")

        if args.limit:
            tickers = sorted(worklist)[: args.limit]
            worklist = {t: worklist[t] for t in tickers}
            log.info(f"Limited to {args.limit}: {tickers}")

        # Initialize the API client (one shared instance reuses HTTP connection)
        # max_retries=5 lets the SDK weather a TPM rate-limit hit that
        # exceeds a single 60s rolling window. Combined with the inter-fund
        # sleep below, the run survives bursts cleanly even on the entry
        # tier (30k input TPM on Sonnet).
        client = anthropic.Anthropic(max_retries=5) if not args.dry_run else None

        results = []
        tickers_list = list(worklist.items())
        for idx, (ticker, docs) in enumerate(tickers_list):
            # Re-check budget before each fund
            if not args.dry_run:
                try:
                    check_budget_or_abort()
                except BudgetExceeded:
                    log.error("Halting: budget exceeded mid-run")
                    break

            result = process_fund(
                client=client,
                ticker=ticker,
                docs=docs,
                system_prompt=system_prompt,
                classificacao=classificacao,
                overwrite=args.overwrite_manual,
                dry_run=args.dry_run,
            )
            results.append(result)

            # Pace between funds to stay under per-minute token rate limit.
            # Skip pacing after the last fund and during dry-run.
            if not args.dry_run and idx < len(tickers_list) - 1 and INTER_FUND_DELAY_S > 0:
                log.info(f"Sleeping {INTER_FUND_DELAY_S}s before next fund (rate limit)")
                time.sleep(INTER_FUND_DELAY_S)

        # Summary
        by_status: dict[str, int] = defaultdict(int)
        total_cost = 0.0
        for r in results:
            by_status[r["status"]] += 1
            total_cost += r["cost"]
        log.info(
            f"Done. Processed {len(results)} fund(s). "
            f"Status counts: {dict(by_status)}. "
            f"Run cost: ${total_cost:.4f}"
        )
    finally:
        close_pool()

    return 0


if __name__ == "__main__":
    sys.exit(main())
