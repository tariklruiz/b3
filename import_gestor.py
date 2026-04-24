"""
import_gestor.py — FII Guia
Imports JSON files from relatorios/Output/ into gestores.db.

Each JSON file should be named <TICKER>.json (e.g. MXRF11.json)
and contain the structured output from the Claude chat prompts.

Usage:
    python import_gestor.py                    # import all JSONs in Output/
    python import_gestor.py --ticker MXRF11    # import single file
    python import_gestor.py --force            # overwrite existing entries
"""

import sqlite3
import json
import argparse
from pathlib import Path
from datetime import datetime

# ─────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────
OUTPUT_DIR = Path(r"C:\Users\tarik.lauar\Dropbox\Personal Tarik\01 - Documentos\b3app\relatorios\Output")
DB_PATH    = Path(r"C:\Users\tarik.lauar\Dropbox\Personal Tarik\01 - Documentos\b3app\backend\data\gestores.db")
FUND_TYPES = Path(r"C:\Users\tarik.lauar\Dropbox\Personal Tarik\01 - Documentos\b3app\fund_types.json")

# ─────────────────────────────────────────────────────────────────
# DB SETUP
# ─────────────────────────────────────────────────────────────────
def init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gestores (
            id                       INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker                   TEXT NOT NULL,
            competencia              TEXT NOT NULL,
            classificacao            TEXT,
            tom_gestor               TEXT,
            pl_total_brl             REAL,
            cota_mercado             REAL,
            cota_patrimonial         REAL,
            spread_credito_bps       REAL,
            ltv_medio                REAL,
            resultado_por_cota       REAL,
            distribuicao_por_cota    REAL,
            reserva_monetaria_brl    REAL,
            vacancia_pct             REAL,
            contratos_vencer_12m_pct REAL,
            cap_rate                 REAL,
            contexto_meses           TEXT,
            cris_em_observacao       TEXT,
            alocacao_fundos          TEXT,
            mudancas_portfolio       TEXT,
            resumo                   TEXT,
            alertas_dados            TEXT,
            processado_em            TEXT,
            UNIQUE(ticker, competencia)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ticker ON gestores (ticker)")

    # Migrate: add new columns if they don't exist yet
    existing = {r[1] for r in conn.execute("PRAGMA table_info(gestores)").fetchall()}
    new_cols = {
        "vacancia_pct":             "REAL",
        "contratos_vencer_12m_pct": "REAL",
        "cap_rate":                 "REAL",
        "alocacao_fundos":          "TEXT",
    }
    for col, dtype in new_cols.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE gestores ADD COLUMN {col} {dtype}")
            print(f"  [MIGRATE] Added column: {col}")

    conn.commit()
    return conn


# ─────────────────────────────────────────────────────────────────
# CLASSIFICATION LOOKUP
# ─────────────────────────────────────────────────────────────────
def get_classificacao(ticker: str) -> str | None:
    if not FUND_TYPES.exists():
        return None
    data = json.loads(FUND_TYPES.read_text(encoding="utf-8"))
    cls = data.get("fundos", {}).get(ticker.upper())
    return cls if cls and cls != "Outros" else None


# ─────────────────────────────────────────────────────────────────
# UPSERT
# ─────────────────────────────────────────────────────────────────
def upsert(conn: sqlite3.Connection, data: dict, ticker: str, force: bool):
    # Ensure ticker is correct
    data["ticker"] = ticker.upper()

    competencia = data.get("competencia")
    if not competencia:
        print(f"  [SKIP] {ticker} — campo 'competencia' ausente no JSON")
        return False

    # Check if already exists
    if not force:
        exists = conn.execute(
            "SELECT 1 FROM gestores WHERE ticker = ? AND competencia = ?",
            (ticker, competencia)
        ).fetchone()
        if exists:
            print(f"  [SKIP] {ticker} — {competencia} já existe (use --force para sobrescrever)")
            return False

    classificacao = get_classificacao(ticker)

    conn.execute("""
        INSERT INTO gestores (
            ticker, competencia, classificacao,
            tom_gestor, pl_total_brl, cota_mercado, cota_patrimonial,
            spread_credito_bps, ltv_medio, resultado_por_cota,
            distribuicao_por_cota, reserva_monetaria_brl,
            vacancia_pct, contratos_vencer_12m_pct, cap_rate,
            contexto_meses, cris_em_observacao, alocacao_fundos,
            mudancas_portfolio, resumo, alertas_dados, processado_em
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(ticker, competencia) DO UPDATE SET
            classificacao              = excluded.classificacao,
            tom_gestor                 = excluded.tom_gestor,
            pl_total_brl               = excluded.pl_total_brl,
            cota_mercado               = excluded.cota_mercado,
            cota_patrimonial           = excluded.cota_patrimonial,
            spread_credito_bps         = excluded.spread_credito_bps,
            ltv_medio                  = excluded.ltv_medio,
            resultado_por_cota         = excluded.resultado_por_cota,
            distribuicao_por_cota      = excluded.distribuicao_por_cota,
            reserva_monetaria_brl      = excluded.reserva_monetaria_brl,
            vacancia_pct               = excluded.vacancia_pct,
            contratos_vencer_12m_pct   = excluded.contratos_vencer_12m_pct,
            cap_rate                   = excluded.cap_rate,
            contexto_meses             = excluded.contexto_meses,
            cris_em_observacao         = excluded.cris_em_observacao,
            alocacao_fundos            = excluded.alocacao_fundos,
            mudancas_portfolio         = excluded.mudancas_portfolio,
            resumo                     = excluded.resumo,
            alertas_dados              = excluded.alertas_dados,
            processado_em              = excluded.processado_em
    """, (
        data["ticker"],
        competencia,
        classificacao,
        data.get("tom_gestor"),
        data.get("pl_total_brl"),
        data.get("cota_mercado"),
        data.get("cota_patrimonial"),
        data.get("spread_credito_bps"),
        data.get("ltv_medio"),
        data.get("resultado_por_cota"),
        data.get("distribuicao_por_cota"),
        data.get("reserva_monetaria_brl"),
        data.get("vacancia_pct"),
        data.get("contratos_vencer_12m_pct"),
        data.get("cap_rate"),
        json.dumps(data.get("contexto_meses", []), ensure_ascii=False),
        json.dumps(data.get("cris_em_observacao", []), ensure_ascii=False),
        json.dumps(data.get("alocacao_fundos"), ensure_ascii=False) if data.get("alocacao_fundos") else None,
        data.get("mudancas_portfolio"),
        data.get("resumo"),
        data.get("alertas_dados"),
        datetime.now().strftime("%Y-%m-%d %H:%M"),
    ))
    conn.commit()
    return True


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Import Claude JSON outputs into gestores.db")
    parser.add_argument("--ticker", help="Import single ticker (e.g. MXRF11)")
    parser.add_argument("--force",  action="store_true", help="Overwrite existing entries")
    args = parser.parse_args()

    if not OUTPUT_DIR.exists():
        print(f"Output dir not found: {OUTPUT_DIR}")
        return

    conn = init_db(DB_PATH)

    if args.ticker:
        files = [OUTPUT_DIR / f"{args.ticker.upper()}.json"]
    else:
        files = sorted(OUTPUT_DIR.glob("*.json"))

    print(f"\nFII Guia — Gestor Import")
    print(f"Source: {OUTPUT_DIR}")
    print(f"DB:     {DB_PATH}")
    print(f"Files:  {len(files)}\n")

    ok = skip = fail = 0
    for f in files:
        ticker = f.stem.upper()
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            imported = upsert(conn, data, ticker, args.force)
            if imported:
                ok += 1
                comp = data.get("competencia", "?")
                tom  = data.get("tom_gestor", "?")
                print(f"  [OK]   {ticker} — {comp} · tom: {tom}")
            else:
                skip += 1
        except json.JSONDecodeError as e:
            print(f"  [FAIL] {ticker} — JSON inválido: {e}")
            fail += 1
        except Exception as e:
            print(f"  [FAIL] {ticker} — {e}")
            fail += 1

    conn.close()
    print(f"\nConcluído — {ok} importados · {skip} ignorados · {fail} erros")
    print(f"DB: {DB_PATH}")


if __name__ == "__main__":
    main()
