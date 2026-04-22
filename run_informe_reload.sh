#!/bin/bash
# run_informe_reload.sh
# ============================================================================
# Drops and reloads the informe_mensal table in Postgres with the full
# 26-column schema. Downloads the SQLite source from Dropbox and runs the
# migrator with --tables informe_mensal.
#
# Expected downtime: ~3 minutes for the /fundo/informe endpoint while the
# table is being reloaded.
# ============================================================================

set -euo pipefail

echo "=========================================="
echo "FII Guia: informe_mensal reload"
echo "Started: $(date)"
echo "=========================================="

if [ -z "${DATABASE_URL:-}" ]; then
    echo "ERROR: DATABASE_URL not set."
    exit 1
fi

mkdir -p backend/data

echo ""
echo "[1/3] Downloading informe_mensal.db from Dropbox..."

wget --quiet --show-progress -O backend/data/informe_mensal.db \
    "https://www.dropbox.com/scl/fi/6sxikc6sb9zdrjwqzpkbp/informe_mensal.db?rlkey=on9yrbva9oy3xe7v0rfzkvsqd&st=z1mbats9&dl=1"

echo "Downloaded:"
ls -lh backend/data/informe_mensal.db

echo ""
echo "Verifying SQLite header..."
header=$(head -c 15 backend/data/informe_mensal.db)
if [ "$header" != "SQLite format 3" ]; then
    echo "FAIL: downloaded file is not a SQLite database"
    head -c 300 backend/data/informe_mensal.db
    exit 1
fi
echo "OK"

echo ""
echo "[2/3] Applying schema (drops and recreates informe_mensal)..."
python -c "
import os, psycopg2
with open('migrations/001_initial.sql') as f:
    sql = f.read()
conn = psycopg2.connect(os.environ['DATABASE_URL'])
with conn.cursor() as cur:
    cur.execute(sql)
conn.commit()
conn.close()
print('Schema applied — informe_mensal is now 26-column.')
"

echo ""
echo "[3/3] Reloading informe_mensal from SQLite..."
python migrate_sqlite_to_pg.py --tables informe_mensal

echo ""
echo "=========================================="
echo "Reload complete: $(date)"
echo "Delete this Railway service when verified."
echo "=========================================="
