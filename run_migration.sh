#!/bin/bash
set -euo pipefail

echo "=========================================="
echo "FII Guia: SQLite -> Postgres migration"
echo "Started: $(date)"
echo "=========================================="

if [ -z "${DATABASE_URL:-}" ]; then
    echo "ERROR: DATABASE_URL not set."
    exit 1
fi

mkdir -p backend/data

echo ""
echo "[1/4] Downloading SQLite files from Dropbox..."

wget --quiet --show-progress -O backend/data/b3.db "https://www.dropbox.com/scl/fi/h9p6dkp2wy91bmpa91d8u/b3.db?rlkey=ec23sb9j2mkmyqnwez1me5p48&st=wuqbnp72&dl=1"
wget --quiet --show-progress -O backend/data/dividendos.db "https://www.dropbox.com/scl/fi/1je6v5ewqk5fjubr3m0vy/dividendos.db?rlkey=pfbhmudlo4vy8kaasl73byxah&dl=1"
wget --quiet --show-progress -O backend/data/informe_mensal.db "https://www.dropbox.com/scl/fi/6sxikc6sb9zdrjwqzpkbp/informe_mensal.db?rlkey=on9yrbva9oy3ke7v0rfzkvsqd&dl=1"
wget --quiet --show-progress -O backend/data/gestores.db "https://www.dropbox.com/scl/fi/oo31qrrlvypnhpz8qyqwa/gestores.db?rlkey=tf75spkbvyyzsbh1q2h0u1hln&st=zq1gk8ax&dl=1"

echo "Downloaded:"
ls -lh backend/data/

echo ""
echo "[2/4] Applying schema..."
python -c "import os, psycopg2; sql = open('migrations/001_initial.sql').read(); conn = psycopg2.connect(os.environ['DATABASE_URL']); cur = conn.cursor(); cur.execute(sql); conn.commit(); conn.close(); print('Schema applied.')"

echo ""
echo "[3/4] Running migration (all tables)..."
python migrate_sqlite_to_pg.py --all

echo ""
echo "[4/4] Verifying row counts..."
python migrate_sqlite_to_pg.py --verify

echo ""
echo "=========================================="
echo "Migration complete: $(date)"
echo "Delete this Railway service when ready."
echo "=========================================="